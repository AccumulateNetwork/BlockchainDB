package blockchainDB

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// Simulations for the key table: a hash-partitioned index from key to
// the file and offset holding its entry, sitting under the window and
// over the segments (the design conversation behind #86 and #88).
//
// Nothing here is shipped code.  These are models, run to choose the
// shape before writing it: the questions are what a lookup costs in
// page touches, what maintenance costs in bytes written per key, how
// many files and bytes the thing occupies, and how badly the work
// clumps when every bucket fills at the same moment.
//
// The structure being modelled:
//
//	A table of B buckets, a key routed to one by its hash.  A bucket
//	is a SORTED body plus an UNSORTED tail: new keys are appended to
//	the tail, which costs only the bytes appended, and the tail is
//	merged into the body -- rewriting the bucket -- once it has grown
//	to some fraction of it.  A bucket that outgrows its size limit
//	SPLITS in two, which is where B grows.
//
// The two knobs that matter are the tail fraction, which sets how
// often a bucket is rewritten and therefore the write amplification,
// and the split limit, which sets bucket size and therefore both the
// read cost and the size of one unit of staged work.
//
// A lookup reads the tail and searches the body.  Three ways to search
// the body are measured against real keys in TestKeyTableBucketSearch:
// linear, binary, and a directory on the key's leading bits.  Keys are
// hashes, so they are uniform, which is what makes the directory work.

// ktEntry is one table entry, in bytes.
//
// The full form is key(32) + file(4) + offset(8) + length(8).  The
// short form keeps only a PREFIX of the key -- 10 bytes -- with
// file(4) and offset(6), because the entry does not have to identify
// the key beyond doubt: the record it points at carries the full key
// in its header, and a read that does not find the key it asked for
// falls back to the walk (segment.readVerified).  A prefix collision
// therefore costs one wasted read, never a wrong answer, and 80 bits
// over a billion keys collides with probability ~5e-8.
const (
	ktEntryFull  = 52
	ktEntryShort = 20
	ktPageSize   = 4096
)

// ktParams is one configuration of the table
type ktParams struct {
	Name             string
	EntryBytes       int     // ktEntryFull or ktEntryShort
	StartBuckets     int     // B at the beginning
	SplitAt          int     // Keys in a bucket that trigger a split
	TailFrac         float64 // Merge the tail once it reaches this fraction of the body
	MinTail          int     // ...but never merge a tail smaller than this
	MaxSplitsPerStep int     // 0 = unlimited: split storms land whole
}

// ktBucket is a bucket's occupancy.  The keys themselves are not
// modelled: they are uniform hashes, so what a bucket holds is a
// count, and where the interesting behaviour lives is in the counts.
type ktBucket struct {
	sorted, tail int
}

// ktResult is what one run reports at a checkpoint
type ktResult struct {
	Keys         int64
	Buckets      int
	BytesWritten int64
	TableBytes   int64
	MeanBucket   int
	MaxBucket    int
	MeanTail     int
	StepBytesMax int64 // The worst single step: what staging has to absorb
}

// simulateKeyTable feeds `total` keys through the table in steps,
// applying the tail-merge and split rules, and reports at each
// checkpoint.  Keys per step is chosen so the run is quick; the model
// is insensitive to it because the rules are threshold-based.
func simulateKeyTable(p ktParams, total int64, checkpoints []int64) []ktResult {
	const keysPerStep = 1 << 20

	rng := rand.New(rand.NewSource(1))
	buckets := make([]ktBucket, p.StartBuckets)
	var written, keys int64
	var results []ktResult
	next := 0
	var stepBytesMax int64

	for keys < total {
		n := int64(keysPerStep)
		if keys+n > total {
			n = total - keys
		}
		keys += n
		var stepBytes int64

		// Distribute the step's keys over the buckets.  Uniform
		// hashing, so each bucket's share is the mean plus noise of
		// the square root of the mean -- which is the whole reason the
		// splits arrive together: at a hundred thousand keys a bucket
		// the spread between buckets is a third of a percent.
		mean := float64(n) / float64(len(buckets))
		sd := math.Sqrt(mean)
		var placed int64
		for i := range buckets {
			c := int(mean + sd*rng.NormFloat64())
			if c < 0 {
				c = 0
			}
			buckets[i].tail += c
			placed += int64(c)
		}
		// Whatever rounding lost or gained goes to bucket 0; the
		// totals have to be exact for the amplification number to mean
		// anything
		if d := n - placed; d != 0 {
			b := &buckets[0]
			if b.tail+int(d) >= 0 {
				b.tail += int(d)
			}
		}
		stepBytes += n * int64(p.EntryBytes) // The append itself

		// Merge tails that have grown enough to be worth a rewrite
		for i := range buckets {
			b := &buckets[i]
			threshold := int(float64(b.sorted) * p.TailFrac)
			if threshold < p.MinTail {
				threshold = p.MinTail
			}
			if b.tail >= threshold {
				stepBytes += int64(b.sorted+b.tail) * int64(p.EntryBytes)
				b.sorted += b.tail
				b.tail = 0
			}
		}

		// Split what has outgrown the limit.  Splitting rewrites the
		// bucket into two, so it costs what a merge costs and the two
		// halves come out sorted.
		splits := 0
		for i := 0; i < len(buckets); i++ {
			b := &buckets[i]
			if b.sorted+b.tail < p.SplitAt {
				continue
			}
			if p.MaxSplitsPerStep > 0 && splits >= p.MaxSplitsPerStep {
				break // Deferred: the bucket runs over until a later step
			}
			splits++
			// Read the whole bucket out BEFORE overwriting it: b
			// points into the slice, so assigning to buckets[i] first
			// and reading b afterwards loses the second half's keys
			whole := b.sorted + b.tail
			half := whole / 2
			stepBytes += int64(whole) * int64(p.EntryBytes)
			buckets[i] = ktBucket{sorted: half}
			buckets = append(buckets, ktBucket{sorted: whole - half})
		}

		written += stepBytes
		if stepBytes > stepBytesMax {
			stepBytesMax = stepBytes
		}

		for next < len(checkpoints) && keys >= checkpoints[next] {
			results = append(results, measureKeyTable(p, buckets, keys, written, stepBytesMax))
			stepBytesMax = 0
			next++
		}
	}
	return results
}

func measureKeyTable(p ktParams, buckets []ktBucket, keys, written, stepMax int64) ktResult {
	r := ktResult{Keys: keys, Buckets: len(buckets), BytesWritten: written, StepBytesMax: stepMax}
	var total, tails int64
	for _, b := range buckets {
		n := b.sorted + b.tail
		total += int64(n)
		tails += int64(b.tail)
		if n > r.MaxBucket {
			r.MaxBucket = n
		}
	}
	r.MeanBucket = int(total / int64(len(buckets)))
	r.MeanTail = int(tails / int64(len(buckets)))
	r.TableBytes = total * int64(p.EntryBytes)
	return r
}

// TestKeyTableSim
// How the table behaves from a million keys to a billion: what it
// costs to keep, what it occupies, and how much work one step has to
// absorb.  A measurement, not an assertion.
func TestKeyTableSim(t *testing.T) {
	if testing.Short() {
		t.Skip("simulation; skipped in -short")
	}
	const total = 1 << 30 // ~1.07 billion keys
	checkpoints := []int64{1 << 20, 1 << 23, 1 << 26, 1 << 28, 1 << 30}

	// 120k keys a bucket is ~6 MB of full entries: small enough to
	// rewrite as one staged unit, large enough to keep the file count
	// in the thousands.  Starting at 8 buckets is the store's own
	// shard count, which is where the keys arrive from.
	base := ktParams{EntryBytes: ktEntryFull, StartBuckets: 8, SplitAt: 120_000, MinTail: 1024}

	runs := []ktParams{
		func() ktParams { p := base; p.Name = "rewrite every fold (no tail)"; p.TailFrac = 0; return p }(),
		func() ktParams { p := base; p.Name = "tail 10% of body"; p.TailFrac = 0.10; return p }(),
		func() ktParams { p := base; p.Name = "tail 25% of body"; p.TailFrac = 0.25; return p }(),
		func() ktParams { p := base; p.Name = "tail 50% of body"; p.TailFrac = 0.50; return p }(),
		func() ktParams {
			p := base
			p.Name = "tail 25%, short entries"
			p.TailFrac, p.EntryBytes = 0.25, ktEntryShort
			return p
		}(),
		func() ktParams {
			p := base
			p.Name = "tail 25%, <=64 splits a step"
			p.TailFrac, p.MaxSplitsPerStep = 0.25, 64
			return p
		}(),
	}

	for _, p := range runs {
		t.Logf("=== %s (entry %d B, split at %d keys) ===", p.Name, p.EntryBytes, p.SplitAt)
		t.Logf("%12s %8s %10s %10s %9s %9s %12s",
			"keys", "buckets", "table", "written", "mean/bkt", "mean tail", "worst step")
		for _, r := range simulateKeyTable(p, total, checkpoints) {
			amp := float64(r.BytesWritten) / float64(r.Keys*int64(p.EntryBytes))
			t.Logf("%12s %8d %10s %10s %9d %9d %12s   (%.1fx write amplification)",
				ktCount(r.Keys), r.Buckets, ktBytes(r.TableBytes), ktBytes(r.BytesWritten),
				r.MeanBucket, r.MeanTail, ktBytes(r.StepBytesMax), amp)
		}
	}
}

// TestKeyTableBucketSearch
// What it costs to find a key INSIDE a bucket, measured on real keys
// rather than modelled: comparisons, and the 4 KB pages a search
// touches, which is what becomes preads.
//
// Three ways to search the sorted body, and the unsorted tail scanned
// alongside whichever is used:
//
//   - linear, which is what a small bucket should do
//   - binary, log2(n) probes landing on scattered pages
//   - a directory on the key's leading bits, which works because keys
//     are hashes and therefore uniform: the directory says which slice
//     of the body a key must be in, and that slice is about one page
//
// The crossover between linear and binary is what sets the "linear
// until N keys" rule.
func TestKeyTableBucketSearch(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement; skipped in -short")
	}
	const entry = ktEntryFull
	perPage := ktPageSize / entry

	sizes := []int{16, 64, 100, 256, 1024, 4096, 16_384, 65_536, 120_000}
	t.Logf("entries of %d bytes, %d to a %d-byte page", entry, perPage, ktPageSize)
	t.Logf("%9s %18s %18s %22s", "body", "linear (cmp/pages)", "binary (cmp/pages)", "directory 4096 (cmp/pages)")

	for _, n := range sizes {
		keys := ktSortedKeys(n, int64(n))
		probes := ktSortedKeys(256, 99) // Keys to look for: absent, so worst case
		var linCmp, linPg, binCmp, binPg, dirCmp, dirPg float64
		dir := ktBuildDirectory(keys, 4096)
		for _, k := range probes {
			c, p := ktLinear(keys, k, entry)
			linCmp += float64(c)
			linPg += float64(p)
			c, p = ktBinary(keys, k, entry)
			binCmp += float64(c)
			binPg += float64(p)
			c, p = ktDirectory(keys, dir, k, entry)
			dirCmp += float64(c)
			dirPg += float64(p)
		}
		m := float64(len(probes))
		t.Logf("%9d %9.0f %8.1f %9.1f %8.1f %13.1f %8.1f",
			n, linCmp/m, linPg/m, binCmp/m, binPg/m, dirCmp/m, dirPg/m)
	}

	// The tail is scanned in full on every lookup, so its size is a
	// flat cost added to whichever body search is used
	t.Logf("")
	t.Logf("unsorted tail scanned in full on every lookup:")
	for _, n := range []int{100, 1024, 4096, 16_384, 30_000} {
		t.Logf("%9d entries -> %5.1f pages", n, math.Ceil(float64(n*entry)/ktPageSize))
	}
}

// ktSortedKeys makes n sorted random 32-byte keys: content-addressed
// keys are hashes, so uniform is the right distribution
func ktSortedKeys(n int, seed int64) [][32]byte {
	rng := rand.New(rand.NewSource(seed))
	keys := make([][32]byte, n)
	for i := range keys {
		for j := 0; j < 32; j += 8 {
			v := rng.Uint64()
			for b := 0; b < 8; b++ {
				keys[i][j+b] = byte(v >> (8 * b))
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		for b := 0; b < 32; b++ {
			if keys[i][b] != keys[j][b] {
				return keys[i][b] < keys[j][b]
			}
		}
		return false
	})
	return keys
}

func ktCompare(a, b [32]byte) int {
	for i := 0; i < 32; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// ktPagesOf counts the distinct pages a set of entry indexes touches
type ktPages map[int]struct{}

func (p ktPages) touch(i, entry int) { p[(i*entry)/ktPageSize] = struct{}{} }

func ktLinear(keys [][32]byte, k [32]byte, entry int) (cmp, pages int) {
	pg := ktPages{}
	for i := range keys {
		cmp++
		pg.touch(i, entry)
		if ktCompare(keys[i], k) >= 0 {
			break
		}
	}
	return cmp, len(pg)
}

func ktBinary(keys [][32]byte, k [32]byte, entry int) (cmp, pages int) {
	pg := ktPages{}
	lo, hi := 0, len(keys)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		cmp++
		pg.touch(mid, entry)
		switch ktCompare(keys[mid], k) {
		case 0:
			return cmp, len(pg)
		case -1:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	return cmp, len(pg)
}

// ktBuildDirectory records where each leading-bits slot starts.  With
// uniform keys a slot holds about n/slots entries, so the slice a
// lookup scans is about one page when the directory is sized for the
// bucket.
func ktBuildDirectory(keys [][32]byte, slots int) []int {
	dir := make([]int, slots+1)
	at := 0
	for s := 0; s < slots; s++ {
		for at < len(keys) && ktSlot(keys[at], slots) <= s {
			at++
		}
		dir[s+1] = at
	}
	return dir
}

func ktSlot(k [32]byte, slots int) int {
	// The leading bits, as a fraction of the key space
	v := uint64(k[0])<<24 | uint64(k[1])<<16 | uint64(k[2])<<8 | uint64(k[3])
	return int((v * uint64(slots)) >> 32)
}

func ktDirectory(keys [][32]byte, dir []int, k [32]byte, entry int) (cmp, pages int) {
	slots := len(dir) - 1
	s := ktSlot(k, slots)
	pg := ktPages{}
	// One touch for the directory itself; in a real bucket it is a
	// small array at the head of the file, read once and cached
	pg[-1] = struct{}{}
	for i := dir[s]; i < dir[s+1]; i++ {
		cmp++
		pg.touch(i, entry)
		if ktCompare(keys[i], k) >= 0 {
			break
		}
	}
	return cmp, len(pg)
}

func ktBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	}
	return fmt.Sprintf("%d B", b)
}

func ktCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.2fB", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1000:
		return fmt.Sprintf("%.0fk", float64(n)/1e3)
	}
	return fmt.Sprintf("%d", n)
}

// The first two simulations disagree, and the disagreement is the
// design.
//
// TestKeyTableSim says the unsorted tail is what keeps the write cost
// down: rewriting a bucket on every fold costs 55x write amplification
// at a billion keys and still climbing, while letting a tail gather to
// a quarter of the body holds it at 6x and flat.
//
// TestKeyTableBucketSearch says that same tail is what destroys the
// read: a quarter of a 120,000-key bucket is 30,000 entries, and
// scanning them is ~380 page touches, which is worse than the segment
// walk this whole structure exists to replace.
//
// Both are right, and the way out is that a tail does not have to be
// unsorted.  A fold already has its keys in memory and in order, so it
// can write a SORTED RUN as cheaply as an unsorted one, and a sorted
// run carries a directory like the body does -- so searching it costs
// about a page rather than its whole length.  A bucket then holds a
// few sorted runs plus a small unsorted buffer, runs are merged on the
// same worth-it ratio compaction already uses here, and the read cost
// is the number of runs rather than the size of the tail.
//
// That is what this simulates: the buffer, the runs, the ratio, and
// what each choice costs at both ends.

// ktRunPages is what a directory search of a sorted run of n entries
// touches, from the measurements in TestKeyTableBucketSearch.  It is
// nearly flat -- one page for the directory slot, one or two for the
// entries it points at -- which is the property the whole design rests
// on, and it holds because keys are hashes and so spread evenly.
func ktRunPages(n int) float64 {
	switch {
	case n == 0:
		return 0
	case n <= 256:
		return 1.1
	case n <= 1024:
		return 1.2
	case n <= 4096:
		return 1.7
	case n <= 16384:
		return 2.0
	case n <= 65536:
		return 2.1
	}
	return 2.2
}

// ktScanPages is what reading an unsorted buffer of n entries touches:
// all of it, every time
func ktScanPages(n, entry int) float64 {
	return math.Ceil(float64(n*entry) / ktPageSize)
}

// ktRunBucket is a bucket as runs: an unsorted buffer, then sorted
// runs largest first
type ktRunBucket struct {
	buffer int
	runs   []int
}

func (b *ktRunBucket) total() int {
	n := b.buffer
	for _, r := range b.runs {
		n += r
	}
	return n
}

// mergeRuns folds the runs that have gathered enough behind a larger
// one to be worth its rewrite -- CompactRatio's rule, inside a bucket
// -- and reports the entries rewritten
func (b *ktRunBucket) mergeRuns(ratio float64) (written int) {
	for {
		merged := false
		for i := 0; i < len(b.runs); i++ {
			behind := 0
			for _, r := range b.runs[i+1:] {
				behind += r
			}
			if behind == 0 || float64(b.runs[i])*ratio > float64(behind) {
				continue
			}
			total := b.runs[i] + behind
			written += total
			b.runs = append(b.runs[:i], total)
			merged = true
			break
		}
		if !merged {
			return written
		}
	}
}

type ktRunParams struct {
	Name         string
	EntryBytes   int
	StartBuckets int
	SplitAt      int
	BufferCap    int     // Entries held unsorted before becoming a run
	Ratio        float64 // Merge a run once this share of it has gathered behind
}

// simulateKeyTableRuns is simulateKeyTable with runs instead of one
// unsorted tail, reporting the read cost as well as the write cost
func simulateKeyTableRuns(p ktRunParams, total int64, checkpoints []int64) {
	const keysPerStep = 1 << 20
	rng := rand.New(rand.NewSource(1))
	buckets := make([]*ktRunBucket, p.StartBuckets)
	for i := range buckets {
		buckets[i] = &ktRunBucket{}
	}
	var written, keys int64
	next := 0
	var stepBytesMax int64

	for keys < total {
		n := int64(keysPerStep)
		if keys+n > total {
			n = total - keys
		}
		keys += n
		var stepEntries int64

		mean := float64(n) / float64(len(buckets))
		sd := math.Sqrt(mean)
		var placed int64
		for _, b := range buckets {
			c := int(mean + sd*rng.NormFloat64())
			if c < 0 {
				c = 0
			}
			b.buffer += c
			placed += int64(c)
		}
		if d := n - placed; d > 0 {
			buckets[0].buffer += int(d)
		}
		stepEntries += n // The append

		for _, b := range buckets {
			if b.buffer >= p.BufferCap {
				stepEntries += int64(b.buffer) // Sorted into a run
				b.runs = append(b.runs, b.buffer)
				b.buffer = 0
				sort.Sort(sort.Reverse(sort.IntSlice(b.runs)))
				stepEntries += int64(b.mergeRuns(p.Ratio))
			}
		}
		for i := 0; i < len(buckets); i++ {
			b := buckets[i]
			if b.total() < p.SplitAt {
				continue
			}
			t := b.total()
			stepEntries += int64(t)
			buckets[i] = &ktRunBucket{runs: []int{t / 2}}
			buckets = append(buckets, &ktRunBucket{runs: []int{t - t/2}})
		}

		stepBytes := stepEntries * int64(p.EntryBytes)
		written += stepBytes
		if stepBytes > stepBytesMax {
			stepBytesMax = stepBytes
		}

		for next < len(checkpoints) && keys >= checkpoints[next] {
			var tot, runs int64
			var pages float64
			for _, b := range buckets {
				tot += int64(b.total())
				runs += int64(len(b.runs))
				p := ktScanPages(b.buffer, p.EntryBytes)
				for _, r := range b.runs {
					p += ktRunPages(r)
				}
				pages += p
			}
			nb := float64(len(buckets))
			amp := float64(written) / float64(keys*int64(p.EntryBytes))
			fmt.Printf("%12s %8d %10s %10s %7.1f %10.1f %11s  %.1fx\n",
				ktCount(keys), len(buckets), ktBytes(tot*int64(p.EntryBytes)),
				ktBytes(written), float64(runs)/nb, pages/nb,
				ktBytes(stepBytesMax), amp)
			stepBytesMax = 0
			next++
		}
	}
}

// TestKeyTableRunsSim
// The two costs together: what a lookup touches, and what keeping the
// table costs, for a bucket built of sorted runs.
func TestKeyTableRunsSim(t *testing.T) {
	if testing.Short() {
		t.Skip("simulation; skipped in -short")
	}
	const total = 1 << 30
	checkpoints := []int64{1 << 20, 1 << 23, 1 << 26, 1 << 28, 1 << 30}

	base := ktRunParams{EntryBytes: ktEntryFull, StartBuckets: 8, SplitAt: 120_000, BufferCap: 78}
	runs := []ktRunParams{
		func() ktRunParams { p := base; p.Name = "ratio 0.10"; p.Ratio = 0.10; return p }(),
		func() ktRunParams { p := base; p.Name = "ratio 0.25"; p.Ratio = 0.25; return p }(),
		func() ktRunParams { p := base; p.Name = "ratio 0.50"; p.Ratio = 0.50; return p }(),
		func() ktRunParams {
			p := base
			p.Name = "ratio 1.00 (merge as soon as anything gathers)"
			p.Ratio = 1.0
			return p
		}(),
		func() ktRunParams {
			p := base
			p.Name = "ratio 0.25, short entries"
			p.Ratio, p.EntryBytes = 0.25, ktEntryShort
			return p
		}(),
		func() ktRunParams {
			p := base
			p.Name = "ratio 0.25, 12k-key buckets"
			p.Ratio, p.SplitAt = 0.25, 12_000
			return p
		}(),
	}
	for _, p := range runs {
		fmt.Printf("=== %s (entry %d B, split at %d, buffer %d) ===\n", p.Name, p.EntryBytes, p.SplitAt, p.BufferCap)
		fmt.Printf("%12s %8s %10s %10s %7s %10s %11s\n",
			"keys", "buckets", "table", "written", "runs", "read pages", "worst step")
		simulateKeyTableRuns(p, total, checkpoints)
	}
}

// Absence is the hot case, so it is worth a filter (#88: 11,152
// lookups a commit that the window proves absent and that then walk
// the whole of history to prove absent again).  Searching the runs of
// a bucket to prove a key is in none of them costs about six page
// touches; a filter that answers the same question in one would be
// most of the lookup cost gone.
//
// Two things decide whether it is worth having, and both are measured
// here rather than assumed.
//
// SIZE.  A filter is bits per key, held for every key in the table
// forever, so it is a permanent tax on the disk beside the entries.
//
// BUILD TIME.  A filter over a bucket has to keep up with the bucket.
// The saving grace is that a bloom filter takes INSERTIONS without a
// rebuild -- adding a key is setting bits -- so a fold sets bits for
// the keys it is already writing and no extra pass exists.  A rebuild
// is needed only where keys LEAVE a bucket, which is a split or a
// merge that drops superseded records, and both of those already read
// every key in the bucket.  So the number that matters is the cost of
// one Set, not the cost of a pass.
//
// The other question is how many PAGES a probe touches.  The store's
// Bloom scatters its K bits over the whole bitmap, so a probe of a
// 180 KB filter touches up to K different pages -- and at K=3, three
// page touches to save six is a poor trade.  A BLOCKED filter puts all
// of a key's bits inside one page, so a probe is always one page, and
// because K is then free it can be raised to where the false positive
// rate actually wants it.  Both are measured.

// ktBlocked is a blocked bloom filter: the block is chosen by one part
// of the key and every bit for that key lands inside it, so a probe
// reads one page however large the filter is or how many hash
// functions it uses.
type ktBlocked struct {
	blockBits uint64
	blocks    uint64
	k         int
	m         []byte
}

func newKtBlocked(keys uint64, bitsPerKey uint64, k int, blockBytes uint64) *ktBlocked {
	bytes := keys * bitsPerKey / 8
	if bytes < blockBytes {
		bytes = blockBytes
	}
	blocks := (bytes + blockBytes - 1) / blockBytes
	return &ktBlocked{blockBits: blockBytes * 8, blocks: blocks, k: k,
		m: make([]byte, blocks*blockBytes)}
}

// positions derives the block and the k bit offsets within it.  Double
// hashing off two 64-bit words of the key, which is what a hash key
// affords for free: no hashing, just slicing.
func (b *ktBlocked) positions(key [32]byte, fn func(idx uint64, mask byte) bool) bool {
	blk := binary.BigEndian.Uint64(key[0:]) % b.blocks
	h1 := binary.BigEndian.Uint64(key[8:])
	h2 := binary.BigEndian.Uint64(key[16:]) | 1
	base := blk * (b.blockBits / 8)
	for i := 0; i < b.k; i++ {
		bit := (h1 + uint64(i)*h2) % b.blockBits
		if !fn(base+bit/8, 1<<(bit%8)) {
			return false
		}
	}
	return true
}

func (b *ktBlocked) Set(key [32]byte) {
	b.positions(key, func(i uint64, mask byte) bool { b.m[i] |= mask; return true })
}

func (b *ktBlocked) Test(key [32]byte) bool {
	return b.positions(key, func(i uint64, mask byte) bool { return b.m[i]&mask != 0 })
}

// TestKeyTableBloomSim
// What a per-bucket filter costs and what it buys: size, build time,
// false positive rate, and pages touched per probe.
func TestKeyTableBloomSim(t *testing.T) {
	if testing.Short() {
		t.Skip("simulation; skipped in -short")
	}
	const bucketKeys = 120_000 // The split limit: a filter is sized for this
	const probes = 200_000     // Absent keys, to measure false positives

	keys := make([][32]byte, bucketKeys)
	rng := rand.New(rand.NewSource(7))
	fill := func(dst [][32]byte) {
		for i := range dst {
			for j := 0; j < 32; j += 8 {
				binary.BigEndian.PutUint64(dst[i][j:], rng.Uint64())
			}
		}
	}
	fill(keys)
	absent := make([][32]byte, probes)
	fill(absent)

	t.Logf("a bucket of %d keys; false positives measured over %d absent keys", bucketKeys, probes)
	t.Logf("%-26s %9s %10s %9s %10s %10s", "filter", "size", "pages/probe", "false+", "build", "probe")

	// The store's own Bloom, scattered bits, as it is used today
	for _, bits := range []uint64{8, 12, 16} {
		for _, k := range []int{3, 4} {
			f := newBloomSized(bucketKeys, bits, k)
			start := time.Now()
			for _, key := range keys {
				f.Set(key)
			}
			build := time.Since(start)
			fp := 0
			start = time.Now()
			for _, key := range absent {
				if f.Test(key) {
					fp++
				}
			}
			probe := time.Since(start)
			t.Logf("%-26s %9s %10d %8.3f%% %7.1f ns %7.1f ns",
				fmt.Sprintf("scattered %d bits k=%d", bits, k), ktBytes(int64(f.NumBytes)), k,
				100*float64(fp)/float64(probes),
				float64(build.Nanoseconds())/float64(bucketKeys),
				float64(probe.Nanoseconds())/float64(probes))
		}
	}
	// Blocked: every bit for a key inside one page, so K is free
	for _, bits := range []uint64{8, 12, 16} {
		for _, k := range []int{3, 6, 8, 11} {
			f := newKtBlocked(bucketKeys, bits, k, ktPageSize)
			start := time.Now()
			for _, key := range keys {
				f.Set(key)
			}
			build := time.Since(start)
			fp := 0
			start = time.Now()
			for _, key := range absent {
				if f.Test(key) {
					fp++
				}
			}
			probe := time.Since(start)
			t.Logf("%-26s %9s %10d %8.3f%% %7.1f ns %7.1f ns",
				fmt.Sprintf("blocked %d bits k=%d", bits, k), ktBytes(int64(len(f.m))), 1,
				100*float64(fp)/float64(probes),
				float64(build.Nanoseconds())/float64(bucketKeys),
				float64(probe.Nanoseconds())/float64(probes))
		}
	}

	// A bucket is only full just before it splits; for most of its life
	// it holds half that, and a filter sized for the limit is better
	// than its rating meanwhile
	t.Logf("")
	t.Logf("blocked 12 bits k=8, sized for %d keys, as the bucket fills:", bucketKeys)
	for _, held := range []int{15_000, 30_000, 60_000, 90_000, 120_000} {
		f := newKtBlocked(bucketKeys, 12, 8, ktPageSize)
		for _, key := range keys[:held] {
			f.Set(key)
		}
		fp := 0
		for _, key := range absent {
			if f.Test(key) {
				fp++
			}
		}
		t.Logf("  %7d keys held (%3.0f%% of capacity): %6.3f%% false positives",
			held, 100*float64(held)/bucketKeys, 100*float64(fp)/float64(probes))
	}

	// What it costs across the whole table
	t.Logf("")
	t.Logf("across the table, at 12 bits per key:")
	for _, n := range []int64{1 << 20, 1 << 26, 1 << 30} {
		size := n * 12 / 8
		t.Logf("  %8s keys: %9s of filter (%.1f%% of a %s table of 52-byte entries)",
			ktCount(n), ktBytes(size), 100*float64(size)/float64(n*ktEntryFull), ktBytes(n*ktEntryFull))
	}
}

// Relocation: the question that decides whether a location table can
// exist at all, and over which layer.
//
// A table entry says where a record IS.  Maintenance moves records: a
// perm merge concatenates bodies, a dyna compaction rewrites them and
// drops what has been superseded, a pack copies them into a set.  Every
// entry pointing at a record that has moved is wrong.  Reading it is
// safe -- the record header carries the key, so a moved record reads as
// a miss and the lookup falls back to the walk -- but a table whose
// entries are mostly wrong is a table that costs more than it saves.
//
// So the number that matters is how long an entry stays true, against
// how long an entry LIVES in the table.  Both are already determined by
// choices made elsewhere:
//
//   - An entry stays true until compaction rewrites its record.  The
//     layer is rewritten end to end every N/(A*I) seconds, where N is
//     the layer's keys, I the ingest rate and A compaction's write
//     amplification.  That is the half-life of the truth.
//
//   - An entry lives in the table until the run holding it is merged,
//     which the 0.25 ratio puts at a quarter of a bucket's keys
//     arriving.  That is what keeps the table's own write cost at 14x
//     rather than 76x, so it cannot simply be shortened.
//
// When the second is longer than the first, the table is stale before
// it is rewritten, and no amount of tuning inside the table fixes it,
// because the two numbers move in opposite directions.
func TestKeyTableRelocationSim(t *testing.T) {
	if testing.Short() {
		t.Skip("simulation; skipped in -short")
	}
	const (
		ingest     = 500.0   // Keys a second, the soak's rate
		buckets    = 16460.0 // Where a billion keys lands (TestKeyTableRunsSim)
		bucketKeys = 120000.0
		ratio      = 0.25 // A run is merged once this much has gathered
		tablePages = 3.0  // What a lookup costs when the entry is true
	)
	day := 86400.0

	t.Logf("ingest %.0f keys/s, %.0f buckets, run merge ratio %.2f", ingest, buckets, ratio)
	t.Logf("%10s %6s %14s %14s %10s %12s", "layer keys", "A", "rewritten every", "entry lives", "stale", "read pages")
	for _, n := range []float64{1e6, 1e7, 1e8, 1e9} {
		// Buckets in proportion, so a bucket holds bucketKeys at 1e9
		b := math.Max(8, buckets*n/1e9)
		perBucket := ingest / b
		entryLife := ratio * math.Min(bucketKeys, n/b) / perBucket // Seconds until its run is merged

		for _, a := range []float64{0, 1, 4} {
			var stale float64
			turnover := math.Inf(1)
			if a > 0 {
				turnover = n / (a * ingest)
				// Uniformly distributed rewrites: the chance a record
				// has been moved in the time its entry has been sitting
				stale = 1 - math.Exp(-entryLife/turnover/2)
			}
			// The fallback is the walk this was built to replace:
			// history segments at compaction's pass budget, 3 preads each
			segs := math.Max(1, n/float64(CompactPassRecords))
			walk := 3 * segs
			read := (1-stale)*tablePages + stale*walk
			label := "settled (no rewrite)"
			if a > 0 {
				label = fmt.Sprintf("%.0f", a)
			}
			t.Logf("%10s %6s %14s %14s %9.1f%% %12.1f",
				ktCount(int64(n)), label, ktDuration(turnover, day), ktDuration(entryLife, day),
				100*stale, read)
		}
	}
}

func ktDuration(s, day float64) string {
	switch {
	case math.IsInf(s, 1):
		return "never"
	case s >= day:
		return fmt.Sprintf("%.1f days", s/day)
	case s >= 3600:
		return fmt.Sprintf("%.1f hours", s/3600)
	}
	return fmt.Sprintf("%.0f min", s/60)
}
