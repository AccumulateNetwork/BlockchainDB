// bdbench drives sharded stores the way Accumulate's adapter does,
// without Accumulate: one block per interval, each block a fixed
// volume of dynamic rewrites, permanent appends and reads, then a
// seal; maintenance on the adapter's cadence, off the block goroutine.
// Several stores can run at once in one process, sharing the disk the
// way a validator's partitions and a test network's nodes do.
//
// It reports, once a minute, what the spec's latency rule (1.2) is
// about: the seal and the block against the interval, put and read
// latency, what each maintenance pass cost and how much disk it moved,
// and the size of the store -- so a cost that grows with the age of
// the store shows up as a column that climbs.  The report is served as
// a live page (-http); a run is launched by opening it.
//
// The default volumes are the 2026-09-16 soak's BVN store at 500 tps:
// ~11k dynamic puts, ~8k permanent puts and ~40k lookups per 1 s
// block (stats.json of run 20260916T185711Z, 2,000 commits).
//
// Configuration is flags only (spec 1.10); no environment variables.
package main

import (
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	blockchainDB "github.com/AccumulateNetwork/BlockchainDB/database"
)

type config struct {
	dir           string
	stores        int
	duration      time.Duration
	interval      time.Duration
	shards        int
	sealLimit     uint64
	window        uint64
	compressEvery uint64
	packEvery     uint64
	dynaPuts      int
	permPuts      int
	reads         int
	hotKeys       int
	valueMin      uint
	valueMax      uint
	csvPath       string
	sealBudget    time.Duration
	seed          uint64
	pprof         string
	http          string
	dynaHeap      bool
	permFiles     bool
	phase         bool
}

//go:embed live.html
var livePage []byte

func parseFlags() (config, error) {
	var c config
	flag.StringVar(&c.dir, "dir", "", "run directory (required; created if absent, must be empty); each store lives in store-N under it")
	flag.IntVar(&c.stores, "stores", 1, "stores driven at once, each with its own blocks and maintenance, sharing the disk")
	flag.DurationVar(&c.duration, "duration", 10*time.Minute, "how long to run")
	flag.DurationVar(&c.interval, "interval", time.Second, "block interval; a block that runs longer is counted over budget")
	flag.IntVar(&c.shards, "shards", 8, "storage shards")
	flag.Uint64Var(&c.sealLimit, "seal-limit", 12_500, "records one shard's layer accumulates before it seals (adapter: SealLimit)")
	flag.Uint64Var(&c.window, "window", 20, "blocks in the active window (adapter: MergeLag; store: SetFilterBlocks)")
	flag.Uint64Var(&c.compressEvery, "compress-every", 20, "blocks between maintenance passes (adapter: CompressEvery); 0 disables maintenance")
	flag.Uint64Var(&c.packEvery, "pack-every", 1000, "blocks between cross-shard packs (adapter: PackEvery); 0 disables")
	flag.IntVar(&c.dynaPuts, "dyna", 11_000, "dynamic-layer puts per block, rewrites over the hot key set")
	flag.IntVar(&c.permPuts, "perm", 8_300, "permanent-layer puts per block, always new keys")
	flag.IntVar(&c.reads, "reads", 40_000, "lookups per block: half hot dynamic keys, a third permanent keys of all ages, the rest absent")
	flag.IntVar(&c.hotKeys, "hot", 500_000, "size of the dynamic key set being rewritten")
	flag.UintVar(&c.valueMin, "value-min", 64, "smallest value, bytes")
	flag.UintVar(&c.valueMax, "value-max", 512, "largest value, bytes")
	flag.StringVar(&c.csvPath, "csv", "", "append one row per minute here (default: <dir>/bdbench.csv)")
	flag.DurationVar(&c.sealBudget, "seal-budget", 100*time.Millisecond, "a minute whose seal p90 exceeds this is flagged")
	flag.Uint64Var(&c.seed, "seed", 1, "random seed")
	flag.StringVar(&c.pprof, "pprof", "", "serve net/http/pprof on this address (e.g. 127.0.0.1:6061)")
	flag.StringVar(&c.http, "http", "127.0.0.1:8098", "serve the live page and the run's files here; empty disables")
	flag.BoolVar(&c.dynaHeap, "dyna-heap", false, "dynamic layer as a heap with holes (proposal 2026-09-16) instead of sealed segments")
	flag.BoolVar(&c.permFiles, "perm-files", false, "both layers as files of entries: the heap and the permanent layer with buckets of keys (implies -dyna-heap)")
	flag.BoolVar(&c.phase, "maintenance-phase", false, "offset each store's maintenance cadence by its share of the period, so stores in lockstep do not all maintain at once")
	flag.Parse()
	if flag.NArg() != 0 {
		return c, fmt.Errorf("unexpected arguments: %q", flag.Args())
	}
	switch {
	case c.dir == "":
		return c, errors.New("-dir is required")
	case c.stores < 1 || c.shards < 1:
		return c, errors.New("-stores and -shards must be at least 1")
	case c.interval <= 0 || c.duration <= 0:
		return c, errors.New("-interval and -duration must be positive")
	case c.valueMin == 0 || c.valueMax < c.valueMin:
		return c, errors.New("-value-min must be positive and no larger than -value-max")
	case c.hotKeys < 1 || c.dynaPuts < 0 || c.permPuts < 0 || c.reads < 0:
		return c, errors.New("-hot must be positive; -dyna, -perm and -reads may not be negative")
	case c.window == 0:
		return c, errors.New("-window must be positive")
	}
	if c.csvPath == "" {
		c.csvPath = filepath.Join(c.dir, "bdbench.csv")
	}
	return c, nil
}

// samples collects durations for one report period and answers
// percentiles.  A minute of blocks is tens of thousands of puts, which
// sorts in milliseconds; nothing here needs a histogram.
type samples struct {
	mu sync.Mutex
	v  []time.Duration
}

func (s *samples) add(d time.Duration) {
	s.mu.Lock()
	s.v = append(s.v, d)
	s.mu.Unlock()
}

func (s *samples) take() []time.Duration {
	s.mu.Lock()
	v := s.v
	s.v = nil
	s.mu.Unlock()
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	return v
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

func ms(d time.Duration) string { return strconv.FormatFloat(float64(d)/1e6, 'f', 1, 64) }
func us(d time.Duration) string { return strconv.FormatInt(int64(d/time.Microsecond), 10) }

// recent is one block as the live page sees it: when it ended and
// what it and its seal cost.
type recent struct {
	at          time.Time
	block, seal time.Duration
}

// tallies is what every store adds to and the report takes from.
type tallies struct {
	blockTimes, sealTimes, dynaPut, permPut, readT samples
	blocks, over, mismatches                       atomic.Uint64
	inFlight                                       atomic.Int64 // Maintenance passes running now

	ringMu sync.Mutex
	ring   [1024]recent // The last blocks, for the live state
	ringN  uint64

	mu      sync.Mutex
	passes  map[string]int
	spent   map[string]time.Duration
	skipped int
	lastErr error
}

func (t *tallies) note(kind string, d time.Duration) {
	t.mu.Lock()
	t.passes[kind]++
	t.spent[kind] += d
	t.mu.Unlock()
}

func (t *tallies) takeMaintenance() (passes map[string]int, spent map[string]time.Duration, skipped int, err error) {
	t.mu.Lock()
	passes, spent, skipped, err = t.passes, t.spent, t.skipped, t.lastErr
	t.passes, t.spent, t.skipped, t.lastErr = map[string]int{}, map[string]time.Duration{}, 0, nil
	t.mu.Unlock()
	return
}

// store is one sharded store and the goroutine that drives it: its
// own key sets, its own block height, its own maintenance in flight.
type store struct {
	id       int
	kv       *blockchainDB.KVShard
	rnd      *blockchainDB.FastRandom
	hot      [][32]byte
	permKeys [][32]byte
	permNext int
	// A few hot keys are checked on read: the value the store returns
	// must be the value last written.  A platform that only times
	// answers cannot tell a fast wrong answer from a fast right one.
	last        map[[32]byte][]byte
	height      uint64
	phase       uint64 // Blocks this store's maintenance cadence is offset by
	maintaining atomic.Bool
	maintWG     sync.WaitGroup
}

const (
	permSample = 200_000
	checked    = 1024
)

func openStore(c config, id int) (*store, error) {
	dir := filepath.Join(c.dir, fmt.Sprintf("store-%d", id))
	open := blockchainDB.NewKVShardN
	if c.dynaHeap {
		open = blockchainDB.NewKVShardHeapN
	}
	if c.permFiles {
		open = blockchainDB.NewKVShardFilesN
	}
	kv, err := open(dir, c.shards, c.sealLimit)
	if err != nil {
		return nil, fmt.Errorf("open store %d: %w", id, err)
	}
	if err := kv.SetFilterBlocks(c.window); err != nil {
		return nil, fmt.Errorf("store %d window: %w", id, err)
	}
	seed := make([]byte, 16)
	for i := 0; i < 8; i++ {
		seed[i] = byte(c.seed >> (8 * i))
		seed[8+i] = byte(uint64(id+1) >> (8 * i))
	}
	s := &store{id: id, kv: kv, rnd: blockchainDB.NewFastRandom(seed), hot: make([][32]byte, c.hotKeys),
		permKeys: make([][32]byte, 0, permSample), last: make(map[[32]byte][]byte, checked)}
	if c.phase && c.compressEvery > 0 {
		s.phase = uint64(id) * c.compressEvery / uint64(c.stores)
	}
	// Hot dynamic keys are rewritten with a skew (index = hot * r^2, so
	// the low indexes take most writes); permanent keys are always new,
	// and a bounded sample of them, across all ages, is what the
	// permanent reads look up.
	for i := range s.hot {
		s.hot[i] = s.rnd.NextHash()
	}
	return s, nil
}

func (s *store) block(c config, t *tallies) error {
	s.height++
	start := time.Now()
	for i := 0; i < c.dynaPuts; i++ {
		r := float64(s.rnd.UintN(1<<20)) / (1 << 20)
		k := s.hot[int(r*r*float64(c.hotKeys))%c.hotKeys]
		v := s.rnd.RandBuff(c.valueMin, c.valueMax)
		at := time.Now()
		if err := s.kv.PutDyna(k, v); err != nil {
			return fmt.Errorf("store %d PutDyna: %w", s.id, err)
		}
		t.dynaPut.add(time.Since(at))
		if len(s.last) < checked || s.last[k] != nil {
			s.last[k] = v
		}
	}
	for i := 0; i < c.permPuts; i++ {
		k := s.rnd.NextHash()
		v := s.rnd.RandBuff(c.valueMin, c.valueMax)
		at := time.Now()
		if err := s.kv.PutPerm(k, v); err != nil {
			return fmt.Errorf("store %d PutPerm: %w", s.id, err)
		}
		t.permPut.add(time.Since(at))
		if len(s.permKeys) < permSample {
			s.permKeys = append(s.permKeys, k)
		} else if s.rnd.UintN(64) == 0 { // Keep the sample spread across every age
			s.permKeys[s.permNext%permSample] = k
			s.permNext++
		}
	}
	for i := 0; i < c.reads; i++ {
		var k [32]byte
		var get func([32]byte) ([]byte, error)
		switch pick := s.rnd.UintN(6); {
		case pick < 3:
			k, get = s.hot[int(s.rnd.UintN(uint(c.hotKeys)))], s.kv.GetDyna
		case pick < 5 && len(s.permKeys) > 0:
			k, get = s.permKeys[int(s.rnd.UintN(uint(len(s.permKeys))))], s.kv.GetPerm
		default:
			k, get = s.rnd.NextHash(), s.kv.Get
		}
		at := time.Now()
		v, err := get(k)
		t.readT.add(time.Since(at))
		if err != nil && !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("store %d read: %w", s.id, err)
		}
		if want, ok := s.last[k]; ok && err == nil && string(v) != string(want) {
			t.mismatches.Add(1)
		}
	}
	at := time.Now()
	if err := s.kv.SealBlock(s.height); err != nil {
		return fmt.Errorf("store %d SealBlock: %w", s.id, err)
	}
	sealTook := time.Since(at)
	t.sealTimes.add(sealTook)
	took := time.Since(start)
	t.blockTimes.add(took)
	t.blocks.Add(1)
	t.ringMu.Lock()
	t.ring[t.ringN%uint64(len(t.ring))] = recent{at: time.Now(), block: took, seal: sealTook}
	t.ringN++
	t.ringMu.Unlock()
	if took > c.interval {
		t.over.Add(1)
	} else {
		time.Sleep(c.interval - took)
	}
	if c.compressEvery > 0 && (s.height+s.phase)%c.compressEvery == 0 {
		s.maintain(c, t)
	}
	return nil
}

// maintain is the adapter's cadence: one pass in flight per store, a
// pass that lands while another runs is skipped, never queued.
func (s *store) maintain(c config, t *tallies) {
	if !s.maintaining.CompareAndSwap(false, true) {
		t.mu.Lock()
		t.skipped++
		t.mu.Unlock()
		return
	}
	height := s.height
	s.maintWG.Add(1)
	t.inFlight.Add(1)
	go func() {
		defer s.maintWG.Done()
		defer s.maintaining.Store(false)
		defer t.inFlight.Add(-1)
		at := time.Now()
		err := s.kv.Compress()
		t.note("compress", time.Since(at))
		if err == nil && height > c.window {
			at = time.Now()
			_, err = s.kv.MergeFinalized(height - c.window)
			t.note("merge", time.Since(at))
			if err == nil && c.packEvery > 0 && height >= c.packEvery+c.window && height%c.packEvery < c.compressEvery {
				at = time.Now()
				_, _, err = s.kv.PackFinalized(height - c.window)
				t.note("pack", time.Since(at))
			}
		}
		if err != nil {
			t.mu.Lock()
			t.lastErr = fmt.Errorf("store %d: %w", s.id, err)
			t.mu.Unlock()
		}
	}()
}

// procIO reads the process's own disk traffic (Linux /proc/self/io).
func procIO() (read, write uint64) {
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		n, _ := strconv.ParseUint(f[1], 10, 64)
		switch f[0] {
		case "read_bytes:":
			read = n
		case "write_bytes:":
			write = n
		}
	}
	return
}

// dirSize counts the bytes the files occupy, not their apparent size:
// a heap releases regions with punched holes and keeps its length.
func dirSize(dir string) (files int, bytes int64) {
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			files++
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				bytes += st.Blocks * 512
			} else {
				bytes += info.Size()
			}
		}
		return nil
	})
	return
}

// liveState is what the page shows between report rows: the last ten
// seconds of blocks and seals, and the run's running totals.  Written
// to live.json every two seconds.
func (t *tallies) liveState(c config, stores []*store, start time.Time) []byte {
	t.ringMu.Lock()
	cut := time.Now().Add(-10 * time.Second)
	var bt, st []time.Duration
	n := t.ringN
	if n > uint64(len(t.ring)) {
		n = uint64(len(t.ring))
	}
	for i := uint64(0); i < n; i++ {
		r := t.ring[(t.ringN-1-i)%uint64(len(t.ring))]
		if r.at.Before(cut) {
			break
		}
		bt = append(bt, r.block)
		st = append(st, r.seal)
	}
	t.ringMu.Unlock()
	sort.Slice(bt, func(i, j int) bool { return bt[i] < bt[j] })
	sort.Slice(st, func(i, j int) bool { return st[i] < st[j] })
	over := 0
	for _, d := range bt {
		if d > c.interval {
			over++
		}
	}
	var height uint64
	var holes, live int64
	var scanned, moved, syncs, syncBytes uint64
	var heapFsync, deltaSync time.Duration
	var permMerges, permFolds, permPacks, permIndexBytes uint64
	for _, s := range stores {
		if s.height > height {
			height = s.height
		}
		for _, sh := range s.kv.Shards {
			if sh.Heap != nil {
				h, l := sh.Heap.HoleRatio()
				holes, live = holes+h, live+l
				sc, mv := sh.Heap.Cleaned()
				scanned, moved = scanned+sc, moved+mv
				n, b, hf, ds := sh.Heap.SyncCost()
				syncs, syncBytes, heapFsync, deltaSync = syncs+n, syncBytes+b, heapFsync+hf, deltaSync+ds
			}
			if sh.Perm != nil {
				m, f, pk, ib := sh.Perm.Counters()
				permMerges, permFolds, permPacks, permIndexBytes = permMerges+m, permFolds+f, permPacks+pk, permIndexBytes+ib
			}
		}
	}
	b, _ := json.Marshal(map[string]any{
		"elapsedSec": int(time.Since(start).Seconds()), "blocks": t.blocks.Load(), "height": height,
		"last10s": map[string]any{"blocks": len(bt), "over": over,
			"blockP50ms": float64(pct(bt, .5)) / 1e6, "blockP90ms": float64(pct(bt, .9)) / 1e6, "blockMaxMs": float64(pct(bt, 1)) / 1e6,
			"sealP50ms": float64(pct(st, .5)) / 1e6, "sealP90ms": float64(pct(st, .9)) / 1e6, "sealMaxMs": float64(pct(st, 1)) / 1e6},
		"maintenanceInFlight": t.inFlight.Load(), "mismatches": t.mismatches.Load(),
		"heapHoleMB": float64(holes) / 1e6, "heapLiveMB": float64(live) / 1e6,
		"heapScannedMB": float64(scanned) / 1e6, "heapMovedMB": float64(moved) / 1e6,
		"heapSyncs": syncs, "heapSyncKBAvg": float64(syncBytes) / 1e3 / float64(max(syncs, 1)),
		"permMerges": permMerges, "permFolds": permFolds, "permPacks": permPacks, "permIndexMB": float64(permIndexBytes) / 1e6,
		"heapFsyncMsAvg": float64(heapFsync) / 1e6 / float64(max(syncs, 1)), "heapDeltaMsAvg": float64(deltaSync) / 1e6 / float64(max(syncs, 1)),
	})
	return b
}

func fail(what string, err error) {
	fmt.Fprintln(os.Stderr, "bdbench:", what+":", err)
	os.Exit(1)
}

func main() {
	c, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bdbench:", err)
		flag.Usage()
		os.Exit(2)
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		fail("run directory", err)
	}
	if entries, err := os.ReadDir(c.dir); err != nil || len(entries) != 0 {
		fmt.Fprintf(os.Stderr, "bdbench: -dir %s must be an empty directory (a run is a fresh store)\n", c.dir)
		os.Exit(1)
	}
	stores := make([]*store, c.stores)
	for i := range stores {
		if stores[i], err = openStore(c, i); err != nil {
			fail("open", err)
		}
	}
	if c.pprof != "" {
		go func() {
			if err := http.ListenAndServe(c.pprof, nil); err != nil {
				fail("pprof", err)
			}
		}()
	}
	// The live page: the run's directory served as it is written, and
	// the page that reads bdbench.csv and run.json from it.  A long run
	// is launched by opening this page, not by tailing a log.
	runJSON, _ := json.MarshalIndent(map[string]any{
		"dir": c.dir, "stores": c.stores, "duration": c.duration.String(), "interval": c.interval.String(), "shards": c.shards,
		"sealLimit": c.sealLimit, "window": c.window, "compressEvery": c.compressEvery, "packEvery": c.packEvery,
		"dynaPuts": c.dynaPuts, "permPuts": c.permPuts, "reads": c.reads, "hotKeys": c.hotKeys,
		"valueMin": c.valueMin, "valueMax": c.valueMax, "seed": c.seed, "dynaHeap": c.dynaHeap || c.permFiles, "permFiles": c.permFiles, "maintenancePhase": c.phase, "started": time.Now().UTC().Format(time.RFC3339),
	}, "", "  ")
	if err := os.WriteFile(filepath.Join(c.dir, "run.json"), runJSON, 0o644); err != nil {
		fail("run.json", err)
	}
	if c.http != "" {
		mux := http.NewServeMux()
		files := http.FileServer(http.Dir(c.dir))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = w.Write(livePage)
				return
			}
			w.Header().Set("Cache-Control", "no-store")
			files.ServeHTTP(w, r)
		})
		go func() {
			if err := http.ListenAndServe(c.http, mux); err != nil {
				fail("http", err)
			}
		}()
		fmt.Printf("live page: http://%s/\n", c.http)
	}
	csvFile, err := os.OpenFile(c.csvPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fail("csv", err)
	}
	defer csvFile.Close()
	csvw := csv.NewWriter(csvFile)
	_ = csvw.Write([]string{"minute", "blocks", "over_budget", "block_p50_ms", "block_p90_ms", "block_max_ms",
		"seal_p50_ms", "seal_p90_ms", "seal_max_ms", "dyna_put_p99_us", "perm_put_p99_us", "read_p99_us",
		"compress_passes", "compress_s", "merge_passes", "merge_s", "pack_passes", "pack_s", "skipped",
		"read_MBps", "write_MBps", "store_MB", "files", "perm_history", "dyna_history", "bloom_MB", "mismatches"})
	csvw.Flush()

	fmt.Printf("bdbench: %d store(s) x %d shards, seal limit %d, window %d, maintenance every %d blocks, pack every %d; per block per store %d dyna + %d perm puts, %d reads; %s blocks for %s; %s\n",
		c.stores, c.shards, c.sealLimit, c.window, c.compressEvery, c.packEvery, c.dynaPuts, c.permPuts, c.reads, c.interval, c.duration, c.dir)

	t := &tallies{passes: map[string]int{}, spent: map[string]time.Duration{}}
	start := time.Now()
	deadline := start.Add(c.duration)
	ioR0, ioW0 := procIO()
	period := start

	report := func() {
		elapsed := time.Since(period)
		period = time.Now()
		bt, st := t.blockTimes.take(), t.sealTimes.take()
		dp, pp, rt := t.dynaPut.take(), t.permPut.take(), t.readT.take()
		passes, spent, skipped, merr := t.takeMaintenance()
		over := t.over.Swap(0)
		ioR1, ioW1 := procIO()
		rMB := float64(ioR1-ioR0) / 1e6 / elapsed.Seconds()
		wMB := float64(ioW1-ioW0) / 1e6 / elapsed.Seconds()
		ioR0, ioW0 = ioR1, ioW1
		files, bytes := dirSize(c.dir)
		var permHist, dynaHist int
		var bloom uint64
		for _, s := range stores {
			perm, dyna := s.kv.Stats()
			permHist += perm.HistorySegments
			dynaHist += dyna.HistorySegments
			bloom += perm.ResidentBloomBytes + dyna.ResidentBloomBytes
		}
		note := ""
		if pct(st, 0.9) > c.sealBudget {
			note = "  SEAL p90 OVER BUDGET"
		}
		if over > 0 {
			note += fmt.Sprintf("  %d blocks over the interval", over)
		}
		if merr != nil {
			note += "  MAINTENANCE ERROR: " + merr.Error()
		}
		minute := int(time.Since(start) / time.Minute)
		mism := t.mismatches.Load()
		fmt.Printf("%3dm blocks %4d | block p50/p90/max %s/%s/%s ms | seal p50/p90/max %s/%s/%s ms | put p99 dyna %s perm %s us | read p99 %s us | maint compress %d (%.1fs) merge %d (%.1fs) pack %d (%.1fs) skipped %d | disk r %.0f w %.0f MB/s | store %.0f MB %d files | history perm %d dyna %d | bloom %.0f MB | mismatches %d%s\n",
			minute, len(bt), ms(pct(bt, .5)), ms(pct(bt, .9)), ms(pct(bt, 1)), ms(pct(st, .5)), ms(pct(st, .9)), ms(pct(st, 1)),
			us(pct(dp, .99)), us(pct(pp, .99)), us(pct(rt, .99)),
			passes["compress"], spent["compress"].Seconds(), passes["merge"], spent["merge"].Seconds(), passes["pack"], spent["pack"].Seconds(), skipped,
			rMB, wMB, float64(bytes)/1e6, files, permHist, dynaHist, float64(bloom)/1e6, mism, note)
		_ = csvw.Write([]string{strconv.Itoa(minute), strconv.Itoa(len(bt)), strconv.FormatUint(over, 10),
			ms(pct(bt, .5)), ms(pct(bt, .9)), ms(pct(bt, 1)), ms(pct(st, .5)), ms(pct(st, .9)), ms(pct(st, 1)),
			us(pct(dp, .99)), us(pct(pp, .99)), us(pct(rt, .99)),
			strconv.Itoa(passes["compress"]), strconv.FormatFloat(spent["compress"].Seconds(), 'f', 2, 64),
			strconv.Itoa(passes["merge"]), strconv.FormatFloat(spent["merge"].Seconds(), 'f', 2, 64),
			strconv.Itoa(passes["pack"]), strconv.FormatFloat(spent["pack"].Seconds(), 'f', 2, 64), strconv.Itoa(skipped),
			strconv.FormatFloat(rMB, 'f', 1, 64), strconv.FormatFloat(wMB, 'f', 1, 64),
			strconv.FormatFloat(float64(bytes)/1e6, 'f', 0, 64), strconv.Itoa(files),
			strconv.Itoa(permHist), strconv.Itoa(dynaHist), strconv.FormatFloat(float64(bloom)/1e6, 'f', 1, 64),
			strconv.FormatUint(mism, 10)})
		csvw.Flush()
	}

	// The live state, every two seconds, beside the per-minute rows
	liveStop := make(chan struct{})
	go func() {
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				_ = os.WriteFile(filepath.Join(c.dir, "live.json"), t.liveState(c, stores, start), 0o644)
			case <-liveStop:
				return
			}
		}
	}()
	defer close(liveStop)

	// Every store drives its own blocks; the first error stops the run.
	stop := make(chan struct{})
	var stopOnce sync.Once
	var runErr error
	var wg sync.WaitGroup
	for _, s := range stores {
		wg.Add(1)
		go func(s *store) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				select {
				case <-stop:
					return
				default:
				}
				if err := s.block(c, t); err != nil {
					stopOnce.Do(func() { runErr = err; close(stop) })
					return
				}
			}
		}(s)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
wait:
	for {
		select {
		case <-ticker.C:
			report()
		case <-sig:
			fmt.Println("interrupted")
			stopOnce.Do(func() { close(stop) })
		case <-done:
			break wait
		}
	}
	for _, s := range stores {
		s.maintWG.Wait()
	}
	report()
	fmt.Printf("done: %d blocks across %d store(s)\n", t.blocks.Load(), c.stores)
	for _, s := range stores {
		if err := s.kv.Close(); err != nil {
			fail("close", err)
		}
	}
	if runErr != nil {
		fail("run", runErr)
	}
	if n := t.mismatches.Load(); n > 0 {
		fmt.Fprintf(os.Stderr, "bdbench: %d reads returned a value other than the last written\n", n)
		os.Exit(1)
	}
}
