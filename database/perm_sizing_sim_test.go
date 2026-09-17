package blockchainDB

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"testing"
)

var simFlag = flag.Bool("sim", false, "run the permanent-index sizing simulation (TestPermSizingSim)")

// A sizing simulation for the permanent layer's bucketed long search
// (docs/proposals/2026-09-16-entries-written-once.md, step 2).  Not a
// test of the store: it models the index maintenance the proposal
// describes and prints, for a sweep of bucket counts, what a shard
// pays per block and what a lookup walks.  Run with
//
//	go test ./database/ -run TestPermSizingSim -v -sim
//
// The model, per shard: every block appends K records and seals a
// delta of K sorted 44-byte index records with a filter.  A delta
// leaving the window (N blocks) feeds each record to its bucket's
// resident recent section.  Every block the next bucket in rotation
// writes its recent section as a new sorted run and folds its runs by
// the store's ratio rule (compactionRunWithin: a suffix of runs is
// folded while each older run is no larger than 1/ratio of what has
// gathered behind it).  Today's cost, for comparison: every
// MergeEvery blocks the shard folds its finished segments by copying
// bodies (~V bytes a record) and rebuilding the index, under the same
// ratio rule over merged segments; every PackEvery blocks the merged
// segments are copied again into a set.
func TestPermSizingSim(t *testing.T) {
	if !*simFlag {
		t.Skip("a sizing simulation; run with -sim")
	}
	const (
		K          = 1040 // Permanent records per block per shard (soak: 8.3k per store, 8 shards)
		V          = 300  // Bytes a record's body costs to copy
		rec        = 44   // Bytes an index record costs
		N          = 20   // The window, blocks
		ratio      = 0.25
		mergeEvery = 20
		packEvery  = 1000
		bloomBits  = 12 // Bits per key in a filter
	)
	horizons := []int{86_400, 7 * 86_400, 30 * 86_400}
	buckets := []int{1, 64, 256, 1024, 4096}
	splits := []int64{32 << 20, 64 << 20} // Bucket index bytes at which a bucket splits in two
	const (
		M              = 256      // Every bucket is merged every M blocks: B/M buckets a block
		residentBudget = 64 << 20 // Filter bytes resident per shard, newest runs first; the rest are probed cold
		runFile        = 64 << 20 // Runs are appended to run files of this size; a run is (file, offset, length)
	)

	var out strings.Builder
	fmt.Fprintf(&out, "\nPermanent index, one shard, K=%d records/block, ratio %.2f, window %d blocks\n", K, ratio, N)
	for _, blocks := range horizons {
		fmt.Fprintf(&out, "\n== %d blocks (%.0f days), %d M records ==\n", blocks, float64(blocks)/86_400, K*blocks/1_000_000)
		// Today
		today := simToday(blocks, K, V, rec, ratio, mergeEvery, packEvery)
		fmt.Fprintf(&out, "today: index+body bytes written %.1f GB (%.1f MB/block avg), largest pass %.0f MB, merged segments at end %d, sets %d\n",
			float64(today.written)/1e9, float64(today.written)/float64(blocks)/1e6, float64(today.largest)/1e6, today.segments, today.sets)
		fmt.Fprintf(&out, "%-22s %12s %11s %11s %11s %9s %11s %11s %9s\n", "buckets", "idx KB/blk", "largest MB", "runs/bkt", "probes", "files", "resident MB", "cold probes", "recent MB")
		for _, b := range buckets {
			r := simBuckets(blocks, K, rec, N, ratio, b, 0, M, bloomBits, residentBudget, runFile)
			fmt.Fprintf(&out, "%-22d %12.0f %11.1f %11.1f %11.1f %9d %11.1f %11.1f %9.1f\n",
				b, float64(r.written)/float64(blocks)/1e3, float64(r.largest)/1e6, r.runsAvg, r.probes, r.files, float64(r.resident)/1e6, r.coldProbes, float64(r.recent)/1e6)
		}
		for _, sp := range splits {
			for _, rt := range []float64{ratio, 0.1} {
				r := simBuckets(blocks, K, rec, N, rt, 256, sp, M, bloomBits, residentBudget, runFile)
				fmt.Fprintf(&out, "%-22s %12.0f %11.1f %11.1f %11.1f %9d %11.1f %11.1f %9.1f  -> %d buckets\n",
					fmt.Sprintf("256 split@%dMB r=%.2f", sp>>20, rt), float64(r.written)/float64(blocks)/1e3, float64(r.largest)/1e6, r.runsAvg, r.probes, r.files, float64(r.resident)/1e6, r.coldProbes, float64(r.recent)/1e6, r.buckets)
			}
		}
	}
	fmt.Fprintf(&out, "\nidx KB/blk: index bytes written per block, runs and folds; largest: the biggest single fold (or split);\n")
	fmt.Fprintf(&out, "runs/bkt: sorted runs a lookup below the window probes (one filter each); probes: for a key not in the\n")
	fmt.Fprintf(&out, "shard, window filter + recent section + runs, of which cold probes are on disk (K byte reads each);\n")
	fmt.Fprintf(&out, "files: run files of %d MB; resident: filters within a %d MB budget, newest runs first; recent: the\n", runFile>>20, residentBudget>>20)
	fmt.Fprintf(&out, "recent sections in memory (every bucket merged every %d blocks); split@S: a bucket whose runs exceed S\n", M)
	fmt.Fprintf(&out, "splits in two; r: the fold ratio (a run folds into the older while the older is no larger than 1/r of it).\n")
	t.Log(out.String())
}

type simResult struct {
	written, largest int64
	runsAvg, probes  float64
	coldProbes       float64
	files, buckets   int
	resident, recent int64
}

// simBuckets models the bucketed long search.  split > 0 splits a
// bucket in two once its runs hold more than split bytes (each half
// keeps half of every run: the keys are hashed, so a split is a
// rewrite of the bucket, counted as a fold).  B/M buckets are merged
// each block so every bucket is merged every M blocks.
func simBuckets(blocks, K, rec, N int, ratio float64, B int, split int64, M, bloomBits int, residentBudget, runFile int64) simResult {
	type bucket struct {
		recent int64   // Records in the resident recent section
		runs   []int64 // Records per run, oldest first
	}
	bs := make([]bucket, B)
	var r simResult
	rotation := 0
	fold := func(b *bucket) {
		if len(b.runs) < 2 {
			return
		}
		var behind int64
		i := len(b.runs) - 1
		for ; i >= 0; i-- {
			if i < len(b.runs)-1 && float64(b.runs[i])*ratio > float64(behind) {
				break
			}
			behind += b.runs[i]
		}
		run := b.runs[i+1:]
		if len(run) >= 2 {
			var total int64
			for _, n := range run {
				total += n
			}
			r.written += total * int64(rec)
			if total*int64(rec) > r.largest {
				r.largest = total * int64(rec)
			}
			b.runs = append(b.runs[:i+1], total)
		}
	}
	for blk := 0; blk < blocks; blk++ {
		if blk >= N {
			perBucket := int64(K) / int64(len(bs))
			extra := int64(K) - perBucket*int64(len(bs))
			for i := range bs {
				bs[i].recent += perBucket
			}
			for i := int64(0); i < extra; i++ {
				bs[(blk*7+int(i))%len(bs)].recent++
			}
		}
		visits := (len(bs) + M - 1) / M
		for v := 0; v < visits; v++ {
			b := &bs[rotation%len(bs)]
			rotation++
			if split > 0 {
				var total int64
				for _, n := range b.runs {
					total += n
				}
				if total*int64(rec) > split {
					r.written += total * int64(rec)
					if total*int64(rec) > r.largest {
						r.largest = total * int64(rec)
					}
					half := bucket{recent: b.recent / 2}
					b.recent -= half.recent
					for i, n := range b.runs {
						b.runs[i] = n / 2
						half.runs = append(half.runs, n-n/2)
					}
					bs = append(bs, half)
					b = &bs[(rotation-1)%len(bs)]
				}
			}
			if b.recent == 0 {
				continue
			}
			r.written += b.recent * int64(rec)
			b.runs = append(b.runs, b.recent)
			b.recent = 0
			fold(b)
		}
	}
	// Filters: newest runs first across the shard, within the budget
	var runs, cold int
	type runRef struct{ n int64 }
	var newest, older []int64 // Per bucket: the newest run's records, then the rest
	for _, b := range bs {
		runs += len(b.runs)
		r.recent += b.recent * int64(rec)
		for i, n := range b.runs {
			if i == len(b.runs)-1 {
				newest = append(newest, n)
			} else {
				older = append(older, n)
			}
		}
	}
	budget := residentBudget
	for _, n := range append(newest, older...) {
		bytes := n * int64(bloomBits) / 8
		if budget >= bytes {
			budget -= bytes
			r.resident += bytes
		} else {
			cold++
		}
	}
	r.resident += r.recent
	var indexBytes int64
	for _, b := range bs {
		for _, n := range b.runs {
			indexBytes += n * int64(rec)
		}
	}
	r.files = int((indexBytes + runFile - 1) / runFile)
	r.buckets = len(bs)
	r.runsAvg = float64(runs) / float64(len(bs))
	r.coldProbes = float64(cold) / float64(len(bs))
	r.probes = 2 + r.runsAvg
	_ = runRef{}
	return r
}

type simTodayResult struct {
	written, largest int64
	segments, sets   int
}

// simToday models the store as it is: segments folded by copying
// bodies every mergeEvery blocks under the ratio rule, and packed
// every packEvery blocks by copying again.
func simToday(blocks, K, V, rec int, ratio float64, mergeEvery, packEvery int) simTodayResult {
	perRecord := int64(V + rec)
	var segs []int64 // Records per merged segment, oldest first
	var r simTodayResult
	pending := 0 // Finished blocks not yet merged
	for blk := 1; blk <= blocks; blk++ {
		pending++
		if blk%mergeEvery == 0 {
			// The finished blocks become one merged segment, copied
			n := int64(pending * K)
			r.written += n * perRecord
			segs = append(segs, n)
			pending = 0
			var behind int64
			i := len(segs) - 1
			for ; i >= 0; i-- {
				if i < len(segs)-1 && float64(segs[i])*ratio > float64(behind) {
					break
				}
				behind += segs[i]
			}
			run := segs[i+1:]
			if len(run) >= 2 {
				var total int64
				for _, n := range run {
					total += n
				}
				r.written += total * perRecord
				if total*perRecord > r.largest {
					r.largest = total * perRecord
				}
				segs = append(segs[:i+1], total)
			}
		}
		if blk%packEvery == 0 {
			// Everything merged so far below the watermark is copied
			// into a set and dropped from the shard
			var total int64
			for _, n := range segs {
				total += n
			}
			r.written += total * perRecord
			if total*perRecord > r.largest {
				r.largest = total * perRecord
			}
			segs = segs[:0]
			r.sets++
		}
	}
	r.segments = len(segs)
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return r
}
