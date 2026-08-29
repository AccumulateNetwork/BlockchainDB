package blockchainDB

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Crash-injection test for issue #5.
//
// The durability contract under test:
//
//	KV2.Close() is a durability point.  After a process is SIGKILLed at
//	an arbitrary moment, OpenKV2 must succeed and every key written
//	before the last completed Close must be readable with its correct
//	value.  Writes after the last Close may or may not be present, but
//	must never corrupt the database.
//
// TestCrashChildProcess is the workload: it opens the DB, writes
// deterministic keys to both layers, and prints "CHECKPOINT n" after
// each completed Close.  TestCrashRecovery runs it as a child process,
// kills it at a random moment, reopens the database, and verifies the
// contract - several rounds, so kills land in different code paths
// (buffered writes, a seal, a compaction, a manifest commit).
//
// The environment variables below are a parent/child handshake for the
// re-executed test binary, not configuration.  Both are checked
// strictly: crashChildEnv decides whether this process is the child at
// all, and a missing or malformed crashChildStartEnv is a hard failure
// rather than a default -- restarting the workload from 0 would make
// the parent's checkpoint count regress and the suite would report a
// pass of far fewer keys than it verified.
const crashChildEnv = "BDB_CRASH_CHILD_DIR"
const crashChildStartEnv = "BDB_CRASH_CHILD_START"

// crashChildModeEnv selects which durability point the child reaches at
// each checkpoint.  Both are part of the contract and they fail
// differently:
//
//	close -- Close() flushes and fsyncs both layers and shuts the files.
//	seal  -- Seal(height) closes a block on a running store.  This is
//	         what a node actually does per block; it does not Close, so
//	         it is the only one that catches a layer whose writes are
//	         still sitting in a buffer (issue #29).
const crashChildModeEnv = "BDB_CRASH_CHILD_MODE"

const (
	crashModeClose = "close"
	crashModeSeal  = "seal"
)

// crashSealLimit is deliberately small so that the few hundred keys a
// round gets through before the kill still seal many times.  Seals,
// and the compactions they enable, are the crash windows this test
// exists to land kills in.
const crashSealLimit = 50

// crashCompressEvery compacts the Dyna layer every N writes.  Nothing
// in KV2 compacts on its own -- sealPermIfFull and sealDynaIfFull seal
// and stop there -- so without this the compaction path (issue #19,
// the one multi-step commit in the write path) is never under the
// kill.
//
// N must exceed 2*crashSealLimit, because only odd i reach Dyna and
// Compress empties the tail before returning: at N=50 the tail peaks
// at 25 records, sealDynaIfFull never fires, and adding compaction
// would silently cost the Dyna auto-seal window it was meant to
// complement.  Measured over 1200 puts: N=50 gives 0 Dyna auto-seals,
// N=137 gives 9.
const crashCompressEvery = 137

// crashKV generates the deterministic key/value sequence, skipping the
// first `skip` pairs
func crashKV(skip int) (kr, vr *FastRandom) {
	kr = NewFastRandom([]byte{81})
	vr = NewFastRandom([]byte{81, 81})
	for i := 0; i < skip; i++ {
		kr.NextHash()
		vr.RandBuff(10, 50)
	}
	return kr, vr
}

// crashPut writes pair i to the layer it belongs to.  Even pairs go to
// Perm and odd to Dyna, so a kill can land in either layer's path.
func crashPut(kv *KV2, i int, key [32]byte, value []byte) (err error) {
	if i%2 == 0 {
		_, err = kv.PutPerm(key, value)
	} else {
		_, err = kv.PutDyna(key, value)
	}
	return err
}

// TestCrashChildProcess
// Helper process for TestCrashRecovery; skipped unless launched by it
func TestCrashChildProcess(t *testing.T) {
	dir := os.Getenv(crashChildEnv)
	if dir == "" {
		t.Skip("helper process for TestCrashRecovery")
	}
	startEnv := os.Getenv(crashChildStartEnv)
	start, err := strconv.Atoi(startEnv)
	require.NoErrorf(t, err, "child: %s must be an integer, got %q", crashChildStartEnv, startEnv)

	mode := os.Getenv(crashChildModeEnv)
	require.Containsf(t, []string{crashModeClose, crashModeSeal}, mode,
		"child: %s must be %q or %q, got %q", crashChildModeEnv, crashModeClose, crashModeSeal, mode)

	kv, err := OpenKV2(dir)
	require.NoError(t, err, "child: open")
	require.NoError(t, kv.Open(), "child: open files")

	kr, vr := crashKV(start)
	const batch = 20
	for i := start; i < start+100_000; i++ {
		require.NoError(t, crashPut(kv, i, kr.NextHash(), vr.RandBuff(10, 50)), "child: put")
		if (i+1)%crashCompressEvery == 0 {
			require.NoError(t, kv.Compress(), "child: compress")
		}
		if (i+1)%batch != 0 {
			continue
		}
		switch mode {
		case crashModeClose:
			require.NoError(t, kv.Close(), "child: close")
			fmt.Printf("CHECKPOINT %d\n", i+1) // Durability point reached
			require.NoError(t, kv.Open(), "child: reopen")
		case crashModeSeal:
			// Seal the block being accumulated rather than a counted
			// one: a kill between a seal and its CHECKPOINT rolls the
			// parent's start back to before that seal, and re-sealing
			// a block the store has already closed is an error.
			_, err := kv.Seal(kv.PermKV.BlockHeight())
			require.NoError(t, err, "child: seal")
			fmt.Printf("CHECKPOINT %d\n", i+1)
		}
	}
}

// TestCrashRecovery
// Kill the child at random moments; verify that Close is a durability
// point.
func TestCrashRecovery(t *testing.T) {
	runCrashRounds(t, crashModeClose)
}

// TestCrashRecoverySeal
// The same contract with Seal as the durability point and no Close:
// what a node does at a block boundary.
//
// This is the case issue #29 broke.  Seal made the Perm layer durable
// and left the Dyna layer's newest writes in a 32 KB buffer, so a
// SIGKILL here came back with permanent records -- the even-numbered
// keys -- present and the dynamic records interleaved with them gone.
// Close hid it, because Close flushes and fsyncs both layers.
func TestCrashRecoverySeal(t *testing.T) {
	runCrashRounds(t, crashModeSeal)
}

// runCrashRounds is the crash-injection loop: run the child, kill it at
// a random moment, and verify every key it reported durable
func runCrashRounds(t *testing.T, mode string) {
	if os.Getenv(crashChildEnv) != "" {
		t.Skip("running as child")
	}
	dir := filepath.Join(os.TempDir(), "CrashRecovery_"+mode)
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Create the database once, durably, before any crashing starts.
	// (Initial creation itself is not crash-atomic; see the durability
	// design notes.)
	kv, err := NewKV2(dir, crashSealLimit)
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	durable := 0
	rng := NewFastRandom([]byte{82})
	for round := 0; round < 6; round++ {
		var lastCheckpoint atomic.Int64
		lastCheckpoint.Store(int64(durable))

		cmd := exec.Command(os.Args[0], "-test.run", "TestCrashChildProcess$")
		cmd.Env = append(os.Environ(),
			crashChildEnv+"="+dir,
			crashChildModeEnv+"="+mode,
			crashChildStartEnv+"="+strconv.Itoa(durable))
		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)
		var childErr bytes.Buffer
		cmd.Stderr = &childErr
		require.NoError(t, cmd.Start())

		done := make(chan struct{})
		progressed := make(chan struct{})
		var childOut []string
		var outMu sync.Mutex
		go func() {
			defer close(done)
			signaled := false
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				line := sc.Text()
				outMu.Lock()
				childOut = append(childOut, line)
				outMu.Unlock()
				if n, ok := strings.CutPrefix(line, "CHECKPOINT "); ok {
					if v, err := strconv.Atoi(n); err == nil {
						lastCheckpoint.Store(int64(v))
						if !signaled {
							signaled = true
							close(progressed)
						}
					}
				}
			}
		}()

		// Wait until the child is past startup and actually writing
		// (first new checkpoint), then kill it a random moment later so
		// the kill lands inside the write/seal/compact/commit paths
		select {
		case <-progressed:
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
			<-done // StdoutPipe: all reads must finish before Wait closes it
			cmd.Wait()
			outMu.Lock()
			out := strings.Join(childOut, "\n")
			outMu.Unlock()
			t.Fatalf("child made no progress (durable=%d)\nchild stdout:\n%s\nchild stderr:\n%s",
				durable, out, childErr.String())
		}
		time.Sleep(time.Duration(rng.UintN(120)) * time.Millisecond)
		require.NoError(t, cmd.Process.Kill())
		<-done     // StdoutPipe: draining must finish before Wait closes it,
		cmd.Wait() // or buffered CHECKPOINT lines are lost and durable undercounts
		durable = int(lastCheckpoint.Load())

		// The contract: the DB opens, and every checkpointed key reads
		// back correctly
		kv, err := OpenKV2(dir)
		require.NoErrorf(t, err, "round %d: reopen after kill (durable=%d)", round, durable)
		require.NoErrorf(t, kv.Open(), "round %d: open files", round)
		kr, vr := crashKV(0)
		for i := 0; i < durable; i++ {
			key := kr.NextHash()
			value := vr.RandBuff(10, 50)
			got, err := kv.Get(key)
			if err != nil {
				t.Fatalf("round %d: durable key %d lost (durable=%d): %v\n%s",
					round, i, durable, err, crashDiagnose(kv, dir, i, key))
			}
			require.Equalf(t, value, got, "round %d: durable key %d corrupt", round, i)
		}
		require.NoError(t, kv.Close())
		t.Logf("round %d: killed with %d durable keys; all verified", round, durable)
	}
}

// crashDiagnose
// Describe where a key that should have been durable actually is, so
// that an intermittent failure says something more useful than "not
// found".  Which layer holds it, what each layer's tail and segments
// look like, and what is on disk.
func crashDiagnose(kv *KV2, dir string, i int, key [32]byte) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n--- diagnosis for key %d (%x), %s layer ---\n",
		i, key[:8], map[bool]string{true: "Perm", false: "Dyna"}[i%2 == 0])

	for _, l := range []struct {
		name  string
		store *SegmentStore
	}{{"Perm", kv.PermKV}, {"Dyna", kv.DynaKV}} {
		s := l.store
		s.Mutex.Lock()
		_, inLive := s.live[key]
		inFilter := s.filterTest(key)
		fmt.Fprintf(&b, "%s: %d segments, live keys %d, records %d, blockHeight %d, filters=%d inLive=%v filterSays=%v\n",
			l.name, len(s.segments), len(s.live), s.liveRecords, s.blockHeight,
			len(s.filters), inLive, inFilter)
		for _, seg := range s.segments {
			dbb, found, err := seg.lookup(key)
			mark := ""
			if found {
				mark = fmt.Sprintf("  <-- HOLDS IT at offset %d", dbb.Offset)
			}
			if err != nil {
				mark = fmt.Sprintf("  <-- lookup error: %v", err)
			}
			fmt.Fprintf(&b, "   seg (%d,%d) count=%d records=%d file=%s%s\n",
				seg.meta.Height, seg.meta.Seq, seg.count, seg.records, seg.meta.File, mark)
		}
		s.Mutex.Unlock()
	}

	for _, sub := range []string{PermDirName, DynaDirName} {
		entries, err := os.ReadDir(filepath.Join(dir, sub))
		if err != nil {
			fmt.Fprintf(&b, "%s dir: %v\n", sub, err)
			continue
		}
		fmt.Fprintf(&b, "%s dir:", sub)
		for _, e := range entries {
			info, _ := e.Info()
			size := int64(-1)
			if info != nil {
				size = info.Size()
			}
			fmt.Fprintf(&b, " %s(%d)", e.Name(), size)
		}
		b.WriteString("\n")
	}
	return b.String()
}
