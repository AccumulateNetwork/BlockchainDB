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
		if (i+1)%batch == 0 {
			require.NoError(t, kv.Close(), "child: close")
			fmt.Printf("CHECKPOINT %d\n", i+1) // Durability point reached
			require.NoError(t, kv.Open(), "child: reopen")
		}
	}
}

// TestCrashRecovery
// Kill the child at random moments; verify the durability contract
func TestCrashRecovery(t *testing.T) {
	if os.Getenv(crashChildEnv) != "" {
		t.Skip("running as child")
	}
	dir := filepath.Join(os.TempDir(), "CrashRecovery")
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
			require.NoErrorf(t, err, "round %d: durable key %d lost (durable=%d)", round, i, durable)
			require.Equalf(t, value, got, "round %d: durable key %d corrupt", round, i)
		}
		require.NoError(t, kv.Close())
		t.Logf("round %d: killed with %d durable keys; all verified", round, durable)
	}
}
