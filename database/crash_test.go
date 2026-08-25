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
//	KV.Close() is a durability point.  After a process is SIGKILLed at
//	an arbitrary moment, OpenKV must succeed and every key written
//	before the last completed Close must be readable with its correct
//	value.  Writes after the last Close may or may not be present, but
//	must never corrupt the database.
//
// TestCrashChildProcess is the workload: it opens the DB, writes
// deterministic keys, and prints "CHECKPOINT n" after each completed
// Close.  TestCrashRecovery runs it as a child process, kills it at a
// random moment, reopens the database, and verifies the contract -
// several rounds, so kills land in different code paths (buffered
// writes, kfile rewrite, history push, bloom save).

const crashChildEnv = "BDB_CRASH_CHILD_DIR"
const crashChildStartEnv = "BDB_CRASH_CHILD_START"

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

// TestCrashChildProcess
// Helper process for TestCrashRecovery; skipped unless launched by it
func TestCrashChildProcess(t *testing.T) {
	dir := os.Getenv(crashChildEnv)
	if dir == "" {
		t.Skip("helper process for TestCrashRecovery")
	}
	start, _ := strconv.Atoi(os.Getenv(crashChildStartEnv))

	kv, err := OpenKV(dir)
	require.NoError(t, err, "child: open")
	require.NoError(t, kv.Open(), "child: open files")

	kr, vr := crashKV(start)
	const batch = 20
	for i := start; i < start+100_000; i++ {
		require.NoError(t, kv.Put(kr.NextHash(), vr.RandBuff(10, 50)), "child: put")
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
	// KeyLimit 50 forces history pushes; MaxCachedBlocks 2 forces
	// frequent kfile rewrites - both are crash windows we want kills
	// to land in.
	kv, err := NewKV(true, dir, 16, 50, 2)
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
		// the kill lands inside the write/rewrite/push/bloom-save paths
		select {
		case <-progressed:
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
			cmd.Wait()
			<-done
			outMu.Lock()
			t.Fatalf("child made no progress (durable=%d)\nchild stdout:\n%s\nchild stderr:\n%s",
				durable, strings.Join(childOut, "\n"), childErr.String())
		}
		time.Sleep(time.Duration(rng.UintN(120)) * time.Millisecond)
		require.NoError(t, cmd.Process.Kill())
		cmd.Wait()
		<-done
		durable = int(lastCheckpoint.Load())

		// The contract: the DB opens, and every checkpointed key reads
		// back correctly
		kv, err := OpenKV(dir)
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
