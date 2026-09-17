package blockchainDB

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const heapCrashDirEnv = "BDB_HEAP_CRASH_DIR"

// The child: blocks of the same keys, every block rewriting every key
// with a value that says which block, a sync per block and a mover
// pass every few, and a line on stdout at each durability point.
// Killed by the parent at a moment of its choosing.
func TestHeapCrashChild(t *testing.T) {
	dir := os.Getenv(heapCrashDirEnv)
	if dir == "" {
		t.Skip("helper process for TestHeapCrashRecovery")
	}
	HeapFileBytes = 64 << 10
	h, err := NewHeapStore(dir)
	require.NoError(t, err, "child: new")
	for b := uint64(1); ; b++ {
		h.AdvanceBlock(b)
		for i := 0; i < 300; i++ {
			require.NoError(t, h.Put(crashKey(i), crashValue(b, i)), "child: put")
		}
		p, err := h.beginBlockSync()
		require.NoError(t, err, "child: sync")
		require.NoError(t, p.finish(), "child: finish")
		fmt.Printf("CHECKPOINT %d\n", b)
		if b%4 == 0 {
			_, err := h.compact(HeapCleanBytes)
			require.NoError(t, err, "child: compact")
		}
	}
}

func crashKey(i int) (k [32]byte) {
	k[0], k[1] = byte(i>>8), byte(i)
	return
}

func crashValue(b uint64, i int) []byte {
	v := make([]byte, 40+i%60)
	v[0], v[1], v[2] = byte(b>>16), byte(b>>8), byte(b)
	v[3] = byte(i)
	return v
}

// The parent: run the child, let it reach some durability points,
// kill it dead, and check that the heap opens to exactly the last
// durable block -- every key at that block's value, nothing torn,
// nothing from the block in flight -- and that a repair from the data
// alone agrees.
func TestHeapCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns processes")
	}
	for round := 0; round < 3; round++ {
		dir := filepath.Join(t.TempDir(), fmt.Sprintf("crash-%d", round))
		cmd := exec.Command(os.Args[0], "-test.run", "TestHeapCrashChild$")
		cmd.Env = append(os.Environ(), heapCrashDirEnv+"="+dir)
		out, err := cmd.StdoutPipe()
		require.NoError(t, err)
		require.NoError(t, cmd.Start())
		var last uint64
		sc := bufio.NewScanner(out)
		want := uint64(6 + round*5)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, "CHECKPOINT ") {
				continue
			}
			n, err := strconv.ParseUint(strings.TrimPrefix(line, "CHECKPOINT "), 10, 64)
			require.NoError(t, err)
			last = n
			if last >= want {
				break
			}
		}
		// Somewhere inside the next block: the child is mid-write or
		// mid-sync when it dies
		time.Sleep(time.Duration(round*7) * time.Millisecond)
		require.NoError(t, cmd.Process.Kill())
		_ = cmd.Wait()

		check := func(h *HeapStore, what string) {
			t.Helper()
			for i := 0; i < 300; i++ {
				v, err := h.Get(crashKey(i))
				require.NoError(t, err, "%s: key %d", what, i)
				require.Equal(t, crashValue(h.height, i), v, "%s: key %d is at the durable block", what, i)
			}
		}
		h, err := OpenHeapStore(dir)
		require.NoError(t, err)
		require.GreaterOrEqual(t, h.height, last, "the durable block is at least the last checkpoint the parent saw")
		check(h, "open")
		durable := h.height
		require.NoError(t, h.Close())

		// The index thrown away: the data alone must say the same
		_, gens, err := listHeap(dir)
		require.NoError(t, err)
		for _, g := range gens {
			require.NoError(t, os.Remove(filepath.Join(dir, indexName(g))))
		}
		r, err := RepairHeapStore(dir, durable)
		require.NoError(t, err)
		require.Equal(t, durable, r.height)
		check(r, "repair")
		require.NoError(t, r.Close())
	}
}
