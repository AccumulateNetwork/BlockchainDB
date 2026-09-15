package blockchainDB

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Auditing the permanent layer (audit.go).
//
// Immutability is enforced over a window, so a permanent key rewritten
// with a different value far enough apart is accepted silently.  That
// is a bug in whatever wrote it, and these pin that the audit finds
// it: across segments, across packed sets, and without mistaking an
// identical rewrite for a fault.

// permConflictFixture writes `key` with one value, seals past the
// window so the immutability check can no longer see it, and writes it
// again with another.  Returns the store and the two values, newest
// first.
func permConflictFixture(t *testing.T, dir string, seed byte) (store *SegmentStore, newest, oldest []byte) {
	t.Helper()
	store, err := NewSegmentStore(dir, false)
	require.NoError(t, err)
	require.NoError(t, store.SetFilterBlocks(MinFilterBlocks))

	kr := NewFastRandom([]byte{seed})
	key := kr.NextHash()
	oldest, newest = []byte("the value it was written with"), []byte("a different value entirely")
	require.NoError(t, store.Put(key, oldest))

	for h := uint64(1); h <= 3*MinFilterBlocks; h++ {
		require.NoError(t, store.Put(kr.NextHash(), []byte{byte(h)}))
		_, err = store.Seal(h)
		require.NoError(t, err)
	}
	// Outside the window now, so the store cannot see the conflict and
	// takes the write.  That is the behaviour the audit exists for.
	require.NoError(t, store.Put(key, newest),
		"the premise: a rewrite beyond the window is accepted")
	return store, newest, oldest
}

// TestAuditFindsAPermanentValueThatChanged
func TestAuditFindsAPermanentValueThatChanged(t *testing.T) {
	store, newest, oldest := permConflictFixture(t, storeDir(t, "audit-conflict"), 211)
	defer store.Close()

	var found []PermConflict
	audit, err := auditPermShard(0, store, nil, func(c PermConflict) error {
		found = append(found, c)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), audit.Conflicts, "one key, two values: %s", audit)
	require.Len(t, found, 1)
	require.Len(t, found[0].Values, 2)
	require.Equal(t, newest, found[0].Values[0], "newest first: what a read returns")
	require.Equal(t, oldest, found[0].Values[1])
	require.Contains(t, found[0].String(), "held with 2 values")

	// It survives sealing the second copy into a segment, which is
	// where a real one would be found
	_, err = store.Seal(3*MinFilterBlocks + 1)
	require.NoError(t, err)
	audit, err = auditPermShard(0, store, nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), audit.Conflicts)
}

// TestAuditIsQuietOnACleanStore
// Every key written once, and an identical rewrite, which is a no-op
// rather than a second copy.
func TestAuditIsQuietOnACleanStore(t *testing.T) {
	dir := storeDir(t, "audit-clean")
	kvs, keys, values := packedFixture(t, dir, 212, 2*MinFilterBlocks, 5)
	defer kvs.Close()
	for _, k := range keys[:20] {
		require.NoError(t, kvs.PutPerm(k, values[k]), "an identical rewrite is a no-op")
	}

	audit, err := kvs.AuditPermanentValues(func(c PermConflict) error {
		return fmt.Errorf("unexpected conflict: %s", c)
	})
	require.NoError(t, err)
	require.Equal(t, uint64(0), audit.Conflicts)
	require.Equal(t, uint64(len(keys)), audit.Keys, "every key accounted for: %s", audit)
}

// TestAuditSpansPackedSets
// A key whose older copy has been packed into a block set and whose
// newer one is still in a shard: the audit has to merge both to see it.
func TestAuditSpansPackedSets(t *testing.T) {
	dir := storeDir(t, "audit-packed")
	kvs, keys, values := packedFixture(t, dir, 213, 3*MinFilterBlocks, 5)
	defer kvs.Close()

	_, packed, err := kvs.PackFinalized(2 * MinFilterBlocks)
	require.NoError(t, err)
	require.True(t, packed, "the fixture must leave something packed")

	clean, err := kvs.AuditPermanentValues(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), clean.Conflicts, "packing alone is not a conflict: %s", clean)
	require.Equal(t, uint64(len(keys)), clean.Keys)

	// Rewrite one packed key with a different value.  It is far outside
	// the window, so the store takes it.
	victim := keys[0]
	require.NotEqual(t, values[victim], []byte("changed"))
	require.NoError(t, kvs.PutPerm(victim, []byte("changed")))

	var found []PermConflict
	audit, err := kvs.AuditPermanentValues(func(c PermConflict) error {
		found = append(found, c)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), audit.Conflicts, "%s", audit)
	require.Len(t, found, 1)
	require.Equal(t, victim, found[0].Key)
	require.Equal(t, []byte("changed"), found[0].Values[0])
	require.Equal(t, values[victim], found[0].Values[1])
	require.Contains(t, found[0].Places[1].File, setFileSuffix, "the older copy is in a set")
}
