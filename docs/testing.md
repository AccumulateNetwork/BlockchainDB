# BlockchainDB Testing Documentation

This document provides an overview of the testing approach used in BlockchainDB, evaluating what works well and what could be improved.

## Overview of Testing Approach

BlockchainDB uses Go's standard testing framework along with the `testify` package for assertions. The tests are primarily focused on functional verification and performance benchmarking of the various components.

## Test Files Structure

The repository contains the following test files:

- `bfile_test.go` - Buffered File component
- `bloom_test.go`, `bloomset_test.go` - Bloom filter and the layered,
  persisted filter built on it
- `concurrency_test.go` - Concurrent access to `KVShard` and `KVView`
- `crash_test.go` - SIGKILL injection: the durability contract in
  [durability.md](design/durability.md), enforced by killing a child
  process mid-write and verifying every checkpointed key
- `dyna_test.go` - The mutable (Dyna) layer: compaction, live-tail
  bounds, and crash-midway recovery
- `fastrandom_test.go` - The deterministic generator used to build test data
- `keys_test.go` - Key record marshalling
- `kv_2_test.go` - The two-layer `KV2` store
- `kv_shard_test.go`, `kv_shard_writer_test.go` - The sharded store and
  its multi-core ingest path
- `kv_test.go` - End-to-end key/value round trips
- `loadtest_test.go` - The `-load` flag that gates the multi-GB load tests
- `profile_test.go`, `kv2_profile_test.go` - Profiling harnesses
- `reopen_test.go` - The create -> write -> close -> open -> read round trip
- `segment_test.go` - Block export/import between nodes
- `segstore_test.go`, `segstore_bench_test.go` - `SegmentStore` behavior
  and its cost measurements
- `view_kv_test.go` - The View KV implementation

## Testing Utilities

The codebase includes several testing utilities:

1. **MakeDir** - Creates a temporary directory for testing and returns a function to clean it up
2. **MakeFilename** - Creates a temporary file for testing
3. **FastRandom** - A deterministic random number generator for creating test data

## What Works Well

### 1. Comprehensive Component Testing

Each major component of BlockchainDB has dedicated test files that verify its functionality. This ensures that individual components work as expected before they're integrated.

### 2. Performance Measurement

Many tests include performance measurements, reporting metrics like operations per second. This helps track performance characteristics and identify regressions.

Example from `kv_test.go`:
```go
wps := cntWrites / time.Since(start).Seconds()
rps := cntReads / time.Since(start).Seconds()
fmt.Printf("Writes per second %10.3f Reads per second %10.3f\n", wps, rps)
```

### 3. Data Integrity Verification

Tests verify data integrity by writing values and then reading them back to ensure they match. This is crucial for a database system.

Example from `kv_test.go`:
```go
value2, err := kv.Get(key)
assert.NoError(t, err, "Failed to put")
assert.Equal(t, value, value2, "Didn't the the value back")
```

### 4. Edge Case Testing

Some tests check edge cases such as:
- Reading after closing and reopening files
- Handling non-existent keys
- Testing compression operations
- Verifying behavior with large datasets

### 5. Cleanup Mechanisms

Tests use defer statements with cleanup functions to ensure test resources are properly released, even if tests fail.

Example:
```go
dir, rm := MakeDir()
defer rm()
```

## Areas for Improvement

### 1. Test Coverage Reporting

The repository doesn't appear to include test coverage reporting. Adding coverage analysis would help identify untested code paths.

Recommendation:
```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### 2. Mocking External Dependencies

The tests directly interact with the filesystem, which can make tests slower and less reliable. Introducing interfaces and mocks for external dependencies would improve test isolation.

### 3. Table-Driven Tests

Many tests could benefit from a table-driven approach, which would make it easier to test multiple scenarios with less code duplication.

Example improvement:
```go
func TestBloom(t *testing.T) {
    testCases := []struct {
        name          string
        numEntries    int
        falsePositive float64
    }{
        {"small", 1000, 0.1},
        {"medium", 10000, 0.05},
        {"large", 100000, 0.01},
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            bloom := NewBloom(4.5)
            fr := NewFastRandom([]byte{1})
            
            // Test implementation...
        })
    }
}
```

### 4. Benchmarking

Several tests measure performance by reporting rates, which is useful
for tracking but noisy. Formal Go benchmarks using the `testing.B`
type are used for lookups (`BenchmarkSegmentStoreGet`) but not for
writes; adding them would give more consistent measurements.

Example:
```go
func BenchmarkKVPut(b *testing.B) {
    dir, rm := MakeDir()
    defer rm()

    kvs, _ := NewKVShard(dir, 100_000)
    fr := NewFastRandom([]byte{1})

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        key := fr.NextHash()
        value := fr.RandBuff(100, 200)
        kvs.PutPerm(key, value)
    }
}
```

### 5. Integration Testing

Most tests focus on individual components. Adding more integration tests that verify the interaction between components would improve confidence in the system as a whole.

### 6. Parameterized Tests

Some tests use hard-coded values for parameters like buffer sizes. Parameterizing these tests would help identify optimal configurations and edge cases.

### 7. Error Injection

Adding tests that deliberately inject errors (like disk full scenarios or corrupted files) would help verify error handling paths.

## Test Execution

To run the tests in BlockchainDB:

```bash
# Run all tests
go test ./...

# Run a specific test
go test -run TestKV ./database/

# Run tests with verbose output
go test -v ./...

# Run tests and generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

Two flags change what runs:

- `-load` opts in to the multi-GB load tests, which build databases
  measured in tens of gigabytes and run for tens of minutes to hours.
  They are skipped by default so `go test ./...` stays a suite someone
  can wait for; the skip line names the flag.
- `-short` skips the measurement tests (`TestSyncCost`,
  `TestDynaCost`, `TestMultiCoreScaling`), which are
  correctness-neutral and exist to report numbers.

## What CI runs

Split by cost, so that a mistake a compiler catches reports in a
minute rather than behind the suite.

**On every push and pull request** (`.github/workflows/ci.yml`):

| job | what | why |
|---|---|---|
| `build` | `go build`, `go vet`, `gofmt -l` | seconds; catches the unused import, the shadowed error, the unformatted file |
| `test` | `go test -short ./...` | every correctness test; the measurement tests prove nothing on a shared runner |
| `race` | `-race` over the concurrency tests | the sharded write path, the file pool, iteration alongside compaction |

**Nightly** (`.github/workflows/nightly.yml`): the full suite with
timings, the full suite under `-race`, and the crash-recovery tests
repeated 25 times.

That last job earns its place. The durability tests kill a child
process at a random moment, so each run samples a different window and
a single run proves little. Issue #35 — an empty key filter accepted
as covering segment `(0,0)`, so `Get` reported keys absent that were
sitting on disk — failed **twice in fifty-five runs**. A pre-merge run
sees green and merges it. Repetition is what finds that class of bug,
and repetition is too slow to gate a push.

The suite is slow enough (~27 minutes, ~20 with `-short`) that this
split is a necessity rather than a preference. Most of that time is
fsync: a block boundary seals all 512 shards, and a shard with no
writes still pays two fsyncs for a manifest it did not change. That is
issues #32 and #33; if they are fixed, the gate can be simpler.

## Conclusion

The BlockchainDB testing approach provides good functional verification of components and includes performance measurements. However, there are opportunities to improve test coverage, isolation, and consistency through more structured testing approaches and better use of Go's testing features.
