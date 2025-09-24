# BlockchainDB Upgrade Plan

## Overview

This document outlines a phased approach to addressing the critical performance and architectural issues identified in the BlockchainDB codebase. The plan is designed to minimize risk while delivering incremental improvements.

## Upgrade Philosophy

1. **Backward Compatibility**: Maintain API compatibility throughout
2. **Incremental Delivery**: Ship improvements in small, tested batches
3. **Performance First**: Prioritize changes with highest performance impact
4. **Zero Downtime**: Enable rolling upgrades without service interruption
5. **Measurable Progress**: Define success metrics for each phase

---

## Phase 0: Foundation & Instrumentation (Week 1-2)

### Objective
Establish baseline metrics and safety mechanisms before making changes.

### Tasks

#### 0.1 Add Performance Instrumentation
```go
// Add metrics package
type Metrics struct {
    GetLatency    *prometheus.HistogramVec
    PutLatency    *prometheus.HistogramVec
    CacheHitRate  *prometheus.GaugeVec
    AllocRate     *prometheus.GaugeVec
    FileHandles   *prometheus.GaugeVec
}
```

#### 0.2 Create Benchmark Suite
```bash
# Create benchmark baselines
go test -bench=. -benchmem -benchtime=10s > baseline.txt
```

#### 0.3 Add Integration Tests
- File handle limit tests
- Memory pressure tests
- Concurrent access tests

#### 0.4 Setup CI Performance Gates
```yaml
# .github/workflows/performance.yml
performance-regression:
  - max-latency-p99: 10ms
  - min-throughput: 10000 ops/sec
  - max-memory: 1GB
```

### Deliverables
- [ ] Metrics dashboard deployed
- [ ] Baseline performance documented
- [ ] CI gates preventing regression
- [ ] Load testing framework ready

### Success Criteria
- Visibility into all key metrics
- Automated regression detection
- Baseline for comparison

---

## Phase 1: Critical Performance Fixes (Week 3-4)

### Objective
Fix P0 issues causing immediate production impact.

### Tasks

#### 1.1 Implement Buffer Pooling

**File**: `database/kv.go`

```go
// Before
func (k *KV) Get(key [32]byte) ([]byte, error) {
    value = make([]byte, dbbKey.Length)  // Allocation
}

// After
var bufferPool = sync.Pool{
    New: func() interface{} {
        return make([]byte, 0, 4096)
    },
}

func (k *KV) Get(key [32]byte) ([]byte, error) {
    buf := bufferPool.Get().([]byte)
    defer bufferPool.Put(buf[:0])
    value = buf[:dbbKey.Length]
}
```

#### 1.2 Fix File Handle Management

**File**: `database/kv_shard.go`

```go
// Add connection pool
type ShardPool struct {
    shards [512]*ShardConnection
    mu     sync.RWMutex
}

type ShardConnection struct {
    kv       *KV2
    lastUsed time.Time
    mu       sync.RWMutex
}

func (k *KVShard) getShardConnection(index int) *KV2 {
    k.pool.mu.RLock()
    conn := k.pool.shards[index]
    k.pool.mu.RUnlock()

    if conn == nil {
        conn = k.openShardLazy(index)
    }
    return conn.kv
}
```

#### 1.3 Implement Binary Search for Keys

**File**: `database/kfile.go`

```go
// Before: O(n) linear search
for len(keys) >= DBKeyFullSize {
    if [32]byte(keys) == Key {
        // Found
    }
}

// After: O(log n) binary search
func binarySearchKey(keys []byte, target [32]byte) (DBBKeyFull, bool) {
    count := len(keys) / DBKeyFullSize
    left, right := 0, count-1

    for left <= right {
        mid := (left + right) / 2
        offset := mid * DBKeyFullSize
        midKey := [32]byte(keys[offset:offset+32])

        cmp := bytes.Compare(midKey[:], target[:])
        if cmp == 0 {
            return unmarshalDBBKeyFull(keys[offset:]), true
        } else if cmp < 0 {
            left = mid + 1
        } else {
            right = mid - 1
        }
    }
    return DBBKeyFull{}, false
}
```

### Validation
```bash
# Run performance tests
go test -bench=BenchmarkGet -benchmem
# Expected: 100x fewer allocations

# Test file handles
ulimit -n 256  # Set low limit
go test -run TestConcurrentShards
# Expected: Pass without "too many open files"
```

### Rollback Plan
- Feature flags for each optimization
- A/B testing in production
- Quick revert via configuration

### Success Criteria
- [ ] Memory allocations reduced by 100x
- [ ] No file handle exhaustion under load
- [ ] Get operations 60x faster for large datasets
- [ ] All existing tests pass

---

## Phase 2: Storage Optimization (Week 5-6)

### Objective
Reduce storage footprint and improve I/O efficiency.

### Tasks

#### 2.1 Enable Compression

**File**: `database/kv.go`

```go
type CompressionConfig struct {
    Enabled   bool
    Algorithm string  // "snappy", "zstd", "lz4"
    Level     int
}

func (k *KV) CompressWithConfig(config CompressionConfig) error {
    if !config.Enabled {
        return nil
    }

    compressor := getCompressor(config.Algorithm)
    // Implement compression logic
}
```

#### 2.2 Persist Bloom Filters

**File**: `database/kfile.go`

```go
type BloomFilter struct {
    *Bloom
    version   uint32
    checksum  uint32
}

func (k *KFile) saveBloomFilter() error {
    path := k.Filename + ".bloom"
    data := k.Bloom.Marshal()
    return os.WriteFile(path, data, 0644)
}

func (k *KFile) loadBloomFilter() error {
    path := k.Filename + ".bloom"
    if data, err := os.ReadFile(path); err == nil {
        return k.Bloom.Unmarshal(data)
    }
    return k.rebuildBloomFilter()  // Fallback
}
```

#### 2.3 Implement Cache Management

**File**: `database/kfile.go`

```go
type LRUCache struct {
    capacity int
    size     int
    items    map[[32]byte]*list.Element
    lru      *list.List
    mu       sync.RWMutex
}

func (c *LRUCache) Get(key [32]byte) (DBBKeyFull, bool) {
    c.mu.RLock()
    elem, ok := c.items[key]
    c.mu.RUnlock()

    if ok {
        c.mu.Lock()
        c.lru.MoveToFront(elem)
        c.mu.Unlock()
        return elem.Value.(DBBKeyFull), true
    }
    return DBBKeyFull{}, false
}
```

### Success Criteria
- [ ] Storage reduced by 70% with compression
- [ ] Startup time <10 seconds
- [ ] Cache hit rate >80%

---

## Phase 3: Concurrency Enhancement (Week 7-8)

### Objective
Enable true parallel processing across shards.

### Tasks

#### 3.1 Per-Shard Locking

```go
type Shard struct {
    kv *KV2
    mu sync.RWMutex  // Per-shard lock
}

func (s *Shard) Get(key [32]byte) ([]byte, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.kv.Get(key)
}
```

#### 3.2 Async History Processing

```go
type AsyncHistory struct {
    queue chan HistoryUpdate
    done  chan struct{}
}

func (h *AsyncHistory) ProcessAsync() {
    for update := range h.queue {
        h.processUpdate(update)
    }
}
```

#### 3.3 Read-Ahead Prefetching

```go
type Prefetcher struct {
    cache map[[32]byte][]byte
    queue chan [32]byte
}

func (p *Prefetcher) PrefetchKeys(keys [][32]byte) {
    for _, key := range keys {
        p.queue <- key
    }
}
```

### Success Criteria
- [ ] 16x throughput improvement
- [ ] Linear scaling with CPU cores
- [ ] No lock contention in profiles

---

## Phase 4: Advanced Indexing (Week 9-10)

### Objective
Replace linear structures with efficient indexes.

### Tasks

#### 4.1 B-Tree Implementation

```go
type BTreeIndex struct {
    root   *BTreeNode
    degree int
    mu     sync.RWMutex
}

func (bt *BTreeIndex) Search(key [32]byte) (*DBBKeyFull, bool) {
    bt.mu.RLock()
    defer bt.mu.RUnlock()
    return bt.root.search(key)
}
```

#### 4.2 Secondary Indexes

```go
type SecondaryIndex struct {
    indices map[string]*BTreeIndex  // Named indexes
}

func (si *SecondaryIndex) CreateIndex(name string, extractor func([]byte) []byte) {
    // Build index from existing data
}
```

### Success Criteria
- [ ] O(log n) complexity for all operations
- [ ] Support for range queries
- [ ] Index rebuild <1 minute

---

## Phase 5: Production Hardening (Week 11-12)

### Objective
Ensure production readiness and operational excellence.

### Tasks

#### 5.1 Comprehensive Error Handling

```go
type DBError struct {
    Op       string
    Key      [32]byte
    Err      error
    Severity string
}

func (e *DBError) Error() string {
    return fmt.Sprintf("%s operation failed for key %x: %v",
        e.Op, e.Key[:8], e.Err)
}
```

#### 5.2 Graceful Degradation

```go
type CircuitBreaker struct {
    failures  int
    threshold int
    state     string  // "closed", "open", "half-open"
}
```

#### 5.3 Monitoring & Alerting

```yaml
alerts:
  - name: high_latency
    expr: db_get_latency_p99 > 10ms
    for: 5m

  - name: low_cache_hit
    expr: db_cache_hit_rate < 0.5
    for: 10m
```

### Success Criteria
- [ ] 99.99% availability SLA
- [ ] MTTR <5 minutes
- [ ] All critical paths instrumented

---

## Migration Strategy

### Data Migration

#### Option 1: In-Place Upgrade
```go
func MigrateDatabase(oldPath, newPath string) error {
    old := OpenOldFormat(oldPath)
    new := CreateNewFormat(newPath)

    iterator := old.Iterator()
    for iterator.Next() {
        new.Put(iterator.Key(), iterator.Value())
    }
    return new.Close()
}
```

#### Option 2: Live Migration
```go
type DualWriteAdapter struct {
    old Database
    new Database
}

func (d *DualWriteAdapter) Put(key, value []byte) error {
    if err := d.old.Put(key, value); err != nil {
        return err
    }
    return d.new.Put(key, value)
}
```

### Rollout Plan

1. **Canary Deployment** (5% traffic)
   - Monitor metrics for 24 hours
   - Compare against baseline

2. **Progressive Rollout** (25%, 50%, 100%)
   - Increase traffic gradually
   - Monitor error rates and latency

3. **Full Deployment**
   - Complete migration
   - Decommission old code

### Rollback Procedure

```bash
#!/bin/bash
# Quick rollback script
kubectl rollout undo deployment/blockchaindb
kubectl rollout status deployment/blockchaindb

# Verify rollback
curl -s http://metrics/health | jq .version
```

---

## Timeline & Milestones

| Phase | Duration | Milestone | Success Metric |
|-------|----------|-----------|----------------|
| Phase 0 | Week 1-2 | Instrumentation Complete | Metrics visible |
| Phase 1 | Week 3-4 | Critical Fixes Deployed | 100x fewer allocations |
| Phase 2 | Week 5-6 | Storage Optimized | 70% size reduction |
| Phase 3 | Week 7-8 | Concurrency Enhanced | 16x throughput |
| Phase 4 | Week 9-10 | Indexing Upgraded | O(log n) operations |
| Phase 5 | Week 11-12 | Production Ready | 99.99% SLA |

## Resource Requirements

### Team
- 2 Senior Engineers (full-time)
- 1 DevOps Engineer (50%)
- 1 QA Engineer (50%)

### Infrastructure
- Performance testing environment
- Staging cluster matching production
- Monitoring infrastructure (Prometheus/Grafana)

### Tools
- Load testing: k6 or Gatling
- Profiling: pprof, trace
- Monitoring: Prometheus, Grafana
- APM: OpenTelemetry

## Risk Management

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Data corruption during migration | Low | Critical | Extensive testing, checksums, backups |
| Performance regression | Medium | High | CI gates, canary deployments |
| Breaking API changes | Low | High | Compatibility layer, versioning |
| Resource exhaustion | Medium | Medium | Resource limits, circuit breakers |

### Operational Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Extended downtime | Low | Critical | Rolling updates, rollback plan |
| Monitoring gaps | Medium | Medium | Pre-deployment validation |
| Team knowledge gaps | Medium | Low | Documentation, training |

## Success Metrics

### Performance KPIs
- **Throughput**: 200,000 ops/sec (from 12,000)
- **Latency P99**: <10ms (from 120ms)
- **Storage Efficiency**: 75% reduction
- **Startup Time**: <10 seconds (from 2+ minutes)
- **Memory Usage**: 80% reduction

### Operational KPIs
- **Availability**: 99.99% SLA
- **MTTR**: <5 minutes
- **Deploy Frequency**: Daily
- **Change Failure Rate**: <5%

## Communication Plan

### Stakeholders
- Engineering Leadership: Weekly updates
- Product Team: Milestone completions
- Operations: Daily standup during deployment
- Customers: Release notes for visible changes

### Documentation
- Technical design docs for each phase
- API migration guide
- Operations runbook
- Performance tuning guide

---

## Appendix A: Code Examples

### Buffer Pool Implementation
```go
package database

import (
    "sync"
)

type BufferPool struct {
    pools map[int]*sync.Pool
}

func NewBufferPool() *BufferPool {
    bp := &BufferPool{
        pools: make(map[int]*sync.Pool),
    }

    // Pre-create pools for common sizes
    sizes := []int{256, 1024, 4096, 16384, 65536}
    for _, size := range sizes {
        size := size  // Capture loop variable
        bp.pools[size] = &sync.Pool{
            New: func() interface{} {
                return make([]byte, 0, size)
            },
        }
    }

    return bp
}

func (bp *BufferPool) Get(size int) []byte {
    // Round up to next power of 2
    poolSize := 256
    for poolSize < size && poolSize < 65536 {
        poolSize *= 2
    }

    if pool, ok := bp.pools[poolSize]; ok {
        buf := pool.Get().([]byte)
        return buf[:size]
    }

    return make([]byte, size)
}

func (bp *BufferPool) Put(buf []byte) {
    capacity := cap(buf)
    if pool, ok := bp.pools[capacity]; ok {
        pool.Put(buf[:0])
    }
}
```

### Feature Flag System
```go
package database

type FeatureFlags struct {
    BufferPooling      bool
    BinarySearch       bool
    Compression        bool
    PerShardLocking    bool
    AsyncHistory       bool
}

var flags = FeatureFlags{
    BufferPooling:   true,  // Phase 1
    BinarySearch:    true,  // Phase 1
    Compression:     false, // Phase 2
    PerShardLocking: false, // Phase 3
    AsyncHistory:    false, // Phase 3
}

func (k *KV) Get(key [32]byte) ([]byte, error) {
    if flags.BufferPooling {
        return k.getWithPooling(key)
    }
    return k.getLegacy(key)
}
```

---

## Appendix B: Testing Strategy

### Unit Test Template
```go
func TestBufferPooling(t *testing.T) {
    // Setup
    pool := NewBufferPool()

    // Test allocation
    buf := pool.Get(1024)
    assert.Equal(t, 1024, len(buf))
    assert.Equal(t, 1024, cap(buf))

    // Test reuse
    ptr := &buf[0]
    pool.Put(buf)

    buf2 := pool.Get(1024)
    ptr2 := &buf2[0]
    assert.Equal(t, ptr, ptr2, "Buffer should be reused")
}
```

### Benchmark Template
```go
func BenchmarkGetWithPooling(b *testing.B) {
    kv := setupKV()
    keys := generateKeys(1000)

    b.ResetTimer()
    b.RunParallel(func(pb *testing.PB) {
        i := 0
        for pb.Next() {
            key := keys[i%len(keys)]
            _, _ = kv.Get(key)
            i++
        }
    })

    b.StopTimer()
    b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}
```

### Load Test Script
```javascript
// k6 load test
import http from 'k6/http';
import { check } from 'k6';

export let options = {
    stages: [
        { duration: '5m', target: 100 },  // Ramp up
        { duration: '10m', target: 100 }, // Stay at 100
        { duration: '5m', target: 0 },    // Ramp down
    ],
    thresholds: {
        http_req_duration: ['p(99)<10'],  // 99% under 10ms
        http_req_failed: ['rate<0.01'],   // Error rate under 1%
    },
};

export default function() {
    let response = http.get('http://localhost:8080/get?key=testkey');
    check(response, {
        'status is 200': (r) => r.status === 200,
        'latency OK': (r) => r.timings.duration < 10,
    });
}
```

---

*Document Version: 1.0*
*Last Updated: 2025-01-23*
*Author: BlockchainDB Team*