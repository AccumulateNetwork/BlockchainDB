# BlockchainDB Migration Guide

## Overview

This guide provides step-by-step instructions for migrating existing BlockchainDB deployments to the optimized version. The migration preserves all data while enabling significant performance improvements.

## Migration Strategies

### Strategy 1: Blue-Green Deployment (Recommended)
- **Downtime**: Zero
- **Risk**: Low
- **Complexity**: Medium
- **Best for**: Production systems with high availability requirements

### Strategy 2: In-Place Upgrade
- **Downtime**: 30-60 minutes
- **Risk**: Medium
- **Complexity**: Low
- **Best for**: Development/staging environments

### Strategy 3: Rolling Upgrade
- **Downtime**: Zero
- **Risk**: Medium
- **Complexity**: High
- **Best for**: Sharded deployments with multiple nodes

---

## Pre-Migration Checklist

### System Requirements
- [ ] Go 1.21+ installed
- [ ] 2x current storage available (for safety)
- [ ] Backup system operational
- [ ] Monitoring infrastructure ready
- [ ] Load balancer configured (for zero-downtime)

### Data Validation
```bash
# 1. Calculate data checksum
./tools/checksum.sh /path/to/db > checksums.before

# 2. Verify data integrity
go run tools/verify.go -db /path/to/db

# 3. Record metrics
curl http://localhost:8080/metrics > metrics.before

# 4. Count total keys
./tools/keycount.sh /path/to/db
```

### Backup Procedures
```bash
# Full backup
tar -czf backup-$(date +%Y%m%d-%H%M%S).tar.gz /path/to/db/

# Incremental backup (if supported)
rsync -av --progress /path/to/db/ /backup/location/

# Verify backup
tar -tzf backup-*.tar.gz | head -20
```

---

## Blue-Green Migration (Zero Downtime)

### Step 1: Setup New Environment

```bash
# Clone and build new version
git clone https://github.com/BlockchainDB/blockchaindb.git
cd blockchaindb
git checkout v2.0.0
go build ./...

# Create new data directory
mkdir -p /data/blockchaindb-v2

# Copy configuration
cp /etc/blockchaindb/config.yml /etc/blockchaindb/config-v2.yml
```

### Step 2: Initial Data Sync

```go
// sync.go - Initial data synchronization
package main

import (
    olddb "github.com/BlockchainDB/database/v1"
    newdb "github.com/BlockchainDB/database/v2"
)

func syncDatabases(oldPath, newPath string) error {
    // Open old database
    old, err := olddb.Open(oldPath)
    if err != nil {
        return err
    }
    defer old.Close()

    // Create new database with optimizations
    new, err := newdb.OpenWithConfig(newPath, newdb.Config{
        BufferPooling:   true,
        Compression:     true,
        BinarySearch:    true,
        PerShardLocking: true,
    })
    if err != nil {
        return err
    }
    defer new.Close()

    // Copy all data
    iterator := old.NewIterator()
    batch := new.NewBatch()
    count := 0

    for iterator.Next() {
        key := iterator.Key()
        value := iterator.Value()

        batch.Put(key, value)
        count++

        // Flush every 10,000 records
        if count%10000 == 0 {
            if err := batch.Write(); err != nil {
                return err
            }
            batch = new.NewBatch()
            log.Printf("Synced %d records", count)
        }
    }

    // Final flush
    if err := batch.Write(); err != nil {
        return err
    }

    log.Printf("Sync complete: %d total records", count)
    return nil
}
```

### Step 3: Setup Dual-Write Proxy

```go
// proxy.go - Dual-write proxy for live migration
package main

type DualWriteProxy struct {
    primary   Database  // Old version (serving reads)
    secondary Database  // New version (building up)
    mu        sync.RWMutex
}

func (p *DualWriteProxy) Put(key, value []byte) error {
    p.mu.Lock()
    defer p.mu.Unlock()

    // Write to both databases
    if err := p.primary.Put(key, value); err != nil {
        return err
    }

    // Async write to secondary
    go func() {
        if err := p.secondary.Put(key, value); err != nil {
            log.Printf("Secondary write failed: %v", err)
        }
    }()

    return nil
}

func (p *DualWriteProxy) Get(key []byte) ([]byte, error) {
    p.mu.RLock()
    defer p.mu.RUnlock()

    // Read from primary only
    return p.primary.Get(key)
}
```

### Step 4: Verification

```bash
#!/bin/bash
# verify.sh - Verify data consistency

echo "Comparing databases..."

# Compare key counts
OLD_COUNT=$(./tools/keycount.sh /data/blockchaindb-v1)
NEW_COUNT=$(./tools/keycount.sh /data/blockchaindb-v2)

if [ "$OLD_COUNT" != "$NEW_COUNT" ]; then
    echo "ERROR: Key count mismatch: $OLD_COUNT vs $NEW_COUNT"
    exit 1
fi

# Sample verification (1% of keys)
./tools/sample-compare.go \
    -old /data/blockchaindb-v1 \
    -new /data/blockchaindb-v2 \
    -sample 0.01

echo "Verification complete"
```

### Step 5: Traffic Cutover

```nginx
# nginx.conf - Load balancer configuration
upstream blockchaindb {
    # Start with old version
    server old-db:8080 weight=100;
    server new-db:8080 weight=0;
}

# Gradual migration (update weights)
# Phase 1: 95/5
# Phase 2: 75/25
# Phase 3: 50/50
# Phase 4: 25/75
# Phase 5: 0/100
```

```bash
# Monitor during cutover
watch -n 1 'curl -s http://localhost:8080/metrics | grep -E "(errors|latency|throughput)"'
```

### Step 6: Cleanup

```bash
# After successful migration
# 1. Stop old database
systemctl stop blockchaindb-v1

# 2. Archive old data
tar -czf archive-v1-$(date +%Y%m%d).tar.gz /data/blockchaindb-v1/

# 3. Remove old deployment
rm -rf /opt/blockchaindb-v1/

# 4. Update systemd
systemctl disable blockchaindb-v1
systemctl enable blockchaindb-v2
```

---

## In-Place Migration (With Downtime)

### Step 1: Stop Service

```bash
# Stop application
systemctl stop blockchaindb

# Verify stopped
ps aux | grep blockchaindb
```

### Step 2: Backup Data

```bash
# Create backup
cp -r /data/blockchaindb /data/blockchaindb.backup

# Verify backup
du -sh /data/blockchaindb.backup
```

### Step 3: Run Migration Tool

```go
// migrate.go - In-place migration tool
package main

import (
    "github.com/BlockchainDB/database/migration"
)

func main() {
    migrator := migration.New(migration.Config{
        DataPath:        "/data/blockchaindb",
        EnableCompression: true,
        RebuildIndexes:   true,
        OptimizeStorage:  true,
    })

    // Run migration phases
    phases := []migration.Phase{
        migration.PhaseBackup,
        migration.PhaseReindex,
        migration.PhaseCompress,
        migration.PhaseOptimize,
        migration.PhaseVerify,
    }

    for _, phase := range phases {
        log.Printf("Running phase: %s", phase)
        if err := migrator.Run(phase); err != nil {
            log.Fatalf("Migration failed at %s: %v", phase, err)
        }
    }

    log.Println("Migration complete")
}
```

### Step 4: Update Configuration

```yaml
# /etc/blockchaindb/config.yml
version: 2.0

performance:
  buffer_pooling: true
  binary_search: true
  compression:
    enabled: true
    algorithm: snappy
    level: 6

cache:
  type: lru
  size: 1GB
  ttl: 3600

sharding:
  enabled: true
  count: 512
  locking: per-shard

monitoring:
  enabled: true
  port: 9090
```

### Step 5: Start New Version

```bash
# Install new binary
cp /tmp/blockchaindb-v2 /usr/local/bin/blockchaindb

# Start service
systemctl start blockchaindb

# Verify
systemctl status blockchaindb
curl http://localhost:8080/health
```

---

## Rolling Migration (Sharded Clusters)

### Step 1: Identify Shard Groups

```bash
# List all shards
./tools/list-shards.sh

# Group shards by node
SHARDS_NODE1=(0 1 2 3 128 129 130 131)
SHARDS_NODE2=(4 5 6 7 132 133 134 135)
# ... etc
```

### Step 2: Migrate One Shard Group

```bash
#!/bin/bash
# migrate-shard.sh

SHARD_ID=$1
OLD_PATH="/data/shards/shard-$SHARD_ID"
NEW_PATH="/data/shards-v2/shard-$SHARD_ID"

# Stop shard
./tools/stop-shard.sh $SHARD_ID

# Migrate data
./tools/migrate-shard \
    -input $OLD_PATH \
    -output $NEW_PATH \
    -compress true \
    -optimize true

# Start new version
./tools/start-shard-v2.sh $SHARD_ID

# Verify
./tools/verify-shard.sh $SHARD_ID
```

### Step 3: Progressive Rollout

```python
# rollout.py - Orchestrate rolling migration
import time
import subprocess

def migrate_shard_group(shards):
    """Migrate a group of shards"""
    for shard_id in shards:
        print(f"Migrating shard {shard_id}")
        result = subprocess.run(
            ["./migrate-shard.sh", str(shard_id)],
            capture_output=True
        )

        if result.returncode != 0:
            print(f"Failed to migrate shard {shard_id}")
            return False

        # Wait for stabilization
        time.sleep(30)

    return True

# Migrate in waves
waves = [
    [0, 1, 2, 3],      # Wave 1: 1%
    [4, 5, 6, 7],      # Wave 2: 2%
    list(range(8, 64)), # Wave 3: 12.5%
    # ... etc
]

for i, wave in enumerate(waves):
    print(f"Starting wave {i+1}")
    if not migrate_shard_group(wave):
        print("Migration failed, rolling back")
        rollback(wave)
        break

    # Monitor for issues
    time.sleep(300)
    if not health_check():
        rollback(wave)
        break
```

---

## Data Format Changes

### Key Format (Unchanged)
```
Old: [32]byte hash
New: [32]byte hash (same)
```

### Value Format Changes
```
Old Format:
[Length: 8 bytes][Data: variable]

New Format:
[Flags: 1 byte][Length: 8 bytes][Data: variable]

Flags:
- Bit 0: Compressed
- Bit 1: Encrypted
- Bits 2-7: Reserved
```

### Index Format Changes
```
Old: Linear array of offsets
New: B-tree structure with:
  - Node type (leaf/internal)
  - Key count
  - Child pointers
  - Key-value pairs
```

---

## Rollback Procedures

### Quick Rollback (Blue-Green)

```bash
# Switch load balancer back
curl -X POST http://loadbalancer/switch?target=old

# Stop new version
systemctl stop blockchaindb-v2

# Verify old version is serving
curl http://old-db:8080/health
```

### Full Rollback (In-Place)

```bash
# Stop new version
systemctl stop blockchaindb

# Restore backup
rm -rf /data/blockchaindb
mv /data/blockchaindb.backup /data/blockchaindb

# Install old binary
cp /backup/blockchaindb-v1 /usr/local/bin/blockchaindb

# Start old version
systemctl start blockchaindb
```

---

## Troubleshooting

### Common Issues

#### Issue: "too many open files"
```bash
# Solution: Increase file descriptor limits
ulimit -n 65536
echo "* soft nofile 65536" >> /etc/security/limits.conf
echo "* hard nofile 65536" >> /etc/security/limits.conf
```

#### Issue: Migration hangs
```bash
# Check progress
tail -f /var/log/blockchaindb/migration.log

# Check disk space
df -h

# Check CPU/Memory
top -p $(pgrep blockchaindb)
```

#### Issue: Data corruption detected
```bash
# Run integrity check
./tools/integrity-check.go -db /path/to/db -fix

# Restore from backup if needed
./restore-backup.sh
```

#### Issue: Performance degradation after migration
```bash
# Rebuild indexes
./tools/rebuild-index.sh

# Clear cache
echo 3 > /proc/sys/vm/drop_caches

# Analyze query patterns
./tools/profile.sh -duration 60s
```

---

## Performance Validation

### Benchmark Comparison

```bash
#!/bin/bash
# benchmark.sh - Compare old vs new performance

echo "Benchmarking old version..."
./benchmark-tool \
    -db /data/blockchaindb-v1 \
    -operations 100000 \
    -concurrency 100 \
    > benchmark-old.txt

echo "Benchmarking new version..."
./benchmark-tool \
    -db /data/blockchaindb-v2 \
    -operations 100000 \
    -concurrency 100 \
    > benchmark-new.txt

# Compare results
./tools/compare-benchmarks.py benchmark-old.txt benchmark-new.txt
```

### Expected Improvements

| Metric | Old Version | New Version | Improvement |
|--------|------------|-------------|-------------|
| Get Latency (p99) | 120ms | 2ms | 60x |
| Put Latency (p99) | 200ms | 5ms | 40x |
| Throughput | 12K ops/s | 200K ops/s | 16x |
| Storage Size | 100GB | 25GB | 4x |
| Memory Usage | 8GB | 2GB | 4x |
| Startup Time | 5 minutes | 10 seconds | 30x |

---

## Post-Migration Tasks

### 1. Update Documentation
```bash
# Update README
sed -i 's/v1.0/v2.0/g' README.md

# Update API docs
./generate-docs.sh
```

### 2. Update Monitoring
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'blockchaindb'
    static_configs:
      - targets: ['localhost:9090']
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: 'blockchaindb_v2_.*'
        action: keep
```

### 3. Update Backup Scripts
```bash
# backup.sh
#!/bin/bash
# Updated for v2 format

DB_PATH="/data/blockchaindb-v2"
BACKUP_PATH="/backup/$(date +%Y%m%d-%H%M%S)"

# Use new backup tool with compression
./blockchaindb-backup \
    -source $DB_PATH \
    -dest $BACKUP_PATH \
    -compress \
    -verify
```

### 4. Train Team
- Conduct training session on new features
- Update runbooks
- Review new monitoring dashboards
- Practice rollback procedures

---

## Support

### Resources
- Migration Hotline: +1-555-DB-HELP
- Slack Channel: #blockchaindb-migration
- Documentation: https://docs.blockchaindb.io/migration
- Issue Tracker: https://github.com/BlockchainDB/issues

### Emergency Contacts
- On-call Engineer: Use PagerDuty
- Escalation: engineering-leads@blockchaindb.io

---

*Migration Guide Version: 2.0*
*Last Updated: 2025-01-23*
*Next Review: Post-migration retrospective*