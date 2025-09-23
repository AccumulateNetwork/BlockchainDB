# Local Monitoring Setup for BlockchainDB

## Quick Start

1. **Start the monitoring stack:**
   ```bash
   docker-compose up -d
   ```

2. **Access the dashboards:**
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000 (admin/admin)

3. **Stop the stack:**
   ```bash
   docker-compose down
   ```

## Integrating Metrics into Your Code

Add Prometheus client to your project:
```bash
go get github.com/prometheus/client_golang/prometheus
go get github.com/prometheus/client_golang/prometheus/promhttp
```

Use the instrumented wrapper in your code:
```go
package main

import (
    "your-module/blockchainDB"
)

func main() {
    // Start metrics server
    blockchainDB.StartMetricsServer(":2112")

    // Open database with instrumentation
    db, _ := blockchainDB.OpenKV2("./data")
    instrumentedDB := &blockchainDB.InstrumentedKV2{KV2: db}

    // Use instrumentedDB - metrics collected automatically
    instrumentedDB.Put(key, value)
    instrumentedDB.Get(key)
}
```

## Available Metrics

- `blockchaindb_operations_total` - Counter of all operations
- `blockchaindb_operation_duration_seconds` - Operation latency histogram
- `blockchaindb_storage_bytes` - Database size by type
- `blockchaindb_keys_total` - Total number of keys
- `blockchaindb_cache_hits_total` - Cache hit counter
- `blockchaindb_cache_misses_total` - Cache miss counter
- `blockchaindb_shards_total` - Number of shards
- `blockchaindb_shard_keys` - Keys per shard

## Grafana Dashboard

Pre-configured dashboard includes:
- Operations rate graph
- P95 latency gauge
- Storage size over time
- Cache hit rate
- Total keys trend
- Shard distribution

## Testing Your Metrics

1. Run your application with metrics enabled
2. Check raw metrics: http://localhost:2112/metrics
3. Query in Prometheus: http://localhost:9090
4. View dashboard in Grafana: http://localhost:3000

## Troubleshooting

- **Can't connect to metrics:** Ensure your app is running on port 2112
- **No data in Grafana:** Check Prometheus targets at http://localhost:9090/targets
- **Docker issues:** Use `docker-compose logs prometheus` or `docker-compose logs grafana`

## Resource Usage (Local)

- Prometheus: ~100MB RAM
- Grafana: ~150MB RAM
- Total disk: <1GB for typical testing