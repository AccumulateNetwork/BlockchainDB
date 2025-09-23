package blockchainDB

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Operation metrics
	opsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blockchaindb_operations_total",
			Help: "Total number of operations processed",
		},
		[]string{"operation", "status"},
	)

	operationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "blockchaindb_operation_duration_seconds",
			Help:    "Duration of operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
		},
		[]string{"operation"},
	)

	// Storage metrics
	dbSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "blockchaindb_storage_bytes",
			Help: "Current database size in bytes",
		},
		[]string{"type"},
	)

	keysStored = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "blockchaindb_keys_total",
			Help: "Total number of keys stored",
		},
	)

	// Cache metrics
	cacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockchaindb_cache_hits_total",
			Help: "Total number of cache hits",
		},
	)

	cacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "blockchaindb_cache_misses_total",
			Help: "Total number of cache misses",
		},
	)

	// Shard metrics
	shardCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "blockchaindb_shards_total",
			Help: "Number of active shards",
		},
	)

	shardBalance = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "blockchaindb_shard_keys",
			Help: "Number of keys per shard",
		},
		[]string{"shard"},
	)
)

// InstrumentedKV2 wraps KV2 with metrics
type InstrumentedKV2 struct {
	*KV2
}

// Put with metrics
func (i *InstrumentedKV2) Put(key [32]byte, value []byte) (int64, error) {
	timer := prometheus.NewTimer(operationDuration.WithLabelValues("put"))
	defer timer.ObserveDuration()

	hash, err := i.KV2.Put(key, value)

	if err != nil {
		opsProcessed.WithLabelValues("put", "error").Inc()
	} else {
		opsProcessed.WithLabelValues("put", "success").Inc()
	}

	return hash, err
}

// Get with metrics
func (i *InstrumentedKV2) Get(key [32]byte) ([]byte, error) {
	timer := prometheus.NewTimer(operationDuration.WithLabelValues("get"))
	defer timer.ObserveDuration()

	value, err := i.KV2.Get(key)

	if err != nil {
		opsProcessed.WithLabelValues("get", "error").Inc()
		cacheMisses.Inc()
	} else {
		opsProcessed.WithLabelValues("get", "success").Inc()
		cacheHits.Inc()
	}

	return value, err
}

// StartMetricsServer starts the Prometheus metrics endpoint
func StartMetricsServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())

	// Health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			panic(err)
		}
	}()
}

// UpdateStorageMetrics updates storage-related metrics periodically
func UpdateStorageMetrics(db *KV2) {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for range ticker.C {
			// Update metrics based on actual DB state
			// These are examples - implement based on your actual methods

			// keysStored.Set(float64(db.GetKeyCount()))
			// dbSize.WithLabelValues("permanent").Set(float64(db.GetPermanentSize()))
			// dbSize.WithLabelValues("dynamic").Set(float64(db.GetDynamicSize()))
			// shardCount.Set(float64(db.GetShardCount()))
		}
	}()
}