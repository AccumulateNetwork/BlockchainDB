package blockchainDB

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// KeyPipeline is a high-performance, reusable key generation pipeline
// that maintains a constant depth of pre-generated batches for optimal throughput
type KeyPipeline struct {
	// Configuration
	BatchSize     int           // Number of keys per batch
	PipelineDepth int           // Number of batches to keep in-flight
	TotalBatches  int           // Total number of batches to generate
	SortForHistory bool         // Whether to sort batches for HistoryFile

	// Channels
	outputChan     chan *PreparedBatch  // Output channel for completed batches
	generateSignal chan struct{}        // Signal to generate new batches
	stopSignal     chan struct{}        // Signal to stop generation

	// Statistics
	batchesGenerated atomic.Int64
	keysGenerated    atomic.Int64
	nextBatchID      atomic.Int64
	startTime        time.Time

	// Wait group for goroutines
	wg sync.WaitGroup
}

// PreparedBatch represents a batch of keys ready for database insertion
type PreparedBatch struct {
	BatchID    int           // Unique batch identifier
	Keys       [][32]byte    // Generated keys
	SortedKeys []DBBKeyFull  // Keys sorted for HistoryFile (if applicable)
	Buffer     []byte        // Pre-built byte buffer for direct writing
	GenTime    time.Duration // Time taken to generate this batch
	PrepTime   time.Duration // Time taken to prepare/sort this batch
}

// NewKeyPipeline creates a new key generation pipeline
func NewKeyPipeline(totalKeys, batchSize, pipelineDepth int) *KeyPipeline {
	numBatches := totalKeys / batchSize
	if totalKeys%batchSize != 0 {
		numBatches++
	}

	return &KeyPipeline{
		BatchSize:      batchSize,
		PipelineDepth:  pipelineDepth,
		TotalBatches:   numBatches,
		outputChan:     make(chan *PreparedBatch, pipelineDepth),
		generateSignal: make(chan struct{}, pipelineDepth),
		stopSignal:     make(chan struct{}),
	}
}

// Start begins the pipeline with the specified number of generator workers
func (kp *KeyPipeline) Start() {
	kp.startTime = time.Now()

	// Preload the pipeline
	initialBatches := kp.PipelineDepth
	if initialBatches > kp.TotalBatches {
		initialBatches = kp.TotalBatches
	}

	// Start generator workers
	for w := 0; w < kp.PipelineDepth; w++ {
		kp.wg.Add(1)
		go kp.generatorWorker(w)
	}

	// Trigger initial batch generation
	for i := 0; i < initialBatches; i++ {
		select {
		case kp.generateSignal <- struct{}{}:
		default:
		}
	}
}

// Stop gracefully shuts down the pipeline
func (kp *KeyPipeline) Stop() {
	close(kp.stopSignal)
	close(kp.generateSignal)
	kp.wg.Wait()
	close(kp.outputChan)
}

// GetBatch retrieves the next batch from the pipeline and triggers generation of a replacement
func (kp *KeyPipeline) GetBatch() (*PreparedBatch, bool) {
	batch, ok := <-kp.outputChan
	if !ok {
		return nil, false
	}

	// Trigger generation of a replacement batch if we haven't generated all batches yet
	if kp.nextBatchID.Load() < int64(kp.TotalBatches) {
		select {
		case kp.generateSignal <- struct{}{}:
		default:
		}
	}

	return batch, true
}

// GetBatchWithTimeout retrieves a batch with a timeout
func (kp *KeyPipeline) GetBatchWithTimeout(timeout time.Duration) (*PreparedBatch, bool) {
	select {
	case batch, ok := <-kp.outputChan:
		if ok {
			// Trigger replacement generation
			if kp.nextBatchID.Load() < int64(kp.TotalBatches) {
				select {
				case kp.generateSignal <- struct{}{}:
				default:
				}
			}
		}
		return batch, ok
	case <-time.After(timeout):
		return nil, false
	}
}

// Stats returns current pipeline statistics
func (kp *KeyPipeline) Stats() PipelineStats {
	return PipelineStats{
		BatchesGenerated: kp.batchesGenerated.Load(),
		KeysGenerated:    kp.keysGenerated.Load(),
		QueueDepth:       len(kp.outputChan),
		ElapsedTime:      time.Since(kp.startTime),
		KeysPerSecond:    float64(kp.keysGenerated.Load()) / time.Since(kp.startTime).Seconds(),
	}
}

// PipelineStats contains runtime statistics for the pipeline
type PipelineStats struct {
	BatchesGenerated int64
	KeysGenerated    int64
	QueueDepth       int
	ElapsedTime      time.Duration
	KeysPerSecond    float64
}

// generatorWorker is the worker goroutine that generates batches
func (kp *KeyPipeline) generatorWorker(workerID int) {
	defer kp.wg.Done()

	for {
		select {
		case <-kp.stopSignal:
			return
		case <-kp.generateSignal:
			// Check if we've generated all batches
			batchID := int(kp.nextBatchID.Add(1)) - 1
			if batchID >= kp.TotalBatches {
				return
			}

			// Generate the batch
			batch := kp.generateBatch(batchID)
			if batch != nil {
				kp.batchesGenerated.Add(1)
				kp.keysGenerated.Add(int64(len(batch.Keys)))

				// Send to output channel
				select {
				case kp.outputChan <- batch:
				case <-kp.stopSignal:
					return
				}
			}
		}
	}
}

// generateBatch generates a single batch of keys
func (kp *KeyPipeline) generateBatch(batchID int) *PreparedBatch {
	genStart := time.Now()

	// Create deterministic seed from batch ID
	seed := []byte{
		byte(batchID),
		byte(batchID >> 8),
		byte(batchID >> 16),
		byte(batchID >> 24),
	}
	fr := NewFastRandom(seed)

	// Determine actual batch size (last batch might be smaller)
	actualBatchSize := kp.BatchSize
	if batchID == kp.TotalBatches-1 {
		remaining := (kp.TotalBatches * kp.BatchSize) - (batchID * kp.BatchSize)
		if remaining < kp.BatchSize {
			actualBatchSize = remaining
		}
	}

	// Generate keys
	batch := &PreparedBatch{
		BatchID: batchID,
		Keys:    make([][32]byte, actualBatchSize),
	}

	for i := 0; i < actualBatchSize; i++ {
		batch.Keys[i] = fr.NextHash()
	}

	batch.GenTime = time.Since(genStart)

	// Prepare for HistoryFile if requested
	if kp.SortForHistory {
		prepStart := time.Now()
		batch.prepareSortedBuffer()
		batch.PrepTime = time.Since(prepStart)
	}

	return batch
}

// prepareSortedBuffer prepares a sorted buffer for HistoryFile
func (pb *PreparedBatch) prepareSortedBuffer() {
	// Create DBBKeyFull entries
	pb.SortedKeys = make([]DBBKeyFull, len(pb.Keys))
	for i, key := range pb.Keys {
		pb.SortedKeys[i] = DBBKeyFull{
			Key: key,
			DBBKey: DBBKey{
				Offset: uint64(pb.BatchID*len(pb.Keys)+i) * 1024,
				Length: uint64(256), // Default length
			},
		}
	}

	// Build buffer (unsorted for now - caller must sort by bin)
	pb.Buffer = make([]byte, 0, len(pb.Keys)*DBKeyFullSize)
	for _, dbKey := range pb.SortedKeys {
		pb.Buffer = append(pb.Buffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
	}
}

// SortForHistoryFile sorts the batch keys by HistoryFile bin index
func (pb *PreparedBatch) SortForHistoryFile(hf *HistoryFile) {
	if pb.SortedKeys == nil {
		pb.prepareSortedBuffer()
	}

	// Sort by HistoryFile index (bin)
	sort.Slice(pb.SortedKeys, func(i, j int) bool {
		idxI := hf.Index(pb.SortedKeys[i].Key)
		idxJ := hf.Index(pb.SortedKeys[j].Key)
		if idxI != idxJ {
			return idxI < idxJ
		}
		// Within same bin, sort by key
		return string(pb.SortedKeys[i].Key[:]) < string(pb.SortedKeys[j].Key[:])
	})

	// Rebuild buffer in sorted order
	pb.Buffer = make([]byte, 0, len(pb.SortedKeys)*DBKeyFullSize)
	for _, dbKey := range pb.SortedKeys {
		pb.Buffer = append(pb.Buffer, dbKey.DBBKey.Bytes(dbKey.Key)...)
	}
}

// PrintStats prints formatted statistics
func (ps PipelineStats) String() string {
	return fmt.Sprintf("Batches: %d, Keys: %d, Queue: %d, Rate: %.0f keys/sec",
		ps.BatchesGenerated, ps.KeysGenerated, ps.QueueDepth, ps.KeysPerSecond)
}

// KeyPipelineConfig allows customization of pipeline behavior
type KeyPipelineConfig struct {
	TotalKeys      int
	BatchSize      int
	PipelineDepth  int
	SortForHistory bool
	CustomOffsets  bool // Use custom offset/length calculation
	OffsetFunc     func(batchID, keyIndex int) uint64
	LengthFunc     func(batchID, keyIndex int) uint64
}

// NewKeyPipelineWithConfig creates a pipeline with custom configuration
func NewKeyPipelineWithConfig(config KeyPipelineConfig) *KeyPipeline {
	kp := NewKeyPipeline(config.TotalKeys, config.BatchSize, config.PipelineDepth)
	kp.SortForHistory = config.SortForHistory

	// Store config for custom offset/length functions if needed
	// This would require adding config storage to KeyPipeline struct

	return kp
}