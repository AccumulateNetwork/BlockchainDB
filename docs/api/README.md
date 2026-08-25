# BlockchainDB API Reference

This section provides detailed API documentation for the main components of BlockchainDB.

## Component APIs

- [BFile (Buffered File)](#bfile-buffered-file)
- [BloomFilter](#bloomfilter)

This reference covers the low-level file and filter primitives only.
The database's own API -- `KVShard`, `KV2`, and `SegmentStore` -- is
described in the design notes, starting with
[Segments as storage](../design/segment-store.md).  (The `KV`, `KFile`,
and `HistoryFile` sections that used to be here documented the v1
storage layer, which has been removed.)

## BFile (Buffered File)

### Types

```go
type BFile struct {
    File     *os.File
    Filename string
    Buffer   [BufferSize]byte
    EOB      uint64
    EOD      uint64
}
```

### Functions

#### NewBFile

```go
func NewBFile(filename string) (file *BFile, err error)
```

Creates a new BFile.

**Parameters:**
- `filename` - Path to the file

**Returns:**
- `file` - The new BFile instance
- `err` - Error, if any

#### OpenBFile

```go
func OpenBFile(filename string) (bFile *BFile, err error)
```

Opens an existing BFile.

**Parameters:**
- `filename` - Path to the file

**Returns:**
- `bFile` - The opened BFile instance
- `err` - Error, if any

### Methods

#### Write

```go
func (b *BFile) Write(Data []byte) (update bool, err error)
```

Writes data to the file.

**Parameters:**
- `Data` - Data to write

**Returns:**
- `update` - Whether an actual file update occurred
- `err` - Error, if any

#### ReadAt

```go
func (b *BFile) ReadAt(offset uint64, data []byte) (err error)
```

Reads data from a specific offset.

**Parameters:**
- `offset` - Offset to read from
- `data` - Buffer to read into

**Returns:**
- `err` - Error, if any

#### Flush

```go
func (b *BFile) Flush() (err error)
```

Flushes the buffer to disk.

**Returns:**
- `err` - Error, if any

#### Close

```go
func (b *BFile) Close() (err error)
```

Closes the file.

**Returns:**
- `err` - Error, if any

#### Offset

```go
func (b *BFile) Offset() (offset uint64, err error)
```

Returns the current real size of the BFile.

**Returns:**
- `offset` - Current offset
- `err` - Error, if any

## BloomFilter

### Types

```go
type BloomFilter struct {
    Filter []byte
    K      int
}
```

### Functions

#### NewBloomFilter

```go
func NewBloomFilter(size uint64, k int) *BloomFilter
```

Creates a new BloomFilter.

**Parameters:**
- `size` - Size of the filter
- `k` - Number of hash functions

**Returns:**
- BloomFilter instance

### Methods

#### Add

```go
func (b *BloomFilter) Add(data []byte)
```

Adds an element to the filter.

**Parameters:**
- `data` - Data to add

#### Test

```go
func (b *BloomFilter) Test(data []byte) bool
```

Tests if an element might be in the set.

**Parameters:**
- `data` - Data to test

**Returns:**
- Whether the element might be in the set
