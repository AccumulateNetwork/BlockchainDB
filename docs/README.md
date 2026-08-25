# BlockchainDB Documentation

Welcome to the BlockchainDB documentation. This guide provides comprehensive information about the BlockchainDB project, its components, usage, and API reference.

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Core Components](#core-components)
4. [Usage Examples](#usage-examples)
5. [API Reference](#api-reference)
6. [Performance Considerations](#performance-considerations)
7. [Testing](#testing)

## Introduction

BlockchainDB is a specialized key-value database designed for blockchain applications. It provides efficient storage and retrieval of data with features specifically tailored for blockchain use cases.

## Installation

BlockchainDB is written in Go and can be installed using Go modules:

```bash
go get github.com/AccumulateNetwork/BlockchainDB
```

## Core Components

BlockchainDB consists of several core components:

- **`KVShard`** - the main database interface: a sharded store of `KV2`
  instances, each with an immutable (Perm) and a mutable (Dyna) layer
- **`SegmentStore`** - the storage layer both layers are built on:
  sealed, hashed segments that double as the sync transport.  See
  [Segments as storage](design/segment-store.md)
- [Buffered File (BFile)](components/bfile.md) - Efficient file I/O operations
- [Bloom Filter](components/bloom.md) - Efficient membership testing

The design notes in [design/](design/) are the current description of
how the database works:

- [Segments as storage](design/segment-store.md)
- [Block segmentation](design/block-segmentation.md)
- [Crash durability](design/durability.md)
- [Multi-core ingest](design/multicore-ingest.md)

## Usage Examples

See the [examples](examples/README.md) directory for code samples demonstrating how to use BlockchainDB.

## API Reference

Detailed API documentation for each component can be found in the [API Reference](api/README.md) section.

## Performance Considerations

For information about optimizing performance and understanding the trade-offs in different usage scenarios, see the [Performance Guide](performance.md).

## Testing

For information about the testing approach used in BlockchainDB, including what works well and areas for improvement, see the [Testing Documentation](testing.md).
