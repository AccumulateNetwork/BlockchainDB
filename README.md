# BlockchainDB

A high-performance key-value database optimized for blockchain applications, written in Go.

## Overview

BlockchainDB is a specialized database designed for the unique requirements of blockchain systems. It provides efficient storage and retrieval of key-value pairs with features tailored for immutable, append-only data structures common in blockchain implementations.

## Recent Updates (2025-01-25)

### Major Reorganization Completed ✅
- Restructured codebase from flat directory to proper Go package hierarchy
- Eliminated circular dependencies between packages
- Improved code organization with clear separation of concerns
- Fixed critical O(n) performance bug, achieving 100-1000x speedup
- Consolidated KV implementations, removing experimental code

### Performance Improvements
- **Write Performance**: Now achieving 1.73M keys/sec (previously ~120K/sec)
- **Read Performance**: Now achieving 1.88M keys/sec
- **Key Innovation**: Hybrid sorted/unsorted approach eliminates O(n²) degradation

## Features

- **High Performance**: 1.7M+ writes/sec, 1.8M+ reads/sec with no degradation
- **Append-Only Design**: Built for blockchain's immutable data patterns
- **Hybrid Storage**: Combines sorted and unsorted structures for optimal performance
- **History Management**: Efficient storage and retrieval of historical states
- **Bloom Filters**: Fast membership testing to reduce unnecessary disk reads (0% false positive rate)
- **Content-Addressed Storage**: Hash-based addressing for immutable data

## Project Structure

```
BlockchainDB/
├── database/           # Core database implementation (organized by layers)
│   ├── kv/            # Key-Value Store Layer
│   │   ├── kv.go                 # Main KV store interface
│   │   ├── view_kv.go            # View-based KV operations
│   │   └── *_test.go             # KV unit tests
│   │
│   ├── storage/       # Storage Layer (low-level file operations)
│   │   ├── bfile.go              # Buffered file I/O
│   │   ├── kfile.go              # Key file management
│   │   ├── kfile_header.go       # Key file headers
│   │   └── *_test.go             # Storage unit tests
│   │
│   ├── history/       # History/Versioning Layer
│   │   ├── history_file.go       # History tracking implementation
│   │   └── *_test.go             # History unit tests
│   │
│   ├── utils/         # Utility Components
│   │   ├── bloom.go              # Bloom filter implementation
│   │   ├── fastrandom.go         # Fast random number generation
│   │   ├── keys.go               # Key utilities
│   │   ├── key_pipeline.go       # Key generation pipeline
│   │   └── *_test.go             # Utility unit tests
│   │
│   ├── benchmarks/    # Performance benchmarks
│   ├── demos/         # Demo and example tests
│   ├── integration/   # Integration tests
│   └── stress/        # Stress and parallel tests
│
├── experimental/       # Alternative implementations and experiments
│   ├── storage/        # Alternative storage engines (binary tree, hash tables)
│   ├── history/        # History management experiments
│   └── kv/            # Key-value store variations
│
├── docs/              # Documentation
│   ├── components/    # Component-specific documentation
│   ├── design/        # Design documents and proposals
│   ├── performance/   # Performance analysis and benchmarks
│   └── releases/      # Release notes and changelogs
│
└── tools/             # Development tools
    └── profiling/     # Performance profiling outputs
```

## Installation

```bash
go get github.com/AccumulateNetwork/BlockchainDB
```

## Quick Start

```go
package main

import (
    "github.com/AccumulateNetwork/BlockchainDB/database"
)

func main() {
    // Create a new key-value store
    kv := blockchainDB.NewKV()

    // Store a key-value pair
    kv.Put([]byte("key"), []byte("value"))

    // Retrieve a value
    value, err := kv.Get([]byte("key"))
    if err != nil {
        // Handle error
    }
}
```

## Testing

Run all tests:
```bash
go test ./...
```

Run benchmarks:
```bash
go test -bench=. ./database
```

Run specific test categories:
```bash
# Unit tests only
go test -run "Test[^Parallel|^Stress|^Bench]" ./database

# Parallel/stress tests
go test -run "TestParallel|TestStress" ./database

# Benchmarks
go test -bench=. -run=^$ ./database
```

## Documentation

Comprehensive documentation is available in the [docs/](docs/) directory:

- [Component Documentation](docs/components/) - Detailed component descriptions
- [Design Documents](docs/design/) - Architecture and design decisions
- [Performance Analysis](docs/performance/) - Benchmarks and optimization guides
- [API Reference](docs/README.md) - Complete API documentation

## Development

### Experimental Features

The `experimental/` directory contains alternative implementations and experimental features that are being evaluated for potential inclusion in the core database. These implementations may have different performance characteristics or trade-offs.

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Organization

- **Core Implementation**: Keep core database functionality in `database/`
- **Tests**: Co-locate tests with their implementation files
- **Experiments**: Place experimental code in `experimental/` with appropriate subdirectories
- **Documentation**: Update relevant docs in `docs/` when making changes

## Performance

BlockchainDB is designed for high-performance blockchain applications. Key performance characteristics:

- **Write Performance**: Optimized for sequential append operations
- **Read Performance**: Efficient random access with caching
- **Memory Usage**: Configurable memory limits with smart eviction
- **Disk I/O**: Minimized through buffering and batch operations

See [Performance Documentation](docs/performance/) for detailed benchmarks and optimization guides.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/AccumulateNetwork/BlockchainDB).