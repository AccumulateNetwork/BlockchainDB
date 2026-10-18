# AI Development Rules - BlockchainDB

## STEP 1: COMMAND OUTPUT HANDLING

### CRITICAL: Redirect All Output
```bash
# Long-running tests - ALWAYS redirect
go test ./... > /tmp/test.log 2>&1
tail -20 /tmp/test.log

# Benchmarks - ALWAYS redirect
go test -bench=. ./database/dkv/ > /tmp/bench.log 2>&1 &
tail -f /tmp/bench.log

# Build output
go build ./... > /tmp/build.log 2>&1
```

### Why This Matters
- Verbose output crashes AI context
- Long-running commands block interaction
- Use `tail` to check results, not streaming

## STEP 2: FILE ORGANIZATION

### Naming Standards (ENFORCED)
- **NEVER** ALL_CAPS_FILE_NAMES
- **ALWAYS** lowercase-with-hyphens
- Tests: `*_test.go` colocated with implementation

### Directory Structure
```
database/
├── kv/          # Primary KV implementations (KVShard, KV, DynaKV)
├── dkv/         # Dynamic KV with LSM-tree architecture
├── storage/     # Low-level file I/O (BFile, KFile)
├── history/     # Historical state tracking
├── utils/       # Bloom filters, key utilities
├── tests/       # Integration tests
└── experimental/ # Alternative implementations (not production)
```

## STEP 3: DEVELOPMENT WORKFLOW

### Before Making Changes
1. **Read existing code** before proposing modifications
2. **Understand the architecture**: KVShard (1024 shards), 20-byte internal keys
3. **Check existing tests** in the target package

### Testing Process
1. Run targeted tests first: `go test ./database/kv/... > /tmp/kv.log 2>&1`
2. Check for failures: `grep -i fail /tmp/kv.log`
3. Run full suite only when needed

### Pre-Commit Checklist
- [ ] `go vet ./...` passes
- [ ] `go build ./...` succeeds
- [ ] Relevant tests pass
- [ ] No experimental code in production paths

## STEP 4: KEY TECHNICAL DETAILS

### Key Sizes
- External API: `[32]byte` keys
- Internal storage: `[20]byte` (truncated for efficiency)
- Do NOT change this without understanding collision implications

### Performance-Critical Paths
- `PutPermAsync` - fire-and-forget writes (hot path)
- `PutPerm` - synchronous writes
- Always call `Flush()` before `Close()` on KVShard

### Benchmarking
```bash
# 100M entry benchmark (long-running)
go test -run '^TestBDB_100M$' -v -timeout 2h ./database/dkv/ > /tmp/100m.log 2>&1 &

# Quick performance check
go test -run '^TestBDB_10M$' -v -timeout 10m ./database/dkv/ > /tmp/10m.log 2>&1
```

## STEP 5: DOCUMENTATION

### Key Docs
- `docs/kv-architecture.md` - KV layer design
- `docs/dkv/` - DKV architecture and API
- `docs/performance/` - Benchmark results

### Updating Docs
- Update CHANGELOG.md for significant changes
- Keep docs in `docs/` directory (not root)
- Technical facts only, no verbose language

## CRITICAL: AI MUST FOLLOW THIS ORDER
1. **Redirect output** - never stream verbose commands
2. **Read before writing** - understand existing code
3. **Test incrementally** - targeted tests first
4. **Check lint** - `go vet ./...` before commits
