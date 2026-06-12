# mythdb

An LSM-tree storage engine written in Go — a from-scratch implementation of the
[mini-lsm](https://skyzh.github.io/mini-lsm/) course. It is a full multi-version
key-value engine: crash-recoverable, self-compacting, and supporting serializable
transactions. **Standard library only, no third-party dependencies.**

## Features

- **Log-structured storage** — writes go to an in-memory skiplist memtable, freeze,
  and flush to immutable on-disk SSTs (Sorted String Tables) with bloom filters.
- **Compaction** — full and leveled strategies, run by a background goroutine, that
  merge SSTs across levels and reclaim space.
- **Durability & crash recovery** — a per-memtable write-ahead log and an append-only
  manifest let `Open` rebuild the exact engine state after a crash. WAL, manifest,
  and SST metadata are CRC-checked.
- **MVCC** — every write is tagged with a monotonic commit timestamp; reads see a
  consistent snapshot at a timestamp.
- **Transactions** — snapshot isolation with optimistic concurrency control and
  **serializable snapshot isolation (SSI)**: conflicting transactions abort at commit.
- **Garbage collection** — a watermark (the oldest open snapshot) drives reclamation
  of obsolete versions during compaction.

## Quick start

Requires Go 1.26+.

```bash
git clone <repo> && cd mythdbv3
go run ./cmd/mythdb     # runs an end-to-end demo
go test ./...           # run the test suite
```

## Usage

The engine lives in `internal/lsm` (`Storage`). The demo in
[`cmd/mythdb/main.go`](cmd/mythdb/main.go) shows the full API; the essentials:

```go
s, err := lsm.Open(lsm.Options{
    Path:          dir,            // data directory
    BlockSize:     4096,           // SST block target
    TargetSSTSize: 1 << 20,        // memtable freeze / SST size threshold
    Compaction: lsm.CompactionOptions{
        Strategy: "leveled",       // "", "full", or "leveled"
        Interval: 10 * time.Millisecond, // background compaction tick; 0 disables
    },
})
defer s.Close()

s.Put([]byte("key"), []byte("value"))
v, found, _ := s.Get([]byte("key"))
s.Delete([]byte("key"))

// Range scan over [lower, upper); nil bounds are unbounded.
it, _ := s.Scan([]byte("a"), []byte("z"))
for it.IsValid() {
    fmt.Printf("%s = %s\n", it.Key(), it.Value())
    it.Next()
}
```

### Transactions

```go
txn := s.Begin()                 // snapshot at the current committed timestamp
txn.Put([]byte("a"), []byte("1"))
v, found, _ := txn.Get([]byte("a"))   // sees its own writes + the snapshot
if err := txn.Commit(); errors.Is(err, lsm.ErrSerialization) {
    // a concurrent commit conflicted — retry
}
// txn.Rollback() discards the transaction.
```

> Note: the engine is under `internal/`, so it is consumed within this module
> (by `cmd/mythdb` and the tests). To reuse it as a library, move `internal/lsm`
> and its dependencies out of `internal/`.

## How it works

mythdb is built bottom-up: `key` → `iterator` → `block`/`bloom`/`sstable` and
`memtable` → `lsm`, with `wal` and `manifest` providing persistence.

The central idea is the **timestamped key encoding**: every stored key is
`userKey || ^timestamp`, so the engine keeps all historical versions of a key and a
reader at timestamp `T` sees the newest version `≤ T`. Writes get a commit
timestamp, flow through a WAL into the memtable, and eventually compact into levels;
compaction garbage-collects versions older than the oldest open snapshot.

For the full architecture, data flow, and invariants (especially the key-ordering
rules), see [CLAUDE.md](CLAUDE.md). The complete design history — one specification
and one implementation plan per phase — is under
[`docs/superpowers/`](docs/superpowers/).

## Project layout

```
internal/
  key/         timestamped key encoding and ordering
  iterator/    StorageIterator interface, merge & concat iterators
  block/       on-disk block format (builder, iterator, CRC)
  bloom/       bloom filter
  sstable/     SST builder, reader, iterator
  memtable/    skiplist memtable + optional WAL
  wal/         write-ahead log
  manifest/    append-only structural change log
  compaction/  compaction strategies (pure logic)
  lsm/         the storage engine: reads, writes, recovery, compaction, transactions
cmd/mythdb/    demo binary
docs/superpowers/  design specs and implementation plans
```

## Testing

```bash
go test ./...                            # all packages
go test -race ./internal/lsm/            # race detector
go test ./internal/lsm/ -run TestName -v # a single test
go vet ./... && gofmt -l internal/ cmd/  # static checks
```

## Limitations

These are intentional simplifications (documented in the design specs):

- The manifest grows unbounded — there is no manifest compaction yet.
- Key and value lengths are encoded as `uint16` at the block/SST level, so values
  or keys larger than 65535 bytes are not supported.

## Credits

A learning implementation following Alex Chi Z.'s
[mini-lsm](https://skyzh.github.io/mini-lsm/) course (Weeks 1–3), reimagined in
idiomatic Go.
