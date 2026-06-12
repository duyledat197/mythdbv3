# MythDB — LSM Tree (Week 1: Core Engine) — Design

**Date:** 2026-06-12
**Status:** Approved
**Reference:** [mini-lsm by skyzh](https://skyzh.github.io/mini-lsm/)

## Context

Build an LSM-tree storage engine in Go, based on the mini-lsm course. The full
course spans three weeks:

- **Week 1 — Core LSM** (this spec): memtable, block, SST, iterators, read/write
  path, bloom filter. An in-memory key-value engine that can flush to on-disk SSTs.
- **Week 2 — Compaction & Persistence**: full/simple/tiered/leveled compaction,
  manifest, WAL, recovery. (Separate spec, built after Week 1.)
- **Week 3 — MVCC & Transactions**: timestamped keys, snapshot reads,
  watermark/GC, OCC, serializable snapshot isolation. (Separate spec.)

Each week is its own spec → plan → implementation cycle. This document covers
**Week 1 only**.

### Decisions (from brainstorming)

- **Scope:** full 3 weeks eventually, decomposed by week; Week 1 first.
- **Style:** idiomatic Go — interfaces, `sync.RWMutex`, explicit error returns,
  minimal external dependencies (implement skiplist/bloom ourselves).
- **Testing:** TDD with `go test`, porting equivalent cases from mini-lsm.

## Goals

A working in-memory LSM engine with on-disk SST flushing that supports:

- `Get(key) → (value, found)`
- `Put(key, value)`
- `Delete(key)` (tombstone)
- `Scan(lower, upper) → iterator` over a key range
- `ForceFreezeMemtable()` — turn the active memtable immutable
- `ForceFlushNextImmMemtable()` — flush the oldest immutable memtable to an L0 SST

## Non-Goals (Week 1)

- Compaction, manifest, WAL, crash recovery (Week 2).
- MVCC / timestamps / transactions (Week 3).
- Concurrency beyond a single-writer / multi-reader `RWMutex` model.

## Architecture

LSM stores data in tiers. Writes go to an in-memory **memtable** (skiplist).
When the active memtable exceeds a size threshold it is **frozen** (moved to an
immutable list) and a fresh memtable replaces it. Frozen memtables are later
**flushed** to disk as **SSTs** (Sorted String Tables) in level L0. Reads merge
across the active memtable → immutable memtables (newest→oldest) → L0 SSTs,
where newer sources win on duplicate keys.

### Package layout

```
mythdbv3/
  go.mod                  // module mythdb, go 1.26
  internal/
    key/        // Key type wrapping []byte (extension point for Week 3 timestamps)
    iterator/   // StorageIterator interface + MergeIterator, TwoMergeIterator, concat
    memtable/   // Memtable + skiplist (sorted, concurrent-read)
    block/      // BlockBuilder, Block, BlockIterator
    bloom/      // Bloom filter
    sstable/    // SsTable, SsTableBuilder, SsTableIterator
    lsm/        // LsmStorage engine: Get/Put/Delete/Scan, freeze, flush, state
  cmd/mythdb/   // demo CLI
```

**Forward-compatibility note:** Week 3 needs keys carrying a timestamp. Week 1
introduces a thin `key.Key` wrapper over `[]byte` so the Week 3 refactor does
not have to touch every call site. In Week 1 it behaves as a plain byte slice.

## Components

### StorageIterator interface

The shared abstraction for every iterator — a forward, one-way cursor over
sorted key-value pairs.

```go
type StorageIterator interface {
    Key() []byte     // current key; only valid when IsValid() is true
    Value() []byte   // current value
    IsValid() bool   // whether a current element exists
    Next() error     // advance to the next element
}
```

### Memtable (`internal/memtable`)

- Backed by a custom skiplist mapping `[]byte → []byte`, sorted by key.
- `Put(key, value)`, `Get(key) → (value, ok)`.
- Delete is represented by writing a **tombstone**: an empty value.
- Tracks an approximate size estimate to decide when to freeze.
- `MemtableIterator` iterates a `[lower, upper)` range, implements
  `StorageIterator`.
- Concurrency: skiplist supports concurrent reads with a single writer
  (guarded at the engine level by `RWMutex`).

### Block (`internal/block`)

The smallest I/O unit (~4 KB target).

- `BlockBuilder` accumulates entries, encoding each as
  `(key_len, key, value_len, value)`, plus a trailing array of entry offsets and
  an entry count; a checksum guards integrity.
- `Block.Encode()/Decode()` round-trip the byte layout.
- `BlockIterator` supports `SeekToFirst`, `SeekToKey(key)`, and `Next`,
  implements `StorageIterator`.

### Bloom filter (`internal/bloom`)

- Built from the hashes of all keys in an SST.
- `MayContain(key)` lets `Get` skip reading an SST that definitely lacks the key.
- Encoded into the SST file and reloaded on open.

### SsTable (`internal/sstable`)

- File format: a sequence of data blocks, then block metadata (first/last key +
  offset per block), then the bloom filter, then a footer with offsets.
- `SsTableBuilder` consumes a sorted key stream, splits into blocks, records
  metadata, builds the bloom filter, and writes the file.
- `SsTable` opens a file and exposes block reads.
- `SsTableIterator` supports `SeekToFirst`, `SeekToKey`, `Next`; implements
  `StorageIterator`.

### Merge iterators (`internal/iterator`)

- `MergeIterator`: heap-based merge of N iterators; on duplicate keys the
  newer source (lower index) wins, others advance past the duplicate.
- `TwoMergeIterator`: merges exactly two iterators (used to combine the
  memtable tier with the SST tier); left wins on ties.

### LsmStorage engine (`internal/lsm`)

State (guarded by `sync.RWMutex`):

- `memtable` — active memtable.
- `immMemtables` — immutable memtables, newest first.
- `l0SSTables` — L0 SST handles, newest first.

API:

- `Get`, `Put`, `Delete`, `Scan`.
- `ForceFreezeMemtable()`, `ForceFlushNextImmMemtable()`.

## Data flow

- **Put / Delete:** write into the active memtable. If its size estimate exceeds
  the threshold, freeze it. Delete writes a tombstone.
- **Get:** search active memtable → immutable memtables (newest→oldest) → L0
  SSTs (check bloom + key range before reading). Return the first match; if it is
  a tombstone, report not-found.
- **Scan(lower, upper):** build an iterator per tier → combine with
  `MergeIterator` / `TwoMergeIterator` → wrap in an `LsmIterator` that skips
  tombstones → wrap in a `FusedIterator` that is safe to call after exhaustion.
- **Flush:** take the oldest immutable memtable → feed its sorted entries to an
  `SsTableBuilder` → write the SST file → prepend the handle to `l0SSTables` and
  drop the flushed memtable.

## Error handling

- I/O and decode operations return `error`; iterators surface errors via
  `Next() error` and an accompanying validity check.
- Corrupted blocks/SSTs (checksum mismatch) return a descriptive error rather
  than panicking.
- `Get`/`Scan` distinguish "key absent" from "I/O error".

## Testing (TDD)

Each package gets `_test.go` written **before** implementation, porting
equivalent cases from mini-lsm:

- `memtable`: ordered insert/get, tombstones, range iterator bounds.
- `block`: encode/decode round-trip, iterator seek to key/first, boundary keys.
- `bloom`: no false negatives; false-positive rate within tolerance.
- `sstable`: build → read round-trip, multi-block seek, bloom integration.
- `iterator`: merge with duplicate keys (newest wins), tombstone handling,
  empty/exhausted sources.
- `lsm`: end-to-end get/put/delete/scan, freeze, flush to L0, read-after-flush.

Run with `go test ./...`.

## Build order (bottom-up, TDD)

1. `internal/key` (thin wrapper) + `internal/iterator` (interface,
   MergeIterator, TwoMergeIterator).
2. `internal/memtable` (skiplist → Memtable → iterator).
3. `internal/block` (builder → iterator).
4. `internal/bloom`.
5. `internal/sstable` (builder → table → iterator).
6. `internal/lsm` (engine: get → put/freeze → scan → flush).
7. `cmd/mythdb` demo CLI.

## Open parameters (defaults)

- Memtable freeze threshold: ~4 MB (configurable).
- Target block size: ~4 KB (configurable).
- Bloom false-positive rate: ~1%.
