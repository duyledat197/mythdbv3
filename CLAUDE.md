# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`mythdb` is an LSM-tree storage engine in Go, built as a faithful port of the
[mini-lsm course](https://skyzh.github.io/mini-lsm/). It is a full MVCC key-value
engine: crash-recoverable, with leveled/full compaction, and serializable
snapshot-isolation transactions. Pure standard library, no third-party deps.

## Commands

```bash
go test ./...                              # full suite
go test -race ./internal/lsm/              # race detector (use on lsm changes)
go test ./internal/lsm/ -run TestName -v   # a single test (white-box; tests are in-package)
go vet ./...
gofmt -l internal/ cmd/                    # list unformatted files; gofmt -w to fix
go run ./cmd/mythdb                         # end-to-end demo (put/get/delete/scan + compaction)
```

There is no build/lint config beyond Go's defaults. Always run `go test ./...`,
`go vet ./...`, and `gofmt -l` before committing; run `go test -race ./internal/lsm/`
for any engine change.

## Architecture

The engine is layered bottom-up; each package depends only on the ones below it,
and everything orders keys through `internal/key`:

```
key → iterator ─┬→ block → bloom → sstable ─┐
                └→ memtable ─────────────────┴→ lsm   (cmd/mythdb is the demo)
                   wal, manifest ────────────────↑
```

- **`internal/lsm`** is the engine (`Storage`). Everything else is a building block.
- Engine state (`state`: active memtable, immutable memtables, `l0 []int`,
  `levels [][]int`, `sstables map[int]*SsTable`) is **copy-on-write under an
  `sync.RWMutex`**. Reads call `snapshot()` to grab the current `*state` and then
  work lock-free; writers build a new `*state` and swap it. SSTs are referenced by
  integer id, resolved through the `sstables` map.

### THE critical invariant: timestamped key encoding (`internal/key`)

Every key the engine stores is `Encode(userKey, ts)` = `userKey || bigEndian(^ts)`
(an 8-byte, bit-complemented timestamp suffix). This is what makes MVCC work, and
it has two consequences you must respect:

- **`key.Compare` is NOT `bytes.Compare`.** It compares the user-key portion first,
  then timestamp **descending** (newest version first). A plain byte compare on the
  concatenation misorders prefix-related keys (`"b"` would sort after `"bc"`) and
  silently drops keys from bounded scans. All ordering — skiplist, merge iterators,
  block/SST binary search — goes through `key.Compare`, so it stays consistent.
- **Lower layers (block, sstable, memtable) treat keys as opaque bytes.** They never
  decode timestamps except: the SST builder hashes `key.UserKey(k)` for the bloom
  filter (so point lookups work across versions) and records `maxTs`. If you feed a
  raw (un-encoded, <8-byte) key into the engine you will corrupt ordering;
  `key.UserKey`/`Timestamp` are defensively tolerant of short keys only so the
  sstable *unit tests* can use raw keys.

### Read path (MVCC)

`Get`/`Scan` read at a timestamp (the latest committed ts for the public API, or a
transaction's snapshot ts). `buildMvccScan` merges all tiers (memtables, an L0
`MergeIterator`, one `SstConcatIterator` per level) newest-first into one stream of
encoded keys, then wraps it in `mvccIterator`, which for each user key emits the
newest version with `ts <= readTs`, skips the rest of that user key's versions, and
hides tombstones (empty value). `mvccIterator.Key()` returns the **decoded** user key.

### Write path

`Put`/`Delete` build a single-op `WriteBatch`; `Write` serializes on the mvcc
controller's `commitMu`, allocates one commit timestamp for the whole batch, encodes
each key with it, logs to the active memtable's WAL, inserts into the skiplist, and
records the batch's write set (so transactions can detect conflicts against
non-transactional writes too). A full memtable freezes (becomes immutable, new
WAL-backed memtable installed); the oldest immutable later flushes to an L0 SST.

### Persistence & recovery (`internal/wal`, `internal/manifest`)

- Each memtable owns a WAL (`<id>.wal`); writes are logged before the skiplist
  insert (`memtable.PutBatch` logs all entries before applying any, for batch
  atomicity). WAL records are CRC-checked; recovery drops an incomplete trailing
  record but errors on a corrupt complete one.
- The `MANIFEST` is an append-only, CRC-framed JSON log of structural changes
  (`NewMemtable` / `Flush` / `Compaction`). On `Open`, if a MANIFEST exists, `recover`
  replays it to rebuild `l0`/`levels`, opens surviving SSTs, replays unflushed WALs
  into immutable memtables, restores the commit-ts counter to `max(SST.MaxTs, ts in
  recovered memtables)`, and deletes orphan files.
- **A flushed SST reuses its memtable's id** (so `Flush{id}` maps memtable→SST).
  **Manifest writes are serialized under `s.mu`** (freeze/flush hold it; compaction
  writes its record before unlocking). Ordering for durability: SST is fsync'd
  before its `Flush` record; the `Compaction` record is written before old files are
  deleted.

### Compaction (`internal/compaction`, `internal/lsm/compact.go`)

`internal/compaction` is pure logic over id slices (`Controller` with `Full` and
`Leveled` strategies; `GenerateTask` / `ApplyResult`), unit-tested without the
engine. The engine's `doCompact` executes a task by merging input SSTs and writing
new ones; `runOnceCompaction` swaps state under the lock. A background goroutine
(enabled via `CompactionOptions.Interval`) drains compaction work per tick; `Close`
stops it cleanly via a stop channel + `WaitGroup` before closing handles.

`doCompact` garbage-collects only at the bottom level, using the **watermark**: it
keeps every version `ts > watermark` plus the newest `ts <= watermark` (dropping
that one too if it is a tombstone). Above the bottom level all versions survive.

### Transactions & SSI (`internal/lsm/mvcc.go`, `txn.go`)

The `mvcc` controller holds the commit-ts counter, a refcounted multiset of open
transactions' read timestamps (the **watermark** = min open reader, or latest ts if
none), and recent committers' write sets (pruned below the watermark). `Begin`
snapshots `readTs`. A `Txn` buffers writes in a local `map[string][]byte`, reads its
buffer merged with the snapshot, and records every accessed key hash. `Commit` holds
`commitMu` for the whole validate→apply→record critical section and aborts with
`ErrSerialization` if any committer with `ts > readTs` wrote a key in the
transaction's access set (this catches both write-write and read-write conflicts).

### Lock ordering (to avoid deadlock)

`commitMu` → `mvcc.mu` → `s.mu` (engine RWMutex) → `idMu`. `mvcc.mu` is always
released before `s.mu` is taken. Background compaction never takes `commitMu`.

## Working conventions

- The full design history lives in `docs/superpowers/specs/` and
  `docs/superpowers/plans/` — one spec + one plan per phase (week1, week2a, week2b,
  week3a, week3b). Read the relevant spec before changing a subsystem; update it if
  you change behavior.
- Tests follow TDD and live in-package (white-box) so they can reach unexported
  helpers like `s.mvcc`, `s.runOnceCompaction`, `snapshot()`.

## Known limitations (intentional, documented in specs)

- The manifest grows unbounded (no manifest compaction yet).
- Block/SST encode key and value lengths as `uint16`; keys or values over 65535
  bytes corrupt silently — add a guard before relying on large values.
