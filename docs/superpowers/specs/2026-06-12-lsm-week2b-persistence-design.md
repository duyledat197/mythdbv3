# MythDB — LSM Tree (Week 2B: Persistence) — Design

**Date:** 2026-06-12
**Status:** Approved
**Reference:** [mini-lsm by skyzh](https://skyzh.github.io/mini-lsm/) chapters 2.5–2.7
**Builds on:** Week 1 + Week 2A (`docs/superpowers/specs/2026-06-12-lsm-week2a-compaction-design.md`)

## Context

Week 2A added compaction but `Open` still starts empty: on restart, all data is
lost. Week 2B makes the engine durable and recoverable:

- A **manifest** records structural changes (new memtable, flush, compaction) so
  the L0/levels layout can be rebuilt on restart.
- A **write-ahead log (WAL)** per memtable records every write so unflushed
  memtable data survives a crash.
- A **WriteBatch** API applies a group of writes atomically and is the single
  write path (sets up Week 3 transactions).
- **Checksums** guard WAL records, manifest records, and the SST meta section.

This is the final Week 2 sub-spec. After it, the engine survives process exit
(including crash without `Close`) with no data loss of acknowledged writes.

### Decisions (from brainstorming)

- Scope: manifest + WAL + WriteBatch + checksums (mini-lsm 2.5–2.7).
- WAL lives **on the memtable** (each memtable optionally owns its WAL), matching
  mini-lsm and keeping WAL lifecycle tied to the memtable it logs.
- Sync policy: WAL records are written through to the file immediately (OS
  buffering, so a process crash without `Close` still recovers); `Sync()` fsyncs
  on freeze and `Close`. An optional `SyncWrites` flag fsyncs every write.
- Manifest uses length+CRC framed JSON records (human-debuggable).
- Style/testing: idiomatic Go, TDD (carried over).

## Goals

- `Open(path)` on a directory with an existing `MANIFEST` reconstructs the full
  engine state: L0/levels SSTs, plus unflushed memtables replayed from their WALs.
- Acknowledged writes (a returned `Write`/`Put`/`Delete`) survive a crash
  (process exit without `Close`) and are visible after reopening.
- `WriteBatch` applies all its operations atomically under one lock and one WAL
  append region.
- Corrupted WAL/manifest/SST-meta records are detected via CRC and handled
  (truncated trailing WAL record is ignored; corrupted committed record errors).

## Non-Goals (Week 2B)

- MVCC/timestamps/transactions semantics (Week 3). WriteBatch here is a durability
  + atomicity primitive, not an MVCC transaction.
- Manifest compaction/snapshotting (the manifest grows unbounded for now; that is
  acceptable at this scale and is a known mini-lsm follow-up).
- Concurrent multi-writer fsync batching (group commit).

## Architecture

### Package layout (additions)

```
internal/
  wal/wal.go            (new)    WAL: framed (key,value)+crc records; Create/Recover/Put/Sync/Close
  manifest/manifest.go  (new)    Manifest: framed JSON records+crc; Create/Recover/AddRecord/Close
  memtable/memtable.go  (modify) optional WAL ownership: NewWithWAL, RecoverWAL, Put logs, SyncWAL, CloseWAL, WALPath
  sstable/builder.go    (modify) write a CRC over the meta section
  sstable/sstable.go    (modify) verify the meta-section CRC on Open
  lsm/storage.go        (modify) WriteBatch + Write(); WAL-backed memtables; manifest writes on freeze/flush/compaction
  lsm/recover.go        (new)    rebuild state from manifest + WALs on Open
```

### WAL (`internal/wal`)

One file per memtable, named `<id>.wal` in the engine's `Path`.

Record format (little-endian):
```
keyLen(u32) key valLen(u32) value crc32(u32)   // crc over keyLen..value
```
A zero-length value is a tombstone.

```go
type WAL struct { /* file handle, optional sync */ }

// Create starts a fresh WAL at path.
func Create(path string, syncWrites bool) (*WAL, error)

// Record is one recovered key/value pair.
type Record struct{ Key, Value []byte }

// Recover reads all intact records from path and returns a WAL positioned to
// append more. A truncated/corrupt trailing record is dropped (treated as a
// crash mid-write); a corrupt non-trailing record returns an error.
func Recover(path string, syncWrites bool) ([]Record, *WAL, error)

func (w *WAL) Put(key, value []byte) error // write-through; fsync if syncWrites
func (w *WAL) Sync() error                  // fsync
func (w *WAL) Close() error
```

### Manifest (`internal/manifest`)

One file `MANIFEST` in `Path`, append-only.

Record framing: `len(u32) jsonBytes crc32(u32)` (crc over jsonBytes).

```go
type RecordKind string

const (
	KindNewMemtable RecordKind = "new_memtable"
	KindFlush       RecordKind = "flush"
	KindCompaction  RecordKind = "compaction"
)

type Record struct {
	Kind       RecordKind `json:"kind"`
	ID         int        `json:"id,omitempty"`          // NewMemtable / Flush memtable id
	UpperLevel int        `json:"upper_level,omitempty"` // Compaction
	UpperIDs   []int      `json:"upper_ids,omitempty"`
	LowerLevel int        `json:"lower_level,omitempty"`
	LowerIDs   []int      `json:"lower_ids,omitempty"`
	NewIDs     []int      `json:"new_ids,omitempty"`
}

func Create(path string) (*Manifest, error)
func Recover(path string) ([]Record, *Manifest, error) // reads all, returns appendable handle
func (m *Manifest) AddRecord(r Record) error           // append + fsync
func (m *Manifest) Close() error
```

### Memtable WAL ownership (`internal/memtable`)

The memtable gains an optional WAL. Existing `New(id)` keeps making a pure
in-memory memtable (used in tests and transient cases).

```go
// NewWithWAL creates a memtable backed by a fresh WAL at walPath.
func NewWithWAL(id int, walPath string, syncWrites bool) (*Memtable, error)

// RecoverWAL rebuilds a memtable by replaying an existing WAL, then keeps the
// WAL open for further appends.
func RecoverWAL(id int, walPath string, syncWrites bool) (*Memtable, error)

func (m *Memtable) SyncWAL() error   // no-op if no WAL
func (m *Memtable) CloseWAL() error  // no-op if no WAL
func (m *Memtable) WALPath() string  // "" if no WAL
```

`Put` writes to the WAL (if present) before inserting into the skiplist, so a
crash never leaves a skiplist entry without a logged record.

### SST meta-section checksum (`internal/sstable`)

`Build` appends a `crc32(u32)` over the encoded block-meta section, written
immediately after the meta section and before the meta offset. `Open` verifies
it and returns an error on mismatch. This is additive to the Week-1 layout:
```
[data blocks][meta section][meta crc u32][meta offset u32][bloom][bloom offset u32]
```
> Block-level data already carries a CRC from Week 1; this adds integrity for the
> metadata that locates blocks.

## Recovery flow (`internal/lsm/recover.go`)

`Open(opts)`:

1. If `Path/MANIFEST` does **not** exist → fresh start: create the directory if
   needed, `manifest.Create`, create memtable id 0 with a WAL (`<0>.wal`), record
   `NewMemtable{0}`. (Existing behavior, now persisted.)
2. If it **exists** → recover:
   a. `manifest.Recover` → ordered records.
   b. Fold records into structure:
      - `NewMemtable{id}` → add `id` to the live memtable-id set; track `maxID`.
      - `Flush{id}` → remove `id` from the memtable set; append `id` to `l0`
        (newest first as flush does).
      - `Compaction{...}` → apply generically: remove `UpperIDs` from their source
        (`l0` when `UpperLevel==0`, else `levels[UpperLevel-1]`), remove `LowerIDs`
        from `levels[LowerLevel-1]`, then set `levels[LowerLevel-1] = NewIDs`. This
        reproduces both the full and leveled runtime results (full uses
        `MaxLevels==1`, so there are no intermediate levels to clear).
   c. The surviving SST ids are the union of `l0` and all `levels`. Open each via
      `sstable.Open(id, sstPath(id))` into the `sstables` map. (Ids whose flush was
      superseded by a later compaction are not opened; their files may still exist
      and are removed.)
   d. The remaining memtable ids (still in the set) are unflushed. Recover each
      with `memtable.RecoverWAL(id, walPath(id))` and place them in `immMemtables`
      ordered newest-first (highest id first). Then create a brand-new active
      memtable with a fresh id + WAL and append a `NewMemtable` record. (Recovered
      memtables stay immutable so the background goroutine flushes them normally.)
   e. `nextID = maxID + 1` (then the new active memtable consumes the next id).
   f. Orphan files (SST/WAL ids not referenced by recovered state) are deleted.

After recovery the engine behaves identically to a fresh one; the background
compaction goroutine (if configured) starts as usual and will flush the recovered
immutable memtables and compact.

## Write path (`internal/lsm/storage.go`)

```go
type WriteBatch struct{ /* ordered ops */ }
func (b *WriteBatch) Put(key, value []byte)
func (b *WriteBatch) Delete(key []byte)

func (s *Storage) Write(b *WriteBatch) error // atomic: one lock, WAL-append all, then skiplist all
```

- `Put(k,v)` and `Delete(k)` become `Write` of a single-op batch.
- `Write` holds `mu.Lock`, appends every op to the active memtable's WAL, then
  applies every op to the skiplist, then checks the freeze threshold. The whole
  batch is logged before any reader can observe partial state (reads take the
  write lock’s snapshot).
- `ForceFreezeMemtable`: `SyncWAL()` the old memtable, create a new memtable with
  a new WAL, append `NewMemtable{newID}` to the manifest.
- `ForceFlushNextImmMemtable`: build the SST as today, append `Flush{id}` to the
  manifest, then `CloseWAL()` and delete the flushed memtable's WAL file.
- `runOnceCompaction`: after the state swap, append `Compaction{...}` to the
  manifest **before** deleting superseded SST files (so a crash mid-delete still
  recovers consistently — the manifest already reflects the new layout).
- `Close`: stop the goroutine, sync + close the active WAL, close the manifest,
  close SST handles.

## Error handling

- WAL/manifest writes return errors; the engine surfaces them from `Write`,
  `ForceFreeze*`, and compaction (logged, as in 2A).
- Recovery: a truncated trailing WAL record is silently dropped (normal after a
  crash mid-append). A CRC mismatch on a non-trailing WAL record, a manifest
  record, or an SST meta section returns an error from `Open`.
- Ordering guarantees: the manifest is fsync'd on each `AddRecord`; a `Flush`
  record is only written after the SST file is durably written; a `Compaction`
  record is written before old files are deleted.

## Testing (TDD)

- `wal`: append→recover roundtrip (incl. tombstones); CRC detects a flipped byte
  (non-trailing → error); truncated trailing record is dropped and the rest
  recovers; empty/missing file recovers to zero records.
- `manifest`: add→recover roundtrip for all three record kinds; CRC detects
  corruption; truncated trailing record dropped.
- `sstable`: meta-section CRC roundtrip; flipping a meta byte makes `Open` fail.
- `memtable`: `NewWithWAL` logs puts; `RecoverWAL` rebuilds identical contents;
  tombstones recover.
- `lsm` (white-box recovery):
  - write N keys, do NOT `Close`, reopen → all keys present (unflushed via WAL).
  - write, freeze+flush, reopen → keys present from L0 SST; flushed WAL is gone.
  - write, flush, run compaction, reopen → keys present; levels rebuilt from the
    `Compaction` manifest record; superseded SST ids not opened.
  - `nextID` after recovery does not collide with recovered ids.
  - `WriteBatch` with several puts/deletes applies atomically and recovers.
- Full suite green with `go test ./...` and `go test -race ./...`.

## Build order (bottom-up, TDD)

1. `internal/wal` — record format, Create/Put/Sync/Close, Recover.
2. `internal/manifest` — framed JSON records, Create/AddRecord/Recover/Close.
3. `internal/sstable` — meta-section CRC (Build + Open).
4. `internal/memtable` — optional WAL ownership (NewWithWAL, RecoverWAL, Put logs).
5. `internal/lsm` — WriteBatch + `Write`; wire WAL-backed memtables and manifest
   writes into freeze/flush/compaction (fresh-start path persists manifest+WAL).
6. `internal/lsm/recover.go` — rebuild state from manifest + WALs on `Open`;
   end-to-end crash-recovery tests.

## Open parameters (defaults)

- `SyncWrites` (new `Options` field) default `false` (write-through, fsync on
  freeze/close). Tests use the default; durability-critical callers set `true`.
- WAL file name `<id>.wal`; manifest file name `MANIFEST`, both under `Path`.
