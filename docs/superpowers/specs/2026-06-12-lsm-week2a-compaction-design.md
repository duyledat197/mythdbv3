# MythDB — LSM Tree (Week 2A: Compaction) — Design

**Date:** 2026-06-12
**Status:** Approved
**Reference:** [mini-lsm by skyzh](https://skyzh.github.io/mini-lsm/) chapters 2.1–2.4
**Builds on:** Week 1 (`docs/superpowers/specs/2026-06-12-lsm-week1-design.md`)

## Context

Week 1 produced an LSM engine that flushes immutable memtables to L0 SSTs. As L0
grows, reads get slower (every L0 SST may hold the key) and space is wasted on
stale/overwritten/deleted entries. **Compaction** merges SSTs, pushes data into
deeper levels, and reclaims space by dropping shadowed entries and tombstones.

Week 2 is split into two sub-specs, built in order:

- **Week 2A — Compaction** (this spec): the compaction framework, full and
  leveled strategies, the level-aware read path, and a background trigger.
- **Week 2B — Persistence**: manifest, WAL, and crash recovery (separate spec).

### Decisions (from brainstorming)

- **Strategies:** full compaction + leveled compaction. (Not tiered/simple.)
- **Leveled flavor:** whole-level merge (when a level triggers, merge that entire
  level with the entire next level into one sorted run). This keeps each level a
  single non-overlapping sorted run and is robust and deterministically testable.
  The partial key-range variant (RocksDB-style) is explicitly out of scope.
- **Trigger:** background goroutine only (no public manual-compaction API). The
  compaction *logic* lives in unexported functions tested directly via white-box
  (same-package) tests; the goroutine is a thin ticker over those functions.
- **Style/testing:** idiomatic Go, TDD (carried over from Week 1).

## Goals

- Engine maintains levels beyond L0: `l0` plus `levels[][]` (L1, L2, …), each
  non-L0 level a single non-overlapping sorted run.
- Full compaction merges everything into the bottom level, dropping tombstones.
- Leveled compaction triggers on L0 file count and per-level size, merging whole
  adjacent levels.
- `Get` and `Scan` read correctly across memtables, L0, and all levels.
- A background goroutine runs leveled compaction periodically; `Close()` stops it
  cleanly.

## Non-Goals (Week 2A)

- Manifest, WAL, crash recovery (Week 2B). On restart the engine still starts
  empty for now.
- Tiered/universal and simple-leveled strategies; partial key-range leveled.
- MVCC/timestamps (Week 3). Tombstones remain "empty value".

## Architecture

### State refactor

Week 1 stored `l0 []*sstable.SsTable`. To support levels and to prepare for
Week 2B (manifest stores ids, not handles), the engine now keys SSTs by integer
id:

```go
type state struct {
	memtable     *memtable.Memtable
	immMemtables []*memtable.Memtable      // newest first
	l0           []int                     // SST ids, newest first
	levels       [][]int                   // levels[i] = ids of L(i+1), key-sorted, non-overlapping
	sstables     map[int]*sstable.SsTable  // id -> open handle
}
```

**Invariant:** for every non-L0 level, its SSTs are sorted by key and their key
ranges do not overlap (a single sorted run). L0 SSTs may overlap.

This refactor touches Week 1 call sites: `ForceFlushNextImmMemtable` now appends
the new SST's id to `l0` and registers it in `sstables`; `Get`/`Scan`/`Close`
resolve ids through `sstables`.

### Package layout (additions)

```
internal/
  iterator/
    concat.go            // SstConcatIterator over a sorted run of SSTs
  compaction/
    compaction.go        // Task type, Controller interface, Options
    full.go              // full-compaction controller
    leveled.go           // leveled-compaction controller
  lsm/
    storage.go           // state refactor, level-aware Get/Scan, flush
    compact.go           // doCompact, runOnceCompaction, background goroutine
```

`internal/compaction` depends only on a small read-only view of state (the id
slices) so the strategies can be unit-tested without the full engine. The engine
(`internal/lsm`) owns SST I/O (`doCompact`) and state installation.

## Components

### SstConcatIterator (`internal/iterator/concat.go`)

Iterates a sorted run of non-overlapping SSTs as one stream. Needs the SST
handles in key order.

```go
func NewConcatIterAndSeekToFirst(tables []*sstable.SsTable) (*SstConcatIterator, error)
func NewConcatIterAndSeekToKey(tables []*sstable.SsTable, key []byte) (*SstConcatIterator, error)
```

- `SeekToKey` binary-searches the tables by `LastKey` to pick the table whose
  range may contain the key, then seeks within it; if exhausted, advances to the
  next table's first key.
- `Next` advances within the current SST; at a table boundary it opens the next
  table's iterator.
- Implements `iterator.StorageIterator`.

> Note: this lives in `internal/iterator` and imports `internal/sstable`.
> `internal/sstable` does not import `internal/iterator`, so there is no cycle.

### Level-aware read path (`internal/lsm/storage.go`)

- **Get:** active memtable → immutable memtables (newest→oldest) → each L0 SST
  (newest→oldest, bloom + range check) → each level L1..Ln (range check, then
  bloom, then a concat seek over that level). First match wins; an empty value
  resolves to not-found (no fall-through past a tombstone).
- **Scan:** build iterators for memtables, an L0 `MergeIterator`, and one
  `SstConcatIterator` per level; combine newest→oldest. Combine the memtable
  merge and the L0 merge and the level concats so newer tiers win ties, then wrap
  in the existing `lsmIterator` (tombstone skip + upper bound) and
  `fusedIterator`. Ordering newest→oldest: memtables, then L0, then L1, …, Ln.

### Compaction core (`internal/lsm/compact.go`)

```go
// doCompact merges the given input SST ids (already ordered newest-first) into
// new SSTs, splitting by target size. When toBottomLevel is true, tombstones and
// empty values are dropped (no lower level can hold the key).
func (s *Storage) doCompact(inputIDs []int, toBottomLevel bool) ([]*sstable.SsTable, error)
```

- Opens an `SsTableIterator` per input id, wraps them in a `MergeIterator`
  (input order is newest-first so the newest value wins on duplicate keys).
- Streams sorted, de-duplicated entries into `sstable.Builder`s, starting a new
  SST when the current exceeds the target SST size.
- Skips entries with empty value when `toBottomLevel`.
- Returns the new (open) SST handles; the caller assigns ids and installs state.

```go
// runOnceCompaction asks the controller for a task, executes it, installs the
// new state, and deletes superseded SST files. Returns whether work was done.
func (s *Storage) runOnceCompaction() (bool, error)
```

### Compaction strategies (`internal/compaction`)

```go
type Task struct {
	// A task merges an upper source and a lower destination into LowerLevel.
	UpperLevel int   // 0 means "L0 source"; otherwise a 1-based level number
	UpperIDs   []int // ids from the upper source (L0 ids when UpperLevel == 0)
	LowerLevel int   // 1-based destination level
	LowerIDs   []int // existing ids in the destination level being merged in
	ToBottom   bool  // destination is the bottom-most level -> drop tombstones
}

// InputIDs returns UpperIDs followed by LowerIDs (newest-first overall, since
// the upper/L0 source holds newer data than the destination level).

type Controller interface {
	// GenerateTask returns the next compaction to run, or nil if none is needed.
	GenerateTask(l0 []int, levels [][]int, sizes func(id int) int64) *Task
	// ApplyResult returns the new l0 and levels after replacing the task's
	// inputs with newIDs at the destination level.
	ApplyResult(l0 []int, levels [][]int, t *Task, newIDs []int) (newL0 []int, newLevels [][]int)
}
```

- **Full** (`full.go`): if there is anything in L0 or any level, produce a task
  whose inputs are all L0 ids + all level ids, destination = bottom level,
  `ToBottom = true`. `ApplyResult` clears L0 and all levels and puts `newIDs`
  into the bottom level.
- **Leveled** (`leveled.go`):
  - If `len(l0) >= L0CompactionTrigger`: task merges all L0 + all of L1 into L1
    (destination L1). `ToBottom` = (L1 is the only/bottom level with data).
  - Else compute each level's target size from `BaseLevelSizeBytes` and
    `LevelSizeMultiplier`. Pick the level whose `currentSize/targetSize` ratio is
    highest and > 1.0; task merges that whole level + the whole next level into
    the next level. `ToBottom` = destination is the last configured level.
  - `ApplyResult` removes inputs from the upper level (and L0), removes inputs
    from the lower level, and sets the lower level to `newIDs` (sorted by first
    key — already sorted since `doCompact` emits in key order).

### Options & background goroutine

```go
type CompactionOptions struct {
	Strategy            string        // "full" or "leveled"
	MaxLevels           int           // default 4 (number of non-L0 levels)
	L0CompactionTrigger int           // default 4
	LevelSizeMultiplier int           // default 10
	BaseLevelSizeBytes  int64         // default 16 MB-equivalent (small in tests)
	TargetSSTSize       int64         // bytes per output SST (default reuses Options.TargetSSTSize)
	Interval            time.Duration // background tick (default 50ms; 0 disables the goroutine)
}
```

`Open` starts the goroutine when `Interval > 0` and a strategy is set. The
goroutine loops: on each tick call `runOnceCompaction()` until it reports no work
or an error. `Close()` signals a stop channel and waits (`sync.WaitGroup`) for
the goroutine to exit before closing SST handles.

## Data flow

- **Flush (updated):** oldest immutable memtable → new SST → id appended to `l0`
  (newest first), handle registered in `sstables`.
- **Background compaction:** ticker → `runOnceCompaction` → controller picks a
  task → `doCompact` writes new SSTs under new ids → state swapped (inputs removed
  from their levels, new ids placed at destination) → superseded SST files closed
  and deleted from disk.
- **Read:** as described in the level-aware read path.

## Concurrency

- State is copy-on-write under `sync.RWMutex` (as in Week 1). `runOnceCompaction`
  does the expensive merge/write **without holding the lock**, then takes the
  write lock only to swap `state` and update `sstables`. Because new SSTs use
  fresh ids and superseded ids are removed atomically in the swap, in-flight
  readers holding an older `*state` snapshot keep working against still-open
  handles.
- File deletion of superseded SSTs happens after the swap. A reader that captured
  the old snapshot may still hold the handle; deletion closes the handle the
  engine owns, but the OS keeps the inode alive until the reader's open `*os.File`
  is also closed. (Week 2A readers finish synchronously within a `Scan`/`Get`
  call, so this is safe; full reference-counted lifetime is a Week 3 concern.)
- The background goroutine is the only compactor (no concurrent compactions), so
  tasks never overlap.

## Error handling

- `doCompact` surfaces I/O/decode errors; on error the state is left unchanged
  and no files are deleted.
- A failed `runOnceCompaction` logs and the goroutine continues on the next tick
  (a transient error must not kill compaction permanently).
- Controllers never panic on empty input; `GenerateTask` returns nil when there
  is nothing to do.

## Testing (TDD, white-box where needed)

- `iterator` (concat): seek-to-first, seek-to-key landing in the right SST,
  cross-SST `Next`, seek past end → invalid, single-SST run.
- `compaction` (pure logic, table-driven): full task generation + apply; leveled
  L0 trigger; leveled size-ratio level selection; `ApplyResult` rebuilds correct
  `l0`/`levels`.
- `lsm` (white-box, same package):
  - `doCompact` drops tombstones when `toBottomLevel`, keeps them otherwise.
  - full compaction collapses L0 + levels into one bottom-level run; overwritten
    keys keep newest value; deleted keys disappear.
  - leveled compaction moves data L0→L1→… and preserves correctness.
  - `Get`/`Scan` correct after data has been pushed into deep levels.
  - background goroutine: with a tiny `Interval`, after writing enough data and
    waiting, L0 shrinks and levels grow; `Close()` stops the goroutine (no leak,
    verified with `-race`).
- Full suite green with `go test ./...` and `go test -race ./...`.

## Build order (bottom-up, TDD)

1. `internal/iterator/concat.go` — SstConcatIterator.
2. `internal/lsm` state refactor to ids + `sstables` map; update flush and
   level-aware `Get`/`Scan`; keep all Week 1 tests green.
3. `internal/compaction` — Task, Controller, full strategy (pure logic + tests).
4. `internal/lsm/compact.go` — `doCompact` + `runOnceCompaction` wired to the
   full controller; tests for tombstone dropping and full compaction.
5. `internal/compaction/leveled.go` — leveled strategy (pure logic + tests).
6. Wire leveled into the engine; background goroutine + `Close()` lifecycle;
   end-to-end tests.

## Open parameters (defaults)

- `MaxLevels` 4, `L0CompactionTrigger` 4, `LevelSizeMultiplier` 10,
  `BaseLevelSizeBytes` 16 MB (overridden small in tests), background `Interval`
  50 ms (0 disables).
