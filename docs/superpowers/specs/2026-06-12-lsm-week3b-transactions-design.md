# MythDB — LSM Tree (Week 3B: Transactions, Watermark, GC, SSI) — Design

**Date:** 2026-06-12
**Status:** Approved
**Reference:** [mini-lsm by skyzh](https://skyzh.github.io/mini-lsm/) chapters 3.3–3.7
**Builds on:** Week 3A (MVCC storage core)

## Context

Week 3A made storage multi-version: reads happen at a timestamp. Week 3B exposes
that through a **transaction** API with snapshot isolation, reclaims old versions
via a **watermark**-driven garbage collection during compaction, and upgrades
isolation to **serializable snapshot isolation (SSI)** by detecting read-write
conflicts at commit.

### Decisions (from brainstorming)

- Full pipeline: snapshot isolation → optimistic concurrency control (write-write)
  → serializable snapshot isolation (read-write), mirroring mini-lsm 3.3–3.6.
- Watermark = the minimum read timestamp among open transactions (or the latest
  commit ts if none are open). Compaction GCs versions at or below it (mini-lsm 3.7).
- Style/testing: idiomatic Go, TDD.

## Goals

- `s.Begin()` returns a transaction reading a consistent snapshot at its start ts.
- `txn.Get/Scan` read the snapshot merged with the transaction's own pending writes;
  `txn.Put/Delete` buffer writes locally; `txn.Commit()` applies them atomically.
- Two transactions that write the same key conflict; the later committer aborts (OCC).
- Serializable: a transaction whose read set intersects a concurrent committer's
  write set aborts (SSI).
- Compaction reclaims versions no open transaction can see (watermark GC), including
  tombstones at the bottom level.

## Non-Goals (Week 3B)

- Distributed transactions, lock-based concurrency, savepoints.
- Manifest compaction (still unbounded, as noted earlier).

## Architecture

### Mvcc controller (`internal/lsm/mvcc.go`)

Owns timestamp and transaction bookkeeping (replaces the small commit-ts holder
from 3A):

```go
type mvccController struct {
	mu        sync.Mutex
	ts        uint64                 // latest committed timestamp
	commitMu  sync.Mutex             // serializes commits (validation + apply)
	readTs    *tsHeap                // multiset of open transactions' read timestamps
	committed []committedTxn         // recent committers, pruned below the watermark
}

type committedTxn struct {
	ts       uint64
	writeSet map[uint64]struct{} // key hashes written
}
```

- `latestTs()` / `nextTs()` as in 3A.
- `addReader(ts)` / `removeReader(ts)` maintain `readTs`.
- `watermark()` returns the minimum open read ts, or `ts` if none are open.
- After each commit, prune `committed` of entries with `ts ≤ watermark()` (no open
  transaction can still need them for validation).

### Transaction (`internal/lsm/txn.go`)

```go
type Txn struct {
	engine    *Storage
	readTs    uint64
	local     *memtable.Memtable // private buffer of pending writes (no WAL)
	readSet   map[uint64]struct{} // key hashes read (for SSI)
	committed bool
}

func (s *Storage) Begin() *Txn
func (t *Txn) Get(key []byte) ([]byte, bool, error)
func (t *Txn) Scan(lower, upper []byte) (iterator.StorageIterator, error)
func (t *Txn) Put(key, value []byte)
func (t *Txn) Delete(key []byte)
func (t *Txn) Commit() error
func (t *Txn) Rollback()
```

- `Begin`: `readTs = mvcc.latestTs()`; `mvcc.addReader(readTs)`; fresh local buffer.
- `Get`: consult the local buffer first (latest pending write wins); else an MVCC
  read at `readTs`. Record `hash(userKey)` in `readSet`.
- `Scan`: merge a local-buffer iterator with the engine's MVCC scan at `readTs`
  (local wins on equal user key). Record each yielded user key's hash in `readSet`.
- `Put/Delete`: write into the local buffer (key encoded with a placeholder ts; the
  real commit ts is applied at commit).
- `Commit`:
  1. Hold `mvcc.commitMu` (serialize the validate-then-apply critical section).
  2. **SSI check:** for every `committedTxn` with `ts > readTs`, if its `writeSet`
     intersects this transaction's `readSet`, abort with a serialization error.
  3. `commitTs = mvcc.nextTs()`; apply the local buffer to the engine as one batch
     at `commitTs` (reusing `Storage.Write`-style encoding).
  4. Record `committedTxn{commitTs, writeSet}` (the hashes of keys this txn wrote).
  5. Release `commitMu`; `mvcc.removeReader(readTs)`; prune committed history.
- `Rollback`: `mvcc.removeReader(readTs)`; discard the local buffer.

> **OCC (write-write):** with full SSI, a pure write-write conflict is caught when
> the later transaction has also *read* the key. To also catch blind-write conflicts,
> Commit additionally treats its own `writeSet` keys as part of the validation set
> (a transaction that blind-writes a key another concurrent txn wrote conflicts).
> This matches mini-lsm's behavior where the serializable check uses the key hashes.

### Watermark-driven compaction GC (`internal/lsm/compact.go`)

`doCompact` takes the current `watermark`. At the bottom level, for each user key:
- keep every version with `ts > watermark` (a snapshot might still read them);
- keep exactly the newest version with `ts ≤ watermark` (the latest a watermark
  reader sees);
- drop all older versions;
- if that newest `≤ watermark` version is a tombstone, drop it too (nothing below
  needs it) — the key disappears.

Above the bottom level, keep all versions (a deeper level may hold older ones that
must remain shadowed correctly). The watermark is read from the mvcc controller when
a compaction task starts.

## Data flow

- **Begin → reads:** snapshot at `readTs`; reads see committed data ≤ `readTs` plus
  the transaction's own pending writes; reads are recorded for SSI.
- **Commit:** serialize → validate read set against newer committers → assign commit
  ts → apply buffered writes as a batch → record write set → prune history.
- **Compaction:** at the bottom level, collapse versions at/below the watermark and
  drop reclaimed tombstones; versions above the watermark survive.

## Error handling

- `Commit` returns a serialization error on conflict; the transaction is left
  rolled back (reader removed, buffer discarded) so the caller can retry.
- A WAL/flush error during the commit apply propagates; the commit fails and the
  transaction rolls back.

## Testing (TDD)

- `mvcc controller`: watermark with zero / several open readers; history pruning
  below the watermark.
- `txn` snapshot isolation: a transaction does not observe commits that land after
  its `readTs`; it does observe its own pending writes; Rollback discards them.
- `txn` OCC: two transactions both write key K; the first commits, the second aborts.
- `txn` SSI: T1 reads K and writes L; T2 writes K and commits before T1 — T1's commit
  aborts (read-write conflict). A non-conflicting pair both commit.
- `compaction GC`: with no open readers (watermark = latest), an overwritten key
  collapses to one version after bottom compaction; with an open reader at an old ts,
  the old version survives until that reader closes; a deleted key's tombstone is
  reclaimed at the bottom level.
- `recovery`: transactions and GC interoperate with restart (commit ts restored).
- Full suite green with `go test ./...` and `go test -race ./...`.

## Build order (bottom-up, TDD)

1. `internal/lsm/mvcc.go` — controller with read-ts multiset, watermark, committed
   history + pruning.
2. `internal/lsm/txn.go` — Txn with local buffer, snapshot Get/Scan, read-set
   recording, Begin/Rollback.
3. `internal/lsm/txn.go` Commit — SSI validation + apply + write-set recording.
4. `internal/lsm/compact.go` — watermark GC in `doCompact`; wire the watermark from
   the controller into `runOnceCompaction`.

## Open parameters

- Read-ts multiset implemented as a `container/heap` of timestamps with reference
  counts (a map `ts → count` plus a min-heap), so identical read timestamps from
  several transactions are handled correctly.
- Committed-history pruning runs after each commit; bounded by the watermark.
