# LSM Tree Week 3B (Transactions, Watermark, GC, SSI) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add transactions with snapshot isolation, a watermark that drives version garbage collection during compaction, and serializable snapshot isolation (SSI) via read/write conflict detection at commit.

**Architecture:** The 3A `mvcc` clock becomes a controller tracking open transactions' read timestamps (for the watermark) and recent committers' write sets (for SSI). A `Txn` reads a consistent snapshot at its start timestamp merged with its own buffered writes, records the keys it touches, and at commit validates its access set against newer committers before applying. Compaction reclaims versions at or below the watermark.

**Tech Stack:** Go 1.26, standard library only (`hash/fnv`). Builds on Week 3A.

**Spec:** `docs/superpowers/specs/2026-06-12-lsm-week3b-transactions-design.md`.

**Conventions:**
- Module `mythdb`. Run from repo root. Commit after each task; use `git -c user.name='Claude' -c user.email='noreply@anthropic.com' commit` if needed.
- After each task run `go test ./...` AND `go vet ./...` AND `go test -race ./internal/lsm/`; confirm green before committing.
- Reads/writes inside a `Txn` use raw USER keys; the engine encodes them with the commit timestamp at commit time.

---

## File Structure

```
internal/lsm/
  mvcc.go        (modify) controller: readers multiset, watermark, committed history; commitMu
  storage.go     (modify) Write serializes via commitMu + records its write set; writeEncodedBatch helper; Begin()
  txn.go         (new)    Txn, local buffer, snapshot Get/Scan, read-set, Commit (SSI), Rollback
  compact.go     (modify) watermark GC in doCompact; pass watermark from runOnceCompaction
```

---

## Task 1: MVCC controller — watermark and committed history

**Files:**
- Modify: `internal/lsm/mvcc.go`
- Modify: `internal/lsm/storage.go` (Write serializes via commitMu and records its write set; add `writeEncodedBatch`)
- Test: `internal/lsm/mvcc_controller_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/lsm/mvcc_controller_test.go`:
```go
package lsm

import "testing"

func TestWatermarkNoReaders(t *testing.T) {
	m := newMvcc(5)
	if m.watermark() != 5 {
		t.Fatalf("watermark with no readers = %d want 5 (latest ts)", m.watermark())
	}
}

func TestWatermarkTracksMinReader(t *testing.T) {
	m := newMvcc(10)
	m.addReader(7)
	m.addReader(3)
	m.addReader(7)
	if m.watermark() != 3 {
		t.Fatalf("watermark = %d want 3 (min open reader)", m.watermark())
	}
	m.removeReader(3)
	if m.watermark() != 7 {
		t.Fatalf("watermark after removing 3 = %d want 7", m.watermark())
	}
	m.removeReader(7) // one of two
	if m.watermark() != 7 {
		t.Fatalf("watermark still = %d want 7 (one reader at 7 remains)", m.watermark())
	}
	m.removeReader(7)
	if m.watermark() != 10 {
		t.Fatalf("watermark with no readers = %d want 10", m.watermark())
	}
}

func TestCommittedHistoryPrunes(t *testing.T) {
	m := newMvcc(0)
	m.recordCommitted(1, map[uint64]struct{}{100: {}})
	m.recordCommitted(2, map[uint64]struct{}{200: {}})
	m.recordCommitted(3, map[uint64]struct{}{300: {}})
	// With no open readers the watermark is the latest ts (3); everything <= 3
	// can be pruned.
	m.setTs(3)
	m.pruneCommitted()
	if n := m.committedCount(); n != 0 {
		t.Fatalf("expected all committed entries pruned, got %d", n)
	}
}

func TestConflictDetection(t *testing.T) {
	m := newMvcc(0)
	m.recordCommitted(5, map[uint64]struct{}{42: {}}) // a committer wrote key-hash 42 at ts 5
	// A reader that started at ts 4 and accessed key-hash 42 conflicts.
	if !m.hasConflict(4, map[uint64]struct{}{42: {}}) {
		t.Fatal("expected conflict: committer at ts 5 > readTs 4 wrote an accessed key")
	}
	// A reader that started at ts 5 (after that committer) does not conflict.
	if m.hasConflict(5, map[uint64]struct{}{42: {}}) {
		t.Fatal("did not expect conflict for readTs == committer ts")
	}
	// A reader accessing a different key does not conflict.
	if m.hasConflict(4, map[uint64]struct{}{99: {}}) {
		t.Fatal("did not expect conflict for disjoint access set")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/lsm/ -run 'TestWatermark|TestCommitted|TestConflict'`
Expected: build failure — `m.watermark undefined`.

- [ ] **Step 3: Extend the controller**

Replace `internal/lsm/mvcc.go` with:
```go
package lsm

import "sync"

// committedTxn records a finished transaction's commit timestamp and the set of
// key hashes it wrote, for serializable-snapshot-isolation conflict checks.
type committedTxn struct {
	ts        uint64
	writeSet  map[uint64]struct{}
}

// mvcc is the multi-version concurrency controller: a monotonic commit-timestamp
// counter, the set of open transactions' read timestamps (for the watermark), and
// recent committers' write sets (for SSI validation).
type mvcc struct {
	mu        sync.Mutex
	ts        uint64
	readers   map[uint64]int // open read timestamp -> active count
	committed []committedTxn

	commitMu sync.Mutex // serializes commit critical sections (validate + apply)
}

func newMvcc(initial uint64) *mvcc {
	return &mvcc{ts: initial, readers: map[uint64]int{}}
}

// latestTs returns the latest committed timestamp.
func (m *mvcc) latestTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ts
}

// nextTs increments and returns the new commit timestamp.
func (m *mvcc) nextTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ts++
	return m.ts
}

// setTs raises the counter to at least ts (used during recovery).
func (m *mvcc) setTs(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ts > m.ts {
		m.ts = ts
	}
}

// addReader registers an open transaction reading at ts.
func (m *mvcc) addReader(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readers[ts]++
}

// removeReader unregisters one open transaction reading at ts.
func (m *mvcc) removeReader(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readers[ts] <= 1 {
		delete(m.readers, ts)
	} else {
		m.readers[ts]--
	}
}

// watermark is the minimum open read timestamp, or the latest commit ts if no
// transaction is open. Versions at or below it (except the newest such per key)
// can be reclaimed.
func (m *mvcc) watermark() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.readers) == 0 {
		return m.ts
	}
	min := ^uint64(0)
	for ts := range m.readers {
		if ts < min {
			min = ts
		}
	}
	return min
}

// recordCommitted appends a committer's write set to the history.
func (m *mvcc) recordCommitted(ts uint64, writeSet map[uint64]struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committed = append(m.committed, committedTxn{ts: ts, writeSet: writeSet})
}

// pruneCommitted drops history entries no open transaction can still need (ts at
// or below the watermark).
func (m *mvcc) pruneCommitted() {
	wm := m.watermark()
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.committed[:0:0]
	for _, c := range m.committed {
		if c.ts > wm {
			kept = append(kept, c)
		}
	}
	m.committed = kept
}

// committedCount reports the number of retained committed-txn records (test aid).
func (m *mvcc) committedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.committed)
}

// hasConflict reports whether any committed transaction with ts > readTs wrote a
// key in accessSet (a serialization conflict for a transaction that read at readTs
// and touched those keys).
func (m *mvcc) hasConflict(readTs uint64, accessSet map[uint64]struct{}) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.committed {
		if c.ts <= readTs {
			continue
		}
		for h := range c.writeSet {
			if _, ok := accessSet[h]; ok {
				return true
			}
		}
	}
	return false
}
```

- [ ] **Step 4: Make non-transactional Write serialize through commitMu and record its write set**

In `internal/lsm/storage.go`, add `"hash/fnv"` to the import block. Add a key-hash
helper near `seekSST`:
```go
// hashKey hashes a user key for transaction conflict tracking.
func hashKey(userKey []byte) uint64 {
	h := fnv.New64a()
	h.Write(userKey)
	return h.Sum64()
}
```

Add an internal helper that applies an already-encoded batch (extracted from the
current `Write` body), then make `Write` go through the commit path so its writes
serialize with transaction commits and are visible to SSI validation. Replace the
existing `Write` function with:
```go
// writeEncodedBatch applies entries (encoded keys) to the active memtable under
// the engine lock, freezing if the memtable is full.
func (s *Storage) writeEncodedBatch(entries []memtable.Entry) error {
	s.mu.Lock()
	if err := s.st.memtable.PutBatch(entries); err != nil {
		s.mu.Unlock()
		return err
	}
	full := s.st.memtable.ApproximateSize() >= s.opts.TargetSSTSize
	s.mu.Unlock()
	if full {
		return s.ForceFreezeMemtable()
	}
	return nil
}

// Write applies a batch atomically at a fresh commit timestamp. It serializes
// with transaction commits and records its write set so open transactions detect
// conflicts against it.
func (s *Storage) Write(b *WriteBatch) error {
	s.mvcc.commitMu.Lock()
	defer s.mvcc.commitMu.Unlock()
	commitTs := s.mvcc.nextTs()
	entries := make([]memtable.Entry, len(b.ops))
	writeSet := make(map[uint64]struct{}, len(b.ops))
	for i, op := range b.ops {
		entries[i] = memtable.Entry{Key: key.Encode(op.key, commitTs), Value: op.value}
		writeSet[hashKey(op.key)] = struct{}{}
	}
	if err := s.writeEncodedBatch(entries); err != nil {
		return err
	}
	s.mvcc.recordCommitted(commitTs, writeSet)
	s.mvcc.pruneCommitted()
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/lsm/ && go test ./... && go vet ./...`
Expected: PASS everywhere. (Existing Put/Get/Scan/recovery tests still pass: with no
open transactions the committed history prunes to empty after each Write, and commit
timestamps remain monotonic.)

- [ ] **Step 6: Commit**

```bash
git add internal/lsm/mvcc.go internal/lsm/storage.go internal/lsm/mvcc_controller_test.go
git commit -m "feat: mvcc controller with watermark and committed history; serialized writes"
```

---

## Task 2: Transaction snapshot reads and buffered writes

**Files:**
- Create: `internal/lsm/txn.go`
- Modify: `internal/lsm/storage.go` (add `Begin`)
- Test: `internal/lsm/txn_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/lsm/txn_test.go`:
```go
package lsm

import (
	"testing"
)

func newTxnStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := Open(Options{Path: t.TempDir(), BlockSize: 4096, TargetSSTSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestTxnReadsOwnWrites(t *testing.T) {
	s := newTxnStorage(t)
	txn := s.Begin()
	txn.Put([]byte("a"), []byte("1"))
	txn.Delete([]byte("b"))
	if v, ok, _ := txn.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("txn should see its own put: %q ok=%v", v, ok)
	}
	if _, ok, _ := txn.Get([]byte("b")); ok {
		t.Fatal("txn should see its own delete")
	}
	// Not yet committed: the engine does not see the write.
	if _, ok, _ := s.Get([]byte("a")); ok {
		t.Fatal("engine should not see uncommitted txn write")
	}
}

func TestTxnSnapshotIsolation(t *testing.T) {
	s := newTxnStorage(t)
	s.Put([]byte("k"), []byte("v0"))
	txn := s.Begin() // snapshot at this point
	// A concurrent write after the snapshot must NOT be visible to txn.
	s.Put([]byte("k"), []byte("v1"))
	if v, ok, _ := txn.Get([]byte("k")); !ok || string(v) != "v0" {
		t.Fatalf("txn snapshot should see v0, got %q ok=%v", v, ok)
	}
}

func TestTxnScanMergesLocalAndSnapshot(t *testing.T) {
	s := newTxnStorage(t)
	s.Put([]byte("a"), []byte("a0"))
	s.Put([]byte("b"), []byte("b0"))
	s.Put([]byte("c"), []byte("c0"))
	txn := s.Begin()
	txn.Put([]byte("b"), []byte("bX")) // override
	txn.Delete([]byte("c"))            // hide
	txn.Put([]byte("d"), []byte("dX")) // new

	it, err := txn.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	type kv struct{ k, v string }
	var got []kv
	for it.IsValid() {
		got = append(got, kv{string(it.Key()), string(it.Value())})
		it.Next()
	}
	want := []kv{{"a", "a0"}, {"b", "bX"}, {"d", "dX"}}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d got %v want %v", i, got[i], want[i])
		}
	}
}

func TestTxnRollbackDiscards(t *testing.T) {
	s := newTxnStorage(t)
	txn := s.Begin()
	txn.Put([]byte("a"), []byte("1"))
	txn.Rollback()
	if _, ok, _ := s.Get([]byte("a")); ok {
		t.Fatal("rolled-back write must not appear in the engine")
	}
	// Watermark returns to latest once the reader is gone.
	if s.mvcc.watermark() != s.mvcc.latestTs() {
		t.Fatal("watermark should equal latest ts after rollback")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/lsm/ -run TestTxn`
Expected: build failure — `s.Begin undefined`.

- [ ] **Step 3: Add Begin to the engine**

In `internal/lsm/storage.go`, add:
```go
// Begin starts a transaction reading a consistent snapshot at the latest
// committed timestamp.
func (s *Storage) Begin() *Txn {
	readTs := s.mvcc.latestTs()
	s.mvcc.addReader(readTs)
	return &Txn{
		engine:    s,
		readTs:    readTs,
		local:     map[string][]byte{},
		accessSet: map[uint64]struct{}{},
	}
}
```

- [ ] **Step 4: Implement the transaction**

Create `internal/lsm/txn.go`:
```go
package lsm

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"mythdb/internal/iterator"
)

// ErrSerialization is returned by Commit when a serialization conflict is found.
var ErrSerialization = errors.New("lsm: transaction aborted due to serialization conflict")

// Txn is an optimistic, serializable-snapshot-isolation transaction. Reads see a
// snapshot at readTs merged with the transaction's own buffered writes; writes are
// staged locally and applied atomically at Commit.
type Txn struct {
	engine *Storage
	readTs uint64

	mu        sync.Mutex
	local     map[string][]byte   // user key -> value; empty value means delete
	accessSet map[uint64]struct{} // hashes of keys read or written
	done      bool
}

// Put stages an insert/overwrite.
func (t *Txn) Put(key, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.local[string(key)] = append([]byte(nil), value...)
	t.accessSet[hashKey(key)] = struct{}{}
}

// Delete stages a tombstone.
func (t *Txn) Delete(key []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.local[string(key)] = []byte{}
	t.accessSet[hashKey(key)] = struct{}{}
}

// Get returns the value for key in the transaction's view (local writes first,
// then the snapshot), recording the access for conflict detection.
func (t *Txn) Get(key []byte) ([]byte, bool, error) {
	t.mu.Lock()
	t.accessSet[hashKey(key)] = struct{}{}
	if v, ok := t.local[string(key)]; ok {
		t.mu.Unlock()
		if len(v) == 0 {
			return nil, false, nil
		}
		return append([]byte(nil), v...), true, nil
	}
	t.mu.Unlock()

	it, err := t.engine.buildMvccScan(key, nil, t.readTs)
	if err != nil {
		return nil, false, err
	}
	if it.IsValid() && bytes.Equal(it.Key(), key) {
		return append([]byte(nil), it.Value()...), true, nil
	}
	return nil, false, nil
}

// Scan returns an iterator over [lower, upper) in the transaction's view.
func (t *Txn) Scan(lower, upper []byte) (iterator.StorageIterator, error) {
	eng, err := t.engine.buildMvccScan(lower, upper, t.readTs)
	if err != nil {
		return nil, err
	}
	local := t.localIterator(lower, upper)
	return newTxnIterator(local, eng, t), nil
}

// localIterator snapshots the local buffer into a sorted, range-bounded iterator
// over user keys (including deletes, so they can shadow snapshot results).
func (t *Txn) localIterator(lower, upper []byte) *txnLocalIterator {
	t.mu.Lock()
	defer t.mu.Unlock()
	keys := make([]string, 0, len(t.local))
	for k := range t.local {
		if lower != nil && k < string(lower) {
			continue
		}
		if upper != nil && k >= string(upper) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	entries := make([]txnLocalEntry, len(keys))
	for i, k := range keys {
		entries[i] = txnLocalEntry{key: []byte(k), value: append([]byte(nil), t.local[k]...)}
	}
	return &txnLocalIterator{entries: entries}
}

// recordRead notes a user key the transaction observed during a scan.
func (t *Txn) recordRead(userKey []byte) {
	t.mu.Lock()
	t.accessSet[hashKey(userKey)] = struct{}{}
	t.mu.Unlock()
}

// Rollback abandons the transaction.
func (t *Txn) Rollback() {
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return
	}
	t.done = true
	t.mu.Unlock()
	t.engine.mvcc.removeReader(t.readTs)
}

type txnLocalEntry struct {
	key, value []byte
}

// txnLocalIterator iterates a sorted slice of local buffer entries (user keys),
// including deletes (empty value).
type txnLocalIterator struct {
	entries []txnLocalEntry
	i       int
}

func (it *txnLocalIterator) IsValid() bool { return it.i < len(it.entries) }
func (it *txnLocalIterator) Key() []byte   { return it.entries[it.i].key }
func (it *txnLocalIterator) Value() []byte { return it.entries[it.i].value }
func (it *txnLocalIterator) Next() error   { it.i++; return nil }

// txnIterator merges the local buffer (user keys, includes deletes) with the
// engine snapshot scan (user keys, live only). Local wins ties; entries whose
// chosen value is empty (a local delete) are skipped. Yielded keys are recorded.
type txnIterator struct {
	local iterator.StorageIterator
	eng   iterator.StorageIterator
	txn   *Txn
	key   []byte
	value []byte
	valid bool
	err   error
}

func newTxnIterator(local, eng iterator.StorageIterator, txn *Txn) *txnIterator {
	it := &txnIterator{local: local, eng: eng, txn: txn}
	it.advance()
	return it
}

func (it *txnIterator) advance() {
	for {
		lv := it.local.IsValid()
		ev := it.eng.IsValid()
		if !lv && !ev {
			it.valid = false
			return
		}
		useLocal := false
		switch {
		case lv && ev:
			c := bytes.Compare(it.local.Key(), it.eng.Key())
			if c < 0 {
				useLocal = true
			} else if c > 0 {
				useLocal = false
			} else {
				// Same key: local overrides; advance the engine past the shadowed version.
				useLocal = true
				if err := it.eng.Next(); err != nil {
					it.err = err
					it.valid = false
					return
				}
			}
		case lv:
			useLocal = true
		default:
			useLocal = false
		}

		var k, v []byte
		if useLocal {
			k = it.local.Key()
			v = it.local.Value()
			if err := it.local.Next(); err != nil {
				it.err = err
				it.valid = false
				return
			}
		} else {
			k = it.eng.Key()
			v = it.eng.Value()
			if err := it.eng.Next(); err != nil {
				it.err = err
				it.valid = false
				return
			}
		}
		if len(v) == 0 {
			continue // local delete; skip
		}
		it.key = append([]byte(nil), k...)
		it.value = append([]byte(nil), v...)
		it.valid = true
		it.txn.recordRead(it.key)
		return
	}
}

func (it *txnIterator) IsValid() bool { return it.err == nil && it.valid }
func (it *txnIterator) Key() []byte   { return it.key }
func (it *txnIterator) Value() []byte { return it.value }

func (it *txnIterator) Next() error {
	if !it.valid {
		return it.err
	}
	it.advance()
	return it.err
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/lsm/ -run TestTxn && go test ./... && go vet ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/lsm/txn.go internal/lsm/storage.go internal/lsm/txn_test.go
git commit -m "feat: transactions with snapshot reads, local buffer, and read-set tracking"
```

---

## Task 3: Transaction commit with SSI validation

**Files:**
- Modify: `internal/lsm/txn.go` (add `Commit`)
- Test: `internal/lsm/txn_commit_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/lsm/txn_commit_test.go`:
```go
package lsm

import (
	"errors"
	"testing"
)

func TestTxnCommitApplies(t *testing.T) {
	s := newTxnStorage(t)
	txn := s.Begin()
	txn.Put([]byte("a"), []byte("1"))
	txn.Put([]byte("b"), []byte("2"))
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := s.Get([]byte("a")); !ok || string(v) != "1" {
		t.Fatalf("committed a = %q ok=%v", v, ok)
	}
	if v, ok, _ := s.Get([]byte("b")); !ok || string(v) != "2" {
		t.Fatalf("committed b = %q ok=%v", v, ok)
	}
}

func TestTxnWriteWriteConflictAborts(t *testing.T) {
	s := newTxnStorage(t)
	t1 := s.Begin()
	t2 := s.Begin()
	t1.Put([]byte("k"), []byte("t1"))
	t2.Put([]byte("k"), []byte("t2"))
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1 commit should succeed: %v", err)
	}
	// t2 wrote the same key and started before t1 committed -> conflict.
	if err := t2.Commit(); !errors.Is(err, ErrSerialization) {
		t.Fatalf("t2 commit should abort with ErrSerialization, got %v", err)
	}
	if v, _, _ := s.Get([]byte("k")); string(v) != "t1" {
		t.Fatalf("k should be t1 (t2 aborted), got %q", v)
	}
}

func TestTxnReadWriteConflictAborts(t *testing.T) {
	s := newTxnStorage(t)
	s.Put([]byte("k"), []byte("v0"))
	t1 := s.Begin()
	// t1 reads k, then plans to write l based on it.
	if _, _, err := t1.Get([]byte("k")); err != nil {
		t.Fatal(err)
	}
	t1.Put([]byte("l"), []byte("derived"))

	// t2 writes k and commits before t1.
	t2 := s.Begin()
	t2.Put([]byte("k"), []byte("v1"))
	if err := t2.Commit(); err != nil {
		t.Fatal(err)
	}
	// t1's read set (k) intersects t2's write set (k) -> abort.
	if err := t1.Commit(); !errors.Is(err, ErrSerialization) {
		t.Fatalf("t1 commit should abort (read-write conflict), got %v", err)
	}
}

func TestTxnNonConflictingCommit(t *testing.T) {
	s := newTxnStorage(t)
	t1 := s.Begin()
	t2 := s.Begin()
	t1.Put([]byte("a"), []byte("1"))
	t2.Put([]byte("b"), []byte("2"))
	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("disjoint txn should commit, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/lsm/ -run TestTxnCommit`
Expected: build failure — `txn.Commit undefined`.

- [ ] **Step 3: Implement Commit**

Add to `internal/lsm/txn.go` (add `"mythdb/internal/key"` and `"mythdb/internal/memtable"` to its import block):
```go
// Commit validates the transaction against newer committers (serializable
// snapshot isolation) and, if it passes, applies its buffered writes atomically
// at a fresh commit timestamp.
func (t *Txn) Commit() error {
	mvccc := t.engine.mvcc
	mvccc.commitMu.Lock()
	defer mvccc.commitMu.Unlock()

	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return errors.New("lsm: transaction already finished")
	}
	access := t.accessSet
	writeSet := make(map[uint64]struct{}, len(t.local))
	entriesKV := make([][2][]byte, 0, len(t.local))
	for k, v := range t.local {
		writeSet[hashKey([]byte(k))] = struct{}{}
		entriesKV = append(entriesKV, [2][]byte{[]byte(k), v})
	}
	t.done = true
	t.mu.Unlock()

	// SSI: abort if a transaction that committed after our snapshot wrote a key we
	// accessed (read or wrote).
	if mvccc.hasConflict(t.readTs, access) {
		mvccc.removeReader(t.readTs)
		return ErrSerialization
	}

	// Nothing to write: a read-only transaction just releases its reader.
	if len(entriesKV) == 0 {
		mvccc.removeReader(t.readTs)
		mvccc.pruneCommitted()
		return nil
	}

	commitTs := mvccc.nextTs()
	entries := make([]memtable.Entry, len(entriesKV))
	for i, kv := range entriesKV {
		entries[i] = memtable.Entry{Key: key.Encode(kv[0], commitTs), Value: kv[1]}
	}
	if err := t.engine.writeEncodedBatch(entries); err != nil {
		// The write failed; the reader is still removed so we do not stall the
		// watermark, but the commit did not take effect.
		mvccc.removeReader(t.readTs)
		return err
	}
	mvccc.recordCommitted(commitTs, writeSet)
	mvccc.removeReader(t.readTs)
	mvccc.pruneCommitted()
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/lsm/ && go test ./... && go vet ./... && go test -race ./internal/lsm/`
Expected: PASS everywhere; no data races.

- [ ] **Step 5: Commit**

```bash
git add internal/lsm/txn.go internal/lsm/txn_commit_test.go
git commit -m "feat: transaction commit with serializable snapshot isolation"
```

---

## Task 4: Watermark-driven garbage collection in compaction

**Files:**
- Modify: `internal/lsm/compact.go` (`doCompact` takes a watermark; GC at the bottom level; `runOnceCompaction` passes it)
- Test: `internal/lsm/gc_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/lsm/gc_test.go`:
```go
package lsm

import (
	"fmt"
	"testing"
)

// sstKeyCount opens every SST referenced by the state and counts total entries
// (all versions) by scanning the raw merged tiers — used to assert GC happened.
func liveVersionCount(t *testing.T, s *Storage, userKey string) int {
	t.Helper()
	st := s.snapshot()
	count := 0
	// Count versions across L0 and levels by scanning each SST.
	for _, id := range st.l0 {
		count += countVersionsInSST(t, s, id, userKey)
	}
	for _, lvl := range st.levels {
		for _, id := range lvl {
			count += countVersionsInSST(t, s, id, userKey)
		}
	}
	return count
}

func TestGCCollapsesVersionsNoReaders(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{
		Path: dir, BlockSize: 256, TargetSSTSize: 1 << 20,
		Compaction: CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Five versions of "k" across two SSTs.
	for i := 0; i < 3; i++ {
		s.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	for i := 3; i < 5; i++ {
		s.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()

	// No open transactions -> watermark = latest -> GC collapses to one version.
	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}
	if n := liveVersionCount(t, s, "k"); n != 1 {
		t.Fatalf("expected 1 retained version after GC, got %d", n)
	}
	if v, ok, _ := s.Get([]byte("k")); !ok || string(v) != "v4" {
		t.Fatalf("newest value must survive GC: %q ok=%v", v, ok)
	}
}

func TestGCKeepsVersionsForOpenReader(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{
		Path: dir, BlockSize: 256, TargetSSTSize: 1 << 20,
		Compaction: CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put([]byte("k"), []byte("v0"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()

	// Open a reader at this snapshot, then write more versions.
	txn := s.Begin()
	for i := 1; i < 4; i++ {
		s.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
	}
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()

	// Watermark is held down by the open reader, so the version it can see (v0)
	// must survive compaction.
	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := txn.Get([]byte("k")); !ok || string(v) != "v0" {
		t.Fatalf("open reader must still see v0 after GC: %q ok=%v", v, ok)
	}
	txn.Rollback()
}

func TestGCReclaimsTombstone(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{
		Path: dir, BlockSize: 256, TargetSSTSize: 1 << 20,
		Compaction: CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put([]byte("k"), []byte("v"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()
	s.Delete([]byte("k"))
	s.ForceFreezeMemtable()
	s.ForceFlushNextImmMemtable()

	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}
	// With no readers, the deleted key and its tombstone are fully reclaimed.
	if n := liveVersionCount(t, s, "k"); n != 0 {
		t.Fatalf("expected tombstone and value reclaimed, got %d versions", n)
	}
	if _, ok, _ := s.Get([]byte("k")); ok {
		t.Fatal("k must read as absent after tombstone GC")
	}
}
```

Also create the SST-version-counting helper `internal/lsm/gc_helper_test.go`:
```go
package lsm

import (
	"testing"

	"mythdb/internal/key"
	"mythdb/internal/sstable"
)

// countVersionsInSST counts how many stored versions of userKey exist in SST id.
func countVersionsInSST(t *testing.T, s *Storage, id int, userKey string) int {
	t.Helper()
	sst := s.snapshot().sstables[id]
	it, err := sstable.NewIterAndSeekToFirst(sst)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for it.IsValid() {
		if string(key.UserKey(it.Key())) == userKey {
			n++
		}
		it.Next()
	}
	return n
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/lsm/ -run TestGC`
Expected: FAIL — compaction currently retains ALL versions (3A), so the version
counts are higher than the GC-collapsed expectations.

- [ ] **Step 3: Add watermark GC to doCompact**

In `internal/lsm/compact.go`, add `"bytes"` and `"mythdb/internal/key"` to the
import block. Change the `doCompact` signature to accept a watermark and replace its
merge loop with version-aware GC. Replace:
```go
func (s *Storage) doCompact(inputIDs []int, toBottomLevel bool) ([]*sstable.SsTable, error) {
```
with:
```go
func (s *Storage) doCompact(inputIDs []int, toBottomLevel bool, watermark uint64) ([]*sstable.SsTable, error) {
```
Then replace the merge loop:
```go
	_ = toBottomLevel // Week 3B uses this with the watermark to GC old versions.
	for merged.IsValid() {
		k := merged.Key()
		v := merged.Value()
		if builder == nil {
			builder = sstable.NewBuilder(s.opts.BlockSize)
		}
		builder.Add(k, v)
		if int64(builder.EstimatedSize()) >= s.opts.TargetSSTSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		if err := merged.Next(); err != nil {
			return nil, err
		}
	}
```
with (only the bottom level garbage-collects; other levels keep all versions so a
deeper level cannot resurrect a shadowed key):
```go
	// effWatermark drives GC only at the bottom level; above it, keep all versions.
	effWatermark := uint64(0)
	if toBottomLevel {
		effWatermark = watermark
	}
	var lastUser []byte
	keptBelow := false // already kept the newest <= effWatermark version for lastUser
	for merged.IsValid() {
		k := merged.Key()
		v := merged.Value()
		user := key.UserKey(k)
		ts := key.Timestamp(k)
		if !bytes.Equal(user, lastUser) {
			lastUser = append([]byte(nil), user...)
			keptBelow = false
		}

		keep := false
		if ts > effWatermark {
			keep = true // a reader above the watermark may still need this version
		} else if !keptBelow {
			keptBelow = true
			// Newest version at or below the watermark: keep it unless it is a
			// tombstone (nothing below needs it at the bottom level).
			if len(v) != 0 {
				keep = true
			}
		}
		// else: an older version <= watermark; reclaim it.

		if keep {
			if builder == nil {
				builder = sstable.NewBuilder(s.opts.BlockSize)
			}
			builder.Add(k, v)
			if int64(builder.EstimatedSize()) >= s.opts.TargetSSTSize {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
		if err := merged.Next(); err != nil {
			return nil, err
		}
	}
```

- [ ] **Step 4: Pass the watermark from runOnceCompaction**

In `internal/lsm/compact.go`, in `runOnceCompaction`, capture the watermark when the
task starts and pass it to `doCompact`. Find:
```go
	inputIDs := task.InputIDs()
	newSSTs, err := s.doCompact(inputIDs, task.ToBottom)
	if err != nil {
		return false, err
	}
```
and replace with:
```go
	inputIDs := task.InputIDs()
	watermark := s.mvcc.watermark()
	newSSTs, err := s.doCompact(inputIDs, task.ToBottom, watermark)
	if err != nil {
		return false, err
	}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/lsm/ && go test ./... && go vet ./... && go test -race ./internal/lsm/`
Expected: PASS everywhere; no data races. The Week 2A `TestFullCompactionDropsTombstones`
and 3A `TestCompactionRetainsVersionsForReads` still pass (with no open readers the
watermark is the latest ts, so GC collapses to the newest version, which the MVCC
read returns; tombstones at the bottom are reclaimed).

- [ ] **Step 6: Run the demo**

Run: `go run ./cmd/mythdb`
Expected: unchanged observable output.

- [ ] **Step 7: Commit**

```bash
git add internal/lsm/compact.go internal/lsm/gc_test.go internal/lsm/gc_helper_test.go
git commit -m "feat: watermark-driven version GC during compaction"
```

---

## Self-Review Notes

- **Spec coverage:** mvcc controller with watermark + committed history (Task 1),
  Txn snapshot reads + local buffer + read-set (Task 2), Commit with SSI validation
  (Task 3), watermark GC in compaction (Task 4). All 3B spec components map to a task.
- **Type consistency:** `mvcc.{addReader,removeReader,watermark,recordCommitted,pruneCommitted,hasConflict,committedCount}`;
  `Storage.{Begin,Write,writeEncodedBatch,hashKey}`; `Txn.{Put,Delete,Get,Scan,Commit,Rollback}` + `ErrSerialization`;
  `txnLocalIterator`, `txnIterator`; `doCompact(inputIDs, toBottomLevel, watermark)`.
- **Serialization correctness:** all commits (transactional and non-transactional
  `Write`) take `commitMu`, allocate the commit ts, apply, and record their write
  set, so the SSI conflict check sees every committer. The access set includes both
  reads and writes, so write-write and read-write conflicts both abort the later
  committer.
- **Watermark/GC correctness:** GC runs only at the bottom level; it keeps every
  version above the watermark plus the newest version at/below it (dropping that one
  too if it is a tombstone). An open transaction holds the watermark down so the
  version it can read survives. With no open transactions the watermark equals the
  latest ts and overwritten keys collapse to one version.
- **Concurrency:** the controller's mutable state is mutex-guarded; commits serialize
  on `commitMu`; the txn's own state is guarded by its mutex (a txn is used by one
  goroutine but the guard keeps `-race` clean). Verified with `go test -race`.
- **Backward compatibility:** existing Week 1/2/3A tests stay green — no open
  transactions means the watermark equals the latest ts and committed history prunes
  to empty after each write.
```
