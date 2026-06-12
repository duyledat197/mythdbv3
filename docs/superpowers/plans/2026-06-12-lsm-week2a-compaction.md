# LSM Tree Week 2A (Compaction) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add level-based compaction (full + leveled) to the Week 1 LSM engine, with a level-aware read path and a background compaction goroutine.

**Architecture:** Refactor engine state to key SSTs by integer id (`l0 []int`, `levels [][]int`, `sstables map[int]*SsTable`). Add an `SstConcatIterator` for sorted runs. A pure `compaction` package decides tasks (full/leveled); the engine executes them by merging input SSTs into new ones (dropping tombstones at the bottom level) and atomically swapping state. A background goroutine drives leveled compaction.

**Tech Stack:** Go 1.26, standard library only. Builds on Week 1 packages.

**Spec:** `docs/superpowers/specs/2026-06-12-lsm-week2a-compaction-design.md`.

**Conventions:**
- Module path `mythdb`. Run tests from repo root.
- Commit after each task with the message shown. Use `git -c user.name='Claude' -c user.email='noreply@anthropic.com' commit` if git identity is unset.
- After every task run `go test ./...` AND `go vet ./...` and confirm green before committing.
- Key ordering always via `key.Compare`. Tombstone = zero-length value.

---

## File Structure

```
internal/
  iterator/concat.go          (new)  SstConcatIterator
  sstable/sstable.go          (modify) add Size()
  sstable/builder.go          (modify) add EstimatedSize(), record size
  compaction/compaction.go    (new)  Task, Controller, helpers
  compaction/full.go          (new)  Full controller
  compaction/leveled.go       (new)  Leveled controller
  lsm/storage.go              (rewrite) id-based state, level-aware Get/Scan, allocID
  lsm/compact.go              (new)  doCompact, runOnceCompaction, background goroutine
```

---

## Task 1: SstConcatIterator

**Files:**
- Create: `internal/iterator/concat.go`
- Test: `internal/iterator/concat_test.go`

This iterates a sorted run of non-overlapping SSTs as one stream. `internal/iterator` may import `internal/sstable` (sstable does not import iterator, so no cycle).

- [ ] **Step 1: Write the failing test**

Create `internal/iterator/concat_test.go`:
```go
package iterator

import (
	"fmt"
	"path/filepath"
	"testing"

	"mythdb/internal/sstable"
)

// makeSST writes keys [startInclusive, endExclusive) as key%05d/val%05d.
func makeSST(t *testing.T, dir string, id, start, end int) *sstable.SsTable {
	t.Helper()
	b := sstable.NewBuilder(4096)
	for i := start; i < end; i++ {
		b.Add([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	sst, err := b.Build(id, filepath.Join(dir, fmt.Sprintf("%d.sst", id)))
	if err != nil {
		t.Fatal(err)
	}
	return sst
}

func TestConcatSeekToFirst(t *testing.T) {
	dir := t.TempDir()
	tables := []*sstable.SsTable{
		makeSST(t, dir, 1, 0, 10),
		makeSST(t, dir, 2, 10, 20),
		makeSST(t, dir, 3, 20, 30),
	}
	it, err := NewConcatIterAndSeekToFirst(tables)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for it.IsValid() {
		want := fmt.Sprintf("key%05d", count)
		if string(it.Key()) != want {
			t.Fatalf("at %d got %q want %q", count, it.Key(), want)
		}
		count++
		it.Next()
	}
	if count != 30 {
		t.Fatalf("count=%d want 30", count)
	}
}

func TestConcatSeekToKeyCrossesSST(t *testing.T) {
	dir := t.TempDir()
	tables := []*sstable.SsTable{
		makeSST(t, dir, 1, 0, 10),
		makeSST(t, dir, 2, 10, 20),
		makeSST(t, dir, 3, 20, 30),
	}
	// key00015 lands in the middle table
	it, err := NewConcatIterAndSeekToKey(tables, []byte("key00015"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.IsValid() || string(it.Key()) != "key00015" {
		t.Fatalf("seek -> %q valid=%v", it.Key(), it.IsValid())
	}
	// key00009 is the last key of the first table
	it, err = NewConcatIterAndSeekToKey(tables, []byte("key00009"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.IsValid() || string(it.Key()) != "key00009" {
		t.Fatalf("seek boundary -> %q", it.Key())
	}
	// past everything
	it, err = NewConcatIterAndSeekToKey(tables, []byte("key99999"))
	if err != nil {
		t.Fatal(err)
	}
	if it.IsValid() {
		t.Fatal("seek past end should be invalid")
	}
	// gap key between tables: key00010 exists (start of table 2); use a gap by
	// seeking to a key just after a table's range that does not exist but lands
	// on next table's first key.
	it, err = NewConcatIterAndSeekToKey(tables, []byte("key00010"))
	if err != nil {
		t.Fatal(err)
	}
	if !it.IsValid() || string(it.Key()) != "key00010" {
		t.Fatalf("seek next-table-first -> %q", it.Key())
	}
}

func TestConcatEmpty(t *testing.T) {
	it, err := NewConcatIterAndSeekToFirst(nil)
	if err != nil {
		t.Fatal(err)
	}
	if it.IsValid() {
		t.Fatal("empty concat should be invalid")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/iterator/ -run TestConcat`
Expected: build failure — `undefined: NewConcatIterAndSeekToFirst`.

- [ ] **Step 3: Write the implementation**

Create `internal/iterator/concat.go`:
```go
package iterator

import (
	"sort"

	"mythdb/internal/key"
	"mythdb/internal/sstable"
)

// SstConcatIterator iterates a sorted run of non-overlapping SSTs as a single
// stream. The tables must be sorted by key with disjoint ranges.
type SstConcatIterator struct {
	tables []*sstable.SsTable
	idx    int
	cur    *sstable.Iterator
}

// NewConcatIterAndSeekToFirst positions on the first key of the run.
func NewConcatIterAndSeekToFirst(tables []*sstable.SsTable) (*SstConcatIterator, error) {
	it := &SstConcatIterator{tables: tables}
	if len(tables) == 0 {
		it.idx = 0
		return it, nil
	}
	cur, err := sstable.NewIterAndSeekToFirst(tables[0])
	if err != nil {
		return nil, err
	}
	it.idx = 0
	it.cur = cur
	if err := it.skipExhausted(); err != nil {
		return nil, err
	}
	return it, nil
}

// NewConcatIterAndSeekToKey positions on the first key >= target across the run.
func NewConcatIterAndSeekToKey(tables []*sstable.SsTable, target []byte) (*SstConcatIterator, error) {
	it := &SstConcatIterator{tables: tables}
	// First table whose LastKey >= target may contain it.
	i := sort.Search(len(tables), func(i int) bool {
		return key.Compare(tables[i].LastKey(), target) >= 0
	})
	if i >= len(tables) {
		it.idx = len(tables)
		return it, nil
	}
	cur, err := sstable.NewIterAndSeekToKey(tables[i], target)
	if err != nil {
		return nil, err
	}
	it.idx = i
	it.cur = cur
	if err := it.skipExhausted(); err != nil {
		return nil, err
	}
	return it, nil
}

// skipExhausted advances to the next table when the current one is exhausted.
func (it *SstConcatIterator) skipExhausted() error {
	for it.cur != nil && !it.cur.IsValid() {
		it.idx++
		if it.idx >= len(it.tables) {
			it.cur = nil
			return nil
		}
		cur, err := sstable.NewIterAndSeekToFirst(it.tables[it.idx])
		if err != nil {
			return err
		}
		it.cur = cur
	}
	return nil
}

func (it *SstConcatIterator) IsValid() bool { return it.cur != nil && it.cur.IsValid() }
func (it *SstConcatIterator) Key() []byte   { return it.cur.Key() }
func (it *SstConcatIterator) Value() []byte { return it.cur.Value() }

func (it *SstConcatIterator) Next() error {
	if err := it.cur.Next(); err != nil {
		return err
	}
	return it.skipExhausted()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/iterator/`
Expected: PASS (existing iterator tests + new concat tests).

- [ ] **Step 5: Commit**

```bash
git add internal/iterator/concat.go internal/iterator/concat_test.go
git commit -m "feat: SstConcatIterator over sorted SST runs"
```

---

## Task 2: Engine state refactor (id-based, level-aware reads)

**Files:**
- Rewrite: `internal/lsm/storage.go`
- Test: existing `internal/lsm/storage_test.go` must still pass (no new test needed; Week 1 behavior preserved).

The state now keys SSTs by id and carries `levels`. `levels` stays empty in this task (no compaction yet), so `Get`/`Scan` still behave exactly as in Week 1.

- [ ] **Step 1: Replace storage.go**

Replace the entire contents of `internal/lsm/storage.go` with:
```go
// Package lsm assembles memtables and SSTs into a read/write LSM engine.
package lsm

import (
	"fmt"
	"path/filepath"
	"sync"

	"mythdb/internal/iterator"
	"mythdb/internal/key"
	"mythdb/internal/memtable"
	"mythdb/internal/sstable"
)

// Options configures a Storage instance.
type Options struct {
	Path          string // directory for SST files
	BlockSize     int    // target block size in bytes
	TargetSSTSize int64  // memtable freeze threshold AND target compaction SST size
}

// state is an immutable-by-convention snapshot of the engine's tiers. SSTs are
// referenced by id and resolved through sstables.
type state struct {
	memtable     *memtable.Memtable
	immMemtables []*memtable.Memtable     // newest first
	l0           []int                    // L0 SST ids, newest first
	levels       [][]int                  // levels[i] = ids of L(i+1), key-sorted, non-overlapping
	sstables     map[int]*sstable.SsTable // id -> open handle
}

// Storage is the LSM engine.
type Storage struct {
	mu   sync.RWMutex
	st   *state
	opts Options

	idMu   sync.Mutex
	nextID int
}

// Open initializes an empty engine. (Recovery from disk arrives in Week 2B.)
func Open(opts Options) (*Storage, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4096
	}
	if opts.TargetSSTSize == 0 {
		opts.TargetSSTSize = 4 << 20
	}
	return &Storage{
		st: &state{
			memtable: memtable.New(0),
			sstables: map[int]*sstable.SsTable{},
		},
		opts:   opts,
		nextID: 1,
	}, nil
}

func (s *Storage) snapshot() *state {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st
}

// allocID hands out a unique, monotonically increasing id for memtables and SSTs.
func (s *Storage) allocID() int {
	s.idMu.Lock()
	defer s.idMu.Unlock()
	id := s.nextID
	s.nextID++
	return id
}

func (s *Storage) sstPath(id int) string {
	return filepath.Join(s.opts.Path, fmt.Sprintf("%05d.sst", id))
}

func cloneSstables(m map[int]*sstable.SsTable) map[int]*sstable.SsTable {
	n := make(map[int]*sstable.SsTable, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}

// Put inserts or overwrites a key.
func (s *Storage) Put(k, v []byte) error {
	s.mu.Lock()
	s.st.memtable.Put(k, v)
	full := s.st.memtable.ApproximateSize() >= s.opts.TargetSSTSize
	s.mu.Unlock()
	if full {
		return s.ForceFreezeMemtable()
	}
	return nil
}

// Delete writes a tombstone for the key.
func (s *Storage) Delete(k []byte) error {
	return s.Put(k, []byte{})
}

// getFromSST returns (found, value) for k in one SST, consulting the bloom
// filter first. found==true with an empty value means a tombstone.
func getFromSST(sst *sstable.SsTable, k []byte) (bool, []byte, error) {
	if !sst.MayContain(k) {
		return false, nil, nil
	}
	it, err := sstable.NewIterAndSeekToKey(sst, k)
	if err != nil {
		return false, nil, err
	}
	if it.IsValid() && key.Compare(it.Key(), k) == 0 {
		return true, it.Value(), nil
	}
	return false, nil, nil
}

// Get returns the value for k and whether it exists (tombstones read as absent).
func (s *Storage) Get(k []byte) ([]byte, bool, error) {
	st := s.snapshot()

	if v, ok := st.memtable.Get(k); ok {
		return resolve(v)
	}
	for _, m := range st.immMemtables {
		if v, ok := m.Get(k); ok {
			return resolve(v)
		}
	}

	// L0: each SST may overlap; check newest first.
	for _, id := range st.l0 {
		found, v, err := getFromSST(st.sstables[id], k)
		if err != nil {
			return nil, false, err
		}
		if found {
			return resolve(v)
		}
	}

	// Levels: each level is a sorted, non-overlapping run.
	for _, level := range st.levels {
		for _, id := range level {
			sst := st.sstables[id]
			if key.Compare(k, sst.FirstKey()) < 0 || key.Compare(k, sst.LastKey()) > 0 {
				continue
			}
			found, v, err := getFromSST(sst, k)
			if err != nil {
				return nil, false, err
			}
			if found {
				return resolve(v)
			}
		}
	}
	return nil, false, nil
}

// resolve maps a stored value to the Get contract: empty value == deleted.
func resolve(v []byte) ([]byte, bool, error) {
	if len(v) == 0 {
		return nil, false, nil
	}
	return v, true, nil
}

func seekSST(sst *sstable.SsTable, lower []byte) (iterator.StorageIterator, error) {
	if lower == nil {
		return sstable.NewIterAndSeekToFirst(sst)
	}
	return sstable.NewIterAndSeekToKey(sst, lower)
}

// Scan returns an iterator over [lower, upper). nil bounds are unbounded.
func (s *Storage) Scan(lower, upper []byte) (iterator.StorageIterator, error) {
	st := s.snapshot()

	// Memtables (newest first) as one merge.
	memIters := []iterator.StorageIterator{st.memtable.Iter(lower, upper)}
	for _, m := range st.immMemtables {
		memIters = append(memIters, m.Iter(lower, upper))
	}
	memMerge := iterator.NewMergeIterator(memIters)

	// L0 SSTs (newest first) as one merge.
	var l0Iters []iterator.StorageIterator
	for _, id := range st.l0 {
		it, err := seekSST(st.sstables[id], lower)
		if err != nil {
			return nil, err
		}
		l0Iters = append(l0Iters, it)
	}
	l0Merge := iterator.NewMergeIterator(l0Iters)

	// Combine tiers newest -> oldest: memtables, L0, then each level concat.
	allIters := []iterator.StorageIterator{memMerge, l0Merge}
	for _, level := range st.levels {
		tables := make([]*sstable.SsTable, len(level))
		for i, id := range level {
			tables[i] = st.sstables[id]
		}
		var ci iterator.StorageIterator
		var err error
		if lower == nil {
			ci, err = iterator.NewConcatIterAndSeekToFirst(tables)
		} else {
			ci, err = iterator.NewConcatIterAndSeekToKey(tables, lower)
		}
		if err != nil {
			return nil, err
		}
		allIters = append(allIters, ci)
	}
	combined := iterator.NewMergeIterator(allIters)

	lsmIt, err := newLsmIterator(combined, upper)
	if err != nil {
		return nil, err
	}
	return newFusedIterator(lsmIt), nil
}

// ForceFreezeMemtable turns the active memtable immutable and installs a fresh one.
func (s *Storage) ForceFreezeMemtable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.memtable.IsEmpty() {
		return nil
	}
	old := s.st.memtable
	newImm := make([]*memtable.Memtable, 0, len(s.st.immMemtables)+1)
	newImm = append(newImm, old)
	newImm = append(newImm, s.st.immMemtables...)
	s.st = &state{
		memtable:     memtable.New(s.allocID()),
		immMemtables: newImm,
		l0:           s.st.l0,
		levels:       s.st.levels,
		sstables:     s.st.sstables,
	}
	return nil
}

// ForceFlushNextImmMemtable flushes the oldest immutable memtable to an L0 SST.
func (s *Storage) ForceFlushNextImmMemtable() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.st.immMemtables) == 0 {
		return fmt.Errorf("lsm: no immutable memtable to flush")
	}
	flushIdx := len(s.st.immMemtables) - 1
	target := s.st.immMemtables[flushIdx]

	id := s.allocID()
	builder := sstable.NewBuilder(s.opts.BlockSize)
	it := target.Iter(nil, nil)
	for it.IsValid() {
		builder.Add(it.Key(), it.Value())
		it.Next()
	}
	sst, err := builder.Build(id, s.sstPath(id))
	if err != nil {
		return err
	}

	newImm := append([]*memtable.Memtable(nil), s.st.immMemtables[:flushIdx]...)
	newL0 := append([]int{id}, s.st.l0...)
	newSstables := cloneSstables(s.st.sstables)
	newSstables[id] = sst
	s.st = &state{
		memtable:     s.st.memtable,
		immMemtables: newImm,
		l0:           newL0,
		levels:       s.st.levels,
		sstables:     newSstables,
	}
	return nil
}

// Close releases all SST file handles.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sst := range s.st.sstables {
		sst.Close()
	}
	return nil
}
```

- [ ] **Step 2: Run the full suite to verify Week 1 behavior is preserved**

Run: `go test ./... && go vet ./...`
Expected: PASS everywhere (the lsm tests from Week 1 are unchanged and still green; `levels` is empty so reads behave as before).

- [ ] **Step 3: Commit**

```bash
git add internal/lsm/storage.go
git commit -m "refactor: id-based engine state with level-aware Get/Scan"
```

---

## Task 3: SSTable Size() and Builder.EstimatedSize()

**Files:**
- Modify: `internal/sstable/sstable.go` (add `size` field + `Size()`)
- Modify: `internal/sstable/builder.go` (record size on Build + add `EstimatedSize()`)
- Test: `internal/sstable/size_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/sstable/size_test.go`:
```go
package sstable

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestSizeReflectsFileLength(t *testing.T) {
	dir := t.TempDir()
	b := NewBuilder(4096)
	for i := 0; i < 100; i++ {
		b.Add([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	sst, err := b.Build(1, filepath.Join(dir, "1.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if sst.Size() <= 0 {
		t.Fatalf("size should be positive, got %d", sst.Size())
	}
	reopened, err := Open(1, filepath.Join(dir, "1.sst"))
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Size() != sst.Size() {
		t.Fatalf("reopened size %d != built size %d", reopened.Size(), sst.Size())
	}
}

func TestBuilderEstimatedSizeGrows(t *testing.T) {
	b := NewBuilder(64)
	for i := 0; i < 50; i++ {
		b.Add([]byte(fmt.Sprintf("key%05d", i)), []byte(fmt.Sprintf("val%05d", i)))
	}
	if b.EstimatedSize() == 0 {
		t.Fatal("estimated size should grow after many entries flush blocks")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sstable/ -run 'TestSize|TestBuilderEstimated'`
Expected: build failure — `sst.Size undefined`, `b.EstimatedSize undefined`.

- [ ] **Step 3: Add `size` field and `Size()` to sstable.go**

In `internal/sstable/sstable.go`, add a `size int64` field to the `SsTable` struct (place it after `id int`):
```go
type SsTable struct {
	file      *os.File
	path      string
	id        int
	size      int64
	blockMeta []BlockMeta
	bloom     *bloom.Bloom
	firstKey  []byte
	lastKey   []byte
}
```

In `Open`, set the size from the stat already taken. Find the line `t := &SsTable{file: f, path: path, id: id, blockMeta: metas, bloom: bl}` and replace it with:
```go
	t := &SsTable{file: f, path: path, id: id, size: size, blockMeta: metas, bloom: bl}
```

Add this method near `ID()`:
```go
// Size returns the on-disk file size in bytes.
func (t *SsTable) Size() int64 { return t.size }
```

- [ ] **Step 4: Record size in builder.go and add EstimatedSize()**

In `internal/sstable/builder.go`, in `Build`, after `if err := os.WriteFile(path, buf, 0o644); err != nil { ... }` and before returning, set `size` on the returned struct. Replace the final `return &SsTable{...}` block with:
```go
	return &SsTable{
		file:      f,
		path:      path,
		id:        id,
		size:      int64(len(buf)),
		blockMeta: b.meta,
		bloom:     bl,
		firstKey:  b.firstKey,
		lastKey:   append([]byte(nil), b.lastKey...),
	}, nil
```

Add this method to `builder.go`:
```go
// EstimatedSize approximates the bytes written so far (finished data blocks).
func (b *Builder) EstimatedSize() int { return len(b.data) }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/sstable/`
Expected: PASS (existing SST tests + new size tests).

- [ ] **Step 6: Commit**

```bash
git add internal/sstable/
git commit -m "feat: SsTable.Size and Builder.EstimatedSize for compaction"
```

---

## Task 4: Compaction package — Task, Controller, Full strategy

**Files:**
- Create: `internal/compaction/compaction.go`
- Create: `internal/compaction/full.go`
- Test: `internal/compaction/compaction_test.go`

Pure logic over id slices — no engine or I/O dependencies.

- [ ] **Step 1: Write the failing tests**

Create `internal/compaction/compaction_test.go`:
```go
package compaction

import (
	"reflect"
	"testing"
)

func zeroSizes(int) int64 { return 0 }

func TestInputIDsUpperThenLower(t *testing.T) {
	task := &Task{UpperIDs: []int{3, 2}, LowerIDs: []int{1}}
	got := task.InputIDs()
	want := []int{3, 2, 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestFullNoWorkWhenEmpty(t *testing.T) {
	f := &Full{MaxLevels: 1}
	if task := f.GenerateTask(nil, [][]int{nil}, zeroSizes); task != nil {
		t.Fatalf("expected nil task, got %+v", task)
	}
}

func TestFullCompactsL0IntoBottom(t *testing.T) {
	f := &Full{MaxLevels: 1}
	l0 := []int{5, 4} // newest first
	levels := [][]int{{2, 1}}
	task := f.GenerateTask(l0, levels, zeroSizes)
	if task == nil {
		t.Fatal("expected a task")
	}
	if task.UpperLevel != 0 {
		t.Fatalf("UpperLevel=%d want 0", task.UpperLevel)
	}
	if !reflect.DeepEqual(task.UpperIDs, []int{5, 4}) {
		t.Fatalf("UpperIDs=%v", task.UpperIDs)
	}
	if !reflect.DeepEqual(task.LowerIDs, []int{2, 1}) {
		t.Fatalf("LowerIDs=%v", task.LowerIDs)
	}
	if task.LowerLevel != 1 || !task.ToBottom {
		t.Fatalf("LowerLevel=%d ToBottom=%v", task.LowerLevel, task.ToBottom)
	}
}

func TestFullNoWorkWhenAllInBottom(t *testing.T) {
	// L0 empty and everything already in the single bottom level -> nothing to do.
	f := &Full{MaxLevels: 1}
	if task := f.GenerateTask(nil, [][]int{{1, 2}}, zeroSizes); task != nil {
		t.Fatalf("expected nil task, got %+v", task)
	}
}

func TestFullApplyResult(t *testing.T) {
	f := &Full{MaxLevels: 1}
	l0 := []int{5, 4}
	levels := [][]int{{2, 1}}
	task := f.GenerateTask(l0, levels, zeroSizes)
	newL0, newLevels := f.ApplyResult(l0, levels, task, []int{9, 10})
	if len(newL0) != 0 {
		t.Fatalf("newL0=%v want empty", newL0)
	}
	if !reflect.DeepEqual(newLevels, [][]int{{9, 10}}) {
		t.Fatalf("newLevels=%v", newLevels)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/compaction/`
Expected: build failure — `undefined: Task`, `undefined: Full`.

- [ ] **Step 3: Write the framework**

Create `internal/compaction/compaction.go`:
```go
// Package compaction decides which SSTs to merge. It operates purely on id
// slices so strategies can be unit-tested without the storage engine.
package compaction

// Task describes one compaction: merge an upper source and a lower destination
// into LowerLevel.
type Task struct {
	UpperLevel int   // 0 means "L0 source"; otherwise a 1-based level number
	UpperIDs   []int // ids from the upper source (L0 ids when UpperLevel == 0)
	LowerLevel int   // 1-based destination level
	LowerIDs   []int // existing ids in the destination level being merged in
	ToBottom   bool  // destination is the bottom-most level -> drop tombstones
}

// InputIDs returns UpperIDs followed by LowerIDs. The upper source holds newer
// data than the destination, so this ordering is newest-first for merging.
func (t *Task) InputIDs() []int {
	out := make([]int, 0, len(t.UpperIDs)+len(t.LowerIDs))
	out = append(out, t.UpperIDs...)
	out = append(out, t.LowerIDs...)
	return out
}

// Controller selects compaction work and applies its result to engine levels.
type Controller interface {
	// NumLevels is how many non-L0 levels the engine should allocate.
	NumLevels() int
	// GenerateTask returns the next compaction, or nil if none is needed.
	// sizes maps an SST id to its on-disk byte size.
	GenerateTask(l0 []int, levels [][]int, sizes func(id int) int64) *Task
	// ApplyResult returns the new l0 and levels after replacing the task's
	// inputs with newIDs at the destination level.
	ApplyResult(l0 []int, levels [][]int, t *Task, newIDs []int) (newL0 []int, newLevels [][]int)
}

// removeIDs returns src with every id in drop removed, preserving order.
func removeIDs(src, drop []int) []int {
	if len(drop) == 0 {
		return append([]int(nil), src...)
	}
	dropSet := make(map[int]struct{}, len(drop))
	for _, id := range drop {
		dropSet[id] = struct{}{}
	}
	out := make([]int, 0, len(src))
	for _, id := range src {
		if _, ok := dropSet[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// cloneLevels deep-copies a levels slice.
func cloneLevels(levels [][]int) [][]int {
	out := make([][]int, len(levels))
	for i, lv := range levels {
		out[i] = append([]int(nil), lv...)
	}
	return out
}
```

- [ ] **Step 4: Write the Full controller**

Create `internal/compaction/full.go`:
```go
package compaction

// Full merges everything into a single bottom level (MaxLevels). It only
// produces a task when there is data above the bottom level (any L0 SST, or any
// non-bottom level populated), so it does not re-compact an already-merged run.
type Full struct {
	MaxLevels int // number of non-L0 levels; bottom level is this value (1-based)
}

func (f *Full) NumLevels() int { return f.MaxLevels }

func (f *Full) GenerateTask(l0 []int, levels [][]int, sizes func(int) int64) *Task {
	hasUpper := len(l0) > 0
	for i := 0; i < len(levels)-1; i++ { // every level except the bottom
		if len(levels[i]) > 0 {
			hasUpper = true
		}
	}
	if !hasUpper {
		return nil
	}
	var lowerIDs []int
	for _, lv := range levels {
		lowerIDs = append(lowerIDs, lv...)
	}
	return &Task{
		UpperLevel: 0,
		UpperIDs:   append([]int(nil), l0...),
		LowerLevel: f.MaxLevels,
		LowerIDs:   lowerIDs,
		ToBottom:   true,
	}
}

func (f *Full) ApplyResult(l0 []int, levels [][]int, t *Task, newIDs []int) ([]int, [][]int) {
	// Preserve any L0 SST that arrived after the task was generated (e.g. a
	// concurrent flush); only the task's own inputs are superseded.
	newL0 := removeIDs(l0, t.UpperIDs)
	newLevels := make([][]int, len(levels))
	newLevels[len(levels)-1] = append([]int(nil), newIDs...)
	return newL0, newLevels
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/compaction/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/compaction/compaction.go internal/compaction/full.go internal/compaction/compaction_test.go
git commit -m "feat: compaction framework and full-compaction strategy"
```

---

## Task 5: Engine compaction core (doCompact + runOnceCompaction, full strategy)

**Files:**
- Modify: `internal/lsm/storage.go` (add `Compaction` to Options, controller + level sizing in Open)
- Create: `internal/lsm/compact.go`
- Test: `internal/lsm/compact_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/lsm/compact_test.go`:
```go
package lsm

import (
	"fmt"
	"testing"
)

func newFullStorage(t *testing.T) *Storage {
	t.Helper()
	s, err := Open(Options{
		Path:          t.TempDir(),
		BlockSize:     256,
		TargetSSTSize: 1 << 20,
		Compaction:    CompactionOptions{Strategy: "full"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// flushAll freezes and flushes the active memtable into an L0 SST.
func flushAll(t *testing.T, s *Storage) {
	t.Helper()
	if err := s.ForceFreezeMemtable(); err != nil {
		t.Fatal(err)
	}
	if err := s.ForceFlushNextImmMemtable(); err != nil {
		t.Fatal(err)
	}
}

func TestFullCompactionCollapsesL0(t *testing.T) {
	s := newFullStorage(t)
	// Two L0 SSTs where the second overwrites and deletes keys from the first.
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("b"), []byte("1"))
	s.Put([]byte("c"), []byte("1"))
	flushAll(t, s)
	s.Put([]byte("b"), []byte("2")) // overwrite
	s.Delete([]byte("c"))           // tombstone
	s.Put([]byte("d"), []byte("2"))
	flushAll(t, s)

	// Before compaction: 2 L0 SSTs.
	if got := len(s.snapshot().l0); got != 2 {
		t.Fatalf("expected 2 L0 SSTs, got %d", got)
	}

	did, err := s.runOnceCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if !did {
		t.Fatal("expected compaction to run")
	}

	st := s.snapshot()
	if len(st.l0) != 0 {
		t.Fatalf("L0 should be empty after full compaction, got %d", len(st.l0))
	}
	// Bottom level (levels[0] for full with MaxLevels=1) holds the merged run.
	if len(st.levels[len(st.levels)-1]) == 0 {
		t.Fatal("bottom level should hold merged SSTs")
	}

	// Correctness: newest values win, tombstoned key is gone.
	check := func(k, want string, wantOK bool) {
		v, ok, err := s.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if ok != wantOK || (wantOK && string(v) != want) {
			t.Fatalf("get %q -> %q ok=%v (want %q ok=%v)", k, v, ok, want, wantOK)
		}
	}
	check("a", "1", true)
	check("b", "2", true)
	check("c", "", false) // tombstone dropped
	check("d", "2", true)

	// A second compaction with nothing new is a no-op.
	did, err = s.runOnceCompaction()
	if err != nil {
		t.Fatal(err)
	}
	if did {
		t.Fatal("expected no-op on second compaction")
	}
}

func TestFullCompactionDropsTombstones(t *testing.T) {
	s := newFullStorage(t)
	for i := 0; i < 20; i++ {
		s.Put([]byte(fmt.Sprintf("key%03d", i)), []byte("v"))
	}
	flushAll(t, s)
	for i := 0; i < 20; i++ {
		s.Delete([]byte(fmt.Sprintf("key%03d", i)))
	}
	flushAll(t, s)

	if _, err := s.runOnceCompaction(); err != nil {
		t.Fatal(err)
	}

	// After dropping tombstones at the bottom level, a full scan yields nothing.
	it, err := s.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if it.IsValid() {
		t.Fatalf("expected empty scan, first key=%q", it.Key())
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/lsm/ -run TestFullCompaction`
Expected: build failure — `unknown field Compaction`, `s.runOnceCompaction undefined`.

- [ ] **Step 3: Add Compaction options + controller wiring to storage.go**

In `internal/lsm/storage.go`, add the import for the compaction package (in the import block, keeping alphabetical-ish grouping):
```go
	"mythdb/internal/compaction"
```

Add the `CompactionOptions` type and a `Compaction` field on `Options`. Replace the `Options` struct with:
```go
// Options configures a Storage instance.
type Options struct {
	Path          string // directory for SST files
	BlockSize     int    // target block size in bytes
	TargetSSTSize int64  // memtable freeze threshold AND target compaction SST size

	Compaction CompactionOptions
}

// CompactionOptions configures background compaction. Strategy "" disables it.
type CompactionOptions struct {
	Strategy            string        // "", "full", or "leveled"
	MaxLevels           int           // non-L0 levels (full defaults 1, leveled defaults 4)
	L0CompactionTrigger int           // leveled: L0 file count that triggers L0->L1 (default 4)
	LevelSizeMultiplier int           // leveled: size ratio between levels (default 10)
	BaseLevelSizeBytes  int64         // leveled: bottom-level base target (default 16 MiB)
	Interval            time.Duration // background tick; 0 disables the goroutine
}
```

Add a `controller` field to the `Storage` struct:
```go
type Storage struct {
	mu   sync.RWMutex
	st   *state
	opts Options

	idMu   sync.Mutex
	nextID int

	controller compaction.Controller
}
```

Add the `"time"` import as well (needed by CompactionOptions). The import block becomes:
```go
import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"mythdb/internal/compaction"
	"mythdb/internal/iterator"
	"mythdb/internal/key"
	"mythdb/internal/memtable"
	"mythdb/internal/sstable"
)
```

Replace the `Open` function with a version that builds the controller and sizes `levels`:
```go
// Open initializes an empty engine. (Recovery from disk arrives in Week 2B.)
func Open(opts Options) (*Storage, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4096
	}
	if opts.TargetSSTSize == 0 {
		opts.TargetSSTSize = 4 << 20
	}

	s := &Storage{opts: opts, nextID: 1}
	s.controller = buildController(opts.Compaction)

	var levels [][]int
	if s.controller != nil {
		levels = make([][]int, s.controller.NumLevels())
	}
	s.st = &state{
		memtable: memtable.New(0),
		levels:   levels,
		sstables: map[int]*sstable.SsTable{},
	}
	return s, nil
}

// buildController constructs the compaction controller for the given options,
// or nil when compaction is disabled.
func buildController(c CompactionOptions) compaction.Controller {
	switch c.Strategy {
	case "full":
		levels := c.MaxLevels
		if levels == 0 {
			levels = 1
		}
		return &compaction.Full{MaxLevels: levels}
	case "leveled":
		return newLeveledController(c)
	default:
		return nil
	}
}
```

> Note: `newLeveledController` is defined in Task 6. To keep this task compiling on its own, add a temporary stub at the bottom of `storage.go` for now:
> ```go
> // newLeveledController is implemented in Task 6 (leveled compaction).
> func newLeveledController(c CompactionOptions) compaction.Controller { return nil }
> ```
> Task 6 replaces this stub with the real implementation.

- [ ] **Step 4: Write compact.go**

Create `internal/lsm/compact.go`:
```go
package lsm

import (
	"fmt"
	"os"

	"mythdb/internal/iterator"
	"mythdb/internal/sstable"
)

// doCompact merges the given input SST ids (ordered newest-first) into new
// SSTs, splitting by the target SST size. When toBottomLevel is true, entries
// with an empty value (tombstones) are dropped.
func (s *Storage) doCompact(inputIDs []int, toBottomLevel bool) ([]*sstable.SsTable, error) {
	st := s.snapshot()

	var iters []iterator.StorageIterator
	for _, id := range inputIDs {
		sst := st.sstables[id]
		if sst == nil {
			return nil, fmt.Errorf("lsm: compaction input %d missing", id)
		}
		it, err := sstable.NewIterAndSeekToFirst(sst)
		if err != nil {
			return nil, err
		}
		iters = append(iters, it)
	}
	merged := iterator.NewMergeIterator(iters)

	var result []*sstable.SsTable
	var builder *sstable.Builder
	flush := func() error {
		if builder == nil {
			return nil
		}
		id := s.allocID()
		sst, err := builder.Build(id, s.sstPath(id))
		if err != nil {
			return err
		}
		result = append(result, sst)
		builder = nil
		return nil
	}

	for merged.IsValid() {
		k := merged.Key()
		v := merged.Value()
		if toBottomLevel && len(v) == 0 {
			if err := merged.Next(); err != nil {
				return nil, err
			}
			continue
		}
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
	if err := flush(); err != nil {
		return nil, err
	}
	return result, nil
}

// runOnceCompaction asks the controller for a task, executes it, atomically
// swaps in the new state, and deletes superseded SST files. It returns whether
// any work was done.
func (s *Storage) runOnceCompaction() (bool, error) {
	if s.controller == nil {
		return false, nil
	}
	st := s.snapshot()
	sizes := func(id int) int64 {
		if sst := st.sstables[id]; sst != nil {
			return sst.Size()
		}
		return 0
	}
	task := s.controller.GenerateTask(st.l0, st.levels, sizes)
	if task == nil {
		return false, nil
	}

	inputIDs := task.InputIDs()
	newSSTs, err := s.doCompact(inputIDs, task.ToBottom)
	if err != nil {
		return false, err
	}
	newIDs := make([]int, len(newSSTs))
	for i, sst := range newSSTs {
		newIDs[i] = sst.ID()
	}

	s.mu.Lock()
	newL0, newLevels := s.controller.ApplyResult(s.st.l0, s.st.levels, task, newIDs)
	newSstables := cloneSstables(s.st.sstables)
	for _, sst := range newSSTs {
		newSstables[sst.ID()] = sst
	}
	oldSstables := s.st.sstables
	for _, id := range inputIDs {
		delete(newSstables, id)
	}
	s.st = &state{
		memtable:     s.st.memtable,
		immMemtables: s.st.immMemtables,
		l0:           newL0,
		levels:       newLevels,
		sstables:     newSstables,
	}
	s.mu.Unlock()

	// Close and delete superseded SSTs after the swap.
	for _, id := range inputIDs {
		if sst := oldSstables[id]; sst != nil {
			sst.Close()
			os.Remove(s.sstPath(id))
		}
	}
	return true, nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/lsm/ && go vet ./...`
Expected: PASS (Week 1 lsm tests + new compaction tests).

- [ ] **Step 6: Commit**

```bash
git add internal/lsm/storage.go internal/lsm/compact.go internal/lsm/compact_test.go
git commit -m "feat: engine compaction core with full strategy"
```

---

## Task 6: Leveled compaction strategy

**Files:**
- Create: `internal/compaction/leveled.go`
- Modify: `internal/lsm/storage.go` (replace the `newLeveledController` stub)
- Test: `internal/compaction/leveled_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/compaction/leveled_test.go`:
```go
package compaction

import (
	"reflect"
	"testing"
)

func constSizes(size int64) func(int) int64 {
	return func(int) int64 { return size }
}

func TestLeveledL0Trigger(t *testing.T) {
	l := &Leveled{MaxLevels: 3, L0Trigger: 2, Multiplier: 10, BaseBytes: 1 << 20}
	levels := [][]int{nil, nil, nil}
	// 2 L0 files meets the trigger -> L0 into L1.
	task := l.GenerateTask([]int{5, 4}, levels, constSizes(0))
	if task == nil {
		t.Fatal("expected an L0 task")
	}
	if task.UpperLevel != 0 || task.LowerLevel != 1 {
		t.Fatalf("upper=%d lower=%d", task.UpperLevel, task.LowerLevel)
	}
	if !reflect.DeepEqual(task.UpperIDs, []int{5, 4}) {
		t.Fatalf("UpperIDs=%v", task.UpperIDs)
	}
}

func TestLeveledNoL0TaskBelowTrigger(t *testing.T) {
	l := &Leveled{MaxLevels: 3, L0Trigger: 4, Multiplier: 10, BaseBytes: 1 << 20}
	levels := [][]int{nil, nil, nil}
	// 1 L0 file, all levels empty -> nothing to do.
	if task := l.GenerateTask([]int{1}, levels, constSizes(0)); task != nil {
		t.Fatalf("expected nil, got %+v", task)
	}
}

func TestLeveledSizeRatioSelectsLevel(t *testing.T) {
	// 3 levels. Bottom (L3) base target large; L1 over its small target triggers.
	l := &Leveled{MaxLevels: 3, L0Trigger: 100, Multiplier: 10, BaseBytes: 100}
	// L1 has one big SST (size 1000), L2 empty, L3 has data (size 100).
	levels := [][]int{{1}, nil, {9}}
	sizes := func(id int) int64 {
		switch id {
		case 1:
			return 1000
		case 9:
			return 100
		}
		return 0
	}
	task := l.GenerateTask(nil, levels, sizes)
	if task == nil {
		t.Fatal("expected a size-triggered task")
	}
	// L1 (1-based) is way over target -> compact L1 into L2.
	if task.UpperLevel != 1 || task.LowerLevel != 2 {
		t.Fatalf("upper=%d lower=%d", task.UpperLevel, task.LowerLevel)
	}
	if !reflect.DeepEqual(task.UpperIDs, []int{1}) {
		t.Fatalf("UpperIDs=%v", task.UpperIDs)
	}
}

func TestLeveledApplyResultL0(t *testing.T) {
	l := &Leveled{MaxLevels: 3, L0Trigger: 2, Multiplier: 10, BaseBytes: 1 << 20}
	l0 := []int{5, 4}
	levels := [][]int{{3}, nil, nil}
	task := &Task{UpperLevel: 0, UpperIDs: []int{5, 4}, LowerLevel: 1, LowerIDs: []int{3}}
	newL0, newLevels := l.ApplyResult(l0, levels, task, []int{7})
	if len(newL0) != 0 {
		t.Fatalf("newL0=%v want empty", newL0)
	}
	if !reflect.DeepEqual(newLevels, [][]int{{7}, nil, nil}) {
		t.Fatalf("newLevels=%v", newLevels)
	}
}

func TestLeveledApplyResultLevel(t *testing.T) {
	l := &Leveled{MaxLevels: 3, L0Trigger: 100, Multiplier: 10, BaseBytes: 100}
	levels := [][]int{{1}, {2}, nil}
	task := &Task{UpperLevel: 1, UpperIDs: []int{1}, LowerLevel: 2, LowerIDs: []int{2}}
	newL0, newLevels := l.ApplyResult(nil, levels, task, []int{8})
	if newL0 != nil {
		t.Fatalf("newL0=%v want nil", newL0)
	}
	if !reflect.DeepEqual(newLevels, [][]int{nil, {8}, nil}) {
		t.Fatalf("newLevels=%v", newLevels)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/compaction/ -run TestLeveled`
Expected: build failure — `undefined: Leveled`.

- [ ] **Step 3: Write the Leveled controller**

Create `internal/compaction/leveled.go`:
```go
package compaction

// Leveled compaction keeps each non-L0 level a single sorted run whose target
// size grows by Multiplier per level. When a level exceeds its target it is
// merged wholesale into the next level. L0 is merged into L1 once it reaches
// L0Trigger files.
type Leveled struct {
	MaxLevels  int   // number of non-L0 levels
	L0Trigger  int   // L0 file count that triggers L0 -> L1
	Multiplier int   // size ratio between adjacent levels
	BaseBytes  int64 // base target size for the bottom level
}

func (l *Leveled) NumLevels() int { return l.MaxLevels }

func (l *Leveled) GenerateTask(l0 []int, levels [][]int, sizes func(int) int64) *Task {
	// L0 -> L1 when enough L0 files have accumulated.
	if len(l0) >= l.L0Trigger && l.L0Trigger > 0 {
		return &Task{
			UpperLevel: 0,
			UpperIDs:   append([]int(nil), l0...),
			LowerLevel: 1,
			LowerIDs:   append([]int(nil), levels[0]...),
			ToBottom:   l.MaxLevels == 1,
		}
	}

	bottom := l.MaxLevels - 1

	cur := make([]int64, l.MaxLevels)
	for i := 0; i < l.MaxLevels; i++ {
		for _, id := range levels[i] {
			cur[i] += sizes(id)
		}
	}

	target := make([]int64, l.MaxLevels)
	target[bottom] = cur[bottom]
	if target[bottom] < l.BaseBytes {
		target[bottom] = l.BaseBytes
	}
	for i := bottom - 1; i >= 0; i-- {
		if target[i+1] > l.BaseBytes {
			target[i] = target[i+1] / int64(l.Multiplier)
		} else {
			target[i] = 0
		}
	}

	// Pick the non-bottom level most over its target.
	best := -1
	var bestRatio float64
	for i := 0; i < bottom; i++ {
		if target[i] <= 0 || cur[i] <= target[i] {
			continue
		}
		ratio := float64(cur[i]) / float64(target[i])
		if ratio > bestRatio {
			bestRatio = ratio
			best = i
		}
	}
	if best == -1 {
		return nil
	}
	lower := best + 1
	return &Task{
		UpperLevel: best + 1, // 1-based level number
		UpperIDs:   append([]int(nil), levels[best]...),
		LowerLevel: lower + 1,
		LowerIDs:   append([]int(nil), levels[lower]...),
		ToBottom:   lower == bottom,
	}
}

func (l *Leveled) ApplyResult(l0 []int, levels [][]int, t *Task, newIDs []int) ([]int, [][]int) {
	newL0 := l0
	newLevels := cloneLevels(levels)

	if t.UpperLevel == 0 {
		newL0 = removeIDs(l0, t.UpperIDs)
	} else {
		newLevels[t.UpperLevel-1] = removeIDs(newLevels[t.UpperLevel-1], t.UpperIDs)
	}
	// Whole-level merge: the destination level becomes exactly newIDs.
	newLevels[t.LowerLevel-1] = append([]int(nil), newIDs...)

	if t.UpperLevel == 0 {
		return newL0, newLevels
	}
	return l0, newLevels
}
```

> Note on `ApplyResult` return for level→level tasks: `l0` is returned unchanged
> (the L0 slice is not touched by a level compaction). For an L0 task, the
> reduced `newL0` is returned.

- [ ] **Step 4: Replace the stub in storage.go**

In `internal/lsm/storage.go`, replace the temporary stub:
```go
// newLeveledController is implemented in Task 6 (leveled compaction).
func newLeveledController(c CompactionOptions) compaction.Controller { return nil }
```
with the real constructor that applies defaults:
```go
// newLeveledController builds a leveled controller, filling in defaults.
func newLeveledController(c CompactionOptions) compaction.Controller {
	maxLevels := c.MaxLevels
	if maxLevels == 0 {
		maxLevels = 4
	}
	trigger := c.L0CompactionTrigger
	if trigger == 0 {
		trigger = 4
	}
	mult := c.LevelSizeMultiplier
	if mult == 0 {
		mult = 10
	}
	base := c.BaseLevelSizeBytes
	if base == 0 {
		base = 16 << 20
	}
	return &compaction.Leveled{
		MaxLevels:  maxLevels,
		L0Trigger:  trigger,
		Multiplier: mult,
		BaseBytes:  base,
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/compaction/ && go test ./internal/lsm/ && go vet ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/compaction/leveled.go internal/compaction/leveled_test.go internal/lsm/storage.go
git commit -m "feat: leveled compaction strategy"
```

---

## Task 7: Background compaction goroutine + lifecycle

**Files:**
- Modify: `internal/lsm/compact.go` (add `startCompaction`, `stopCompaction`)
- Modify: `internal/lsm/storage.go` (Storage gets stop channel + WaitGroup; Open starts goroutine; Close stops it)
- Test: `internal/lsm/compact_test.go` (add background + leveled end-to-end tests)

- [ ] **Step 1: Write the failing tests**

Append to `internal/lsm/compact_test.go`:
```go

func TestLeveledBackgroundCompaction(t *testing.T) {
	s, err := Open(Options{
		Path:          t.TempDir(),
		BlockSize:     256,
		TargetSSTSize: 1024, // small so compaction output splits and levels move
		Compaction: CompactionOptions{
			Strategy:            "leveled",
			MaxLevels:           3,
			L0CompactionTrigger: 1, // drain every flushed L0 SST deterministically
			LevelSizeMultiplier: 4,
			BaseLevelSizeBytes:  2048,
			Interval:            5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Write several flushed SSTs so the L0 trigger fires repeatedly.
	for batch := 0; batch < 6; batch++ {
		for i := 0; i < 30; i++ {
			k := []byte(fmt.Sprintf("key%05d", batch*30+i))
			s.Put(k, []byte(fmt.Sprintf("val%05d", batch*30+i)))
		}
		flushAll(t, s)
	}

	// Wait for the background goroutine to drain L0 into levels.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(s.snapshot().l0) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	st := s.snapshot()
	if len(st.l0) != 0 {
		t.Fatalf("expected L0 drained by background compaction, still %d", len(st.l0))
	}
	levelTotal := 0
	for _, lv := range st.levels {
		levelTotal += len(lv)
	}
	if levelTotal == 0 {
		t.Fatal("expected SSTs to have moved into levels")
	}

	// All written keys must still be readable.
	for i := 0; i < 180; i++ {
		k := fmt.Sprintf("key%05d", i)
		v, ok, err := s.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(v) != fmt.Sprintf("val%05d", i) {
			t.Fatalf("get %q -> %q ok=%v", k, v, ok)
		}
	}
}

func TestCloseStopsBackgroundGoroutine(t *testing.T) {
	s, err := Open(Options{
		Path:          t.TempDir(),
		BlockSize:     256,
		TargetSSTSize: 1024,
		Compaction: CompactionOptions{
			Strategy: "leveled",
			Interval: 5 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Put([]byte("a"), []byte("1"))
	flushAll(t, s)
	// Close must return promptly and stop the goroutine (verified under -race).
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
```

Add the `"time"` import to the test file's import block:
```go
import (
	"fmt"
	"testing"
	"time"
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/lsm/ -run 'TestLeveledBackground|TestCloseStops'`
Expected: compile/runtime failure — background goroutine never starts, so L0 is never drained (test times out / fails the assertion). It may also fail to compile if `time` is unused elsewhere; that is expected until Step 3.

- [ ] **Step 3: Add goroutine fields and lifecycle to storage.go**

In `internal/lsm/storage.go`, add stop-channel and WaitGroup fields to `Storage`:
```go
type Storage struct {
	mu   sync.RWMutex
	st   *state
	opts Options

	idMu   sync.Mutex
	nextID int

	controller compaction.Controller

	stopCh chan struct{}
	wg     sync.WaitGroup
}
```

In `Open`, after `s.controller = buildController(opts.Compaction)` and after `s.st` is assigned, start the goroutine when configured. Replace the `return s, nil` at the end of `Open` with:
```go
	if s.controller != nil && opts.Compaction.Interval > 0 {
		s.startCompaction(opts.Compaction.Interval)
	}
	return s, nil
```

Replace `Close` with a version that stops the goroutine before closing handles:
```go
// Close stops background compaction and releases all SST file handles.
func (s *Storage) Close() error {
	s.stopCompaction()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sst := range s.st.sstables {
		sst.Close()
	}
	return nil
}
```

- [ ] **Step 4: Add goroutine start/stop to compact.go**

Append to `internal/lsm/compact.go` (and add `"time"` to its import block, which becomes `import ( "fmt"; "os"; "time"; ... )`):
```go
// startCompaction launches the background compaction loop.
func (s *Storage) startCompaction(interval time.Duration) {
	s.stopCh = make(chan struct{})
	s.wg.Add(1)
	go s.compactionLoop(interval)
}

// stopCompaction signals the loop to exit and waits for it.
func (s *Storage) stopCompaction() {
	if s.stopCh == nil {
		return
	}
	close(s.stopCh)
	s.wg.Wait()
	s.stopCh = nil
}

// compactionLoop runs compaction on each tick until stopped, draining all
// available work per tick.
func (s *Storage) compactionLoop(interval time.Duration) {
	defer s.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			for {
				did, err := s.runOnceCompaction()
				if err != nil || !did {
					break
				}
			}
		}
	}
}
```

Update the import block at the top of `compact.go` to include `"time"`:
```go
import (
	"fmt"
	"os"
	"time"

	"mythdb/internal/iterator"
	"mythdb/internal/sstable"
)
```

- [ ] **Step 5: Run tests to verify they pass (including the race detector)**

Run: `go test ./... && go test -race ./internal/lsm/ && go vet ./...`
Expected: PASS everywhere; no data races reported.

- [ ] **Step 6: Update the demo to show compaction (optional but recommended)**

Modify `cmd/mythdb/main.go` so the engine is opened with leveled compaction enabled, demonstrating the feature. Change the `lsm.Open` call to:
```go
	s, err := lsm.Open(lsm.Options{
		Path:          dir,
		BlockSize:     4096,
		TargetSSTSize: 1 << 20,
		Compaction: lsm.CompactionOptions{
			Strategy: "leveled",
			Interval: 10 * time.Millisecond,
		},
	})
```
Add `"time"` to the imports of `cmd/mythdb/main.go`. Run `go run ./cmd/mythdb` and confirm the Get/Scan output is unchanged from Week 1 (compaction must not alter observable results).

- [ ] **Step 7: Commit**

```bash
git add internal/lsm/ cmd/mythdb/main.go
git commit -m "feat: background compaction goroutine with clean shutdown"
```

---

## Self-Review Notes

- **Spec coverage:** state refactor (Task 2), SstConcatIterator (Task 1), level-aware Get/Scan (Task 2), `doCompact` + tombstone dropping (Task 5), full strategy (Task 4), leveled strategy (Task 6), background goroutine + `Close` lifecycle (Task 7), `Size`/`EstimatedSize` enablers (Task 3). All spec components map to a task.
- **Type consistency:** `compaction.Task{UpperLevel,UpperIDs,LowerLevel,LowerIDs,ToBottom}` + `InputIDs()`; `Controller{NumLevels,GenerateTask,ApplyResult}` implemented by `Full` and `Leveled`; engine helpers `allocID`, `sstPath`, `cloneSstables`, `getFromSST`, `seekSST`; `doCompact(inputIDs []int, toBottomLevel bool)`; `runOnceCompaction() (bool, error)`. Names are consistent across tasks.
- **Termination/no-infinite-loop:** `Full` has `MaxLevels=1` and only triggers when L0 (or a non-bottom level) is non-empty, so a fully merged run is not re-compacted. `Leveled` returns nil once levels are within target. The background loop drains per tick then waits.
- **Concurrency:** `doCompact` runs lock-free; only the state swap holds the write lock. Superseded SSTs are closed/deleted after the swap. `Close` stops the goroutine (via `stopCh` + `WaitGroup`) before closing handles. Verified with `go test -race` in Task 7.
- **Week 1 preservation:** Task 2 keeps all Week 1 lsm tests green (empty `levels` ⇒ identical read behavior); the SST format and lower-layer packages are only extended additively (Task 3).
- **Deferred (Week 2B):** manifest, WAL, crash recovery — `Open` still starts empty.
```
