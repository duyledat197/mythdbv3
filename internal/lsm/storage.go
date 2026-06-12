// Package lsm assembles memtables and SSTs into a read/write LSM engine.
package lsm

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

	controller compaction.Controller
}

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

// newLeveledController is implemented in Task 6 (leveled compaction).
func newLeveledController(c CompactionOptions) compaction.Controller { return nil }

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
