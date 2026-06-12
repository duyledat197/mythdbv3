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
	TargetSSTSize int64  // memtable freeze threshold in bytes
}

// state is an immutable-by-convention snapshot of the engine's tiers.
type state struct {
	memtable     *memtable.Memtable
	immMemtables []*memtable.Memtable // newest first
	l0           []*sstable.SsTable   // newest first
}

// Storage is the LSM engine.
type Storage struct {
	mu     sync.RWMutex
	st     *state
	opts   Options
	nextID int
}

// Open initializes an empty engine. (Recovery from disk arrives in Week 2.)
func Open(opts Options) (*Storage, error) {
	if opts.BlockSize == 0 {
		opts.BlockSize = 4096
	}
	if opts.TargetSSTSize == 0 {
		opts.TargetSSTSize = 4 << 20
	}
	return &Storage{
		st:     &state{memtable: memtable.New(0)},
		opts:   opts,
		nextID: 1,
	}, nil
}

func (s *Storage) snapshot() *state {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st
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

	for _, sst := range st.l0 {
		if !sst.MayContain(k) {
			continue
		}
		it, err := sstable.NewIterAndSeekToKey(sst, k)
		if err != nil {
			return nil, false, err
		}
		if it.IsValid() && key.Compare(it.Key(), k) == 0 {
			return resolve(it.Value())
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

// Scan returns an iterator over [lower, upper). nil bounds are unbounded.
func (s *Storage) Scan(lower, upper []byte) (iterator.StorageIterator, error) {
	st := s.snapshot()

	var memIters []iterator.StorageIterator
	memIters = append(memIters, st.memtable.Iter(lower, upper))
	for _, m := range st.immMemtables {
		memIters = append(memIters, m.Iter(lower, upper))
	}
	memMerge := iterator.NewMergeIterator(memIters)

	var sstIters []iterator.StorageIterator
	for _, sst := range st.l0 {
		var it iterator.StorageIterator
		var err error
		if lower == nil {
			it, err = sstable.NewIterAndSeekToFirst(sst)
		} else {
			it, err = sstable.NewIterAndSeekToKey(sst, lower)
		}
		if err != nil {
			return nil, err
		}
		sstIters = append(sstIters, it)
	}
	sstMerge := iterator.NewMergeIterator(sstIters)

	combined, err := iterator.NewTwoMergeIterator(memMerge, sstMerge)
	if err != nil {
		return nil, err
	}
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
		memtable:     memtable.New(s.nextID),
		immMemtables: newImm,
		l0:           s.st.l0,
	}
	s.nextID++
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

	id := s.nextID
	s.nextID++
	path := filepath.Join(s.opts.Path, fmt.Sprintf("%05d.sst", id))
	builder := sstable.NewBuilder(s.opts.BlockSize)
	it := target.Iter(nil, nil)
	for it.IsValid() {
		builder.Add(it.Key(), it.Value())
		it.Next()
	}
	sst, err := builder.Build(id, path)
	if err != nil {
		return err
	}

	newImm := append([]*memtable.Memtable(nil), s.st.immMemtables[:flushIdx]...)
	newL0 := append([]*sstable.SsTable{sst}, s.st.l0...)
	s.st = &state{
		memtable:     s.st.memtable,
		immMemtables: newImm,
		l0:           newL0,
	}
	return nil
}

// Close releases SST file handles.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sst := range s.st.l0 {
		sst.Close()
	}
	return nil
}
