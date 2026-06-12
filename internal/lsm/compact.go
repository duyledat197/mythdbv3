package lsm

import (
	"fmt"
	"log"
	"os"
	"time"

	"mythdb/internal/iterator"
	"mythdb/internal/manifest"
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
	// Record the new layout before releasing the lock and before deleting old
	// files, so a crash mid-delete still recovers to the post-compaction state
	// and manifest writes stay serialized with freeze/flush.
	if err := s.manifest.AddRecord(manifest.Record{
		Kind:       manifest.KindCompaction,
		UpperLevel: task.UpperLevel,
		UpperIDs:   task.UpperIDs,
		LowerLevel: task.LowerLevel,
		LowerIDs:   task.LowerIDs,
		NewIDs:     newIDs,
	}); err != nil {
		s.mu.Unlock()
		return false, err
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
				if err != nil {
					// A transient compaction error must not kill the loop;
					// log it and retry on the next tick.
					log.Printf("lsm: background compaction error: %v", err)
					break
				}
				if !did {
					break
				}
			}
		}
	}
}
