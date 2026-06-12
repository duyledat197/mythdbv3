package lsm

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"mythdb/internal/manifest"
	"mythdb/internal/memtable"
	"mythdb/internal/sstable"
)

// removeInts returns src with every id in drop removed, preserving order.
func removeInts(src, drop []int) []int {
	if len(drop) == 0 {
		return src
	}
	dropSet := make(map[int]struct{}, len(drop))
	for _, id := range drop {
		dropSet[id] = struct{}{}
	}
	out := src[:0:0]
	for _, id := range src {
		if _, ok := dropSet[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// recover rebuilds engine state from an existing manifest plus WALs.
func (s *Storage) recover(manifestPath string) error {
	records, man, err := manifest.Recover(manifestPath)
	if err != nil {
		return err
	}
	s.manifest = man

	numLevels := 0
	if s.controller != nil {
		numLevels = s.controller.NumLevels()
	}
	levels := make([][]int, numLevels)
	var l0 []int
	memSet := map[int]struct{}{}
	maxID := -1
	bump := func(id int) {
		if id > maxID {
			maxID = id
		}
	}

	for _, r := range records {
		switch r.Kind {
		case manifest.KindNewMemtable:
			memSet[r.ID] = struct{}{}
			bump(r.ID)
		case manifest.KindFlush:
			delete(memSet, r.ID)
			l0 = append([]int{r.ID}, l0...) // newest first, as flush prepends
			bump(r.ID)
		case manifest.KindCompaction:
			if r.UpperLevel == 0 {
				l0 = removeInts(l0, r.UpperIDs)
			} else {
				levels[r.UpperLevel-1] = removeInts(levels[r.UpperLevel-1], r.UpperIDs)
			}
			levels[r.LowerLevel-1] = removeInts(levels[r.LowerLevel-1], r.LowerIDs)
			levels[r.LowerLevel-1] = append([]int(nil), r.NewIDs...)
			for _, id := range r.NewIDs {
				bump(id)
			}
		}
	}

	// Open surviving SSTs.
	sstables := map[int]*sstable.SsTable{}
	openIDs := append([]int(nil), l0...)
	for _, lv := range levels {
		openIDs = append(openIDs, lv...)
	}
	for _, id := range openIDs {
		sst, err := sstable.Open(id, s.sstPath(id))
		if err != nil {
			return err
		}
		sstables[id] = sst
	}

	// Recover unflushed memtables (ascending id), newest-first in immMemtables.
	memIDs := make([]int, 0, len(memSet))
	for id := range memSet {
		memIDs = append(memIDs, id)
	}
	sort.Ints(memIDs)
	imm := make([]*memtable.Memtable, 0, len(memIDs))
	for i := len(memIDs) - 1; i >= 0; i-- {
		mt, err := memtable.RecoverWAL(memIDs[i], s.walPath(memIDs[i]), s.opts.SyncWrites)
		if err != nil {
			return err
		}
		if mt.IsEmpty() {
			// A frozen-but-empty memtable carries no data; drop it so we don't
			// flush an empty SST. Its WAL becomes an orphan, cleaned up below.
			mt.CloseWAL()
			continue
		}
		imm = append(imm, mt)
	}

	// Fresh active memtable above all recovered ids.
	s.nextID = maxID + 1
	activeID := s.allocID()
	active, err := memtable.NewWithWAL(activeID, s.walPath(activeID), s.opts.SyncWrites)
	if err != nil {
		return err
	}
	if err := s.manifest.AddRecord(manifest.Record{Kind: manifest.KindNewMemtable, ID: activeID}); err != nil {
		return err
	}

	s.st = &state{
		memtable:     active,
		immMemtables: imm,
		l0:           l0,
		levels:       levels,
		sstables:     sstables,
	}

	// Remove SST/WAL files no longer referenced by the recovered state (e.g. a
	// superseded SST whose deletion was interrupted by a crash, or a flushed/
	// empty memtable's leftover WAL).
	keepWAL := map[int]struct{}{activeID: {}}
	for _, mt := range imm {
		keepWAL[mt.ID()] = struct{}{}
	}
	keepSST := make(map[int]struct{}, len(openIDs))
	for _, id := range openIDs {
		keepSST[id] = struct{}{}
	}
	s.deleteOrphans(keepWAL, keepSST)
	return nil
}

// deleteOrphans removes *.wal / *.sst files in the data directory whose id is
// not referenced by the recovered state. Best-effort: errors are ignored.
func (s *Storage) deleteOrphans(keepWAL, keepSST map[int]struct{}) {
	entries, err := os.ReadDir(s.opts.Path)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".wal"):
			if id, err := strconv.Atoi(strings.TrimSuffix(name, ".wal")); err == nil {
				if _, keep := keepWAL[id]; !keep {
					os.Remove(filepath.Join(s.opts.Path, name))
				}
			}
		case strings.HasSuffix(name, ".sst"):
			if id, err := strconv.Atoi(strings.TrimSuffix(name, ".sst")); err == nil {
				if _, keep := keepSST[id]; !keep {
					os.Remove(filepath.Join(s.opts.Path, name))
				}
			}
		}
	}
}
