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
