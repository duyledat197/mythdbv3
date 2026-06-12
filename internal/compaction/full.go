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
