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
		target[i] = target[i+1] / int64(l.Multiplier)
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
		newL0 = nilIfEmpty(removeIDs(l0, t.UpperIDs))
	} else {
		newLevels[t.UpperLevel-1] = nilIfEmpty(removeIDs(newLevels[t.UpperLevel-1], t.UpperIDs))
	}
	// Whole-level merge: the destination level becomes exactly newIDs.
	newLevels[t.LowerLevel-1] = nilIfEmpty(append([]int(nil), newIDs...))

	if t.UpperLevel == 0 {
		return newL0, newLevels
	}
	return l0, newLevels
}

// nilIfEmpty returns nil when s is empty, preserving DeepEqual semantics with nil slices.
func nilIfEmpty(s []int) []int {
	if len(s) == 0 {
		return nil
	}
	return s
}
