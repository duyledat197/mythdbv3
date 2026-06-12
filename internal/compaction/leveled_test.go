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
