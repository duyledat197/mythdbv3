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
