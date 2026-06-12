package iterator

import "testing"

// sliceIter is a test-only StorageIterator over a sorted slice of pairs.
type pair struct{ k, v string }

type sliceIter struct {
	data []pair
	i    int
}

func newSliceIter(data []pair) *sliceIter { return &sliceIter{data: data} }
func (s *sliceIter) Key() []byte          { return []byte(s.data[s.i].k) }
func (s *sliceIter) Value() []byte        { return []byte(s.data[s.i].v) }
func (s *sliceIter) IsValid() bool        { return s.i < len(s.data) }
func (s *sliceIter) Next() error          { s.i++; return nil }

func collect(it StorageIterator) []pair {
	var out []pair
	for it.IsValid() {
		out = append(out, pair{string(it.Key()), string(it.Value())})
		if err := it.Next(); err != nil {
			panic(err)
		}
	}
	return out
}

func TestMergeIteratorNewestWins(t *testing.T) {
	// index 0 is newest and must win on duplicate keys.
	a := newSliceIter([]pair{{"a", "1"}, {"c", "1"}})
	b := newSliceIter([]pair{{"a", "2"}, {"b", "2"}, {"c", "2"}})
	m := NewMergeIterator([]StorageIterator{a, b})
	got := collect(m)
	want := []pair{{"a", "1"}, {"b", "2"}, {"c", "1"}}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d got %v want %v", i, got[i], want[i])
		}
	}
}

func TestMergeIteratorEmpty(t *testing.T) {
	m := NewMergeIterator(nil)
	if m.IsValid() {
		t.Fatal("empty merge should be invalid")
	}
}

func TestTwoMergeIteratorLeftWins(t *testing.T) {
	a := newSliceIter([]pair{{"a", "L"}, {"b", "L"}})
	b := newSliceIter([]pair{{"b", "R"}, {"c", "R"}})
	m, err := NewTwoMergeIterator(a, b)
	if err != nil {
		t.Fatal(err)
	}
	got := collect(m)
	want := []pair{{"a", "L"}, {"b", "L"}, {"c", "R"}}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d got %v want %v", i, got[i], want[i])
		}
	}
}
