package iterator

import "mythdb/internal/key"

// TwoMergeIterator merges exactly two iterators. On equal keys, a (left) wins.
type TwoMergeIterator struct {
	a, b StorageIterator
	useA bool
}

// NewTwoMergeIterator combines a (newer) and b (older).
func NewTwoMergeIterator(a, b StorageIterator) (*TwoMergeIterator, error) {
	t := &TwoMergeIterator{a: a, b: b}
	if err := t.skipB(); err != nil {
		return nil, err
	}
	t.useA = t.chooseA()
	return t, nil
}

func (t *TwoMergeIterator) chooseA() bool {
	if !t.a.IsValid() {
		return false
	}
	if !t.b.IsValid() {
		return true
	}
	return key.Compare(t.a.Key(), t.b.Key()) <= 0
}

// skipB advances b past any key that a also holds, so a wins ties.
func (t *TwoMergeIterator) skipB() error {
	if t.a.IsValid() && t.b.IsValid() && key.Compare(t.a.Key(), t.b.Key()) == 0 {
		return t.b.Next()
	}
	return nil
}

func (t *TwoMergeIterator) IsValid() bool {
	if t.useA {
		return t.a.IsValid()
	}
	return t.b.IsValid()
}

func (t *TwoMergeIterator) Key() []byte {
	if t.useA {
		return t.a.Key()
	}
	return t.b.Key()
}

func (t *TwoMergeIterator) Value() []byte {
	if t.useA {
		return t.a.Value()
	}
	return t.b.Value()
}

func (t *TwoMergeIterator) Next() error {
	if t.useA {
		if err := t.a.Next(); err != nil {
			return err
		}
	} else {
		if err := t.b.Next(); err != nil {
			return err
		}
	}
	if err := t.skipB(); err != nil {
		return err
	}
	t.useA = t.chooseA()
	return nil
}
