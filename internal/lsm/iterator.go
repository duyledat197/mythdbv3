package lsm

import (
	"mythdb/internal/iterator"
	"mythdb/internal/key"
)

// lsmIterator wraps a merge iterator to (a) stop at an exclusive upper bound and
// (b) skip tombstones (entries with an empty value).
type lsmIterator struct {
	inner iterator.StorageIterator
	upper []byte // exclusive; nil = unbounded
	err   error
}

func newLsmIterator(inner iterator.StorageIterator, upper []byte) (*lsmIterator, error) {
	it := &lsmIterator{inner: inner, upper: upper}
	if err := it.skipDeleted(); err != nil {
		return nil, err
	}
	return it, nil
}

func (it *lsmIterator) inBound() bool {
	if !it.inner.IsValid() {
		return false
	}
	if it.upper != nil && key.Compare(it.inner.Key(), it.upper) >= 0 {
		return false
	}
	return true
}

// skipDeleted advances past tombstones while staying in bounds.
func (it *lsmIterator) skipDeleted() error {
	for it.inBound() && len(it.inner.Value()) == 0 {
		if err := it.inner.Next(); err != nil {
			it.err = err
			return err
		}
	}
	return nil
}

func (it *lsmIterator) IsValid() bool { return it.err == nil && it.inBound() }
func (it *lsmIterator) Key() []byte   { return it.inner.Key() }
func (it *lsmIterator) Value() []byte { return it.inner.Value() }

func (it *lsmIterator) Next() error {
	if err := it.inner.Next(); err != nil {
		it.err = err
		return err
	}
	return it.skipDeleted()
}

// fusedIterator guards against use after exhaustion or error: once invalid it
// stays invalid and never calls the wrapped iterator again.
type fusedIterator struct {
	inner  iterator.StorageIterator
	hasErr bool
}

func newFusedIterator(inner iterator.StorageIterator) *fusedIterator {
	return &fusedIterator{inner: inner}
}

func (it *fusedIterator) IsValid() bool { return !it.hasErr && it.inner.IsValid() }
func (it *fusedIterator) Key() []byte   { return it.inner.Key() }
func (it *fusedIterator) Value() []byte { return it.inner.Value() }

func (it *fusedIterator) Next() error {
	if it.hasErr {
		return nil
	}
	if !it.inner.IsValid() {
		return nil
	}
	if err := it.inner.Next(); err != nil {
		it.hasErr = true
		return err
	}
	return nil
}
