package lsm

import "mythdb/internal/iterator"

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
