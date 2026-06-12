package lsm

import (
	"bytes"

	"mythdb/internal/iterator"
	"mythdb/internal/key"
)

// mvccIterator reads a merged stream of encoded keys (user key ascending, ts
// descending) at a fixed read timestamp. For each user key it yields the newest
// version with ts <= readTs, skipping tombstones, and decodes the user key.
type mvccIterator struct {
	inner  iterator.StorageIterator // encoded keys
	readTs uint64
	upper  []byte // exclusive user-key bound; nil = unbounded
	curKey []byte // current decoded user key (nil when exhausted)
	curVal []byte
	err    error
}

func newMvccIterator(inner iterator.StorageIterator, readTs uint64, upper []byte) (*mvccIterator, error) {
	it := &mvccIterator{inner: inner, readTs: readTs, upper: upper}
	if err := it.findNext(); err != nil {
		return nil, err
	}
	return it, nil
}

// findNext positions on the next visible (user key, value), or marks exhausted.
func (it *mvccIterator) findNext() error {
	for it.inner.IsValid() {
		user := key.UserKey(it.inner.Key())
		if it.upper != nil && bytes.Compare(user, it.upper) >= 0 {
			it.curKey = nil
			return nil
		}
		// Skip versions of this user key newer than readTs.
		for it.inner.IsValid() &&
			bytes.Equal(key.UserKey(it.inner.Key()), user) &&
			key.Timestamp(it.inner.Key()) > it.readTs {
			if err := it.inner.Next(); err != nil {
				it.err = err
				return err
			}
		}
		if !it.inner.IsValid() || !bytes.Equal(key.UserKey(it.inner.Key()), user) {
			continue // no version <= readTs for this user key; move on
		}
		val := it.inner.Value()
		if len(val) == 0 {
			// Tombstone: skip all remaining versions of this user key.
			if err := it.skipUser(user); err != nil {
				return err
			}
			continue
		}
		it.curKey = append([]byte(nil), user...)
		it.curVal = append([]byte(nil), val...)
		return nil
	}
	it.curKey = nil
	return nil
}

// skipUser advances past every remaining version of the given user key.
func (it *mvccIterator) skipUser(user []byte) error {
	for it.inner.IsValid() && bytes.Equal(key.UserKey(it.inner.Key()), user) {
		if err := it.inner.Next(); err != nil {
			it.err = err
			return err
		}
	}
	return nil
}

func (it *mvccIterator) IsValid() bool { return it.err == nil && it.curKey != nil }
func (it *mvccIterator) Key() []byte   { return it.curKey }
func (it *mvccIterator) Value() []byte { return it.curVal }

func (it *mvccIterator) Next() error {
	if it.curKey == nil {
		return nil
	}
	prev := it.curKey
	it.curKey = nil
	if err := it.skipUser(prev); err != nil {
		return err
	}
	return it.findNext()
}
