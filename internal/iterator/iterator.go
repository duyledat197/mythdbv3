// Package iterator defines the shared forward cursor abstraction and merge
// iterators used to combine LSM tiers.
package iterator

// StorageIterator is a one-way cursor over sorted key-value pairs.
// Key/Value are only meaningful while IsValid reports true.
type StorageIterator interface {
	Key() []byte
	Value() []byte
	IsValid() bool
	Next() error
}
