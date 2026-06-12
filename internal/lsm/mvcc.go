package lsm

import "sync"

// committedTxn records a finished transaction's commit timestamp and the set of
// key hashes it wrote, for serializable-snapshot-isolation conflict checks.
type committedTxn struct {
	ts       uint64
	writeSet map[uint64]struct{}
}

// mvcc is the multi-version concurrency controller: a monotonic commit-timestamp
// counter, the set of open transactions' read timestamps (for the watermark), and
// recent committers' write sets (for SSI validation).
type mvcc struct {
	mu        sync.Mutex
	ts        uint64
	readers   map[uint64]int // open read timestamp -> active count
	committed []committedTxn

	commitMu sync.Mutex // serializes commit critical sections (validate + apply)
}

func newMvcc(initial uint64) *mvcc {
	return &mvcc{ts: initial, readers: map[uint64]int{}}
}

// latestTs returns the latest committed timestamp.
func (m *mvcc) latestTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ts
}

// nextTs increments and returns the new commit timestamp.
func (m *mvcc) nextTs() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ts++
	return m.ts
}

// setTs raises the counter to at least ts (used during recovery).
func (m *mvcc) setTs(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ts > m.ts {
		m.ts = ts
	}
}

// addReader registers an open transaction reading at ts.
func (m *mvcc) addReader(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readers[ts]++
}

// removeReader unregisters one open transaction reading at ts.
func (m *mvcc) removeReader(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readers[ts] <= 1 {
		delete(m.readers, ts)
	} else {
		m.readers[ts]--
	}
}

// watermark is the minimum open read timestamp, or the latest commit ts if no
// transaction is open. Versions at or below it (except the newest such per key)
// can be reclaimed.
func (m *mvcc) watermark() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.readers) == 0 {
		return m.ts
	}
	min := ^uint64(0)
	for ts := range m.readers {
		if ts < min {
			min = ts
		}
	}
	return min
}

// recordCommitted appends a committer's write set to the history.
func (m *mvcc) recordCommitted(ts uint64, writeSet map[uint64]struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committed = append(m.committed, committedTxn{ts: ts, writeSet: writeSet})
}

// pruneCommitted drops history entries no open transaction can still need (ts at
// or below the watermark).
func (m *mvcc) pruneCommitted() {
	wm := m.watermark()
	m.mu.Lock()
	defer m.mu.Unlock()
	kept := m.committed[:0:0]
	for _, c := range m.committed {
		if c.ts > wm {
			kept = append(kept, c)
		}
	}
	m.committed = kept
}

// committedCount reports the number of retained committed-txn records (test aid).
func (m *mvcc) committedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.committed)
}

// hasConflict reports whether any committed transaction with ts > readTs wrote a
// key in accessSet (a serialization conflict for a transaction that read at readTs
// and touched those keys).
func (m *mvcc) hasConflict(readTs uint64, accessSet map[uint64]struct{}) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.committed {
		if c.ts <= readTs {
			continue
		}
		for h := range c.writeSet {
			if _, ok := accessSet[h]; ok {
				return true
			}
		}
	}
	return false
}
