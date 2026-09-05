package cdcstats

import (
	"sync"
	"time"
)

type TableStats struct {
	QualifiedName string
	SchemaName    string
	TableName     string

	Inserts     int64
	Updates     int64
	Deletes     int64
	TotalEvents int64
	LastEventTs time.Time

	dirty bool
}

type Accumulator struct {
	pipelineID string
	mu         sync.Mutex
	byTable    map[string]*TableStats
}

func NewAccumulator(pipelineID string) *Accumulator {
	return &Accumulator{
		pipelineID: pipelineID,
		byTable:    make(map[string]*TableStats),
	}
}

func (a *Accumulator) Observe(u TableUpdate) {
	a.mu.Lock()
	defer a.mu.Unlock()

	st := a.byTable[u.QualifiedName]
	if st == nil {
		st = &TableStats{
			QualifiedName: u.QualifiedName,
			SchemaName:    u.SchemaName,
			TableName:     u.TableName,
		}
		a.byTable[u.QualifiedName] = st
	}

	switch u.Op {
	case "c", "r":
		st.Inserts++
	case "u":
		st.Updates++
	case "d":
		st.Deletes++
	default:
		// ignore unknown
	}
	st.TotalEvents++
	st.LastEventTs = u.Timestamp
	st.dirty = true
}

func (a *Accumulator) FlushDirty() []TableStats {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]TableStats, 0, len(a.byTable))
	for _, st := range a.byTable {
		if !st.dirty {
			continue
		}
		st.dirty = false
		out = append(out, *st)
	}
	return out
}

