package cdcstats

import (
	"sync"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
)

type TableStats struct {
	QualifiedName string
	SchemaName    string
	TableName     string

	// Inserts counts real inserts (op "c") only. Snapshot reads (op "r") are in
	// Reads: a re-snapshot reads every row again, and counting those as inserts
	// made "Captured Inserts" exceed the table's row count.
	Inserts     int64
	Updates     int64
	Deletes     int64
	Reads       int64
	TotalEvents int64
	LastEventTs time.Time

	dirty bool
}

type Accumulator struct {
	pipelineID string
	mu         sync.Mutex
	byTable    map[string]*TableStats
	// snap collects this flush window's snapshot reads per table for the
	// snapshot request tracker (cdcsnapshot); DrainSnapshots empties it.
	snap map[string]*cdcsnapshot.Observation
}

func NewAccumulator(pipelineID string) *Accumulator {
	return &Accumulator{
		pipelineID: pipelineID,
		byTable:    make(map[string]*TableStats),
		snap:       make(map[string]*cdcsnapshot.Observation),
	}
}

// Seed starts the counters from what an earlier worker already reported. A
// worker is recreated on every resume (and orchestrator restart) while its
// consumer group continues from the committed offset; starting from zero froze
// the monotone captured counters at their old value until the new worker's
// count passed it, so every change in between went uncounted.
func (a *Accumulator) Seed(rows []TableStats) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, r := range rows {
		if r.QualifiedName == "" {
			continue
		}
		if _, ok := a.byTable[r.QualifiedName]; ok {
			continue
		}
		st := r
		st.dirty = false
		a.byTable[r.QualifiedName] = &st
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
	case "c":
		st.Inserts++
	case "r":
		st.Reads++
		a.observeSnapshotLocked(u)
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

func (a *Accumulator) observeSnapshotLocked(u TableUpdate) {
	o := a.snap[u.QualifiedName]
	if o == nil {
		o = &cdcsnapshot.Observation{Table: u.QualifiedName, FirstSeen: u.Timestamp}
		a.snap[u.QualifiedName] = o
	}
	o.Rows++
	if u.Timestamp.Before(o.FirstSeen) {
		o.FirstSeen = u.Timestamp
	}
	if u.Timestamp.After(o.LastSeen) {
		o.LastSeen = u.Timestamp
	}
	switch u.Snapshot {
	case "last_in_data_collection":
		o.TableDone = true
	case "last":
		o.TableDone = true
		o.AllDone = true
	case "incremental":
		o.Incremental = true
	}
}

// DrainSnapshots returns and forgets the snapshot reads seen since the last call.
func (a *Accumulator) DrainSnapshots() []cdcsnapshot.Observation {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.snap) == 0 {
		return nil
	}
	out := make([]cdcsnapshot.Observation, 0, len(a.snap))
	for _, o := range a.snap {
		out = append(out, *o)
	}
	a.snap = make(map[string]*cdcsnapshot.Observation)
	return out
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
