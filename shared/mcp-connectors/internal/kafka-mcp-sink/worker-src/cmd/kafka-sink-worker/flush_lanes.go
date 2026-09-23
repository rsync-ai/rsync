package main

import (
	"hash/fnv"
	"os"
	"strconv"
	"strings"
	"sync"
)

// EnvSinkFlushLanes is how many destination flushes one sink worker runs at once.
//
// A CDC data topic is now born with min(3, brokers) partitions
// (kafka.EnvCDCTopicPartitions), and Debezium gives every source table a topic of
// its own, so a single worker is assigned tens of partitions. It consumed all of
// them already — a kafka-go GroupID reader interleaves every assigned partition
// into one FetchMessage stream — but it drained them one at a time, because the
// destination write ran inline on the consume goroutine. One slow table stalled
// every other partition.
//
// Lanes fix the draining, not the consuming. 0 or 1 restores the old behaviour
// exactly: newFlushLanes returns nil and every flush runs inline — the kill switch
// for this whole change.
//
// RSYNC_SINK_ rather than KAFKA_: this is read by the sink WORKER, alongside
// RSYNC_SINK_STALL_WATCHDOG_SECONDS and friends. The KAFKA_ prefix belongs to the
// orchestrator's broker-side settings (kafka.EnvCDCTopicPartitions), which this
// process never reads.
const EnvSinkFlushLanes = "RSYNC_SINK_FLUSH_LANES"

const (
	// defaultFlushLanes overlaps several table writes without opening an alarming
	// number of concurrent connections to somebody else's production database.
	defaultFlushLanes = 4

	// maxFlushLanes is a sanity bound, not a tuning recommendation. Past this the
	// destination, not the sink, is the bottleneck.
	maxFlushLanes = 32

	// flushLaneQueueDepth is deliberately 1.
	//
	// Lanes exist so that flushes for different partitions OVERLAP, not so that
	// batches pile up in memory. Depth 1 means a lane holds at most one running and
	// one queued batch, so the worker's resident batch count is bounded at
	// 2*lanes instead of growing without limit — this sink runs under a 768 MiB
	// limit that was already raised once after OOM kills. It also keeps a queued
	// batch's flush context fresh: a job waits at most one flush, so the window in
	// which a shutdown can cancel a not-yet-started flush stays small.
	flushLaneQueueDepth = 1
)

// offsetSpaceOf returns the "topic|partition" prefix of a batcher key.
//
// This is the whole correctness argument for lanes, so it is worth stating plainly.
// Neither of this worker's two durable offset records tracks gaps: kafka-go's
// offsetStash.merge keeps max(offset) per partition, and highWaterTracker.advance
// does the same for the destination's _rsync_cdc_offsets mark — after which seen()
// SKIPS every offset at or below it. Complete offset 105 before 101 and a restart
// does not merely forget 101-104, it classifies them as already-written duplicates
// and drops them silently.
//
// Sharding on the offset space removes the hazard instead of managing it. Every
// batch from one (topic, partition) lands on one lane, a lane is serial, and the
// dispatcher submits in fetch order — so offsets still complete in ascending order
// within the space that records them, and max(offset) stays correct. That is
// Confluent's parallel-consumer PARTITION ordering; KEY ordering would split one
// partition's offsets across lanes and force an offset encoder to carry the gaps.
//
// Per-row order survives because Debezium keys each event on the source PK and
// Kafka hashes that key to a partition, so one row's events never leave one
// partition. A keyless table would round-robin and break that, which is why the
// executor pins such a pipeline to a single partition before it ever gets here.
//
// Every batcher key in this worker is built as "topic|partition|…", and a Kafka
// topic name cannot contain '|', so the first two fields are unambiguous. A key
// that somehow has fewer than two separators is returned whole: it then shares a
// lane with its own kind, which is conservative rather than wrong.
func offsetSpaceOf(key string) string {
	first := strings.IndexByte(key, '|')
	if first < 0 {
		return key
	}
	rest := strings.IndexByte(key[first+1:], '|')
	if rest < 0 {
		return key
	}
	return key[:first+1+rest]
}

// flushLane is one serial worker: a queue and the goroutine that drains it.
type flushLane struct {
	jobs chan func()
	// wg counts jobs submitted but not finished. Only the dispatcher goroutine
	// calls Add, and only the dispatcher calls Wait, so the reuse is safe — the
	// race WaitGroup forbids is a concurrent Add and Wait.
	wg sync.WaitGroup
}

// flushLanes is a fixed pool of serial lanes, chosen by offset space.
//
// A nil *flushLanes is valid and means "no lanes": submit runs the job inline and
// the barriers are no-ops. Callers therefore never need a nil check.
type flushLanes struct {
	lanes []*flushLane
}

// newFlushLanes starts n lanes, or returns nil when n < 2 (inline flushing).
func newFlushLanes(n int) *flushLanes {
	if n < 2 {
		return nil
	}
	if n > maxFlushLanes {
		n = maxFlushLanes
	}
	f := &flushLanes{lanes: make([]*flushLane, n)}
	for i := range f.lanes {
		l := &flushLane{jobs: make(chan func(), flushLaneQueueDepth)}
		f.lanes[i] = l
		go func(l *flushLane) {
			for job := range l.jobs {
				// Done in a defer of its own so it runs on every exit path, not just
				// the ordinary return. A panic still takes the process down, exactly
				// as it did when flushes ran on the consume goroutine — this worker
				// fails closed on purpose and nothing here recovers.
				func() {
					defer l.wg.Done()
					job()
				}()
			}
		}(l)
	}
	return f
}

// laneFor returns the lane that owns key's offset space.
func (f *flushLanes) laneFor(key string) *flushLane {
	h := fnv.New32a()
	_, _ = h.Write([]byte(offsetSpaceOf(key)))
	return f.lanes[int(h.Sum32()%uint32(len(f.lanes)))]
}

// submit runs job on key's lane, blocking while that lane's queue is full.
//
// Blocking is the backpressure mechanism, and it is the only one available:
// kafka-go's group Reader has no Pause/Resume, so a full queue must stop the
// dispatcher, which stops FetchMessage. That is safe here because kafka-go
// heartbeats from its own goroutine (Generation.Start runs heartbeatLoop), not
// from the fetch path, so a blocked consume loop does not lose group membership
// the way a blocked Java consumer would.
func (f *flushLanes) submit(key string, job func()) {
	if f == nil || len(f.lanes) == 0 {
		job()
		return
	}
	l := f.laneFor(key)
	l.wg.Add(1)
	l.jobs <- job
}

// waitKey blocks until key's lane has finished everything submitted to it.
//
// Callers must use this before any write that has to be ordered after earlier
// ones — a delete after its table's buffered upserts, above all. Submitting the
// pending batch is not enough on its own: an earlier threshold flush for the same
// key may still be running, and a delete that overtakes it resurrects the row.
func (f *flushLanes) waitKey(key string) {
	if f == nil || len(f.lanes) == 0 {
		return
	}
	f.laneFor(key).wg.Wait()
}

// waitAll blocks until every lane is idle. Shutdown and flushAll need this: a
// drain that returns while a flush is still in flight has not drained anything.
func (f *flushLanes) waitAll() {
	if f == nil {
		return
	}
	for _, l := range f.lanes {
		l.wg.Wait()
	}
}

// close drains the lanes and stops their goroutines. Safe to call once.
func (f *flushLanes) close() {
	if f == nil {
		return
	}
	f.waitAll()
	for _, l := range f.lanes {
		close(l.jobs)
	}
}

// resolveFlushLaneCount reads EnvSinkFlushLanes, falling back to defaultFlushLanes.
//
// A malformed value warns and uses the default rather than failing the worker: an
// unparseable lane count is an operator typo, and refusing to stream because of one
// is a worse outcome than streaming at the default width.
func resolveFlushLaneCount() int {
	raw := strings.TrimSpace(os.Getenv(EnvSinkFlushLanes))
	if raw == "" {
		return defaultFlushLanes
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		logf("warning", "ignoring invalid %s=%q, using %d", EnvSinkFlushLanes, raw, defaultFlushLanes)
		return defaultFlushLanes
	}
	if n > maxFlushLanes {
		logf("warning", "%s=%d exceeds the maximum, using %d", EnvSinkFlushLanes, n, maxFlushLanes)
		return maxFlushLanes
	}
	return n
}
