package cdcsnapshot

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// Producer is the part of the Kafka manager the dispatcher needs.
type Producer interface {
	// EnsureSignalTopic creates the signal topic with its short retention (#23).
	EnsureSignalTopic(topic string) error
	ProduceWithContext(ctx context.Context, topic string, key, value []byte) error
}

// Connect is the part of ConnectClient the dispatcher needs (a fake in tests).
type Connect interface {
	State(ctx context.Context, name string) (ConnectorState, error)
	Config(ctx context.Context, name string) (map[string]interface{}, error)
}

// InitialObserveWindow: snapshot rows the connector read longer ago than this
// are not a load happening now (a consumer re-reading its topic from the start).
const InitialObserveWindow = time.Hour

// sendErrorTimeout bounds how long a queued request whose sends fail is retried.
const sendErrorTimeout = 15 * time.Minute

// Dispatcher sends queued snapshot requests once their connector is ready and
// watches the sent ones until the stats consumer sees them finish.
type Dispatcher struct {
	store    *Store
	producer Producer
	connect  Connect
	// tracked: the CDC stats consumer runs, so snapshot rows can confirm a
	// request. Without it a sent request is closed as unconfirmed.
	tracked bool
	timing  Timing
	tick    time.Duration
	now     func() time.Time

	mu sync.Mutex
	// readySince is when each queued request's connector was first seen ready in
	// the current streak; a not-ready tick resets it.
	readySince map[string]time.Time
	// proofs are RUNNING streaks the executor watched for a request (Hurry);
	// kick wakes the loop before the next tick.
	proofs map[string]RunningProof
	kick   chan struct{}
	cancel context.CancelFunc
	done   chan struct{}
	// startedAt is this dispatcher's first tick. No in-flight clock starts
	// earlier: see ClampToStart.
	startedAt time.Time
}

func NewDispatcher(store *Store, producer Producer, connect Connect, tracked bool) *Dispatcher {
	return &Dispatcher{
		store:      store,
		producer:   producer,
		connect:    connect,
		tracked:    tracked,
		timing:     DefaultTiming,
		tick:       5 * time.Second,
		now:        time.Now,
		readySince: map[string]time.Time{},
		proofs:     map[string]RunningProof{},
		kick:       make(chan struct{}, 1),
	}
}

// Start runs the dispatcher until Stop.
func (d *Dispatcher) Start() {
	if d == nil || d.store == nil || d.producer == nil || d.connect == nil {
		return
	}
	d.mu.Lock()
	if d.cancel != nil {
		d.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.done = make(chan struct{})
	d.mu.Unlock()

	go func() {
		defer close(d.done)
		t := time.NewTicker(d.tick)
		defer t.Stop()
		unavailableLogged := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			case <-d.kick:
			}
			if err := d.Tick(ctx); err != nil {
				if errors.Is(err, ErrUnavailable) {
					if !unavailableLogged {
						log.Info("CDC snapshot dispatcher: cdc_snapshot_requests does not exist yet; waiting for migration 113")
						unavailableLogged = true
					}
					continue
				}
				if ctx.Err() == nil {
					log.WithError(err).Warn("CDC snapshot dispatcher: tick failed")
				}
			}
		}
	}()
}

// Stop ends the loop and waits for the current tick.
func (d *Dispatcher) Stop() {
	if d == nil {
		return
	}
	d.mu.Lock()
	cancel, done := d.cancel, d.done
	d.cancel = nil
	d.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

// Observe is the CDC stats consumer's hook: it folds snapshot observations
// into the pipeline's in-flight requests, and records the pipeline's initial
// load from rows no request accounts for.
func (d *Dispatcher) Observe(pipelineID, connectorName string, obs []Observation) {
	if d == nil || d.store == nil || len(obs) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := d.now()
	reqs, err := d.store.ListRecent(ctx, pipelineID, now.Add(-InitialObserveWindow))
	if err != nil {
		if !errors.Is(err, ErrUnavailable) {
			log.WithError(err).WithField("pipeline_id", pipelineID).Warn("CDC snapshot dispatcher: could not read in-flight requests")
		}
		return
	}
	// An initial load takes only the rows no Re-snapshot or Edit tables request
	// accounts for: that request's "last" is not the end of the load.
	free := unclaimed(reqs, obs)
	initialOpen := false
	for i := range reqs {
		r := reqs[i]
		in := obs
		if r.Source == SourceInitial {
			initialOpen = initialOpen || r.Open()
			in = free
		}
		if !ApplyObservations(&r, in, now) {
			continue
		}
		if err := d.store.SaveProgress(ctx, r); err != nil {
			log.WithError(err).WithField("request_id", r.ID).Warn("CDC snapshot dispatcher: could not save progress")
			continue
		}
		if r.Status == StatusCompleted {
			log.WithFields(log.Fields{"pipeline_id": pipelineID, "request_id": r.ID, "source": r.Source, "tables": len(r.Tables)}).
				Info("📸 CDC snapshot request completed")
		}
	}
	if initialOpen {
		return
	}
	// Rows read long ago are a consumer re-reading its topic, not a load now.
	var fresh []Observation
	for _, o := range free {
		if !o.LastSeen.Before(now.Add(-InitialObserveWindow)) {
			fresh = append(fresh, o)
		}
	}
	if len(fresh) > 0 {
		d.recordInitial(ctx, pipelineID, connectorName, fresh, now)
	}
}

// unclaimed keeps the observations with rows or a marker that no Re-snapshot,
// Edit tables or auto-pickup request accounts for. A request claims a table's
// rows read after its first send and, once finished, before it finished (one
// closed without an end time keeps them all: a late start of it is likelier
// than a new load).
func unclaimed(reqs []Request, obs []Observation) []Observation {
	var out []Observation
	for _, o := range obs {
		if o.Rows <= 0 && !o.TableDone && !o.AllDone {
			continue
		}
		claimed := false
		for i := range reqs {
			r := &reqs[i]
			if r.Source == SourceInitial || r.SentAt == nil || o.LastSeen.Before(r.SentAt.Add(-creditSlack)) {
				continue
			}
			if _, ok := r.match(o.Table); !ok {
				continue
			}
			if r.Open() || r.CompletedAt == nil || !o.FirstSeen.After(r.CompletedAt.Add(creditSlack)) {
				claimed = true
				break
			}
		}
		if !claimed {
			out = append(out, o)
		}
	}
	return out
}

// recordInitial records a load no request asked for — the connector's initial
// snapshot, or an incremental one the executor signalled itself — as the
// pipeline's initial load. It is recorded from what was seen, never when a
// connector starts: Debezium skips the snapshot when the connector already has
// offsets, and a row written at start would read "in progress" forever.
func (d *Dispatcher) recordInitial(ctx context.Context, pipelineID, connectorName string, obs []Observation, now time.Time) {
	first := obs[0].FirstSeen
	incremental := false
	for _, o := range obs {
		if !o.FirstSeen.IsZero() && (first.IsZero() || o.FirstSeen.Before(first)) {
			first = o.FirstSeen
		}
		incremental = incremental || o.Incremental
	}
	if first.IsZero() || first.After(now) {
		first = now
	}
	blocked, err := d.store.InitialBlocked(ctx, pipelineID, first)
	if err != nil || blocked {
		if err != nil && !errors.Is(err, ErrUnavailable) {
			log.WithError(err).WithField("pipeline_id", pipelineID).Warn("CDC snapshot dispatcher: could not check for an initial load")
		}
		return
	}
	var tables []string
	if d.connect != nil && connectorName != "" {
		if cfg, cerr := d.connect.Config(ctx, connectorName); cerr == nil {
			tables = PlainIncludeTables(ParseIncludeList(cfg))
		}
	}
	mode := "blocking"
	if incremental {
		mode = "incremental"
	}
	sent := first
	r := Request{
		PipelineID: pipelineID, ConnectorName: connectorName, Mode: mode, Tables: tables,
		Source: SourceInitial, Status: StatusSent, SentAt: &sent, LastSentAt: &sent,
		RequestedAt: first, NotBefore: first,
	}
	ApplyObservations(&r, obs, now)
	inserted, err := d.store.InsertInitial(ctx, r)
	if err != nil {
		if !errors.Is(err, ErrUnavailable) {
			log.WithError(err).WithField("pipeline_id", pipelineID).Warn("CDC snapshot dispatcher: could not record the initial load")
		}
		return
	}
	if inserted {
		log.WithFields(log.Fields{"pipeline_id": pipelineID, "connector": connectorName, "mode": mode,
			"tables": len(r.Tables), "status": r.Status}).Info("📸 CDC initial load seen; tracking it")
	}
}

// Tick does one pass over the open requests.
func (d *Dispatcher) Tick(ctx context.Context) error {
	if d.startedAt.IsZero() {
		d.startedAt = d.now()
	}
	reqs, err := d.store.ListOpen(ctx)
	if err != nil {
		return err
	}
	open := make(map[string]bool, len(reqs))
	for _, r := range reqs {
		open[r.ID] = true
		if ctx.Err() != nil {
			return ctx.Err()
		}
		switch r.Status {
		case StatusQueued:
			d.handleQueued(ctx, r)
		case StatusSent, StatusStarted:
			d.handleInFlight(ctx, r)
		}
	}
	d.mu.Lock()
	for id := range d.readySince {
		if !open[id] {
			delete(d.readySince, id)
		}
	}
	for id := range d.proofs {
		if !open[id] {
			delete(d.proofs, id)
		}
	}
	d.mu.Unlock()
	return nil
}

func (d *Dispatcher) logger(r Request) *log.Entry {
	return log.WithFields(log.Fields{
		"pipeline_id": r.PipelineID,
		"request_id":  r.ID,
		"connector":   r.ConnectorName,
		"mode":        r.Mode,
		"source":      r.Source,
		"tables":      len(r.Tables),
		"attempts":    r.Attempts,
	})
}

// ready reports whether r may be sent now: the connector must be Ready for
// Timing.ReadyStable in a row. A Broken connector past Timing.FailAfter (from
// the request) fails the request; failed=true then.
func (d *Dispatcher) ready(ctx context.Context, r Request) (ok bool, failed bool) {
	now := d.now()
	cs, err := d.connect.State(ctx, r.ConnectorName)
	var state Readiness
	var why string
	if err != nil {
		state, why = NotReady, "kafka connect is unreachable: "+err.Error()
	} else {
		state, why = Assess(cs, r.Tables)
	}
	if state != Ready {
		d.mu.Lock()
		delete(d.readySince, r.ID)
		delete(d.proofs, r.ID)
		d.mu.Unlock()
		// A paused pipeline waits as long as it stays paused; anything else is
		// bounded, measured from the request (or, for a re-send, the last send).
		if state == Paused {
			return false, false
		}
		since := r.RequestedAt
		if r.LastSentAt != nil {
			since = *r.LastSentAt
		}
		if now.Sub(since) >= d.timing.FailAfter {
			msg := fmt.Sprintf("could not send the snapshot signal: %s (waited %s)", why, d.timing.FailAfter)
			if ok, ferr := d.store.Finish(ctx, r, StatusFailed, msg); ferr != nil {
				d.logger(r).WithError(ferr).Warn("CDC snapshot dispatcher: could not fail the request")
			} else if ok {
				d.logger(r).WithField("reason", why).Warn("CDC snapshot request failed: the connector never became ready")
			}
			return false, true
		}
		return false, false
	}
	d.mu.Lock()
	first, seen := d.readySince[r.ID]
	if !seen {
		first = now
		// The executor watched this connector run for ReadyStable just before
		// queueing the request (a Reload): that streak counts.
		if since, ok := d.provenReadySince(r.ID, now); ok {
			first = since
		}
		d.readySince[r.ID] = first
	}
	d.mu.Unlock()
	return now.Sub(first) >= d.timing.ReadyStable, false
}

func (d *Dispatcher) handleQueued(ctx context.Context, r Request) {
	if d.now().Before(r.NotBefore) {
		return
	}
	// A request whose sends keep failing (Unclaim records why) gives up after
	// sendErrorTimeout instead of retrying every tick forever.
	if r.LastError != "" && d.now().Sub(r.RequestedAt) >= sendErrorTimeout {
		if _, err := d.store.Finish(ctx, r, StatusFailed, r.LastError); err != nil {
			d.logger(r).WithError(err).Warn("CDC snapshot dispatcher: could not fail the request")
		}
		return
	}
	ok, failed := d.ready(ctx, r)
	if !ok || failed {
		return
	}
	d.send(ctx, r, r.CleansFolder)
}

func (d *Dispatcher) handleInFlight(ctx context.Context, r Request) {
	action, msg := Watch(ClampToStart(r, d.startedAt), d.now(), d.tracked, d.timing)
	switch action {
	case ActNone:
		return
	case ActComplete:
		if ok, err := d.store.Finish(ctx, r, StatusCompleted, ""); err != nil {
			d.logger(r).WithError(err).Warn("CDC snapshot dispatcher: could not complete the request")
		} else if ok {
			d.logger(r).Info("📸 CDC snapshot request completed (incremental snapshot went idle)")
		}
	case ActUnconfirmed:
		if ok, err := d.store.Finish(ctx, r, StatusUnconfirmed, msg); err != nil {
			d.logger(r).WithError(err).Warn("CDC snapshot dispatcher: could not close the request")
		} else if ok {
			d.logger(r).WithField("reason", msg).Warn("CDC snapshot request unconfirmed")
		}
	case ActFail:
		if ok, err := d.store.Finish(ctx, r, StatusFailed, msg); err != nil {
			d.logger(r).WithError(err).Warn("CDC snapshot dispatcher: could not fail the request")
		} else if ok {
			d.logger(r).WithField("reason", msg).Error("CDC snapshot request failed: the snapshot never finished")
		}
	case ActResend:
		// A sent object-storage request whose clean markers are all gone did
		// start: the sink only consumes a marker when it writes a snapshot batch
		// for that topic. Its rows just were not seen (stats consumer lag), so it
		// must not be sent — and the folder emptied — a second time.
		if r.Status == StatusSent && r.CleansFolder {
			topics, terr := d.reloadTopics(ctx, r)
			if terr == nil && len(topics) > 0 {
				if n, perr := d.store.PendingReloadMarkers(ctx, r.PipelineID, topics); perr == nil && n == 0 {
					if _, ferr := d.store.Finish(ctx, r, StatusStarted, ""); ferr != nil {
						d.logger(r).WithError(ferr).Warn("CDC snapshot dispatcher: could not mark the request started")
					}
					return
				}
			}
		}
		ok, failed := d.ready(ctx, r)
		if !ok || failed {
			return
		}
		// A re-send of a started blocking snapshot re-reads every row, so the
		// folder is emptied again; a re-send of one that never started keeps the
		// markers it already has (re-armed, since their clock is the send).
		d.logger(r).Info("CDC snapshot dispatcher: no snapshot progress — sending the signal again")
		d.send(ctx, r, r.CleansFolder)
	}
}

func (d *Dispatcher) reloadTopics(ctx context.Context, r Request) ([]string, error) {
	cfg, err := d.connect.Config(ctx, r.ConnectorName)
	if err != nil {
		return nil, err
	}
	return ReloadTopics(SignalKey(cfg, r.ConnectorName), r.Tables), nil
}

// send claims r, arms the object-storage clean markers when asked, and produces
// the execute-snapshot signal. Any failure after the claim undoes it.
func (d *Dispatcher) send(ctx context.Context, r Request, writeMarkers bool) {
	cfg, err := d.connect.Config(ctx, r.ConnectorName)
	if err != nil {
		d.logger(r).WithError(err).Warn("CDC snapshot dispatcher: could not read the connector config; retrying")
		return
	}
	topic := SignalTopic(cfg)
	if topic == "" {
		msg := "the CDC connector has no Kafka signal topic (signal.kafka.topic); recreate the pipeline's connector to enable Re-snapshot"
		if _, ferr := d.store.Finish(ctx, r, StatusFailed, msg); ferr != nil {
			d.logger(r).WithError(ferr).Warn("CDC snapshot dispatcher: could not fail the request")
		}
		return
	}
	key := SignalKey(cfg, r.ConnectorName)
	value, err := BuildExecuteSnapshotSignal(r.Mode, r.Tables)
	if err != nil {
		if _, ferr := d.store.Finish(ctx, r, StatusFailed, err.Error()); ferr != nil {
			d.logger(r).WithError(ferr).Warn("CDC snapshot dispatcher: could not fail the request")
		}
		return
	}

	claimed, err := d.store.Claim(ctx, r)
	if err != nil {
		d.logger(r).WithError(err).Warn("CDC snapshot dispatcher: could not claim the request")
		return
	}
	if !claimed {
		return // another tick or orchestrator moved it
	}
	if writeMarkers {
		// Markers first: the sink must see the marker before it writes the first
		// snapshot batch, or that batch lands next to the old copy.
		if err := d.store.WriteReloadMarkers(ctx, r.PipelineID, r.ID, ReloadTopics(key, r.Tables)); err != nil {
			d.unclaim(ctx, r, "could not arm the destination folder clean-up: "+err.Error())
			return
		}
	}
	if terr := d.producer.EnsureSignalTopic(topic); terr != nil {
		// Non-fatal: the produce is the authoritative delivery check.
		d.logger(r).WithError(terr).WithField("topic", topic).Warn("CDC snapshot dispatcher: could not ensure the signal topic exists (producing anyway)")
	}
	if err := d.producer.ProduceWithContext(ctx, topic, []byte(key), value); err != nil {
		d.unclaim(ctx, r, "could not produce the snapshot signal: "+err.Error())
		return
	}
	d.mu.Lock()
	delete(d.readySince, r.ID)
	d.mu.Unlock()
	d.logger(r).WithField("signal_topic", topic).Info("📸 CDC snapshot signal sent")
}

func (d *Dispatcher) unclaim(ctx context.Context, r Request, cause string) {
	d.logger(r).WithField("reason", cause).Warn("CDC snapshot dispatcher: send failed; will retry")
	if err := d.store.Unclaim(ctx, r, cause); err != nil {
		d.logger(r).WithError(err).Warn("CDC snapshot dispatcher: could not undo the claim")
	}
}
