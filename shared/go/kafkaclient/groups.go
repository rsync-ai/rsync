package kafkaclient

// Group qualifies a consumer group id with the deployment's namespace, under
// exactly the contract Topic() applies to topic names: the same
// KAFKA_TOPIC_PREFIX, the same default, the same idempotence, and the same
// empty-string migration lever.
//
// Topic() has been the chokepoint that keeps ~60 producer call sites correctly
// namespaced. Consumer groups had no such function, and the consequence is
// visible on a customer's cluster: group ids are hand-rolled at every call site,
// and one of them mints a new group per pipeline EXECUTION. A customer writing
// exact-match Group ACLs therefore cannot enumerate what to grant — the set is
// unbounded and only known after the fact — and the broker's group metadata
// grows without bound because nothing ever reclaims a group that will never be
// joined again.
//
// Sharing KAFKA_TOPIC_PREFIX rather than adding a second variable is
// deliberate. Topics and groups are granted together in one ACL set; two
// prefixes that could drift apart would mean an operator granting rsync.* on
// topics and something else on groups, which fails at join time with an
// authorization error that names neither variable.
//
// Qualification is idempotent for the same reason it is on topics: group ids are
// persisted and read back on the next run, so an already-qualified id arriving
// here a second time must not become rsync.rsync.cdc-sink.
func Group(name string) string {
	// Deliberately the same computation, not a parallel copy of it: a group
	// prefix that drifted from the topic prefix is exactly the failure this
	// function exists to prevent.
	return Topic(name)
}

// Groups qualifies a list of group ids, for a caller that manages several.
func Groups(names ...string) []string {
	return Topics(names...)
}
