package cdcstats

import (
	"reflect"
	"testing"
)

// A removed table's topic leaves the stats consumer's topic set (so the topic can
// be deleted without the group re-creating it) and comes back when re-included.
func TestExcludedTopicLeavesTheStatsTopicSet(t *testing.T) {
	a := &Agent{}
	all := []string{"rsync_cdc_p1.public.users", "rsync_cdc_p1.public.orders", "rsync_cdc_p2.public.users"}

	if got := a.prefixTopics(all, "rsync_cdc_p1"); !reflect.DeepEqual(got, []string{"rsync_cdc_p1.public.users", "rsync_cdc_p1.public.orders"}) {
		t.Fatalf("before exclusion = %v", got)
	}
	a.ExcludeTopic("rsync_cdc_p1.public.orders")
	if got := a.prefixTopics(all, "rsync_cdc_p1"); !reflect.DeepEqual(got, []string{"rsync_cdc_p1.public.users"}) {
		t.Fatalf("after exclusion = %v", got)
	}
	a.IncludeTopic("rsync_cdc_p1.public.orders")
	if got := a.prefixTopics(all, "rsync_cdc_p1"); len(got) != 2 {
		t.Fatalf("after re-include = %v", got)
	}
}
