// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
	h "github.com/jpedrosouza/asynq/internal/testutil"
	"github.com/jpedrosouza/asynq/internal/timeutil"
)

func seedGroup(t *testing.T, r *RDB, qname, gname string, addedAt ...time.Time) {
	t.Helper()
	for _, at := range addedAt {
		r.SetClock(timeutil.NewSimulatedClock(at))
		msg := h.NewTaskMessageWithQueue("task", nil, qname)
		msg.GroupKey = gname
		if err := r.AddToGroup(context.Background(), msg, gname); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAggregationCheckPolicy(t *testing.T) {
	now := time.Now()
	tests := []struct {
		desc     string
		addedAt  []time.Time
		policy   base.GroupPolicy
		flush    bool
		wantAggr bool
	}{
		{"below min size, grace period elapsed",
			[]time.Time{now.Add(-time.Hour), now.Add(-time.Hour)},
			base.GroupPolicy{MinSize: 3, GracePeriodSec: 60}, false, false},
		{"min size reached, grace period elapsed",
			[]time.Time{now.Add(-time.Hour), now.Add(-time.Hour), now.Add(-time.Hour)},
			base.GroupPolicy{MinSize: 3, GracePeriodSec: 60}, false, true},
		{"grace period disabled",
			[]time.Time{now.Add(-time.Hour)},
			base.GroupPolicy{GracePeriodSec: 0}, false, false},
		{"max delay ignores min size",
			[]time.Time{now.Add(-time.Hour)},
			base.GroupPolicy{MinSize: 10, MaxDelaySec: 60}, false, true},
		{"flush ignores the policy",
			[]time.Time{now},
			base.GroupPolicy{MinSize: 10}, true, true},
	}
	for _, tc := range tests {
		r := setup(t)
		seedGroup(t, r, "default", "g", tc.addedAt...)
		if tc.flush {
			if err := r.RequestGroupFlush("default", "g"); err != nil {
				t.Fatal(err)
			}
		}
		id, err := r.AggregationCheckPolicy("default", "g", now, tc.policy)
		if err != nil {
			t.Fatalf("%s: %v", tc.desc, err)
		}
		if got := id != ""; got != tc.wantAggr {
			t.Errorf("%s: aggregated = %t, want %t", tc.desc, got, tc.wantAggr)
		}
		if tc.flush && r.client.SIsMember(context.Background(), base.FlushGroupsKey("default"), "g").Val() {
			t.Errorf("%s: flush request not cleared", tc.desc)
		}
		r.Close()
	}
}

func TestGroupStatsReportsAgeAndPolicy(t *testing.T) {
	r := setup(t)
	defer r.Close()
	t1 := time.Now().Add(-time.Hour).Truncate(time.Second)
	t2 := time.Now().Truncate(time.Second)
	seedGroup(t, r, "default", "g", t1, t2)
	policy := base.GroupPolicy{MaxSize: 10, MinSize: 5, GracePeriodSec: 30}
	if err := r.PublishGroupPolicies("default", map[string]base.GroupPolicy{"g": policy}); err != nil {
		t.Fatal(err)
	}
	if err := r.RequestGroupFlush("default", "g"); err != nil {
		t.Fatal(err)
	}
	stats, err := r.GroupStats("default")
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 1 {
		t.Fatalf("GroupStats returned %d groups, want 1", len(stats))
	}
	st := stats[0]
	if st.Size != 2 || st.OldestAt != t1.Unix() || st.NewestAt != t2.Unix() || !st.FlushRequested {
		t.Errorf("GroupStats = %+v", st)
	}
	if st.Policy == nil || *st.Policy != policy {
		t.Errorf("Policy = %+v, want %+v", st.Policy, policy)
	}
	if err := r.RequestGroupFlush("default", "nope"); err == nil {
		t.Errorf("RequestGroupFlush of an unknown group returned no error")
	}
}

func TestAddToArchive(t *testing.T) {
	r := setup(t)
	defer r.Close()
	msg := h.NewTaskMessageWithQueue("task", nil, "q")
	msg.ErrorMsg = "boom"
	if err := r.AddToArchive(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	got := h.GetArchivedMessages(t, r.client, "q")
	if len(got) != 1 || got[0].ID != msg.ID || got[0].ErrorMsg != "boom" {
		t.Errorf("archived = %v", got)
	}
	if err := r.AddToArchive(context.Background(), msg); !errors.Is(err, errors.ErrTaskIdConflict) {
		t.Errorf("second AddToArchive returned %v, want ErrTaskIdConflict", err)
	}
	queues, _ := r.AllQueues()
	if len(queues) != 1 || queues[0] != "q" {
		t.Errorf("AllQueues = %v, want [q]", queues)
	}
}
