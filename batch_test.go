// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/rdb"
)

func TestGroupPolicyResolve(t *testing.T) {
	tests := []struct {
		desc   string
		policy GroupPolicy
		want   base.GroupPolicy
	}{
		{"zero value inherits the configuration", GroupPolicy{},
			base.GroupPolicy{MaxSize: 50, GracePeriodSec: 60, MaxDelaySec: 600}},
		{"positive fields override", GroupPolicy{MaxSize: 10, MinSize: 10, GracePeriod: 5 * time.Second, MaxDelay: time.Hour},
			base.GroupPolicy{MaxSize: 10, MinSize: 10, GracePeriodSec: 5, MaxDelaySec: 3600}},
		{"negative fields disable", GroupPolicy{MaxSize: -1, GracePeriod: -1, MaxDelay: -1},
			base.GroupPolicy{}},
		{"sub-second durations round up to a second", GroupPolicy{GracePeriod: time.Millisecond, MaxDelay: time.Millisecond},
			base.GroupPolicy{MaxSize: 50, GracePeriodSec: 1, MaxDelaySec: 1}},
	}
	for _, tc := range tests {
		got := tc.policy.resolve(50, time.Minute, 10*time.Minute)
		if got != tc.want {
			t.Errorf("%s: resolve() = %+v, want %+v", tc.desc, got, tc.want)
		}
	}
	p := newGroupPolicy(&base.GroupPolicy{MaxSize: 0, MinSize: 3, GracePeriodSec: 0, MaxDelaySec: 30})
	want := GroupPolicy{MaxSize: -1, MinSize: 3, GracePeriod: -1, MaxDelay: 30 * time.Second}
	if *p != want {
		t.Errorf("newGroupPolicy() = %+v, want %+v", *p, want)
	}
}

func TestBatchTaskRoundTrip(t *testing.T) {
	member := &base.TaskMessage{ID: "id1", Type: "llm:classify", Payload: []byte("hello"), Queue: "llm", GroupKey: "g",
		Retry: 3, Retried: 1, EnqueuedAt: 1700000000000, Headers: map[string]string{"k": "v"}}
	t1 := NewTaskWithHeaders(member.Type, member.Payload, member.Headers)
	t1.member = member
	t2 := NewTask("llm:classify", []byte("plain")) // a task without member message

	task := NewBatchTask("llm:batch", []*Task{t1, t2}, MaxRetry(2))
	if task.Type() != "llm:batch" || task.Headers()[BatchTaskHeader] != "2" {
		t.Fatalf("NewBatchTask returned type=%q headers=%v", task.Type(), task.Headers())
	}
	b, err := ParseBatch(task)
	if err != nil {
		t.Fatal(err)
	}
	if b.Queue != "llm" || b.Group != "g" || b.Len() != 2 {
		t.Fatalf("batch = %+v", b)
	}
	it := b.Items[0]
	if it.ID != "id1" || string(it.Payload) != "hello" || it.Retried != 1 || it.MaxRetry != 3 ||
		it.Headers["k"] != "v" || it.EnqueuedAt.UnixMilli() != 1700000000000 {
		t.Errorf("item 0 = %+v", it)
	}
	if b.Items[1].ID != "" || string(b.Items[1].Payload) != "plain" || b.Items[1].MaxRetry != defaultMaxRetry {
		t.Errorf("item 1 = %+v", b.Items[1])
	}
	if _, err := ParseBatch(NewTask("x", []byte(`{"foo":1}`))); err == nil {
		t.Errorf("ParseBatch of a regular task returned no error")
	}
	// Settling without a server fails.
	b.Retry(0, errors.New("rate limited"))
	if err := b.settle(context.Background(), nil); err == nil {
		t.Errorf("settle without ResultWriter returned no error")
	}
}

func TestBatchHandlerSettlesItems(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	// Five tasks in a group, aggregated as soon as the group has 5 tasks.
	var ids []string
	for i := 0; i < 5; i++ {
		info, err := client.Enqueue(NewTask("llm:classify", []byte(fmt.Sprintf("doc-%d", i))),
			Queue("llm"), Group("docs"), MaxRetry(1))
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, info.ID)
	}
	agg := newAggregator(aggregatorParams{
		logger:      testLogger,
		broker:      rdbClient,
		queues:      []string{"llm"},
		gracePeriod: time.Minute,
		groupAggregator: GroupAggregatorFunc(func(group string, tasks []*Task) *Task {
			return NewBatchTask("llm:batch", tasks)
		}),
		groupPolicy: func(queue, group string) GroupPolicy {
			return GroupPolicy{MaxSize: 5, MinSize: 5, GracePeriod: -1}
		},
	})
	agg.sema <- struct{}{} // aggregate releases a token
	agg.aggregate(time.Now())

	pending, err := inspector.ListPendingTasks("llm")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Type != "llm:batch" {
		t.Fatalf("pending tasks = %v, want one llm:batch task", pending)
	}
	groups, err := inspector.Groups("llm")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 0 {
		t.Errorf("groups after aggregation = %v, want none", groups)
	}

	var mu sync.Mutex
	var seen []string
	mux := NewServeMux()
	mux.HandleBatchFunc("llm:batch", func(ctx context.Context, b *Batch) error {
		mu.Lock()
		defer mu.Unlock()
		for i, it := range b.Items {
			seen = append(seen, it.ID)
			switch i {
			case 1:
				b.Retry(i, errors.New("transient")) // back to the group (MaxRetry 1)
			case 2:
				b.Fail(i, errors.New("invalid document")) // archived
			}
		}
		return nil
	})
	p := newProcessorForTest(t, rdbClient, mux)
	p.queueConfig = map[string]int{"llm": 1}
	p.start(&sync.WaitGroup{})
	time.Sleep(1500 * time.Millisecond)
	p.shutdown()

	mu.Lock()
	if len(seen) != 5 {
		t.Fatalf("batch handler saw %d items, want 5", len(seen))
	}
	gotIDs := append([]string(nil), seen...)
	mu.Unlock()
	if diff := cmp.Diff(ids, gotIDs, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("batch items mismatch (-want,+got):\n%s", diff)
	}
	retriedID, failedID := gotIDs[1], gotIDs[2]

	groups, err = inspector.Groups("llm")
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 1 || groups[0].Group != "docs" || groups[0].Size != 1 {
		t.Fatalf("groups after the batch = %+v, want the retried item back in group docs", groups)
	}
	if groups[0].Policy == nil || groups[0].Policy.MaxSize != 5 || groups[0].Policy.MinSize != 5 || groups[0].Policy.GracePeriod != -1 {
		t.Errorf("published policy = %+v, want MaxSize 5, MinSize 5, no grace period", groups[0].Policy)
	}
	retried, err := inspector.GetTaskInfo("llm", retriedID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.State != TaskStateAggregating || retried.Retried != 1 || retried.LastErr != "transient" {
		t.Errorf("retried item: state=%v retried=%d err=%q", retried.State, retried.Retried, retried.LastErr)
	}
	archived, err := inspector.ListArchivedTasks("llm")
	if err != nil {
		t.Fatal(err)
	}
	if len(archived) != 1 || archived[0].ID != failedID || archived[0].LastErr != "invalid document" {
		t.Errorf("archived tasks = %+v, want item %s", archived, failedID)
	}

	// Flushing the group aggregates it right away despite its min size.
	if err := inspector.FlushGroup("llm", "docs"); err != nil {
		t.Fatal(err)
	}
	groups, _ = inspector.Groups("llm")
	if len(groups) != 1 || !groups[0].FlushRequested {
		t.Errorf("groups after FlushGroup = %+v, want flush requested", groups)
	}
	agg.sema <- struct{}{} // aggregate releases a token
	agg.aggregate(time.Now())
	pending, _ = inspector.ListPendingTasks("llm")
	if len(pending) != 1 || pending[0].Type != "llm:batch" {
		t.Errorf("pending tasks after flush = %v, want one llm:batch task", pending)
	}
	if err := inspector.FlushGroup("llm", "docs"); err == nil {
		t.Errorf("FlushGroup of an empty group returned no error")
	}
}
