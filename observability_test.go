// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/rdb"
	h "github.com/jpedrosouza/asynq/internal/testutil"
)

func TestProcessorRecordsTaskTiming(t *testing.T) {
	r := setup(t)
	defer r.Close()
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	before := time.Now()
	info, err := client.Enqueue(NewTask("slow", nil), Retention(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if info.EnqueuedAt.Before(before.Add(-time.Second)) || info.EnqueuedAt.After(time.Now().Add(time.Second)) {
		t.Errorf("EnqueuedAt = %v, want about %v", info.EnqueuedAt, before)
	}

	handler := func(ctx context.Context, task *Task) error {
		time.Sleep(60 * time.Millisecond)
		return nil
	}
	p := newProcessorForTest(t, rdb.NewRDB(r), HandlerFunc(handler))
	p.start(&sync.WaitGroup{})
	time.Sleep(1500 * time.Millisecond)
	p.shutdown()

	got, err := inspector.GetTaskInfo("default", info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != TaskStateCompleted {
		t.Fatalf("task state = %v, want completed", got.State)
	}
	if got.EnqueuedAt.UnixMilli() != info.EnqueuedAt.UnixMilli() {
		t.Errorf("EnqueuedAt = %v, want %v", got.EnqueuedAt, info.EnqueuedAt)
	}
	if got.StartedAt.Before(got.EnqueuedAt) {
		t.Errorf("StartedAt %v is before EnqueuedAt %v", got.StartedAt, got.EnqueuedAt)
	}
	if got.Duration < 60*time.Millisecond || got.Duration > time.Second {
		t.Errorf("Duration = %v, want about 60ms", got.Duration)
	}
	if got.WaitTime < 0 || got.WaitTime > 1500*time.Millisecond {
		t.Errorf("WaitTime = %v, want between 0 and 1.5s", got.WaitTime)
	}

	points, err := inspector.TaskMetrics("default", time.Minute, before.Add(-time.Minute), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	total := &TaskMetrics{}
	for _, pt := range points {
		total.Merge(pt.Total())
	}
	if total.Processed != 1 || total.WaitCount != 1 {
		t.Fatalf("metrics: processed=%d waitCount=%d, want 1 and 1", total.Processed, total.WaitCount)
	}
	if d := total.AvgDuration(); d < 60*time.Millisecond || d > time.Second {
		t.Errorf("AvgDuration = %v, want about 60ms", d)
	}
	if p := total.DurationPercentile(95); p < 50*time.Millisecond || p > total.MaxDuration {
		t.Errorf("DurationPercentile(95) = %v, want between 50ms and %v", p, total.MaxDuration)
	}
}

func TestProcessorHonorsQueueRateLimit(t *testing.T) {
	r := setup(t)
	defer r.Close()
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	// One task every 250ms.
	if err := inspector.SetQueueRateLimit("default", RateLimit{Limit: 4, Period: time.Second}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := client.Enqueue(NewTask("limited", nil)); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	var starts []time.Time
	handler := func(ctx context.Context, task *Task) error {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		return nil
	}
	p := newProcessorForTest(t, rdb.NewRDB(r), HandlerFunc(handler))
	p.start(&sync.WaitGroup{})
	time.Sleep(1100 * time.Millisecond)
	p.shutdown()

	mu.Lock()
	defer mu.Unlock()
	// Within 1.1s: tasks at 0, 250, 500, 750 and 1000ms.
	if len(starts) < 4 || len(starts) > 5 {
		t.Fatalf("processed %d tasks in 1.1s, want 4 or 5", len(starts))
	}
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < 200*time.Millisecond {
			t.Errorf("tasks %d and %d started %v apart, want at least 250ms", i-1, i, gap)
		}
	}
	qinfo, err := inspector.GetQueueInfo("default")
	if err != nil {
		t.Fatal(err)
	}
	if qinfo.RateLimit == nil || qinfo.RateLimit.Limit != 4 || qinfo.RateLimit.Period != time.Second {
		t.Errorf("QueueInfo.RateLimit = %+v, want 4 per second", qinfo.RateLimit)
	}
	if qinfo.Pending != 10-len(starts) {
		t.Errorf("QueueInfo.Pending = %d, want %d", qinfo.Pending, 10-len(starts))
	}
}

func TestProcessorDefersTasksOverTheirKeyRateLimit(t *testing.T) {
	r := setup(t)
	defer r.Close()
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	if err := inspector.SetRateLimit("partner-api", RateLimit{Limit: 1, Period: time.Hour}); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i := 0; i < 2; i++ {
		info, err := client.Enqueue(NewTask("call_partner", nil), RateLimitKey("partner-api"), MaxRetry(3))
		if err != nil {
			t.Fatal(err)
		}
		if info.RateLimitKey != "partner-api" {
			t.Errorf("TaskInfo.RateLimitKey = %q, want partner-api", info.RateLimitKey)
		}
		ids = append(ids, info.ID)
	}
	// A task without key is not limited.
	if _, err := client.Enqueue(NewTask("free", nil)); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	processed := map[string]int{}
	handler := func(ctx context.Context, task *Task) error {
		mu.Lock()
		processed[task.Type()]++
		mu.Unlock()
		return nil
	}
	p := newProcessorForTest(t, rdb.NewRDB(r), HandlerFunc(handler))
	p.start(&sync.WaitGroup{})
	time.Sleep(1500 * time.Millisecond)
	p.shutdown()

	mu.Lock()
	defer mu.Unlock()
	if processed["call_partner"] != 1 || processed["free"] != 1 {
		t.Fatalf("processed = %v, want one call_partner and one free task", processed)
	}
	scheduled, err := inspector.ListScheduledTasks("default")
	if err != nil {
		t.Fatal(err)
	}
	if len(scheduled) != 1 {
		t.Fatalf("got %d scheduled tasks, want 1", len(scheduled))
	}
	s := scheduled[0]
	if s.Retried != 0 || s.LastErr != "" {
		t.Errorf("deferred task has retried=%d lastErr=%q, want no failure", s.Retried, s.LastErr)
	}
	if wait := time.Until(s.NextProcessAt); wait < 58*time.Minute || wait > time.Hour {
		t.Errorf("deferred task runs in %v, want about one hour", wait)
	}
	limits, err := inspector.RateLimits()
	if err != nil {
		t.Fatal(err)
	}
	if rl := limits["partner-api"]; rl == nil || rl.Available != 0 {
		t.Errorf("RateLimits()[partner-api] = %+v, want no capacity left", rl)
	}
}

func TestInspectorQueueMetadata(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	if _, err := client.Enqueue(NewTask("report", nil), Queue("reports_daily")); err != nil {
		t.Fatal(err)
	}

	want := QueueMetadata{Category: "Reports", Subcategory: "Daily", Description: "Daily PDF reports"}
	if err := inspector.SetQueueMetadata("reports_daily", QueueMetadata{
		Category: " Reports ", Subcategory: "Daily", Description: "Daily PDF reports",
	}); err != nil {
		t.Fatal(err)
	}
	got, err := inspector.GetQueueMetadata("reports_daily")
	if err != nil || got != want {
		t.Fatalf("GetQueueMetadata = %+v, %v; want %+v", got, err, want)
	}
	info, err := inspector.GetQueueInfo("reports_daily")
	if err != nil {
		t.Fatal(err)
	}
	if info.Metadata != want {
		t.Errorf("QueueInfo.Metadata = %+v, want %+v", info.Metadata, want)
	}
	// A subcategory requires a category.
	if err := inspector.SetQueueMetadata("other", QueueMetadata{Subcategory: "Orphan", Description: "x"}); err != nil {
		t.Fatal(err)
	}
	if md, _ := inspector.GetQueueMetadata("other"); md.Subcategory != "" {
		t.Errorf("subcategory kept without category: %+v", md)
	}
	// Empty metadata deletes it.
	if err := inspector.SetQueueMetadata("reports_daily", QueueMetadata{}); err != nil {
		t.Fatal(err)
	}
	all, err := inspector.ListQueueMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := all["reports_daily"]; ok || len(all) != 1 {
		t.Errorf("ListQueueMetadata = %v, want only the metadata of %q", all, "other")
	}
}

func TestTaskMetricsPercentiles(t *testing.T) {
	m := &TaskMetrics{
		Processed:         100,
		TotalDuration:     100 * 30 * time.Millisecond,
		MaxDuration:       900 * time.Millisecond,
		DurationHistogram: make([]int64, len(TaskMetricsHistogramBounds)+1),
	}
	// 90 tasks in (10ms, 25ms], 10 tasks in (500ms, 1s].
	m.DurationHistogram[3] = 90
	m.DurationHistogram[8] = 10
	if p := m.DurationPercentile(50); p <= 10*time.Millisecond || p > 25*time.Millisecond {
		t.Errorf("p50 = %v, want within (10ms, 25ms]", p)
	}
	if p := m.DurationPercentile(99); p <= 500*time.Millisecond || p > 900*time.Millisecond {
		t.Errorf("p99 = %v, want within (500ms, 900ms]", p)
	}
	if avg := m.AvgDuration(); avg != 30*time.Millisecond {
		t.Errorf("AvgDuration = %v, want 30ms", avg)
	}
	empty := &TaskMetrics{}
	if p := empty.DurationPercentile(95); p != 0 {
		t.Errorf("percentile of empty metrics = %v, want 0", p)
	}
}

func TestRateLimitOptionParsing(t *testing.T) {
	opt, err := parseOption(RateLimitKey("openai").String())
	if err != nil {
		t.Fatal(err)
	}
	if opt.Type() != RateLimitKeyOpt || opt.Value() != "openai" {
		t.Errorf("parsed option = %v, want RateLimitKey(\"openai\")", opt)
	}
	if _, err := composeOptions(RateLimitKey(" ")); err == nil {
		t.Errorf("blank rate limit key accepted")
	}
	if err := NewInspector(RedisClientOpt{Addr: "localhost:0"}).SetQueueRateLimit("q", RateLimit{Limit: 0, Period: time.Second}); err == nil {
		t.Errorf("rate limit with zero limit accepted")
	}
}

// A task recovered after its lease expired was not measured: its stored timing, from an earlier attempt, must not be
// recorded in the task metrics.
func TestRecoveredTaskRecordsNoMetrics(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	msg := h.NewTaskMessage("lost", nil)
	msg.Retry = 3
	msg.StartedAt = time.Now().Add(-time.Hour).UnixMilli() // an earlier attempt
	msg.DurationMs = 1234
	h.SeedActiveQueue(t, r, []*base.TaskMessage{msg}, "default")
	h.SeedLease(t, r, []base.Z{{Message: msg, Score: time.Now().Add(-time.Minute).Unix()}}, "default")

	rec := newRecoverer(recovererParams{
		logger:         testLogger,
		broker:         rdbClient,
		queues:         []string{"default"},
		interval:       time.Second,
		retryDelayFunc: DefaultRetryDelayFunc,
		isFailureFunc:  defaultIsFailureFunc,
	})
	rec.recoverLeaseExpiredTasks()

	info, err := inspector.GetTaskInfo("default", msg.ID)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != TaskStateRetry {
		t.Fatalf("recovered task state = %v, want retry", info.State)
	}
	points, err := inspector.TaskMetrics("default", time.Minute, time.Now().Add(-time.Minute), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 0 {
		t.Errorf("metrics recorded for a recovered task: %+v", points[0].Types)
	}
}
