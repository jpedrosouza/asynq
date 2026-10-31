// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
	h "github.com/jpedrosouza/asynq/internal/testutil"
	"github.com/jpedrosouza/asynq/internal/timeutil"
)

func TestDequeueTaskReportsPendingSince(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now().Truncate(time.Millisecond)
	clock := timeutil.NewSimulatedClock(now)
	r.SetClock(clock)

	msg := h.NewTaskMessage("send_email", nil)
	if err := r.Enqueue(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	clock.AdvanceTime(3 * time.Second)

	got, err := r.DequeueTask("default")
	if err != nil {
		t.Fatalf("DequeueTask returned error: %v", err)
	}
	if got.Msg.ID != msg.ID {
		t.Errorf("DequeueTask returned task %s, want %s", got.Msg.ID, msg.ID)
	}
	if !got.PendingSince.Equal(now) {
		t.Errorf("PendingSince = %v, want %v", got.PendingSince, now)
	}
	if want := now.Add(3 * time.Second).Add(LeaseDuration); !got.LeaseExpiration.Equal(want) {
		t.Errorf("LeaseExpiration = %v, want %v", got.LeaseExpiration, want)
	}
}

func TestDequeueTaskAppliesQueueRateLimit(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	clock := timeutil.NewSimulatedClock(now)
	r.SetClock(clock)

	for i := 0; i < 5; i++ {
		if err := r.Enqueue(context.Background(), h.NewTaskMessage("task", nil)); err != nil {
			t.Fatal(err)
		}
	}
	// 2 tasks per second: one every 500ms.
	if err := r.SetQueueRateLimit("default", base.RateLimit{Limit: 2, PeriodMs: 1000}); err != nil {
		t.Fatal(err)
	}

	if _, err := r.DequeueTask("default"); err != nil {
		t.Fatalf("first DequeueTask returned error: %v", err)
	}
	_, err := r.DequeueTask("default")
	var limited *errors.RateLimitedError
	if !errors.As(err, &limited) {
		t.Fatalf("second DequeueTask returned %v, want a RateLimitedError", err)
	}
	if !errors.Is(err, errors.ErrNoProcessableTask) {
		t.Errorf("RateLimitedError does not match ErrNoProcessableTask")
	}
	if limited.RetryIn <= 0 || limited.RetryIn > 500*time.Millisecond {
		t.Errorf("RetryIn = %v, want (0, 500ms]", limited.RetryIn)
	}
	if diff := cmp.Diff([]string{"default"}, limited.Queues); diff != "" {
		t.Errorf("Queues mismatch (-want,+got):\n%s", diff)
	}

	clock.AdvanceTime(500 * time.Millisecond)
	if _, err := r.DequeueTask("default"); err != nil {
		t.Fatalf("DequeueTask after waiting returned error: %v", err)
	}

	// Removing the rate limit lets the remaining tasks through.
	if err := r.RemoveQueueRateLimit("default"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := r.DequeueTask("default"); err != nil {
			t.Fatalf("DequeueTask without rate limit returned error: %v", err)
		}
	}
	if _, err := r.DequeueTask("default"); !errors.Is(err, errors.ErrNoProcessableTask) || errors.As(err, &limited) {
		t.Errorf("DequeueTask of an empty queue returned %v, want ErrNoProcessableTask", err)
	}
}

func TestDequeueTaskAppliesQueueRateLimitBurst(t *testing.T) {
	r := setup(t)
	defer r.Close()
	clock := timeutil.NewSimulatedClock(time.Now())
	r.SetClock(clock)

	for i := 0; i < 5; i++ {
		if err := r.Enqueue(context.Background(), h.NewTaskMessage("task", nil)); err != nil {
			t.Fatal(err)
		}
	}
	// 10 tasks per minute (one every 6s), with up to 3 back to back.
	if err := r.SetQueueRateLimit("default", base.RateLimit{Limit: 10, PeriodMs: 60000, Burst: 3}); err != nil {
		t.Fatal(err)
	}
	st, err := r.QueueRateLimit("default")
	if err != nil {
		t.Fatal(err)
	}
	if st == nil || st.Available != 3 || st.Limit != 10 || st.Burst != 3 {
		t.Fatalf("QueueRateLimit = %+v, want 3 available out of a burst of 3", st)
	}
	for i := 0; i < 3; i++ {
		if _, err := r.DequeueTask("default"); err != nil {
			t.Fatalf("DequeueTask #%d returned error: %v", i+1, err)
		}
	}
	_, err = r.DequeueTask("default")
	var limited *errors.RateLimitedError
	if !errors.As(err, &limited) {
		t.Fatalf("fourth DequeueTask returned %v, want a RateLimitedError", err)
	}
	if limited.RetryIn != 6*time.Second {
		t.Errorf("RetryIn = %v, want 6s", limited.RetryIn)
	}
	st, err = r.QueueRateLimit("default")
	if err != nil {
		t.Fatal(err)
	}
	if st.Available != 0 {
		t.Errorf("Available = %d, want 0", st.Available)
	}
}

func TestDequeueTaskSkipsRateLimitedQueue(t *testing.T) {
	r := setup(t)
	defer r.Close()
	r.SetClock(timeutil.NewSimulatedClock(time.Now()))

	h.SeedAllPendingQueues(t, r.client, map[string][]*base.TaskMessage{
		"limited": {h.NewTaskMessageWithQueue("a", nil, "limited"), h.NewTaskMessageWithQueue("b", nil, "limited")},
		"free":    {h.NewTaskMessageWithQueue("c", nil, "free")},
	})
	if err := r.SetQueueRateLimit("limited", base.RateLimit{Limit: 1, PeriodMs: 60000}); err != nil {
		t.Fatal(err)
	}
	got, err := r.DequeueTask("limited", "free")
	if err != nil || got.Msg.Queue != "limited" {
		t.Fatalf("DequeueTask = %v, %v; want a task of the limited queue", got, err)
	}
	got, err = r.DequeueTask("limited", "free")
	if err != nil || got.Msg.Queue != "free" {
		t.Fatalf("DequeueTask = %v, %v; want a task of the free queue", got, err)
	}
}

func TestTaskMetricsAreRecordedWhenProcessingEnds(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Date(2026, 10, 31, 15, 4, 30, 0, time.UTC)
	r.SetClock(timeutil.NewSimulatedClock(now))
	ctx := context.Background()

	processed := func(typename string, durationMs, waitMs int64) *base.TaskMessage {
		msg := h.NewTaskMessage(typename, nil)
		msg.StartedAt = now.Add(-time.Duration(durationMs) * time.Millisecond).UnixMilli()
		msg.DurationMs = durationMs
		msg.WaitMs = waitMs
		return msg
	}
	m1 := processed("image:resize", 120, 30)
	m2 := processed("image:resize", 2000, -1)
	m3 := processed("image:resize", 40, 10) // fails
	m4 := processed("cdn:upload", 7, 0)     // retained
	m5 := processed("cdn:upload", 9, 0)     // archived
	m6 := h.NewTaskMessage("legacy", nil)   // no timing information
	m4.Retention = 3600
	all := []*base.TaskMessage{m1, m2, m3, m4, m5, m6}
	h.SeedActiveQueue(t, r.client, all, "default")
	var lease []base.Z
	for _, m := range all {
		lease = append(lease, base.Z{Message: m, Score: now.Add(10 * time.Second).Unix()})
	}
	h.SeedLease(t, r.client, lease, "default")

	for _, m := range []*base.TaskMessage{m1, m2, m6} {
		if err := r.Done(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Retry(ctx, m3, now.Add(time.Minute), "oops", true); err != nil {
		t.Fatal(err)
	}
	if err := r.MarkAsComplete(ctx, m4); err != nil {
		t.Fatal(err)
	}
	if err := r.Archive(ctx, m5, "boom"); err != nil {
		t.Fatal(err)
	}

	for _, res := range []time.Duration{time.Minute, time.Hour} {
		points, err := r.TaskMetrics("default", res, now.Add(-time.Hour), now)
		if err != nil {
			t.Fatal(err)
		}
		if len(points) != 1 {
			t.Fatalf("TaskMetrics(%v) returned %d points, want 1", res, len(points))
		}
		if want := now.Truncate(res); !points[0].Time.Equal(want) {
			t.Errorf("point time = %v, want %v", points[0].Time, want)
		}
		resize := points[0].Types["image:resize"]
		if resize == nil {
			t.Fatalf("no metrics for image:resize: %+v", points[0].Types)
		}
		if resize.Processed != 2 || resize.DurationSumMs != 2120 || resize.DurationMaxMs != 2000 {
			t.Errorf("image:resize processed=%d sum=%d max=%d, want 2, 2120, 2000",
				resize.Processed, resize.DurationSumMs, resize.DurationMaxMs)
		}
		if resize.Failed != 1 || resize.FailedDurationSumMs != 40 {
			t.Errorf("image:resize failed=%d failedSum=%d, want 1, 40", resize.Failed, resize.FailedDurationSumMs)
		}
		if resize.WaitCount != 2 || resize.WaitSumMs != 40 || resize.WaitMaxMs != 30 {
			t.Errorf("image:resize wait count=%d sum=%d max=%d, want 2, 40, 30",
				resize.WaitCount, resize.WaitSumMs, resize.WaitMaxMs)
		}
		if n := resize.DurationBuckets[base.TaskMetricsBucketIndex(120)]; n != 1 {
			t.Errorf("bucket of 120ms has %d tasks, want 1", n)
		}
		if n := resize.DurationBuckets[base.TaskMetricsBucketIndex(2000)]; n != 1 {
			t.Errorf("bucket of 2000ms has %d tasks, want 1", n)
		}
		upload := points[0].Types["cdn:upload"]
		if upload == nil || upload.Processed != 1 || upload.Failed != 1 || upload.DurationSumMs != 7 {
			t.Errorf("cdn:upload metrics = %+v, want 1 processed (7ms) and 1 failed", upload)
		}
		if _, ok := points[0].Types["legacy"]; ok {
			t.Errorf("metrics recorded for a task without timing information")
		}
	}

	ttl := r.client.TTL(ctx, base.TaskMetricsMinuteKey("default", now)).Val()
	if ttl <= 0 || ttl > base.TaskMetricsMinuteRetention+time.Minute {
		t.Errorf("TTL of the minute bucket = %v", ttl)
	}
}

func TestTaskMetricsCanBeDisabled(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	r.SetClock(timeutil.NewSimulatedClock(now))
	r.SetTaskMetricsEnabled(false)

	msg := h.NewTaskMessage("task", nil)
	msg.StartedAt = now.UnixMilli()
	msg.DurationMs = 5
	h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, "default")
	h.SeedLease(t, r.client, []base.Z{{Message: msg, Score: now.Add(time.Minute).Unix()}}, "default")
	if err := r.Done(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	points, err := r.TaskMetrics("default", time.Minute, now.Add(-time.Minute), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 0 {
		t.Errorf("TaskMetrics returned %d points with metrics disabled, want 0", len(points))
	}
}

func TestTaskMetricsRejectsBadResolution(t *testing.T) {
	r := setup(t)
	defer r.Close()
	if _, err := r.TaskMetrics("default", time.Second, time.Now(), time.Now()); err == nil {
		t.Errorf("TaskMetrics with a one second resolution returned no error")
	}
}

func TestReserveRateLimit(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now().Truncate(time.Millisecond)
	clock := timeutil.NewSimulatedClock(now)
	r.SetClock(clock)
	ctx := context.Background()

	slot, err := r.ReserveRateLimit(ctx, "openai")
	if err != nil || !slot.IsZero() {
		t.Fatalf("ReserveRateLimit without a rate limit = %v, %v; want zero time", slot, err)
	}
	if err := r.SetRateLimit("openai", base.RateLimit{Limit: 1, PeriodMs: 1000}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		slot, err := r.ReserveRateLimit(ctx, "openai")
		if err != nil {
			t.Fatal(err)
		}
		if want := now.Add(time.Duration(i) * time.Second); !slot.Equal(want) {
			t.Errorf("slot #%d = %v, want %v", i, slot, want)
		}
	}
	all, err := r.RateLimits()
	if err != nil {
		t.Fatal(err)
	}
	st := all["openai"]
	if st == nil || st.Limit != 1 || st.Available != 0 || st.NextAt != now.Add(3*time.Second).UnixMilli() {
		t.Errorf("RateLimits()[openai] = %+v", st)
	}
	if err := r.RemoveRateLimit("openai"); err != nil {
		t.Fatal(err)
	}
	if all, _ := r.RateLimits(); len(all) != 0 {
		t.Errorf("RateLimits after removal = %v, want none", all)
	}
}

func TestSetRateLimitValidates(t *testing.T) {
	r := setup(t)
	defer r.Close()
	for _, rl := range []base.RateLimit{{Limit: 0, PeriodMs: 1000}, {Limit: 1, PeriodMs: 0}, {Limit: 1, PeriodMs: 1, Burst: -1}} {
		if err := r.SetQueueRateLimit("default", rl); err == nil {
			t.Errorf("SetQueueRateLimit(%+v) returned no error", rl)
		}
		if err := r.SetRateLimit("key", rl); err == nil {
			t.Errorf("SetRateLimit(%+v) returned no error", rl)
		}
	}
}

func TestDefer(t *testing.T) {
	r := setup(t)
	defer r.Close()
	now := time.Now()
	msg := h.NewTaskMessage("task", nil)
	h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, "default")
	h.SeedLease(t, r.client, []base.Z{{Message: msg, Score: now.Add(time.Minute).Unix()}}, "default")

	deferred := *msg
	deferred.RateLimitSlot = now.Add(time.Hour).UnixMilli()
	processAt := now.Add(time.Hour)
	if err := r.Defer(context.Background(), &deferred, processAt); err != nil {
		t.Fatal(err)
	}
	if got := h.GetActiveMessages(t, r.client, "default"); len(got) != 0 {
		t.Errorf("active tasks after Defer = %v, want none", got)
	}
	if got := h.GetLeaseEntries(t, r.client, "default"); len(got) != 0 {
		t.Errorf("lease entries after Defer = %v, want none", got)
	}
	want := []base.Z{{Message: &deferred, Score: processAt.Unix()}}
	if diff := cmp.Diff(want, h.GetScheduledEntries(t, r.client, "default")); diff != "" {
		t.Errorf("scheduled entries mismatch (-want,+got):\n%s", diff)
	}
	if state := r.client.HGet(context.Background(), base.TaskKey("default", msg.ID), "state").Val(); state != "scheduled" {
		t.Errorf("task state = %q, want scheduled", state)
	}
}

func TestQueueMetadata(t *testing.T) {
	r := setup(t)
	defer r.Close()
	md, err := r.GetQueueMetadata("reports")
	if err != nil || md != nil {
		t.Fatalf("GetQueueMetadata of a queue without metadata = %v, %v; want nil", md, err)
	}
	want := &base.QueueMetadata{Category: "Reports", Subcategory: "Daily", Description: "Daily reports"}
	if err := r.SetQueueMetadata("reports", want); err != nil {
		t.Fatal(err)
	}
	if err := r.SetQueueMetadata("video", &base.QueueMetadata{Category: "Video"}); err != nil {
		t.Fatal(err)
	}
	got, err := r.GetQueueMetadata("reports")
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GetQueueMetadata mismatch (-want,+got):\n%s", diff)
	}
	all, err := r.ListQueueMetadata()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all["video"].Category != "Video" {
		t.Errorf("ListQueueMetadata = %v", all)
	}
	if err := r.DeleteQueueMetadata("reports"); err != nil {
		t.Fatal(err)
	}
	if md, _ := r.GetQueueMetadata("reports"); md != nil {
		t.Errorf("metadata still present after deletion: %+v", md)
	}
}
