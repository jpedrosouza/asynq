// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"testing"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
	h "github.com/jpedrosouza/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
)

func markerSet(t *testing.T, r *RDB, qname string) bool {
	t.Helper()
	n, err := r.client.ZCard(context.Background(), base.MarkerKey(qname)).Result()
	if err != nil {
		t.Fatal(err)
	}
	return n > 0
}

func clearMarker(t *testing.T, r *RDB, qname string) {
	t.Helper()
	if err := r.client.Del(context.Background(), base.MarkerKey(qname)).Err(); err != nil {
		t.Fatal(err)
	}
}

// waiterRDB is an RDB on a client of its own, as a server uses for WaitForTask.
func waiterRDB(t *testing.T) *RDB {
	t.Helper()
	if useRedisCluster {
		t.Skip("blocking dequeue is not supported with redis cluster")
	}
	return NewRDB(redis.NewClient(&redis.Options{Addr: redisAddr, DB: redisDB}))
}

func TestMarkerIsSetWheneverATaskBecomesPending(t *testing.T) {
	ctx := context.Background()
	past := time.Now().Add(-time.Minute).Unix()
	tests := []struct {
		name string
		run  func(t *testing.T, r *RDB) error
	}{
		{"enqueue", func(t *testing.T, r *RDB) error {
			return r.Enqueue(ctx, h.NewTaskMessage("task", nil))
		}},
		{"enqueue unique", func(t *testing.T, r *RDB) error {
			msg := h.NewTaskMessage("task", nil)
			msg.UniqueKey = base.UniqueKey(msg.Queue, msg.Type, msg.Payload)
			return r.EnqueueUnique(ctx, msg, time.Minute)
		}},
		{"batch enqueue", func(t *testing.T, r *RDB) error {
			_, err := r.BatchEnqueue(ctx, []base.BatchEnqueueItem{{Msg: h.NewTaskMessage("task", nil)}})
			return err
		}},
		{"requeue an active task", func(t *testing.T, r *RDB) error {
			msg := h.NewTaskMessage("task", nil)
			h.SeedActiveQueue(t, r.client, []*base.TaskMessage{msg}, base.DefaultQueueName)
			h.SeedLease(t, r.client, []base.Z{{Message: msg, Score: time.Now().Add(time.Minute).Unix()}}, base.DefaultQueueName)
			return r.Requeue(ctx, msg)
		}},
		{"forward a scheduled task", func(t *testing.T, r *RDB) error {
			h.SeedScheduledQueue(t, r.client, []base.Z{{Message: h.NewTaskMessage("task", nil), Score: past}}, base.DefaultQueueName)
			return r.ForwardIfReady(base.DefaultQueueName)
		}},
		{"forward a retry", func(t *testing.T, r *RDB) error {
			h.SeedRetryQueue(t, r.client, []base.Z{{Message: h.NewTaskMessage("task", nil), Score: past}}, base.DefaultQueueName)
			return r.ForwardIfReady(base.DefaultQueueName)
		}},
		{"run a task", func(t *testing.T, r *RDB) error {
			msg := h.NewTaskMessage("task", nil)
			h.SeedArchivedQueue(t, r.client, []base.Z{{Message: msg, Score: past}}, base.DefaultQueueName)
			return r.RunTask(base.DefaultQueueName, msg.ID)
		}},
		{"run all archived tasks", func(t *testing.T, r *RDB) error {
			h.SeedArchivedQueue(t, r.client, []base.Z{{Message: h.NewTaskMessage("task", nil), Score: past}}, base.DefaultQueueName)
			_, err := r.RunAllArchivedTasks(base.DefaultQueueName)
			return err
		}},
		{"run all aggregating tasks", func(t *testing.T, r *RDB) error {
			msg := h.NewTaskMessageBuilder().SetType("task").SetGroup("group").Build()
			h.SeedGroup(t, r.client, []base.Z{{Message: msg, Score: past}}, base.DefaultQueueName, "group")
			_, err := r.RunAllAggregatingTasks(base.DefaultQueueName, "group")
			return err
		}},
		{"unpause a queue", func(t *testing.T, r *RDB) error {
			h.SeedPendingQueue(t, r.client, []*base.TaskMessage{h.NewTaskMessage("task", nil)}, base.DefaultQueueName)
			if err := r.Pause(base.DefaultQueueName); err != nil {
				return err
			}
			clearMarker(t, r, base.DefaultQueueName)
			return r.Unpause(base.DefaultQueueName)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := setup(t)
			defer r.Close()
			if err := tc.run(t, r); err != nil {
				t.Fatal(err)
			}
			if !markerSet(t, r, base.DefaultQueueName) {
				t.Error("the marker should be set")
			}
		})
	}
}

func TestForwardOfAGroupedTaskDoesNotSetTheMarker(t *testing.T) {
	r := setup(t)
	defer r.Close()
	msg := h.NewTaskMessageBuilder().SetType("task").SetGroup("group").Build()
	h.SeedScheduledQueue(t, r.client, []base.Z{{Message: msg, Score: time.Now().Add(-time.Minute).Unix()}}, base.DefaultQueueName)
	if err := r.ForwardIfReady(base.DefaultQueueName); err != nil {
		t.Fatal(err)
	}
	if markerSet(t, r, base.DefaultQueueName) {
		t.Error("a task moved to its group is not pending: the marker should not be set")
	}
}

func TestDequeueSetsTheMarkerAgainWhileTasksArePending(t *testing.T) {
	r := setup(t)
	defer r.Close()
	q := base.DefaultQueueName
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{h.NewTaskMessage("a", nil), h.NewTaskMessage("b", nil)}, q)

	if _, _, err := r.Dequeue(q); err != nil {
		t.Fatal(err)
	}
	if !markerSet(t, r, q) {
		t.Error("one task is still pending: the marker should be set for the other servers")
	}
	clearMarker(t, r, q)
	if _, _, err := r.Dequeue(q); err != nil {
		t.Fatal(err)
	}
	if markerSet(t, r, q) {
		t.Error("no task is pending anymore: the marker should not be set")
	}
}

func TestDequeueOfAPausedQueueDoesNotSetTheMarker(t *testing.T) {
	r := setup(t)
	defer r.Close()
	q := base.DefaultQueueName
	h.SeedPendingQueue(t, r.client, []*base.TaskMessage{h.NewTaskMessage("a", nil), h.NewTaskMessage("b", nil)}, q)
	if err := r.Pause(q); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Dequeue(q); err == nil {
		t.Fatal("a paused queue should have no processable task")
	}
	if markerSet(t, r, q) {
		t.Error("the marker would wake the servers over and over while the queue is paused")
	}
}

func TestRemoveQueueDeletesTheMarker(t *testing.T) {
	r := setup(t)
	defer r.Close()
	q := base.DefaultQueueName
	if err := r.Enqueue(context.Background(), h.NewTaskMessage("task", nil)); err != nil {
		t.Fatal(err)
	}
	if err := r.client.Del(context.Background(), base.PendingKey(q)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := r.RemoveQueue(q, false); err != nil {
		t.Fatal(err)
	}
	if markerSet(t, r, q) {
		t.Error("the marker should go with the queue")
	}
}

func TestWaitForTask(t *testing.T) {
	ctx := context.Background()

	t.Run("returns at once when a task was signaled, and consumes the signal", func(t *testing.T) {
		r := setup(t)
		defer r.Close()
		w := waiterRDB(t)
		defer w.Close()
		if err := r.Enqueue(ctx, h.NewTaskMessage("task", nil)); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := w.WaitForTask(ctx, 5*time.Second, base.DefaultQueueName); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("waited %v", d)
		}
		if markerSet(t, r, base.DefaultQueueName) {
			t.Error("the signal should be consumed")
		}
	})

	t.Run("returns after the timeout without error when nothing happens", func(t *testing.T) {
		r := setup(t)
		defer r.Close()
		w := waiterRDB(t)
		defer w.Close()
		start := time.Now()
		if err := w.WaitForTask(ctx, 300*time.Millisecond, base.DefaultQueueName); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d < 250*time.Millisecond || d > 2*time.Second {
			t.Errorf("waited %v, want about the timeout", d)
		}
	})

	t.Run("wakes up when a task is enqueued in any of the queues", func(t *testing.T) {
		r := setup(t)
		defer r.Close()
		w := waiterRDB(t)
		defer w.Close()
		done := make(chan time.Duration, 1)
		start := time.Now()
		go func() {
			if err := w.WaitForTask(ctx, 10*time.Second, "low", "high"); err != nil {
				t.Error(err)
			}
			done <- time.Since(start)
		}()
		time.Sleep(200 * time.Millisecond)
		if err := r.Enqueue(ctx, h.NewTaskMessageWithQueue("task", nil, "high")); err != nil {
			t.Fatal(err)
		}
		select {
		case d := <-done:
			if d > 2*time.Second {
				t.Errorf("woke up after %v", d)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the wait did not end when the task was enqueued")
		}
	})

	t.Run("closing the client ends the wait", func(t *testing.T) {
		w := waiterRDB(t)
		done := make(chan error, 1)
		go func() { done <- w.WaitForTask(ctx, 10*time.Second, base.DefaultQueueName) }()
		time.Sleep(200 * time.Millisecond)
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err == nil {
				t.Error("a wait ended by Close should report it")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not end the wait")
		}
	})
}
