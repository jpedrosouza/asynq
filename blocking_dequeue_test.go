// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"testing"
	"time"

	"github.com/hibiken/asynq/internal/testutil"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

// testBlockingDequeue checks that a server waiting with BlockingDequeue picks up every task as soon
// as it is enqueued, although TaskCheckInterval alone would make the tasks wait for a long time.
func testBlockingDequeue(t *testing.T, c *Client, srv *Server, queues []string) {
	t.Helper()
	type handled struct {
		payload string
		at      time.Time
	}
	done := make(chan handled, len(queues))
	err := srv.Start(HandlerFunc(func(ctx context.Context, task *Task) error {
		done <- handled{string(task.Payload()), time.Now()}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond) // the server found its queues empty and is waiting by now
	for i, qname := range queues {
		payload := string(rune('a' + i))
		enqueuedAt := time.Now()
		if _, err := c.Enqueue(NewTask("task", []byte(payload)), Queue(qname)); err != nil {
			t.Fatal(err)
		}
		select {
		case h := <-done:
			if h.payload != payload {
				t.Errorf("handled %q, want %q", h.payload, payload)
			}
			if d := h.at.Sub(enqueuedAt); d > time.Second {
				t.Errorf("task %q of queue %q was picked up after %v", payload, qname, d)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("task %q of queue %q was not picked up while the server was waiting", payload, qname)
		}
		time.Sleep(300 * time.Millisecond) // waiting again
	}
}

func TestServerWithBlockingDequeue(t *testing.T) {
	if useRedisCluster {
		t.Skip("blocking dequeue is not supported with redis cluster")
	}
	ignoreOpt := goleak.IgnoreTopFunction("github.com/redis/go-redis/v9/internal/pool.(*ConnPool).reaper")
	defer goleak.VerifyNone(t, ignoreOpt)

	redisConnOpt := getRedisConnOpt(t)
	flush := redisConnOpt.MakeRedisClient().(redis.UniversalClient)
	testutil.FlushDB(t, flush)
	defer flush.Close()

	c := NewClient(redisConnOpt)
	defer c.Close()
	srv := NewServer(redisConnOpt, Config{
		Concurrency:       2,
		Queues:            map[string]int{"low": 1, "high": 2},
		TaskCheckInterval: 30 * time.Second,
		BlockingDequeue:   true,
		LogLevel:          testLogLevel,
	})
	testBlockingDequeue(t, c, srv, []string{"low", "high", "low"})

	// The shutdown closes the connection of the wait instead of waiting for the 30 seconds
	start := time.Now()
	srv.Shutdown()
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("shutdown took %v", d)
	}
}

func TestServerFromRedisClientWithBlockingDequeue(t *testing.T) {
	if useRedisCluster {
		t.Skip("blocking dequeue is not supported with redis cluster")
	}
	redisConnOpt := getRedisConnOpt(t)
	redisClient := redisConnOpt.MakeRedisClient().(redis.UniversalClient)
	defer redisClient.Close()
	testutil.FlushDB(t, redisClient)

	c := NewClientFromRedisClient(redisClient)
	// The wait shares the client, so the shutdown waits for it: a short interval
	srv := NewServerFromRedisClient(redisClient, Config{
		Concurrency:       1,
		Queues:            map[string]int{"shared": 1},
		TaskCheckInterval: 2 * time.Second,
		BlockingDequeue:   true,
		LogLevel:          testLogLevel,
	})
	testBlockingDequeue(t, c, srv, []string{"shared", "shared"})
	srv.Shutdown()
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		t.Errorf("the shared client should still be open: %v", err)
	}
}
