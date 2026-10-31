// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jpedrosouza/asynq/internal/rdb"
)

// runFlowProcessor processes the tasks of the queues with the handler and the flows for the given time.
func runFlowProcessor(t *testing.T, r *rdb.RDB, handler Handler, flows []*Flow, queues map[string]int, d time.Duration) {
	t.Helper()
	p := newProcessorForTest(t, r, handler)
	p.flows = newFlowRunner(testLogger, r, flows)
	if queues != nil {
		p.queueConfig = queues
	}
	p.start(&sync.WaitGroup{})
	time.Sleep(d)
	p.shutdown()
}

func TestFlowRunsStepsAndJoins(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	flow := NewFlow("images").
		Then("image:upload", "image:resize", nil).
		Then("image:upload", "image:thumbnail", func(ctx context.Context, parent *FlowTask) ([]*Task, error) {
			return []*Task{NewTask("image:thumbnail", append([]byte("thumb:"), parent.Result...))}, nil
		}).
		Then("image:resize", "cdn:send", nil, Queue("cdn")).
		Join([]string{"cdn:send", "image:thumbnail"}, "image:notify", nil)

	var mu sync.Mutex
	payloads := map[string][]string{}
	mux := NewServeMux()
	mux.HandleFunc("image:", func(ctx context.Context, task *Task) error {
		mu.Lock()
		payloads[task.Type()] = append(payloads[task.Type()], string(task.Payload()))
		mu.Unlock()
		switch task.Type() {
		case "image:upload":
			_, err := task.ResultWriter().Write([]byte(`"s3://bucket/a.png"`))
			return err
		case "image:thumbnail":
			_, err := task.ResultWriter().Write([]byte(`"thumb.png"`))
			return err
		}
		return nil
	})
	mux.HandleFunc("cdn:send", func(ctx context.Context, task *Task) error {
		mu.Lock()
		payloads[task.Type()] = append(payloads[task.Type()], string(task.Payload()))
		mu.Unlock()
		_, err := task.ResultWriter().Write([]byte(`"https://cdn/a.png"`))
		return err
	})

	info, err := client.Enqueue(NewTask("image:upload", []byte("raw")))
	if err != nil {
		t.Fatal(err)
	}
	runFlowProcessor(t, rdbClient, mux, []*Flow{flow}, map[string]int{"default": 1, "cdn": 1}, 4*time.Second)

	mu.Lock()
	want := map[string][]string{
		"image:upload":    {"raw"},
		"image:resize":    {`"s3://bucket/a.png"`}, // nil step: the result of the parent
		"image:thumbnail": {`thumb:"s3://bucket/a.png"`},
		"cdn:send":        {`"s3://bucket/a.png"`}, // the resize wrote no result: the payload of the parent
	}
	notify := payloads["image:notify"]
	delete(payloads, "image:notify")
	if diff := cmp.Diff(want, payloads); diff != "" {
		t.Errorf("processed payloads mismatch (-want,+got):\n%s", diff)
	}
	mu.Unlock()
	if len(notify) != 1 {
		t.Fatalf("image:notify processed %d times, want once", len(notify))
	}
	var joined []struct {
		Type   string          `json:"type"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal([]byte(notify[0]), &joined); err != nil {
		t.Fatalf("join payload %q: %v", notify[0], err)
	}
	sort.Slice(joined, func(i, j int) bool { return joined[i].Type < joined[j].Type })
	if len(joined) != 2 || joined[0].Type != "cdn:send" || string(joined[0].Result) != `"https://cdn/a.png"` ||
		joined[1].Type != "image:thumbnail" || string(joined[1].Result) != `"thumb.png"` {
		t.Errorf("join payload = %s", notify[0])
	}

	run, err := inspector.GetFlowRun(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Flow != "images" || run.RootID != info.ID || run.RootType != "image:upload" || run.State != FlowRunCompleted {
		t.Errorf("run = %+v", run)
	}
	byType := map[string]*FlowNodeInfo{}
	for _, n := range run.Nodes {
		byType[n.Type] = n
		if n.State != "completed" {
			t.Errorf("node %s is %s, want completed", n.Type, n.State)
		}
	}
	if len(run.Nodes) != 5 {
		t.Fatalf("run has %d nodes, want 5: %+v", len(run.Nodes), run.Nodes)
	}
	if p := byType["cdn:send"].Parents; len(p) != 1 || p[0] != byType["image:resize"].ID {
		t.Errorf("cdn:send parents = %v, want the resize task", p)
	}
	if byType["cdn:send"].Queue != "cdn" {
		t.Errorf("cdn:send queue = %q, want cdn", byType["cdn:send"].Queue)
	}
	if p := byType["image:notify"].Parents; len(p) != 2 {
		t.Errorf("image:notify parents = %v, want the cdn and thumbnail tasks", p)
	}
	if byType["image:upload"].Duration < 0 || byType["image:upload"].StartedAt.IsZero() {
		t.Errorf("upload node has no timing: %+v", byType["image:upload"])
	}
	runs, err := inspector.FlowRuns("images")
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].ID != info.ID {
		t.Errorf("FlowRuns = %+v, want the run of %s", runs, info.ID)
	}
	if runs, _ := inspector.FlowRuns("other"); len(runs) != 0 {
		t.Errorf("FlowRuns(other) = %+v, want none", runs)
	}
}

func TestFlowStepErrorFailsTheParent(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	flow := NewFlow("broken").Then("a", "b", func(ctx context.Context, parent *FlowTask) ([]*Task, error) {
		return nil, errors.New("bad result")
	})
	info, err := client.Enqueue(NewTask("a", nil), MaxRetry(3))
	if err != nil {
		t.Fatal(err)
	}
	runFlowProcessor(t, rdbClient, HandlerFunc(func(context.Context, *Task) error { return nil }), []*Flow{flow}, nil, 1500*time.Millisecond)

	got, err := inspector.GetTaskInfo("default", info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != TaskStateRetry || got.Retried != 1 {
		t.Errorf("parent state=%v retried=%d, want retry after a failed step", got.State, got.Retried)
	}
	if _, err := inspector.GetFlowRun(info.ID); !errors.Is(err, ErrFlowRunNotFound) {
		t.Errorf("GetFlowRun = %v, want ErrFlowRunNotFound", err)
	}
}

func TestFlowJoinWaitsAndStopsOnFailure(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	flow := NewFlow("fanin").
		Then("start", "work", func(ctx context.Context, parent *FlowTask) ([]*Task, error) {
			return []*Task{NewTask("work", []byte("ok")), NewTask("work", []byte("fail"))}, nil
		}, MaxRetry(0)).
		Join([]string{"work"}, "done", nil)

	var mu sync.Mutex
	done := 0
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		switch {
		case task.Type() == "work" && string(task.Payload()) == "fail":
			return errors.New("boom")
		case task.Type() == "done":
			mu.Lock()
			done++
			mu.Unlock()
		}
		return nil
	})
	info, err := client.Enqueue(NewTask("start", nil))
	if err != nil {
		t.Fatal(err)
	}
	runFlowProcessor(t, rdbClient, handler, []*Flow{flow}, nil, 2500*time.Millisecond)

	mu.Lock()
	if done != 0 {
		t.Errorf("join ran %d times although a joined task failed", done)
	}
	mu.Unlock()
	run, err := inspector.GetFlowRun(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.State != FlowRunFailed || len(run.Nodes) != 3 {
		t.Errorf("run state=%s nodes=%d, want failed with 3 nodes", run.State, len(run.Nodes))
	}
	failed := 0
	for _, n := range run.Nodes {
		if n.State == "failed" {
			failed++
			if n.Error != "boom" {
				t.Errorf("failed node error = %q, want boom", n.Error)
			}
		}
	}
	if failed != 1 {
		t.Errorf("%d failed nodes, want 1", failed)
	}
}

func TestEnqueueChild(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	if _, err := EnqueueChild(context.Background(), NewTask("x", nil)); err == nil {
		t.Errorf("EnqueueChild outside of a handler returned no error")
	}
	var childInfo *TaskInfo
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		if task.Type() != "parent" {
			return nil
		}
		var err error
		childInfo, err = EnqueueChild(ctx, NewTask("child", []byte("data")), Queue("children"))
		return err
	})
	info, err := client.Enqueue(NewTask("parent", nil))
	if err != nil {
		t.Fatal(err)
	}
	runFlowProcessor(t, rdbClient, handler, nil, map[string]int{"default": 1, "children": 1}, 2*time.Second)

	if childInfo == nil {
		t.Fatal("child was not enqueued")
	}
	if childInfo.FlowRunID != info.ID || childInfo.ParentID != info.ID || childInfo.Queue != "children" {
		t.Errorf("child info: run=%q parent=%q queue=%q", childInfo.FlowRunID, childInfo.ParentID, childInfo.Queue)
	}
	run, err := inspector.GetFlowRun(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Flow != "" || run.State != FlowRunCompleted || len(run.Nodes) != 2 {
		t.Fatalf("run = %+v, want a completed ad hoc run with 2 nodes", run)
	}
	for _, n := range run.Nodes {
		if n.Type == "child" && (len(n.Parents) != 1 || n.Parents[0] != info.ID) {
			t.Errorf("child parents = %v", n.Parents)
		}
	}
}

func TestFlowDefinitionsArePublished(t *testing.T) {
	r := setup(t)
	defer r.Close()
	inspector := NewInspector(getRedisConnOpt(t))
	defer inspector.Close()

	flow := NewFlow("images").Then("a", "b", nil).Join([]string{"b"}, "c", nil)
	pub := newFlowPublisher(testLogger, rdb.NewRDB(r), []*Flow{flow})
	var wg sync.WaitGroup
	pub.start(&wg)
	time.Sleep(300 * time.Millisecond)
	pub.shutdown()
	wg.Wait()

	flows, err := inspector.Flows()
	if err != nil {
		t.Fatal(err)
	}
	if len(flows) != 1 || flows[0].Name != "images" || len(flows[0].Steps) != 2 || !flows[0].Steps[1].Join {
		t.Fatalf("Flows = %+v", flows)
	}
	if err := inspector.DeleteFlow("images"); err != nil {
		t.Fatal(err)
	}
	if flows, _ := inspector.Flows(); len(flows) != 0 {
		t.Errorf("Flows after DeleteFlow = %+v", flows)
	}
	defer func() {
		if recover() == nil {
			t.Errorf("two flows with the same name did not panic")
		}
	}()
	newFlowRegistry([]*Flow{NewFlow("x"), NewFlow("x")})
}

// A handler that outlives its timeout and then calls EnqueueChild must not race with the processor ending the task.
func TestEnqueueChildAfterTimeout(t *testing.T) {
	r := setup(t)
	defer r.Close()
	rdbClient := rdb.NewRDB(r)
	client := NewClient(getRedisConnOpt(t))
	defer client.Close()

	done := make(chan struct{})
	handler := HandlerFunc(func(ctx context.Context, task *Task) error {
		if task.Type() != "slow" {
			return nil
		}
		defer close(done)
		<-ctx.Done() // the task times out
		time.Sleep(50 * time.Millisecond)
		_, err := EnqueueChild(ctx, NewTask("child", nil))
		return err
	})
	if _, err := client.Enqueue(NewTask("slow", nil), Timeout(time.Second), MaxRetry(0)); err != nil {
		t.Fatal(err)
	}
	p := newProcessorForTest(t, rdbClient, handler)
	p.flows = newFlowRunner(testLogger, rdbClient, nil)
	p.start(&sync.WaitGroup{})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return")
	}
	time.Sleep(200 * time.Millisecond)
	p.shutdown()
}
