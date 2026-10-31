// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
	"github.com/jpedrosouza/asynq/internal/log"
	"github.com/jpedrosouza/asynq/internal/rdb"
)

// A Flow chains tasks: when a task completes successfully, the server that processed it enqueues the next tasks
// of the flow, built from the task and its result. The tasks enqueued this way form a flow run, which tools like
// a dashboard can show as a graph (see Inspector.GetFlowRun).
//
// For example, to resize every uploaded image and then send it to a CDN, while also generating a thumbnail:
//
//	flow := asynq.NewFlow("images").
//		Then("image:upload", "image:resize", nil).    // nil: the result of the upload becomes the payload
//		Then("image:upload", "image:thumbnail", nil). // runs at the same time as the resize
//		Then("image:resize", "cdn:send", func(ctx context.Context, parent *asynq.FlowTask) ([]*asynq.Task, error) {
//			return []*asynq.Task{asynq.NewTask("cdn:send", parent.Result, asynq.Queue("cdn"))}, nil
//		}).
//		Join([]string{"cdn:send", "image:thumbnail"}, "image:notify", nil) // once both are done
//
//	srv := asynq.NewServer(redis, asynq.Config{Flows: []*asynq.Flow{flow}})
//
// A flow is defined in code and registered on the servers that process the parent tasks; the servers publish
// the definition to redis so that tools can show it. Steps run at least once: the next tasks get IDs derived from
// their parent so that a parent processed twice does not enqueue them twice while they exist.
//
// Tasks may also be chained by hand from a handler with EnqueueChild.
type Flow struct {
	name  string
	steps []*flowStep
}

type flowStep struct {
	flow   *Flow
	from   []string
	to     string
	join   bool
	fn     FlowFunc
	joinFn JoinFunc
	opts   []Option

	// relevant holds the types of the tasks a join must wait for: the types it joins and their ancestors.
	relevant map[string]bool
}

// FlowTask describes a completed task of a flow run, passed to the functions that build the next tasks.
type FlowTask struct {
	// ID is the ID of the task.
	ID string
	// Queue is the queue of the task.
	Queue string
	// Type is the type name of the task.
	Type string
	// Payload is the payload of the task (not available to a JoinFunc).
	Payload []byte
	// Headers are the headers of the task (not available to a JoinFunc).
	Headers map[string]string
	// Result is the result written by the handler of the task with its ResultWriter, if any. A JoinFunc only gets
	// results up to 64KiB.
	Result []byte
	// RunID is the ID of the flow run.
	RunID string
}

// FlowFunc builds the tasks to enqueue after the given task completed. It may return no task to stop the run there,
// or several tasks to fan out. An error fails the parent task, which is retried.
type FlowFunc func(ctx context.Context, parent *FlowTask) ([]*Task, error)

// JoinFunc builds the tasks to enqueue once all the joined tasks of a run completed.
type JoinFunc func(ctx context.Context, parents []*FlowTask) ([]*Task, error)

// NewFlow returns an empty flow with the given name. The name identifies the flow in tools and must be unique.
func NewFlow(name string) *Flow {
	if isBlank(name) {
		panic("asynq: flow name cannot be empty")
	}
	return &Flow{name: name}
}

// Name returns the name of the flow.
func (f *Flow) Name() string { return f.name }

// Then adds a step to the flow: every time a task of type parent completes successfully, the tasks returned by fn
// are enqueued. Calling Then several times with the same parent fans out: all the steps run.
//
// A nil fn enqueues one task of type child whose payload is the result of the parent, or the payload of the parent
// if the parent wrote no result. The options apply to the enqueued tasks, before their own options; by default the
// tasks are enqueued in the queue of the parent.
func (f *Flow) Then(parent, child string, fn FlowFunc, opts ...Option) *Flow {
	if isBlank(parent) || isBlank(child) {
		panic("asynq: flow step task types cannot be empty")
	}
	f.steps = append(f.steps, &flowStep{flow: f, from: []string{parent}, to: child, fn: fn, opts: opts})
	return f
}

// Join adds a step that waits, in each run, for all the tasks of the given types: once they all completed
// successfully, and no task that could lead to more of them is left, the tasks returned by fn are enqueued.
// The step does not run if one of the joined tasks fails for good.
//
// A nil fn enqueues one task of type child whose payload is a JSON array of the joined tasks, each an object with
// the fields "id", "type" and "result".
func (f *Flow) Join(parents []string, child string, fn JoinFunc, opts ...Option) *Flow {
	if len(parents) == 0 || isBlank(child) {
		panic("asynq: flow join needs task types to join and a child task type")
	}
	f.steps = append(f.steps, &flowStep{flow: f, from: slices.Clone(parents), to: child, join: true, joinFn: fn, opts: opts})
	return f
}

func (f *Flow) definition() *base.FlowDefinition {
	def := &base.FlowDefinition{Name: f.name}
	for _, st := range f.steps {
		def.Steps = append(def.Steps, &base.FlowStep{From: st.from, To: st.to, Join: st.join})
	}
	return def
}

// flowRegistry indexes the steps of the flows registered on a server.
type flowRegistry struct {
	flows  []*Flow
	byType map[string][]*flowStep // non-join steps by parent type
	joins  map[string][]*flowStep // join steps by flow name
}

func newFlowRegistry(flows []*Flow) *flowRegistry {
	reg := &flowRegistry{byType: make(map[string][]*flowStep), joins: make(map[string][]*flowStep)}
	seen := make(map[string]bool)
	for _, f := range flows {
		if f == nil {
			continue
		}
		if seen[f.name] {
			panic(fmt.Sprintf("asynq: two flows are named %q", f.name))
		}
		seen[f.name] = true
		reg.flows = append(reg.flows, f)
		// parents maps each type to the types whose completion enqueues it.
		parents := make(map[string][]string)
		for _, st := range f.steps {
			parents[st.to] = append(parents[st.to], st.from...)
			if !st.join {
				reg.byType[st.from[0]] = append(reg.byType[st.from[0]], st)
			}
		}
		for _, st := range f.steps {
			if !st.join {
				continue
			}
			st.relevant = make(map[string]bool)
			stack := slices.Clone(st.from)
			for len(stack) > 0 {
				t := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if st.relevant[t] {
					continue
				}
				st.relevant[t] = true
				stack = append(stack, parents[t]...)
			}
			reg.joins[f.name] = append(reg.joins[f.name], st)
		}
	}
	return reg
}

func (reg *flowRegistry) empty() bool { return reg == nil || len(reg.flows) == 0 }

// stepsFrom returns the steps that follow the task: the steps of its flow, or of any flow if it is not part of one.
func (reg *flowRegistry) stepsFrom(msg *base.TaskMessage) []*flowStep {
	if reg.empty() {
		return nil
	}
	var res []*flowStep
	for _, st := range reg.byType[msg.Type] {
		if msg.Flow == "" || st.flow.name == msg.Flow {
			res = append(res, st)
		}
	}
	return res
}

// flowOption sets the lineage of a task enqueued by a flow. It is internal and cannot be parsed.
type flowOption struct {
	flow, run, parent string
}

const flowOpt OptionType = -1

func (o flowOption) String() string     { return fmt.Sprintf("flow(%q, %q, %q)", o.flow, o.run, o.parent) }
func (o flowOption) Type() OptionType   { return flowOpt }
func (o flowOption) Value() interface{} { return [3]string{o.flow, o.run, o.parent} }

// flowContext is attached to the context passed to handlers, for EnqueueChild.
//
// The handler may still run while the processor ends the task (e.g. after a timeout), so EnqueueChild does not touch
// the task message: it reads a copy of it, and records the run it starts in runID, which the processor copies into
// the message from its own goroutine (see apply).
type flowContext struct {
	runner *flowRunner
	task   base.TaskMessage // copy of the task message, read only

	mu    sync.Mutex
	runID string // run started by EnqueueChild, guarded by mu
}

func newFlowContext(runner *flowRunner, msg *base.TaskMessage) *flowContext {
	return &flowContext{runner: runner, task: *msg}
}

// apply records in the task message the flow run started by EnqueueChild, if any.
func (fc *flowContext) apply(msg *base.TaskMessage) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if fc.runID != "" {
		msg.FlowRun = fc.runID
	}
}

type flowContextKey struct{}

// flowRunner runs the flow steps for a processor.
type flowRunner struct {
	logger *log.Logger
	broker base.Broker
	client *Client
	reg    *flowRegistry
	clock  func() time.Time
}

func newFlowRunner(logger *log.Logger, broker base.Broker, flows []*Flow) *flowRunner {
	return &flowRunner{
		logger: logger,
		broker: broker,
		client: &Client{broker: broker},
		reg:    newFlowRegistry(flows),
		clock:  time.Now,
	}
}

func taskToFlowTask(msg *base.TaskMessage, result []byte, runID string) *FlowTask {
	return &FlowTask{
		ID:      msg.ID,
		Queue:   msg.Queue,
		Type:    msg.Type,
		Payload: msg.Payload,
		Headers: maps.Clone(msg.Headers),
		Result:  result,
		RunID:   runID,
	}
}

// childEnqueue is a task to enqueue in a flow run.
type childEnqueue struct {
	task *Task
	opts []Option
	opt  option // the composed options, to know the queue and ID before enqueueing
}

func (fr *flowRunner) prepare(task *Task, opts []Option) (*childEnqueue, error) {
	if task == nil || isBlank(task.Type()) {
		return nil, fmt.Errorf("flow step returned a task without type")
	}
	all := append(slices.Clone(opts), task.opts...)
	opt, err := composeOptions(all...)
	if err != nil {
		return nil, err
	}
	return &childEnqueue{task: NewTaskWithHeaders(task.Type(), task.Payload(), task.Headers()), opts: all, opt: opt}, nil
}

// enqueueChildren records the children in the run, then enqueues them.
func (fr *flowRunner) enqueueChildren(ctx context.Context, runID string, meta *base.FlowRunMeta, nodes []*base.FlowNode,
	children []*childEnqueue, flow string, parents map[*childEnqueue][]string) error {
	now := fr.clock().UnixMilli()
	for _, c := range children {
		nodes = append(nodes, &base.FlowNode{
			ID:         c.opt.taskID,
			Queue:      c.opt.queue,
			Type:       c.task.Type(),
			Parents:    parents[c],
			State:      base.FlowNodePending,
			EnqueuedAt: now,
		})
	}
	if err := fr.broker.UpdateFlowRun(ctx, runID, meta, nodes); err != nil {
		return fmt.Errorf("cannot record flow run %s: %w", runID, err)
	}
	for _, c := range children {
		parent := ""
		if ps := parents[c]; len(ps) == 1 {
			parent = ps[0]
		}
		opts := append(slices.Clone(c.opts), flowOption{flow: flow, run: runID, parent: parent})
		_, err := fr.client.EnqueueContext(ctx, c.task, opts...)
		if err != nil && !errors.Is(err, ErrTaskIDConflict) {
			return fmt.Errorf("cannot enqueue %s task of flow run %s: %w", c.task.Type(), runID, err)
		}
	}
	return nil
}

func resultForNode(result []byte) []byte {
	if len(result) > base.MaxFlowNodeResultSize {
		return nil
	}
	return result
}

// onSuccess runs the steps that follow the task, records the task as completed in its run, and runs the joins
// that became ready. It returns an error, which fails the task, if the next tasks could not be built or enqueued.
func (fr *flowRunner) onSuccess(ctx context.Context, msg *base.TaskMessage, result []byte) error {
	steps := fr.reg.stepsFrom(msg)
	if len(steps) == 0 && msg.FlowRun == "" {
		return nil
	}
	runID := msg.FlowRun
	if runID == "" {
		runID = msg.ID
	}
	flow := msg.Flow
	if flow == "" && len(steps) > 0 {
		flow = steps[0].flow.name
	}
	parent := taskToFlowTask(msg, result, runID)
	var children []*childEnqueue
	parents := make(map[*childEnqueue][]string)
	for _, st := range steps {
		var tasks []*Task
		if st.fn == nil {
			payload := result
			if len(payload) == 0 {
				payload = msg.Payload
			}
			tasks = []*Task{NewTask(st.to, payload)}
		} else {
			var err error
			if tasks, err = st.fn(ctx, parent); err != nil {
				return fmt.Errorf("flow %q step %s -> %s failed: %w", st.flow.name, msg.Type, st.to, err)
			}
		}
		for i, t := range tasks {
			defaults := []Option{Queue(msg.Queue), TaskID(fmt.Sprintf("%s>%s#%d", msg.ID, st.to, i))}
			c, err := fr.prepare(t, append(defaults, st.opts...))
			if err != nil {
				return fmt.Errorf("flow %q step %s -> %s: %w", st.flow.name, msg.Type, st.to, err)
			}
			children = append(children, c)
			parents[c] = []string{msg.ID}
		}
	}
	if len(children) == 0 && msg.FlowRun == "" {
		return nil // the run did not start
	}
	var meta *base.FlowRunMeta
	if msg.FlowRun == "" || runID == msg.ID {
		meta = &base.FlowRunMeta{ID: runID, Flow: flow, RootID: msg.ID, RootQueue: msg.Queue, RootType: msg.Type,
			CreatedAt: fr.clock().UnixMilli()}
	}
	self := &base.FlowNode{
		ID:         msg.ID,
		Queue:      msg.Queue,
		Type:       msg.Type,
		State:      base.FlowNodeCompleted,
		EnqueuedAt: msg.EnqueuedAt,
		StartedAt:  msg.StartedAt,
		DurationMs: msg.DurationMs,
		Result:     resultForNode(result),
	}
	if msg.ParentID != "" {
		self.Parents = []string{msg.ParentID}
	}
	if err := fr.enqueueChildren(ctx, runID, meta, []*base.FlowNode{self}, children, flow, parents); err != nil {
		return err
	}
	return fr.runJoins(ctx, runID, flow)
}

// runJoins enqueues the tasks of the joins of the run that are ready.
func (fr *flowRunner) runJoins(ctx context.Context, runID, flow string) error {
	joins := fr.reg.joins[flow]
	if len(joins) == 0 {
		return nil
	}
	run, err := fr.broker.GetFlowRun(runID)
	if err != nil {
		return fmt.Errorf("cannot read flow run %s: %w", runID, err)
	}
	ids := make(map[string]bool, len(run.Nodes))
	for _, n := range run.Nodes {
		ids[n.ID] = true
	}
	for idx, st := range joins {
		joined := joinReady(st, run.Nodes)
		if joined == nil {
			continue
		}
		var tasks []*Task
		parentTasks := make([]*FlowTask, len(joined))
		for i, n := range joined {
			parentTasks[i] = &FlowTask{ID: n.ID, Queue: n.Queue, Type: n.Type, Result: n.Result, RunID: runID}
		}
		if st.joinFn == nil {
			type joinedTask struct {
				ID     string          `json:"id"`
				Type   string          `json:"type"`
				Result json.RawMessage `json:"result,omitempty"`
			}
			items := make([]joinedTask, len(joined))
			for i, n := range joined {
				items[i] = joinedTask{ID: n.ID, Type: n.Type}
				if json.Valid(n.Result) {
					items[i].Result = n.Result
				} else if len(n.Result) > 0 {
					data, _ := json.Marshal(n.Result) // base64 string
					items[i].Result = data
				}
			}
			payload, err := json.Marshal(items)
			if err != nil {
				return fmt.Errorf("cannot encode joined tasks: %w", err)
			}
			tasks = []*Task{NewTask(st.to, payload)}
		} else if tasks, err = st.joinFn(ctx, parentTasks); err != nil {
			return fmt.Errorf("flow %q join -> %s failed: %w", flow, st.to, err)
		}
		var children []*childEnqueue
		parents := make(map[*childEnqueue][]string)
		parentIDs := make([]string, len(joined))
		for i, n := range joined {
			parentIDs[i] = n.ID
		}
		for i, t := range tasks {
			id := fmt.Sprintf("%s>join%d:%s#%d", runID, idx, st.to, i)
			if ids[id] {
				continue // already enqueued
			}
			c, err := fr.prepare(t, append([]Option{Queue(joined[0].Queue), TaskID(id)}, st.opts...))
			if err != nil {
				return fmt.Errorf("flow %q join -> %s: %w", flow, st.to, err)
			}
			children = append(children, c)
			parents[c] = parentIDs
		}
		if len(children) == 0 {
			continue
		}
		if err := fr.enqueueChildren(ctx, runID, nil, nil, children, flow, parents); err != nil {
			return err
		}
	}
	return nil
}

// joinReady returns the nodes joined by the step if the join is ready in the run, nil otherwise.
func joinReady(st *flowStep, nodes []*base.FlowNode) []*base.FlowNode {
	var joined []*base.FlowNode
	for _, n := range nodes {
		if !st.relevant[n.Type] {
			continue
		}
		isJoined := slices.Contains(st.from, n.Type)
		switch n.State {
		case base.FlowNodeCompleted:
			if isJoined {
				joined = append(joined, n)
			}
		case base.FlowNodeFailed:
			if isJoined {
				return nil // never ready
			}
		default:
			return nil // more tasks may come
		}
	}
	return joined
}

// onStart records the task as active in its run.
func (fr *flowRunner) onStart(ctx context.Context, msg *base.TaskMessage) {
	if fr == nil || msg.FlowRun == "" {
		return
	}
	node := &base.FlowNode{ID: msg.ID, Queue: msg.Queue, Type: msg.Type, State: base.FlowNodeActive, StartedAt: msg.StartedAt}
	if err := fr.broker.UpdateFlowRun(ctx, msg.FlowRun, nil, []*base.FlowNode{node}); err != nil {
		fr.logger.Warnf("Could not record the start of task id=%s in flow run %s: %v", msg.ID, msg.FlowRun, err)
	}
}

// onFailure records the failed attempt of the task in its run.
func (fr *flowRunner) onFailure(ctx context.Context, msg *base.TaskMessage, archived bool, errMsg string) {
	if fr == nil || msg.FlowRun == "" {
		return
	}
	state := base.FlowNodeRetry
	if archived {
		state = base.FlowNodeFailed
	}
	node := &base.FlowNode{ID: msg.ID, Queue: msg.Queue, Type: msg.Type, State: state, Error: errMsg,
		StartedAt: msg.StartedAt, DurationMs: msg.DurationMs}
	if err := fr.broker.UpdateFlowRun(ctx, msg.FlowRun, nil, []*base.FlowNode{node}); err != nil {
		fr.logger.Warnf("Could not record the failure of task id=%s in flow run %s: %v", msg.ID, msg.FlowRun, err)
	}
}

// EnqueueChild enqueues a task as a child of the task being processed: the child joins the flow run of the task,
// which starts a run if the task is not part of one yet, so that tools show how the tasks relate. Use it to chain
// tasks by hand from a handler, when the next tasks depend on more than the result of the task.
//
// The child is enqueued right away, in the queue of the task unless the options say otherwise. The context must be
// the one passed to the handler.
func EnqueueChild(ctx context.Context, task *Task, opts ...Option) (*TaskInfo, error) {
	fc, ok := ctx.Value(flowContextKey{}).(*flowContext)
	if !ok {
		return nil, fmt.Errorf("asynq: EnqueueChild must be called with the context passed to a handler")
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fr, msg := fc.runner, &fc.task
	all := append([]Option{Queue(msg.Queue), TaskID(uuid.NewString())}, task.opts...)
	c, err := fr.prepare(task, append(all, opts...))
	if err != nil {
		return nil, err
	}
	runID := msg.FlowRun
	if fc.runID != "" {
		runID = fc.runID
	}
	var meta *base.FlowRunMeta
	if runID == "" {
		runID = msg.ID
		meta = &base.FlowRunMeta{ID: runID, Flow: msg.Flow, RootID: msg.ID, RootQueue: msg.Queue, RootType: msg.Type,
			CreatedAt: fr.clock().UnixMilli()}
	}
	self := &base.FlowNode{ID: msg.ID, Queue: msg.Queue, Type: msg.Type, State: base.FlowNodeActive,
		EnqueuedAt: msg.EnqueuedAt, StartedAt: msg.StartedAt}
	nodes := []*base.FlowNode{self, {
		ID: c.opt.taskID, Queue: c.opt.queue, Type: c.task.Type(), Parents: []string{msg.ID},
		State: base.FlowNodePending, EnqueuedAt: fr.clock().UnixMilli(),
	}}
	if err := fr.broker.UpdateFlowRun(ctx, runID, meta, nodes); err != nil {
		return nil, fmt.Errorf("asynq: cannot record flow run %s: %w", runID, err)
	}
	// The task is part of the run from now on, so its completion is recorded too (see flowContext.apply).
	fc.runID = runID
	return fr.client.EnqueueContext(ctx, c.task, append(c.opts, flowOption{flow: msg.Flow, run: runID, parent: msg.ID})...)
}

// flowPublisher publishes the definitions of the flows registered on a server.
type flowPublisher struct {
	logger   *log.Logger
	broker   base.Broker
	defs     []*base.FlowDefinition
	interval time.Duration
	done     chan struct{}
}

func newFlowPublisher(logger *log.Logger, broker base.Broker, flows []*Flow) *flowPublisher {
	p := &flowPublisher{logger: logger, broker: broker, interval: time.Minute, done: make(chan struct{})}
	for _, f := range flows {
		if f != nil {
			p.defs = append(p.defs, f.definition())
		}
	}
	return p
}

func (p *flowPublisher) start(wg *sync.WaitGroup) {
	if len(p.defs) == 0 {
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-p.done:
				return
			case <-timer.C:
				if err := p.broker.PublishFlows(p.defs); err != nil {
					p.logger.Warnf("Could not publish the flow definitions: %v", err)
				}
				timer.Reset(p.interval)
			}
		}
	}()
}

func (p *flowPublisher) shutdown() {
	if len(p.defs) == 0 {
		return
	}
	close(p.done)
}

// FlowInfo describes a flow, as published by the servers that run it.
type FlowInfo struct {
	// Name is the name of the flow.
	Name string
	// Steps are the steps of the flow.
	Steps []*FlowStepInfo
	// UpdatedAt is the last time a server published the flow.
	UpdatedAt time.Time
}

// FlowStepInfo describes a step of a flow: the completion of tasks of the From types enqueues tasks of type To.
type FlowStepInfo struct {
	From []string
	To   string
	// Join reports whether the step waits for all the tasks of the From types of a run (see Flow.Join).
	Join bool
}

// Flow run states.
const (
	FlowRunRunning   = "running"
	FlowRunCompleted = "completed"
	FlowRunFailed    = "failed"
)

// FlowRunInfo describes a flow run: the tasks enqueued by a flow, or with EnqueueChild, from a first task.
type FlowRunInfo struct {
	// ID is the ID of the run, which is the ID of its first task.
	ID string
	// Flow is the name of the flow, empty for a run made with EnqueueChild only.
	Flow string
	// RootID, RootQueue and RootType describe the first task of the run.
	RootID    string
	RootQueue string
	RootType  string
	// State is FlowRunRunning while some tasks are not done, FlowRunFailed if a task failed for good, and
	// FlowRunCompleted otherwise.
	State string
	// CreatedAt is the time the run started and UpdatedAt the time of its last change.
	CreatedAt time.Time
	UpdatedAt time.Time
	// Nodes are the tasks of the run, in the order they were enqueued.
	Nodes []*FlowNodeInfo
}

// FlowNodeInfo describes a task of a flow run.
type FlowNodeInfo struct {
	ID    string
	Queue string
	Type  string
	// Parents are the IDs of the tasks whose completion enqueued the task (several for a join).
	Parents []string
	// State is one of "pending", "scheduled", "active", "retry", "completed" and "failed".
	State      string
	EnqueuedAt time.Time
	StartedAt  time.Time
	Duration   time.Duration
	// Error is the error of the last failed attempt.
	Error string
	// Result is the result of the task, if it was small enough to be kept in the run.
	Result []byte
}

// ErrFlowRunNotFound indicates that the specified flow run does not exist (or expired).
var ErrFlowRunNotFound = errors.New("flow run not found")

func newFlowRunInfo(run *base.FlowRun) *FlowRunInfo {
	info := &FlowRunInfo{
		ID:        run.Meta.ID,
		Flow:      run.Meta.Flow,
		RootID:    run.Meta.RootID,
		RootQueue: run.Meta.RootQueue,
		RootType:  run.Meta.RootType,
		CreatedAt: fromUnixMilliOrZero(run.Meta.CreatedAt),
		UpdatedAt: run.UpdatedAt,
	}
	for _, n := range run.Nodes {
		info.Nodes = append(info.Nodes, &FlowNodeInfo{
			ID:         n.ID,
			Queue:      n.Queue,
			Type:       n.Type,
			Parents:    n.Parents,
			State:      n.State,
			EnqueuedAt: fromUnixMilliOrZero(n.EnqueuedAt),
			StartedAt:  fromUnixMilliOrZero(n.StartedAt),
			Duration:   time.Duration(n.DurationMs) * time.Millisecond,
			Error:      n.Error,
			Result:     n.Result,
		})
	}
	info.computeState()
	return info
}

func (info *FlowRunInfo) computeState() {
	info.State = FlowRunCompleted
	for _, n := range info.Nodes {
		switch n.State {
		case base.FlowNodeFailed:
			info.State = FlowRunFailed
			return
		case base.FlowNodeCompleted:
		default:
			info.State = FlowRunRunning
		}
	}
}

// Flows returns the flows published by the servers, sorted by name.
func (i *Inspector) Flows() ([]*FlowInfo, error) {
	defs, err := i.rdb.ListFlows()
	if err != nil {
		return nil, err
	}
	res := make([]*FlowInfo, len(defs))
	for idx, d := range defs {
		f := &FlowInfo{Name: d.Name, UpdatedAt: time.Unix(d.UpdatedAt, 0)}
		for _, st := range d.Steps {
			f.Steps = append(f.Steps, &FlowStepInfo{From: st.From, To: st.To, Join: st.Join})
		}
		res[idx] = f
	}
	return res, nil
}

// DeleteFlow deletes the published definition of a flow, e.g. after the flow was removed from the servers.
// A server that still runs the flow publishes it again within a minute.
func (i *Inspector) DeleteFlow(name string) error {
	return i.rdb.DeleteFlow(name)
}

// FlowRuns returns the most recently updated flow runs, newest first, without the results of their tasks.
// If flow is not empty, only the runs of that flow are returned. Runs are kept for 7 days after their last change.
func (i *Inspector) FlowRuns(flow string, opts ...ListOption) ([]*FlowRunInfo, error) {
	opt := composeListOptions(opts...)
	runs, err := i.rdb.ListFlowRuns(flow, rdb.Pagination{Size: opt.pageSize, Page: opt.pageNum - 1})
	if err != nil {
		return nil, err
	}
	res := make([]*FlowRunInfo, len(runs))
	for idx, run := range runs {
		res[idx] = newFlowRunInfo(run)
	}
	return res, nil
}

// GetFlowRun returns the flow run with the given ID. The state of the tasks that are not done is read from the
// tasks themselves, so it is up to date.
func (i *Inspector) GetFlowRun(id string) (*FlowRunInfo, error) {
	run, err := i.rdb.GetFlowRun(id)
	if errors.CanonicalCode(err) == errors.NotFound {
		return nil, fmt.Errorf("%w: id=%q", ErrFlowRunNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	info := newFlowRunInfo(run)
	for _, n := range info.Nodes {
		if n.State == base.FlowNodeCompleted || n.State == base.FlowNodeFailed {
			continue
		}
		ti, err := i.rdb.GetTaskInfo(n.Queue, n.ID)
		if err != nil {
			continue // e.g. the task was processed and deleted, or the node is about to be updated
		}
		switch ti.State {
		case base.TaskStatePending, base.TaskStateAggregating:
			n.State = base.FlowNodePending
		case base.TaskStateScheduled:
			n.State = "scheduled"
		case base.TaskStateActive:
			n.State = base.FlowNodeActive
		case base.TaskStateRetry:
			n.State = base.FlowNodeRetry
			n.Error = ti.Message.ErrorMsg
		case base.TaskStateArchived:
			n.State = base.FlowNodeFailed
			n.Error = ti.Message.ErrorMsg
		case base.TaskStateCompleted:
			n.State = base.FlowNodeCompleted
		}
	}
	info.computeState()
	return info, nil
}
