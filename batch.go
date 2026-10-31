// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
)

// GroupPolicy holds the aggregation criteria of a group (see Config.GroupPolicy).
//
// A group is aggregated as soon as one of these conditions holds:
//   - it has MaxSize tasks;
//   - its oldest task has waited for MaxDelay;
//   - it has at least MinSize tasks and no task was added to it for GracePeriod.
//
// A zero field takes the value of the server configuration (Config.GroupMaxSize, Config.GroupGracePeriod and
// Config.GroupMaxDelay); a negative field disables the condition. For example, to process tasks by batches of
// exactly 100, and the leftover once a day:
//
//	GroupPolicy{MaxSize: 100, MinSize: 100, GracePeriod: -1, MaxDelay: 24 * time.Hour}
//
// Note: A group whose conditions are all disabled is only aggregated when it is flushed by hand
// (see Inspector.FlushGroup).
type GroupPolicy struct {
	MaxSize     int
	MinSize     int
	GracePeriod time.Duration
	MaxDelay    time.Duration
}

// resolve fills the zero fields of the policy from the server configuration.
func (p GroupPolicy) resolve(maxSize int, gracePeriod, maxDelay time.Duration) base.GroupPolicy {
	res := base.GroupPolicy{MinSize: max(p.MinSize, 0)}
	switch {
	case p.MaxSize > 0:
		res.MaxSize = p.MaxSize
	case p.MaxSize == 0:
		res.MaxSize = maxSize
	}
	switch {
	case p.GracePeriod > 0:
		res.GracePeriodSec = max(int64(p.GracePeriod.Seconds()), 1)
	case p.GracePeriod == 0:
		res.GracePeriodSec = max(int64(gracePeriod.Seconds()), 1)
	}
	switch {
	case p.MaxDelay > 0:
		res.MaxDelaySec = max(int64(p.MaxDelay.Seconds()), 1)
	case p.MaxDelay == 0:
		res.MaxDelaySec = int64(maxDelay.Seconds())
	}
	return res
}

func newGroupPolicy(p *base.GroupPolicy) *GroupPolicy {
	if p == nil {
		return nil
	}
	res := &GroupPolicy{
		MaxSize:     p.MaxSize,
		MinSize:     p.MinSize,
		GracePeriod: time.Duration(p.GracePeriodSec) * time.Second,
		MaxDelay:    time.Duration(p.MaxDelaySec) * time.Second,
	}
	if p.MaxSize <= 0 {
		res.MaxSize = -1
	}
	if p.GracePeriodSec <= 0 {
		res.GracePeriod = -1
	}
	if p.MaxDelaySec <= 0 {
		res.MaxDelay = -1
	}
	return res
}

// batchEnvelope is the payload of a task created by NewBatchTask. It is JSON so that tools can show the members.
type batchEnvelope struct {
	Version int          `json:"asynq_batch"`
	Queue   string       `json:"queue"`
	Group   string       `json:"group"`
	Items   []*batchItem `json:"items"`
}

type batchItem struct {
	ID           string            `json:"id"`
	Type         string            `json:"type"`
	Payload      []byte            `json:"payload,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Retry        int               `json:"retry"`
	Retried      int               `json:"retried"`
	Timeout      int64             `json:"timeout,omitempty"`
	Deadline     int64             `json:"deadline,omitempty"`
	Retention    int64             `json:"retention,omitempty"`
	EnqueuedAt   int64             `json:"enqueued_at,omitempty"`
	LastErr      string            `json:"error,omitempty"`
	LastFailedAt int64             `json:"failed_at,omitempty"`
	RateLimitKey string            `json:"rate_limit_key,omitempty"`
}

// BatchTaskHeader is the header set on the tasks created by NewBatchTask; its value is the number of items.
const BatchTaskHeader = "asynq-batch-size"

// NewBatchTask wraps the tasks of a group into a single task of the given type, to be returned by a
// GroupAggregator. The handler of the type reads the original tasks with ParseBatch, or is registered with
// ServeMux.HandleBatch.
//
// Unlike merging the payloads by hand, the batch keeps the identity of each task (ID, type, payload, headers and
// retry counts), so that the handler can retry or fail some of them without failing the whole batch.
//
//	srv := asynq.NewServer(redis, asynq.Config{
//		GroupAggregator: asynq.GroupAggregatorFunc(func(group string, tasks []*asynq.Task) *asynq.Task {
//			return asynq.NewBatchTask("llm:classify:batch", tasks)
//		}),
//	})
func NewBatchTask(typename string, tasks []*Task, opts ...Option) *Task {
	env := batchEnvelope{Version: 1, Items: make([]*batchItem, 0, len(tasks))}
	for _, t := range tasks {
		item := &batchItem{Type: t.Type(), Payload: t.Payload(), Headers: t.Headers(), Retry: defaultMaxRetry}
		if m := t.member; m != nil {
			env.Queue, env.Group = m.Queue, m.GroupKey
			item.ID = m.ID
			item.Retry = m.Retry
			item.Retried = m.Retried
			item.Timeout = m.Timeout
			item.Deadline = m.Deadline
			item.Retention = m.Retention
			item.EnqueuedAt = m.EnqueuedAt
			item.LastErr = m.ErrorMsg
			item.LastFailedAt = m.LastFailedAt
			item.RateLimitKey = m.RateLimitKey
		}
		env.Items = append(env.Items, item)
	}
	payload, err := json.Marshal(&env)
	if err != nil {
		// The envelope only holds strings, byte slices and numbers.
		panic(fmt.Sprintf("asynq: cannot encode batch: %v", err))
	}
	return NewTaskWithHeaders(typename, payload, map[string]string{BatchTaskHeader: fmt.Sprint(len(tasks))}, opts...)
}

// A BatchItem is one of the tasks of a batch.
type BatchItem struct {
	// ID is the ID of the original task.
	ID string
	// Type is the type name of the original task.
	Type string
	// Payload is the payload of the original task.
	Payload []byte
	// Headers are the headers of the original task.
	Headers map[string]string
	// Retried is the number of times the task was put back in the group by Batch.Retry.
	Retried int
	// MaxRetry is the maximum number of times the task can be put back in the group.
	MaxRetry int
	// EnqueuedAt is the time the original task was enqueued, zero if unknown.
	EnqueuedAt time.Time
	// LastErr is the error passed to Batch.Retry the last time the task was put back in the group.
	LastErr string

	item *batchItem
}

type batchOutcome struct {
	retry bool // put back in the group if true, archive if false
	err   error
}

// A Batch holds the tasks of a group aggregated by NewBatchTask.
type Batch struct {
	// Queue is the queue of the group.
	Queue string
	// Group is the name of the group.
	Group string
	// Items are the tasks of the batch, in the order they were added to the group.
	Items []*BatchItem

	outcomes map[int]batchOutcome
}

// ParseBatch returns the batch carried by a task created by NewBatchTask.
func ParseBatch(t *Task) (*Batch, error) {
	var env batchEnvelope
	if err := json.Unmarshal(t.Payload(), &env); err != nil || env.Version == 0 {
		return nil, fmt.Errorf("asynq: task %q does not carry a batch", t.Type())
	}
	b := &Batch{Queue: env.Queue, Group: env.Group, Items: make([]*BatchItem, len(env.Items))}
	for i, it := range env.Items {
		b.Items[i] = &BatchItem{
			ID:         it.ID,
			Type:       it.Type,
			Payload:    it.Payload,
			Headers:    it.Headers,
			Retried:    it.Retried,
			MaxRetry:   it.Retry,
			EnqueuedAt: fromUnixMilliOrZero(it.EnqueuedAt),
			LastErr:    it.LastErr,
			item:       it,
		}
	}
	return b, nil
}

// Len returns the number of tasks in the batch.
func (b *Batch) Len() int { return len(b.Items) }

// Retry marks the i-th task to be put back in its group once the batch is processed successfully, so that it is
// part of a later batch. The task is archived instead if it was already put back MaxRetry times.
//
// Retry has no effect if the batch handler returns an error: the whole batch is retried then.
func (b *Batch) Retry(i int, err error) {
	b.setOutcome(i, batchOutcome{retry: true, err: err})
}

// Fail marks the i-th task to be archived once the batch is processed successfully, with the given error.
//
// Fail has no effect if the batch handler returns an error: the whole batch is retried then.
func (b *Batch) Fail(i int, err error) {
	b.setOutcome(i, batchOutcome{retry: false, err: err})
}

func (b *Batch) setOutcome(i int, o batchOutcome) {
	if i < 0 || i >= len(b.Items) {
		panic(fmt.Sprintf("asynq: batch item index %d out of range [0, %d)", i, len(b.Items)))
	}
	if b.outcomes == nil {
		b.outcomes = make(map[int]batchOutcome)
	}
	if o.err == nil {
		o.err = errors.New("batch item failed")
	}
	b.outcomes[i] = o
}

// settle puts back in the group or archives the tasks marked by Retry and Fail.
func (b *Batch) settle(ctx context.Context, w *ResultWriter) error {
	if len(b.outcomes) == 0 {
		return nil
	}
	if w == nil || w.broker == nil {
		return fmt.Errorf("asynq: cannot settle batch items outside of a server")
	}
	if b.Queue == "" || b.Group == "" {
		return fmt.Errorf("asynq: cannot settle the items of a batch without queue and group")
	}
	now := time.Now()
	for i, o := range b.outcomes {
		it := b.Items[i].item
		msg := &base.TaskMessage{
			ID:           it.ID,
			Type:         it.Type,
			Payload:      it.Payload,
			Headers:      maps.Clone(it.Headers),
			Queue:        b.Queue,
			Retry:        it.Retry,
			Retried:      it.Retried,
			ErrorMsg:     o.err.Error(),
			LastFailedAt: now.Unix(),
			Timeout:      it.Timeout,
			Deadline:     it.Deadline,
			Retention:    it.Retention,
			EnqueuedAt:   it.EnqueuedAt,
			RateLimitKey: it.RateLimitKey,
		}
		if msg.ID == "" {
			return fmt.Errorf("asynq: cannot settle a batch item without ID")
		}
		var err error
		if o.retry && msg.Retried < msg.Retry {
			msg.Retried++
			msg.GroupKey = b.Group
			err = w.broker.AddToGroup(ctx, msg, b.Group)
		} else {
			err = w.broker.AddToArchive(ctx, msg)
		}
		// A conflict means the item was already settled by a previous attempt of the batch.
		if err != nil && !errors.Is(err, errors.ErrTaskIdConflict) {
			return fmt.Errorf("asynq: cannot settle batch item %s: %w", msg.ID, err)
		}
	}
	return nil
}

// A BatchHandler processes the batches created by NewBatchTask.
//
// If ProcessBatch returns an error, the whole batch is retried like any task. Otherwise, the tasks marked with
// Batch.Retry are put back in their group and the tasks marked with Batch.Fail are archived, while the other tasks
// count as processed.
type BatchHandler interface {
	ProcessBatch(context.Context, *Batch) error
}

// The BatchHandlerFunc type is an adapter to allow the use of ordinary functions as a BatchHandler.
type BatchHandlerFunc func(context.Context, *Batch) error

// ProcessBatch calls fn(ctx, b)
func (fn BatchHandlerFunc) ProcessBatch(ctx context.Context, b *Batch) error {
	return fn(ctx, b)
}

// NewBatchHandler returns a Handler that processes the batch carried by the task with h.
func NewBatchHandler(h BatchHandler) Handler {
	return HandlerFunc(func(ctx context.Context, t *Task) error {
		b, err := ParseBatch(t)
		if err != nil {
			return fmt.Errorf("%v: %w", err, SkipRetry)
		}
		if err := h.ProcessBatch(ctx, b); err != nil {
			return err
		}
		return b.settle(ctx, t.ResultWriter())
	})
}

// HandleBatch registers the batch handler for the given pattern (see NewBatchTask).
func (mux *ServeMux) HandleBatch(pattern string, h BatchHandler) {
	if h == nil {
		panic("asynq: nil batch handler")
	}
	mux.Handle(pattern, NewBatchHandler(h))
}

// HandleBatchFunc registers the batch handler function for the given pattern.
func (mux *ServeMux) HandleBatchFunc(pattern string, fn func(context.Context, *Batch) error) {
	if fn == nil {
		panic("asynq: nil batch handler")
	}
	mux.HandleBatch(pattern, BatchHandlerFunc(fn))
}

// FlushGroup requests the group to be aggregated at the next aggregation check of a server, whatever its policy.
func (i *Inspector) FlushGroup(queue, group string) error {
	if err := base.ValidateQueueName(queue); err != nil {
		return err
	}
	return i.rdb.RequestGroupFlush(queue, group)
}
