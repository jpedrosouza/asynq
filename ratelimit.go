// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"fmt"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
)

// RateLimit describes a rate limit: at most Limit tasks per Period.
//
// Burst is the number of tasks that may start back to back when the rate limit has been idle; the other tasks
// are spread evenly over the period. Zero or one means no burst: tasks start one every Period/Limit.
//
// For example, RateLimit{Limit: 200, Period: time.Minute} starts a task every 300ms at most, and
// RateLimit{Limit: 200, Period: time.Minute, Burst: 20} also lets 20 tasks start at once after a pause.
type RateLimit struct {
	Limit  int
	Period time.Duration
	Burst  int
}

func (rl RateLimit) String() string {
	s := fmt.Sprintf("%d per %v", rl.Limit, rl.Period)
	if rl.Burst > 1 {
		s += fmt.Sprintf(" (burst %d)", rl.Burst)
	}
	return s
}

func (rl RateLimit) toBase() (base.RateLimit, error) {
	if rl.Limit < 1 {
		return base.RateLimit{}, fmt.Errorf("asynq: rate limit must allow at least one task per period")
	}
	if rl.Period < time.Millisecond {
		return base.RateLimit{}, fmt.Errorf("asynq: rate limit period must be at least one millisecond")
	}
	if rl.Burst < 0 {
		return base.RateLimit{}, fmt.Errorf("asynq: rate limit burst cannot be negative")
	}
	return base.RateLimit{Limit: rl.Limit, PeriodMs: rl.Period.Milliseconds(), Burst: rl.Burst}, nil
}

// RateLimitInfo describes a rate limit and its current state.
type RateLimitInfo struct {
	RateLimit
	// Available is the number of tasks that may start right now.
	Available int
	// NextAt is the time at which the next task may start; it is not after the current time if Available > 0.
	NextAt time.Time
}

func newRateLimitInfo(st *base.RateLimitStatus) *RateLimitInfo {
	if st == nil {
		return nil
	}
	return &RateLimitInfo{
		RateLimit: RateLimit{Limit: st.Limit, Period: time.Duration(st.PeriodMs) * time.Millisecond, Burst: st.Burst},
		Available: st.Available,
		NextAt:    time.UnixMilli(st.NextAt),
	}
}

// SetQueueRateLimit sets the rate limit of the queue: the servers, together, start at most rl.Limit tasks of the
// queue per rl.Period. The tasks wait in the pending state for the rate limit, so waiting does not count as a retry.
//
// The rate limit is stored in redis and applies right away to every server running this version of the library or
// a later one (older servers ignore it). The queue does not need to exist, and the rate limit is kept if the queue
// is deleted.
func (i *Inspector) SetQueueRateLimit(queue string, rl RateLimit) error {
	if err := base.ValidateQueueName(queue); err != nil {
		return err
	}
	b, err := rl.toBase()
	if err != nil {
		return err
	}
	return i.rdb.SetQueueRateLimit(queue, b)
}

// RemoveQueueRateLimit removes the rate limit of the queue.
func (i *Inspector) RemoveQueueRateLimit(queue string) error {
	if err := base.ValidateQueueName(queue); err != nil {
		return err
	}
	return i.rdb.RemoveQueueRateLimit(queue)
}

// QueueRateLimit returns the rate limit of the queue and its state, or nil if the queue has no rate limit.
func (i *Inspector) QueueRateLimit(queue string) (*RateLimitInfo, error) {
	if err := base.ValidateQueueName(queue); err != nil {
		return nil, err
	}
	st, err := i.rdb.QueueRateLimit(queue)
	if err != nil {
		return nil, err
	}
	return newRateLimitInfo(st), nil
}

// SetRateLimit sets the rate limit of the given key: the servers, together, start at most rl.Limit tasks enqueued
// with the RateLimitKey(key) option per rl.Period, whatever their queue.
//
// Use it to share a limit between several queues or task types, e.g. the rate limit of a third party API, or to
// limit the tasks of each tenant.
func (i *Inspector) SetRateLimit(key string, rl RateLimit) error {
	if isBlank(key) {
		return fmt.Errorf("asynq: rate limit key cannot be empty")
	}
	b, err := rl.toBase()
	if err != nil {
		return err
	}
	return i.rdb.SetRateLimit(key, b)
}

// RemoveRateLimit removes the rate limit of the given key. The tasks enqueued with the key are not limited anymore.
func (i *Inspector) RemoveRateLimit(key string) error {
	return i.rdb.RemoveRateLimit(key)
}

// RateLimits returns the key level rate limits and their state, by key.
func (i *Inspector) RateLimits() (map[string]*RateLimitInfo, error) {
	all, err := i.rdb.RateLimits()
	if err != nil {
		return nil, err
	}
	res := make(map[string]*RateLimitInfo, len(all))
	for key, st := range all {
		res[key] = newRateLimitInfo(st)
	}
	return res, nil
}
