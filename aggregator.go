// Copyright 2022 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/log"
)

// An aggregator is responsible for checking groups and aggregate into one task
// if any of the grouping condition is met.
type aggregator struct {
	logger *log.Logger
	broker base.Broker
	client *Client

	// channel to communicate back to the long running "aggregator" goroutine.
	done chan struct{}

	// list of queue names to check and aggregate.
	queues []string

	// Group configurations
	gracePeriod time.Duration
	maxDelay    time.Duration
	maxSize     int

	// User provided group aggregator.
	ga GroupAggregator

	// policyFn returns the aggregation policy of a group, nil to use the configuration above for every group.
	policyFn func(queue, group string) GroupPolicy

	// published holds the policies last published for each queue, and when they were published.
	published   map[string]map[string]base.GroupPolicy
	publishedAt map[string]time.Time

	// interval used to check for aggregation
	interval time.Duration

	// sema is a counting semaphore to ensure the number of active aggregating function
	// does not exceed the limit.
	sema chan struct{}
}

type aggregatorParams struct {
	logger          *log.Logger
	broker          base.Broker
	queues          []string
	gracePeriod     time.Duration
	maxDelay        time.Duration
	maxSize         int
	groupAggregator GroupAggregator
	groupPolicy     func(queue, group string) GroupPolicy
}

const (
	// Maximum number of aggregation checks in flight concurrently.
	maxConcurrentAggregationChecks = 3

	// Default interval used for aggregation checks. If the provided gracePeriod is less than
	// the default, use the gracePeriod.
	defaultAggregationCheckInterval = 7 * time.Second
)

func newAggregator(params aggregatorParams) *aggregator {
	interval := defaultAggregationCheckInterval
	if params.gracePeriod < interval {
		interval = params.gracePeriod
	}
	return &aggregator{
		logger:      params.logger,
		broker:      params.broker,
		client:      &Client{broker: params.broker},
		done:        make(chan struct{}),
		queues:      params.queues,
		gracePeriod: params.gracePeriod,
		maxDelay:    params.maxDelay,
		maxSize:     params.maxSize,
		ga:          params.groupAggregator,
		policyFn:    params.groupPolicy,
		published:   make(map[string]map[string]base.GroupPolicy),
		publishedAt: make(map[string]time.Time),
		sema:        make(chan struct{}, maxConcurrentAggregationChecks),
		interval:    interval,
	}
}

func (a *aggregator) shutdown() {
	if a.ga == nil {
		return
	}
	a.logger.Debug("Aggregator shutting down...")
	// Signal the aggregator goroutine to stop.
	a.done <- struct{}{}
}

func (a *aggregator) start(wg *sync.WaitGroup) {
	if a.ga == nil {
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(a.interval)
		for {
			select {
			case <-a.done:
				a.logger.Debug("Waiting for all aggregation checks to finish...")
				// block until all aggregation checks released the token
				for i := 0; i < cap(a.sema); i++ {
					a.sema <- struct{}{}
				}
				a.logger.Debug("Aggregator done")
				ticker.Stop()
				return
			case t := <-ticker.C:
				a.exec(t)
			}
		}
	}()
}

func (a *aggregator) exec(t time.Time) {
	select {
	case a.sema <- struct{}{}: // acquire token
		go a.aggregate(t)
	default:
		// If the semaphore blocks, then we are currently running max number of
		// aggregation checks. Skip this round and log warning.
		a.logger.Warnf("Max number of aggregation checks in flight. Skipping")
	}
}

func (a *aggregator) aggregate(t time.Time) {
	defer func() { <-a.sema /* release token */ }()
	for _, qname := range a.queues {
		groups, err := a.broker.ListGroups(qname)
		if err != nil {
			a.logger.Errorf("Failed to list groups in queue: %q", qname)
			continue
		}
		policies := make(map[string]base.GroupPolicy, len(groups))
		for _, gname := range groups {
			policy := a.policy(qname, gname)
			policies[gname] = policy
			aggregationSetID, err := a.broker.AggregationCheckPolicy(qname, gname, t, policy)
			if err != nil {
				a.logger.Errorf("Failed to run aggregation check: queue=%q group=%q", qname, gname)
				continue
			}
			if aggregationSetID == "" {
				a.logger.Debugf("No aggregation needed at this time: queue=%q group=%q", qname, gname)
				continue
			}

			// Aggregate and enqueue.
			msgs, deadline, err := a.broker.ReadAggregationSet(qname, gname, aggregationSetID)
			if err != nil {
				a.logger.Errorf("Failed to read aggregation set: queue=%q, group=%q, setID=%q",
					qname, gname, aggregationSetID)
				continue
			}
			// The group is ordered by second; order the tasks added within the same second by enqueue time.
			sort.SliceStable(msgs, func(i, j int) bool {
				return msgs[i].EnqueuedAt != 0 && msgs[j].EnqueuedAt != 0 && msgs[i].EnqueuedAt < msgs[j].EnqueuedAt
			})
			tasks := make([]*Task, len(msgs))
			for i, m := range msgs {
				tasks[i] = NewTaskWithHeaders(m.Type, m.Payload, m.Headers)
				tasks[i].member = m
			}
			aggregatedTask := a.ga.Aggregate(gname, tasks)
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			if _, err := a.client.EnqueueContext(ctx, aggregatedTask, Queue(qname)); err != nil {
				a.logger.Errorf("Failed to enqueue aggregated task (queue=%q, group=%q, setID=%q): %v",
					qname, gname, aggregationSetID, err)
				cancel()
				continue
			}
			if err := a.broker.DeleteAggregationSet(ctx, qname, gname, aggregationSetID); err != nil {
				a.logger.Warnf("Failed to delete aggregation set: queue=%q, group=%q, setID=%q",
					qname, gname, aggregationSetID)
			}
			cancel()
		}
		a.publishPolicies(qname, policies, t)
	}
}

// policy returns the aggregation policy of the group.
func (a *aggregator) policy(qname, gname string) base.GroupPolicy {
	var p GroupPolicy
	if a.policyFn != nil {
		p = a.policyFn(qname, gname)
	}
	return p.resolve(a.maxSize, a.gracePeriod, a.maxDelay)
}

// policiesRefreshInterval is how often the group policies are published even when they did not change.
const policiesRefreshInterval = 10 * time.Minute

// publishPolicies records the policies of the groups of the queue in redis when they changed, so that tools can
// show how far a group is from being aggregated.
func (a *aggregator) publishPolicies(qname string, policies map[string]base.GroupPolicy, t time.Time) {
	if len(policies) == 0 {
		return
	}
	prev := a.published[qname]
	changed := t.Sub(a.publishedAt[qname]) >= policiesRefreshInterval
	for gname, p := range policies {
		if old, ok := prev[gname]; !ok || old != p {
			changed = true
			break
		}
	}
	if !changed {
		return
	}
	if err := a.broker.PublishGroupPolicies(qname, policies); err != nil {
		a.logger.Warnf("Failed to publish group policies: queue=%q: %v", qname, err)
		return
	}
	a.published[qname] = policies
	a.publishedAt[qname] = t
}
