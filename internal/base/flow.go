// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

import "time"

// Global redis keys of flows.
const (
	// AllFlows is the hash of the flow definitions published by servers, by flow name.
	AllFlows = "asynq:flows" // HASH
	// AllFlowRuns is the index of the flow runs, scored by their last update time.
	AllFlowRuns = "asynq:flowruns" // ZSET
)

// FlowRunTTL is how long a flow run is kept after its last update.
const FlowRunTTL = 7 * 24 * time.Hour

// MaxFlowRuns is the maximum number of flow runs kept in the index.
const MaxFlowRuns = 10000

// FlowRunKey returns the redis key of the hash holding a flow run: the field "meta" holds a FlowRunMeta, and
// a field "n:<task id>" holds the FlowNode of each task of the run. The run ID is used as a hash tag.
func FlowRunKey(runID string) string {
	return "asynq:flowrun:{" + runID + "}"
}

// Flow node states.
const (
	FlowNodePending   = "pending"
	FlowNodeActive    = "active"
	FlowNodeRetry     = "retry"
	FlowNodeCompleted = "completed"
	FlowNodeFailed    = "failed"
)

// FlowRunMeta describes a flow run.
type FlowRunMeta struct {
	ID        string `json:"id"`
	Flow      string `json:"flow,omitempty"`
	RootID    string `json:"root_id"`
	RootQueue string `json:"root_queue"`
	RootType  string `json:"root_type"`
	CreatedAt int64  `json:"created_at"` // Unix time in milliseconds
}

// FlowNode describes a task of a flow run. Updates are merged field by field, so zero fields are omitted.
type FlowNode struct {
	ID         string   `json:"id"`
	Queue      string   `json:"queue,omitempty"`
	Type       string   `json:"type,omitempty"`
	Parents    []string `json:"parents,omitempty"`
	State      string   `json:"state,omitempty"`
	EnqueuedAt int64    `json:"enqueued_at,omitempty"` // Unix time in milliseconds
	StartedAt  int64    `json:"started_at,omitempty"`  // Unix time in milliseconds
	DurationMs int64    `json:"duration_ms,omitempty"`
	Error      string   `json:"error,omitempty"`
	Result     []byte   `json:"result,omitempty"`     // only kept if small, see MaxFlowNodeResultSize
	UpdatedAt  int64    `json:"updated_at,omitempty"` // Unix time in milliseconds
}

// MaxFlowNodeResultSize is the largest task result kept in a flow node, for the flow steps that join tasks.
const MaxFlowNodeResultSize = 64 * 1024

// FlowRun is a flow run with its tasks.
type FlowRun struct {
	Meta      *FlowRunMeta
	Nodes     []*FlowNode
	UpdatedAt time.Time
}

// FlowStep is an edge of a flow definition: the completion of a task of type From enqueues tasks of type To.
type FlowStep struct {
	From []string `json:"from"`
	To   string   `json:"to"`
	// Join reports whether the step waits for all the tasks of the From types of the run.
	Join bool `json:"join,omitempty"`
}

// FlowDefinition describes a flow, as published by the servers that run it.
type FlowDefinition struct {
	Name      string      `json:"name"`
	Steps     []*FlowStep `json:"steps"`
	UpdatedAt int64       `json:"updated_at"` // Unix time in seconds
}
