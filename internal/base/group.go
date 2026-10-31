// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

// GroupPolicy holds the aggregation criteria of a group.
type GroupPolicy struct {
	// MaxSize is the number of tasks that triggers the aggregation right away; zero means no limit.
	MaxSize int `json:"max_size"`
	// MinSize is the number of tasks the group needs before the grace period may trigger the aggregation;
	// only MaxDelay aggregates a smaller group.
	MinSize int `json:"min_size"`
	// GracePeriodSec triggers the aggregation when no task was added to the group for that many seconds;
	// zero or less disables the grace period.
	GracePeriodSec int64 `json:"grace_period_sec"`
	// MaxDelaySec triggers the aggregation when the oldest task of the group waited that many seconds;
	// zero disables the max delay.
	MaxDelaySec int64 `json:"max_delay_sec"`
}

// GroupPoliciesKey returns the redis key of the hash of the aggregation policies of the groups of the queue,
// as last used by a server, by group name.
func GroupPoliciesKey(qname string) string {
	return QueueKeyPrefix(qname) + "group_policies"
}

// FlushGroupsKey returns the redis key of the set of groups of the queue to aggregate at the next check
// regardless of their policy.
func FlushGroupsKey(qname string) string {
	return QueueKeyPrefix(qname) + "flush_groups"
}
