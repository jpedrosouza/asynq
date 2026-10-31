// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

// Rate limits are stored in redis hashes with the fields "limit" (number of tasks), "period" (in milliseconds)
// and "burst" (number of tasks that may run back to back). The state of a rate limit is the theoretical arrival
// time of the next task (see the GCRA), in Unix time with millisecond precision.

// AllRateLimitKeys is the redis key of the set of the rate limit keys configured with a key level rate limit.
const AllRateLimitKeys = "asynq:ratelimits" // SET

// QueueRateLimitKey returns the redis key of the rate limit configuration of the queue.
func QueueRateLimitKey(qname string) string {
	return QueueKeyPrefix(qname) + "ratelimit"
}

// QueueRateLimitStateKey returns the redis key of the rate limit state of the queue.
func QueueRateLimitStateKey(qname string) string {
	return QueueKeyPrefix(qname) + "ratelimit:tat"
}

// RateLimitKey returns the redis key of the configuration of the key level rate limit.
// The key is used as a hash tag so that the configuration and the state live in the same cluster slot.
func RateLimitKey(key string) string {
	return "asynq:ratelimit:{" + key + "}"
}

// RateLimitStateKey returns the redis key of the state of the key level rate limit.
func RateLimitStateKey(key string) string {
	return RateLimitKey(key) + ":tat"
}

// RateLimit describes a rate limit: at most Limit tasks per Period, with up to Burst tasks back to back.
type RateLimit struct {
	Limit    int
	PeriodMs int64
	Burst    int
}

// RateLimitStatus describes the configuration and the current state of a rate limit.
type RateLimitStatus struct {
	RateLimit
	// Available is the number of tasks that may start right now.
	Available int
	// NextAt is the time, in Unix time with millisecond precision, at which the next task may start.
	// It is less than or equal to the current time if a task may start right now.
	NextAt int64
}

// AllQueueMetadata is the redis key of the hash of the queue metadata, by queue name.
const AllQueueMetadata = "asynq:queue_metadata" // HASH

// QueueMetadata holds user defined information about a queue, used to organize queues.
type QueueMetadata struct {
	Category    string `json:"category,omitempty"`
	Subcategory string `json:"subcategory,omitempty"`
	Description string `json:"description,omitempty"`
}
