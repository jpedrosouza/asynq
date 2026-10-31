// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package base

import (
	"strconv"
	"time"
)

// Task metrics are recorded per queue and task type, in time buckets of one minute and of one hour.
// Each bucket is a redis hash whose fields are named "<task type>|<metric>":
//
//	n   number of tasks processed successfully
//	s   sum of the processing durations of the successful tasks, in milliseconds
//	x   longest processing duration of a successful task, in milliseconds
//	d<i> number of successful tasks whose processing duration falls in the i-th duration bucket
//	f   number of failed processing attempts
//	fs  sum of the processing durations of the failed attempts, in milliseconds
//	w   number of processing attempts whose wait time is known
//	ws  sum of the wait times, in milliseconds
//	wx  longest wait time, in milliseconds
//	q<i> number of processing attempts whose wait time falls in the i-th duration bucket
//
// The wait time of an attempt is the time the task spent in the pending list before a worker picked it up.

const (
	// TaskMetricsMinuteRetention is how long the per-minute task metrics are kept.
	TaskMetricsMinuteRetention = 48 * time.Hour
	// TaskMetricsHourRetention is how long the per-hour task metrics are kept.
	TaskMetricsHourRetention = 90 * 24 * time.Hour
)

// TaskMetricsBucketBounds are the upper bounds (inclusive) of the histogram buckets, in milliseconds.
// A value greater than the last bound falls in the last bucket, so there are len(TaskMetricsBucketBounds)+1 buckets.
var TaskMetricsBucketBounds = []int64{
	1, 5, 10, 25, 50, 100, 250, 500,
	1000, 2500, 5000, 10000, 30000, 60000,
	300000, 900000, 3600000,
}

// TaskMetricsBucketCount is the number of histogram buckets.
var TaskMetricsBucketCount = len(TaskMetricsBucketBounds) + 1

// TaskMetricsBucketIndex returns the index of the histogram bucket for the given duration in milliseconds.
func TaskMetricsBucketIndex(ms int64) int {
	for i, b := range TaskMetricsBucketBounds {
		if ms <= b {
			return i
		}
	}
	return len(TaskMetricsBucketBounds)
}

// TaskMetricsMinuteKey returns the redis key of the per-minute task metrics bucket that contains t.
func TaskMetricsMinuteKey(qname string, t time.Time) string {
	return QueueKeyPrefix(qname) + "metrics:m:" + strconv.FormatInt(t.Truncate(time.Minute).Unix(), 10)
}

// TaskMetricsHourKey returns the redis key of the per-hour task metrics bucket that contains t.
func TaskMetricsHourKey(qname string, t time.Time) string {
	return QueueKeyPrefix(qname) + "metrics:h:" + strconv.FormatInt(t.Truncate(time.Hour).Unix(), 10)
}

// TaskMetricsKeyPrefix returns the prefix shared by every task metrics key of the queue.
func TaskMetricsKeyPrefix(qname string) string {
	return QueueKeyPrefix(qname) + "metrics:"
}

// TaskTypeMetrics holds the metrics of one task type over a time bucket.
type TaskTypeMetrics struct {
	Processed           int64
	DurationSumMs       int64
	DurationMaxMs       int64
	DurationBuckets     []int64
	Failed              int64
	FailedDurationSumMs int64
	WaitCount           int64
	WaitSumMs           int64
	WaitMaxMs           int64
	WaitBuckets         []int64
}

// NewTaskTypeMetrics returns empty metrics with the histograms allocated.
func NewTaskTypeMetrics() *TaskTypeMetrics {
	return &TaskTypeMetrics{
		DurationBuckets: make([]int64, TaskMetricsBucketCount),
		WaitBuckets:     make([]int64, TaskMetricsBucketCount),
	}
}

// Merge adds the metrics of o to m.
func (m *TaskTypeMetrics) Merge(o *TaskTypeMetrics) {
	m.Processed += o.Processed
	m.DurationSumMs += o.DurationSumMs
	m.DurationMaxMs = max(m.DurationMaxMs, o.DurationMaxMs)
	m.Failed += o.Failed
	m.FailedDurationSumMs += o.FailedDurationSumMs
	m.WaitCount += o.WaitCount
	m.WaitSumMs += o.WaitSumMs
	m.WaitMaxMs = max(m.WaitMaxMs, o.WaitMaxMs)
	for i := range m.DurationBuckets {
		if i < len(o.DurationBuckets) {
			m.DurationBuckets[i] += o.DurationBuckets[i]
		}
		if i < len(o.WaitBuckets) {
			m.WaitBuckets[i] += o.WaitBuckets[i]
		}
	}
}

// TaskMetricsPercentile estimates the p-th percentile (0 < p <= 100) in milliseconds of the values counted in
// the histogram buckets, interpolating linearly within the bucket, and capping the estimate at maxMs.
// It returns zero if the histogram is empty.
func TaskMetricsPercentile(buckets []int64, p float64, maxMs int64) float64 {
	var total int64
	for _, n := range buckets {
		total += n
	}
	if total == 0 {
		return 0
	}
	rank := p / 100 * float64(total)
	var cum int64
	for i, n := range buckets {
		if n == 0 {
			continue
		}
		if float64(cum+n) >= rank {
			lower := 0.0
			if i > 0 {
				lower = float64(TaskMetricsBucketBounds[i-1])
			}
			upper := float64(maxMs)
			if i < len(TaskMetricsBucketBounds) {
				upper = float64(TaskMetricsBucketBounds[i])
			}
			if maxMs > 0 && upper > float64(maxMs) {
				upper = float64(maxMs)
			}
			if upper < lower {
				return upper
			}
			return lower + (upper-lower)*(rank-float64(cum))/float64(n)
		}
		cum += n
	}
	return float64(maxMs)
}

// TaskMetricsPoint holds the metrics of every task type of a queue over one time bucket.
type TaskMetricsPoint struct {
	// Time is the start of the time bucket.
	Time time.Time
	// Types maps task type names to their metrics.
	Types map[string]*TaskTypeMetrics
}
