// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
)

// TaskMetricsHistogramBounds are the upper bounds (inclusive) of the buckets of the duration histograms in
// TaskMetrics. A duration greater than the last bound falls in the last bucket, so the histograms have
// len(TaskMetricsHistogramBounds)+1 buckets.
var TaskMetricsHistogramBounds = func() []time.Duration {
	res := make([]time.Duration, len(base.TaskMetricsBucketBounds))
	for i, ms := range base.TaskMetricsBucketBounds {
		res[i] = time.Duration(ms) * time.Millisecond
	}
	return res
}()

// TaskMetrics holds the processing metrics of a set of tasks (e.g. the tasks of a type over a time bucket).
//
// Servers record the metrics when a processing attempt ends, unless Config.DisableTaskMetrics is set.
type TaskMetrics struct {
	// Processed is the number of tasks processed successfully.
	Processed int64
	// TotalDuration is the sum of the processing durations of the successful tasks.
	TotalDuration time.Duration
	// MaxDuration is the longest processing duration of a successful task.
	MaxDuration time.Duration
	// DurationHistogram counts the successful tasks by processing duration (see TaskMetricsHistogramBounds).
	DurationHistogram []int64

	// Failed is the number of failed processing attempts.
	Failed int64
	// FailedTotalDuration is the sum of the processing durations of the failed attempts.
	FailedTotalDuration time.Duration

	// WaitCount is the number of processing attempts whose wait time is known.
	WaitCount int64
	// TotalWait is the sum of the wait times, the time a task spent in the pending state before a worker picked it up.
	TotalWait time.Duration
	// MaxWait is the longest wait time.
	MaxWait time.Duration
	// WaitHistogram counts the processing attempts by wait time (see TaskMetricsHistogramBounds).
	WaitHistogram []int64
}

func newTaskMetrics(m *base.TaskTypeMetrics) *TaskMetrics {
	ms := func(n int64) time.Duration { return time.Duration(n) * time.Millisecond }
	return &TaskMetrics{
		Processed:           m.Processed,
		TotalDuration:       ms(m.DurationSumMs),
		MaxDuration:         ms(m.DurationMaxMs),
		DurationHistogram:   append([]int64(nil), m.DurationBuckets...),
		Failed:              m.Failed,
		FailedTotalDuration: ms(m.FailedDurationSumMs),
		WaitCount:           m.WaitCount,
		TotalWait:           ms(m.WaitSumMs),
		MaxWait:             ms(m.WaitMaxMs),
		WaitHistogram:       append([]int64(nil), m.WaitBuckets...),
	}
}

// Merge adds the metrics of o to m.
func (m *TaskMetrics) Merge(o *TaskMetrics) {
	m.Processed += o.Processed
	m.TotalDuration += o.TotalDuration
	m.MaxDuration = max(m.MaxDuration, o.MaxDuration)
	m.Failed += o.Failed
	m.FailedTotalDuration += o.FailedTotalDuration
	m.WaitCount += o.WaitCount
	m.TotalWait += o.TotalWait
	m.MaxWait = max(m.MaxWait, o.MaxWait)
	m.DurationHistogram = mergeHistograms(m.DurationHistogram, o.DurationHistogram)
	m.WaitHistogram = mergeHistograms(m.WaitHistogram, o.WaitHistogram)
}

func mergeHistograms(a, b []int64) []int64 {
	if len(a) < len(b) {
		a = append(a, make([]int64, len(b)-len(a))...)
	}
	for i, n := range b {
		a[i] += n
	}
	return a
}

// AvgDuration returns the average processing duration of the successful tasks.
func (m *TaskMetrics) AvgDuration() time.Duration {
	if m.Processed == 0 {
		return 0
	}
	return m.TotalDuration / time.Duration(m.Processed)
}

// DurationPercentile estimates the p-th percentile (0 < p <= 100) of the processing duration of the successful
// tasks from the histogram.
func (m *TaskMetrics) DurationPercentile(p float64) time.Duration {
	return time.Duration(base.TaskMetricsPercentile(m.DurationHistogram, p, m.MaxDuration.Milliseconds()) * float64(time.Millisecond))
}

// AvgWait returns the average wait time.
func (m *TaskMetrics) AvgWait() time.Duration {
	if m.WaitCount == 0 {
		return 0
	}
	return m.TotalWait / time.Duration(m.WaitCount)
}

// WaitPercentile estimates the p-th percentile (0 < p <= 100) of the wait time from the histogram.
func (m *TaskMetrics) WaitPercentile(p float64) time.Duration {
	return time.Duration(base.TaskMetricsPercentile(m.WaitHistogram, p, m.MaxWait.Milliseconds()) * float64(time.Millisecond))
}

// TaskMetricsPoint holds the processing metrics of the tasks of a queue over a time bucket.
type TaskMetricsPoint struct {
	// Time is the start of the time bucket.
	Time time.Time
	// Types maps task type names to the metrics of the tasks of the type.
	Types map[string]*TaskMetrics
}

// Total returns the metrics of all the task types together.
func (p *TaskMetricsPoint) Total() *TaskMetrics {
	total := &TaskMetrics{}
	for _, m := range p.Types {
		total.Merge(m)
	}
	return total
}

// TaskMetrics returns the processing metrics of the tasks of the queue between start and end (inclusive), in time
// buckets of the given resolution, which must be time.Minute or time.Hour. Buckets without any task are omitted.
//
// Per-minute metrics are kept for 48 hours and per-hour metrics for 90 days.
func (i *Inspector) TaskMetrics(queue string, resolution time.Duration, start, end time.Time) ([]*TaskMetricsPoint, error) {
	if err := base.ValidateQueueName(queue); err != nil {
		return nil, err
	}
	points, err := i.rdb.TaskMetrics(queue, resolution, start, end)
	if err != nil {
		return nil, err
	}
	res := make([]*TaskMetricsPoint, len(points))
	for idx, p := range points {
		types := make(map[string]*TaskMetrics, len(p.Types))
		for typename, m := range p.Types {
			types[typename] = newTaskMetrics(m)
		}
		res[idx] = &TaskMetricsPoint{Time: p.Time, Types: types}
	}
	return res, nil
}
