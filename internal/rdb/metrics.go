// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

// recordTaskMetricsLua is appended to the scripts that end a processing attempt (done, mark as complete,
// retry and archive) to record the task metrics in the same atomic step.
//
// It expects the last two KEYS to be the per-minute and per-hour metrics keys, and the last nine ARGV to be:
//
//	+1 -> "1" to record the metrics, anything else to skip
//	+2 -> task type
//	+3 -> "s" if the attempt succeeded, "f" if it failed
//	+4 -> processing duration in milliseconds
//	+5 -> histogram bucket index of the processing duration
//	+6 -> wait time in milliseconds, negative if unknown
//	+7 -> histogram bucket index of the wait time
//	+8 -> expiration time of the per-minute key in Unix time
//	+9 -> expiration time of the per-hour key in Unix time
const recordTaskMetricsLua = `
local mb = #ARGV - 9
if ARGV[mb+1] == "1" then
	local t = ARGV[mb+2] .. "|"
	local d = tonumber(ARGV[mb+4])
	local w = tonumber(ARGV[mb+6])
	local mkeys = {KEYS[#KEYS-1], KEYS[#KEYS]}
	for i = 1, 2 do
		local k = mkeys[i]
		if ARGV[mb+3] == "s" then
			redis.call("HINCRBY", k, t .. "n", 1)
			redis.call("HINCRBY", k, t .. "s", d)
			redis.call("HINCRBY", k, t .. "d" .. ARGV[mb+5], 1)
			local x = tonumber(redis.call("HGET", k, t .. "x")) or -1
			if d > x then
				redis.call("HSET", k, t .. "x", d)
			end
		else
			redis.call("HINCRBY", k, t .. "f", 1)
			redis.call("HINCRBY", k, t .. "fs", d)
		end
		if w >= 0 then
			redis.call("HINCRBY", k, t .. "w", 1)
			redis.call("HINCRBY", k, t .. "ws", w)
			redis.call("HINCRBY", k, t .. "q" .. ARGV[mb+7], 1)
			local wx = tonumber(redis.call("HGET", k, t .. "wx")) or -1
			if w > wx then
				redis.call("HSET", k, t .. "wx", w)
			end
		end
		redis.call("EXPIREAT", k, ARGV[mb+7+i])
	end
end
`

// SetTaskMetricsEnabled enables or disables the recording of task metrics when processing attempts end.
// Task metrics are enabled by default.
func (r *RDB) SetTaskMetricsEnabled(enabled bool) {
	r.noTaskMetrics = !enabled
}

// appendTaskMetricsArgs appends the keys and arguments expected by recordTaskMetricsLua for the processing
// attempt described by msg. The metrics are recorded only for attempts whose start time is known.
func (r *RDB) appendTaskMetricsArgs(keys []string, argv []interface{}, msg *base.TaskMessage, now time.Time, success bool) ([]string, []interface{}) {
	if r.noTaskMetrics || msg.StartedAt == 0 {
		return r.appendNoTaskMetricsArgs(keys, argv, msg.Queue, now)
	}
	outcome := "f"
	if success {
		outcome = "s"
	}
	duration := max(msg.DurationMs, 0)
	wait := msg.WaitMs
	waitBucket := 0
	if wait >= 0 {
		waitBucket = base.TaskMetricsBucketIndex(wait)
	}
	keys = append(keys, base.TaskMetricsMinuteKey(msg.Queue, now), base.TaskMetricsHourKey(msg.Queue, now))
	argv = append(argv,
		"1",
		msg.Type,
		outcome,
		duration,
		base.TaskMetricsBucketIndex(duration),
		wait,
		waitBucket,
		now.Truncate(time.Minute).Add(base.TaskMetricsMinuteRetention).Unix(),
		now.Truncate(time.Hour).Add(base.TaskMetricsHourRetention).Unix(),
	)
	return keys, argv
}

// appendNoTaskMetricsArgs appends the keys and arguments expected by recordTaskMetricsLua so that it records nothing.
func (r *RDB) appendNoTaskMetricsArgs(keys []string, argv []interface{}, qname string, now time.Time) ([]string, []interface{}) {
	keys = append(keys, base.TaskMetricsMinuteKey(qname, now), base.TaskMetricsHourKey(qname, now))
	argv = append(argv, "0", "", "", 0, 0, -1, 0, 0, 0)
	return keys, argv
}

// TaskMetrics returns the task metrics of the queue recorded between start and end (both inclusive, truncated
// to the resolution), in buckets of the given resolution, which must be time.Minute or time.Hour.
// Buckets with no recorded metrics are omitted.
func (r *RDB) TaskMetrics(qname string, resolution time.Duration, start, end time.Time) ([]*base.TaskMetricsPoint, error) {
	var op errors.Op = "rdb.TaskMetrics"
	var keyFn func(string, time.Time) string
	switch resolution {
	case time.Minute:
		keyFn = base.TaskMetricsMinuteKey
	case time.Hour:
		keyFn = base.TaskMetricsHourKey
	default:
		return nil, errors.E(op, errors.FailedPrecondition, "resolution must be one minute or one hour")
	}
	start, end = start.Truncate(resolution), end.Truncate(resolution)
	if end.Before(start) {
		return nil, nil
	}
	const maxPoints = 10000
	if n := end.Sub(start)/resolution + 1; n > maxPoints {
		return nil, errors.E(op, errors.FailedPrecondition, "time range is too large for the resolution")
	}
	ctx := context.Background()
	pipe := r.client.Pipeline()
	var times []time.Time
	var cmds []*redis.MapStringStringCmd
	for t := start; !t.After(end); t = t.Add(resolution) {
		times = append(times, t)
		cmds = append(cmds, pipe.HGetAll(ctx, keyFn(qname, t)))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hgetall", Err: err})
	}
	var points []*base.TaskMetricsPoint
	for i, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil || len(fields) == 0 {
			continue
		}
		points = append(points, &base.TaskMetricsPoint{Time: times[i], Types: parseTaskMetrics(fields)})
	}
	return points, nil
}

// parseTaskMetrics parses the fields of a task metrics hash (see base.TaskMetricsMinuteKey).
func parseTaskMetrics(fields map[string]string) map[string]*base.TaskTypeMetrics {
	res := make(map[string]*base.TaskTypeMetrics)
	for field, value := range fields {
		idx := strings.LastIndexByte(field, '|')
		if idx < 0 {
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			continue
		}
		typename, metric := field[:idx], field[idx+1:]
		m, ok := res[typename]
		if !ok {
			m = base.NewTaskTypeMetrics()
			res[typename] = m
		}
		switch metric {
		case "n":
			m.Processed = n
		case "s":
			m.DurationSumMs = n
		case "x":
			m.DurationMaxMs = n
		case "f":
			m.Failed = n
		case "fs":
			m.FailedDurationSumMs = n
		case "w":
			m.WaitCount = n
		case "ws":
			m.WaitSumMs = n
		case "wx":
			m.WaitMaxMs = n
		default:
			if len(metric) < 2 || (metric[0] != 'd' && metric[0] != 'q') {
				continue
			}
			i, err := strconv.Atoi(metric[1:])
			if err != nil || i < 0 || i >= base.TaskMetricsBucketCount {
				continue
			}
			if metric[0] == 'd' {
				m.DurationBuckets[i] = n
			} else {
				m.WaitBuckets[i] = n
			}
		}
	}
	return res
}
