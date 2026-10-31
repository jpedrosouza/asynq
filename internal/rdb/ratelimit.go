// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

func validateRateLimit(rl base.RateLimit) error {
	if rl.Limit < 1 {
		return fmt.Errorf("rate limit must allow at least one task per period")
	}
	if rl.PeriodMs < 1 {
		return fmt.Errorf("rate limit period must be at least one millisecond")
	}
	if rl.Burst < 0 {
		return fmt.Errorf("rate limit burst cannot be negative")
	}
	return nil
}

func rateLimitFields(rl base.RateLimit) []interface{} {
	burst := rl.Burst
	if burst < 1 {
		burst = 1
	}
	return []interface{}{"limit", rl.Limit, "period", rl.PeriodMs, "burst", burst}
}

// readRateLimitStatus reads the configuration and the state of the rate limit stored in the given keys.
// It returns nil if no rate limit is configured.
func (r *RDB) readRateLimitStatus(ctx context.Context, cfgKey, stateKey string) (*base.RateLimitStatus, error) {
	pipe := r.client.Pipeline()
	cfgCmd := pipe.HMGet(ctx, cfgKey, "limit", "period", "burst")
	stateCmd := pipe.Get(ctx, stateKey)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	vals, err := cfgCmd.Result()
	if err != nil {
		return nil, err
	}
	parse := func(v interface{}) int64 {
		s, _ := v.(string)
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	}
	rl := base.RateLimit{Limit: int(parse(vals[0])), PeriodMs: parse(vals[1]), Burst: int(parse(vals[2]))}
	if rl.Limit < 1 || rl.PeriodMs < 1 {
		return nil, nil
	}
	if rl.Burst < 1 {
		rl.Burst = 1
	}
	now := r.clock.Now().UnixMilli()
	tat := float64(now)
	if s, err := stateCmd.Result(); err == nil {
		if v, err := strconv.ParseFloat(s, 64); err == nil && v > tat {
			tat = v
		}
	}
	emission := float64(rl.PeriodMs) / float64(rl.Limit)
	nextAt := tat - emission*float64(rl.Burst-1)
	available := 0
	if nextAt <= float64(now) {
		available = int(math.Floor((float64(now)-nextAt)/emission)) + 1
		if available > rl.Burst {
			available = rl.Burst
		}
	}
	return &base.RateLimitStatus{RateLimit: rl, Available: available, NextAt: int64(math.Ceil(nextAt))}, nil
}

// SetQueueRateLimit sets the rate limit of the queue: servers dequeue at most rl.Limit tasks of the queue
// per rl.PeriodMs.
func (r *RDB) SetQueueRateLimit(qname string, rl base.RateLimit) error {
	var op errors.Op = "rdb.SetQueueRateLimit"
	if err := validateRateLimit(rl); err != nil {
		return errors.E(op, errors.FailedPrecondition, err.Error())
	}
	ctx := context.Background()
	key := base.QueueRateLimitKey(qname)
	pipe := r.client.TxPipeline()
	pipe.Del(ctx, key)
	pipe.HSet(ctx, key, rateLimitFields(rl)...)
	if _, err := pipe.Exec(ctx); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	return nil
}

// RemoveQueueRateLimit removes the rate limit of the queue.
func (r *RDB) RemoveQueueRateLimit(qname string) error {
	var op errors.Op = "rdb.RemoveQueueRateLimit"
	ctx := context.Background()
	if err := r.client.Del(ctx, base.QueueRateLimitKey(qname), base.QueueRateLimitStateKey(qname)).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	return nil
}

// QueueRateLimit returns the rate limit of the queue, or nil if the queue has none.
func (r *RDB) QueueRateLimit(qname string) (*base.RateLimitStatus, error) {
	var op errors.Op = "rdb.QueueRateLimit"
	st, err := r.readRateLimitStatus(context.Background(), base.QueueRateLimitKey(qname), base.QueueRateLimitStateKey(qname))
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hmget", Err: err})
	}
	return st, nil
}

// SetRateLimit sets the key level rate limit: at most rl.Limit tasks enqueued with the rate limit key
// start per rl.PeriodMs, whatever their queue.
func (r *RDB) SetRateLimit(key string, rl base.RateLimit) error {
	var op errors.Op = "rdb.SetRateLimit"
	if err := validateRateLimit(rl); err != nil {
		return errors.E(op, errors.FailedPrecondition, err.Error())
	}
	ctx := context.Background()
	cfgKey := base.RateLimitKey(key)
	pipe := r.client.TxPipeline()
	pipe.Del(ctx, cfgKey)
	pipe.HSet(ctx, cfgKey, rateLimitFields(rl)...)
	if _, err := pipe.Exec(ctx); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	if err := r.client.SAdd(ctx, base.AllRateLimitKeys, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	return nil
}

// RemoveRateLimit removes the key level rate limit.
func (r *RDB) RemoveRateLimit(key string) error {
	var op errors.Op = "rdb.RemoveRateLimit"
	ctx := context.Background()
	if err := r.client.Del(ctx, base.RateLimitKey(key), base.RateLimitStateKey(key)).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "del", Err: err})
	}
	if err := r.client.SRem(ctx, base.AllRateLimitKeys, key).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "srem", Err: err})
	}
	return nil
}

// RateLimits returns the key level rate limits by key.
func (r *RDB) RateLimits() (map[string]*base.RateLimitStatus, error) {
	var op errors.Op = "rdb.RateLimits"
	ctx := context.Background()
	keys, err := r.client.SMembers(ctx, base.AllRateLimitKeys).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "smembers", Err: err})
	}
	res := make(map[string]*base.RateLimitStatus, len(keys))
	for _, key := range keys {
		st, err := r.readRateLimitStatus(ctx, base.RateLimitKey(key), base.RateLimitStateKey(key))
		if err != nil {
			return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hmget", Err: err})
		}
		if st != nil {
			res[key] = st
		}
	}
	return res, nil
}

// KEYS[1] -> asynq:ratelimit:{<key>}
// KEYS[2] -> asynq:ratelimit:{<key>}:tat
// ARGV[1] -> current Unix time in milliseconds
//
// Output:
// Returns -1 if no rate limit is configured for the key.
// Returns the number of milliseconds until the reserved slot otherwise: zero means the task may start now.
//
// Every call reserves a slot, moving the theoretical arrival time forward by period/limit.
var reserveRateLimitCmd = redis.NewScript(`
local cfg = redis.call("HMGET", KEYS[1], "limit", "period", "burst")
local limit = tonumber(cfg[1]) or 0
if limit <= 0 then
	return -1
end
local now = tonumber(ARGV[1])
local emission = (tonumber(cfg[2]) or 1000) / limit
local burst = tonumber(cfg[3]) or 1
if burst < 1 then
	burst = 1
end
local tat = tonumber(redis.call("GET", KEYS[2])) or now
if tat < now then
	tat = now
end
local allowAt = tat - emission * (burst - 1)
local newTat = tat + emission
redis.call("SET", KEYS[2], string.format("%.3f", newTat), "PX", math.ceil(newTat - now) + 1000)
if allowAt <= now then
	return 0
end
return math.ceil(allowAt - now)
`)

// ReserveRateLimit reserves a slot in the key level rate limit.
// It returns the time of the slot (at or before the current time if the task may start right away), or the zero
// time if no rate limit is configured for the key.
func (r *RDB) ReserveRateLimit(ctx context.Context, key string) (time.Time, error) {
	var op errors.Op = "rdb.ReserveRateLimit"
	now := r.clock.Now()
	keys := []string{base.RateLimitKey(key), base.RateLimitStateKey(key)}
	n, err := r.runScriptWithErrorCode(ctx, op, reserveRateLimitCmd, keys, now.UnixMilli())
	if err != nil {
		return time.Time{}, err
	}
	if n < 0 {
		return time.Time{}, nil
	}
	return now.Add(time.Duration(n) * time.Millisecond), nil
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:active
// KEYS[3] -> asynq:{<qname>}:lease
// KEYS[4] -> asynq:{<qname>}:scheduled
// -------
// ARGV[1] -> task ID
// ARGV[2] -> updated base.TaskMessage value
// ARGV[3] -> process_at Unix time
var deferCmd = redis.NewScript(`
if redis.call("LREM", KEYS[2], 0, ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
if redis.call("ZREM", KEYS[3], ARGV[1]) == 0 then
  return redis.error_reply("NOT FOUND")
end
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
redis.call("HSET", KEYS[1], "msg", ARGV[2], "state", "scheduled")
return redis.status_reply("OK")`)

// Defer moves the active task back to the scheduled set, to be processed at processAt.
// Unlike Retry, it neither counts as a failure nor updates the processed stats.
func (r *RDB) Defer(ctx context.Context, msg *base.TaskMessage, processAt time.Time) error {
	var op errors.Op = "rdb.Defer"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode message: %v", err))
	}
	keys := []string{
		base.TaskKey(msg.Queue, msg.ID),
		base.ActiveKey(msg.Queue),
		base.LeaseKey(msg.Queue),
		base.ScheduledKey(msg.Queue),
	}
	return r.runScript(ctx, op, deferCmd, keys, msg.ID, encoded, processAt.Unix())
}
