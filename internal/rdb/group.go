// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

// aggregationCheckPolicyCmd checks the given group for whether to create an aggregation set.
// An aggregation set is created if one of the aggregation criteria is met:
// 0) a flush of the group was requested
// 1) group has reached or exceeded its max size
// 2) group's oldest task has reached or exceeded its max delay
// 3) group has at least min size tasks and its latest task has reached or exceeded its grace period
// if aggregation criteria is met, the command moves those tasks from the group
// and put them in an aggregation set. Additionally, if the creation of aggregation set
// empties the group, it will clear the group name from the all groups set.
//
// KEYS[1] -> asynq:{<qname>}:g:<gname>
// KEYS[2] -> asynq:{<qname>}:g:<gname>:<aggregation_set_id>
// KEYS[3] -> asynq:{<qname>}:aggregation_sets
// KEYS[4] -> asynq:{<qname>}:groups
// KEYS[5] -> asynq:{<qname>}:flush_groups
// -------
// ARGV[1] -> max group size (zero means no limit)
// ARGV[2] -> max group delay in seconds (zero means no max delay)
// ARGV[3] -> grace period in seconds (zero or less means no grace period)
// ARGV[4] -> aggregation set expire time
// ARGV[5] -> current time in unix time
// ARGV[6] -> group name
// ARGV[7] -> min group size for the grace period
//
// Output:
// Returns 0 if no aggregation set was created
// Returns 1 if an aggregation set was created
//
// Time Complexity:
// O(log(N) + M) with N being the number tasks in the group zset
// and M being the max size.
var aggregationCheckPolicyCmd = redis.NewScript(`
local size = redis.call("ZCARD", KEYS[1])
if size == 0 then
	redis.call("SREM", KEYS[5], ARGV[6])
	return 0
end
local maxSize = tonumber(ARGV[1])
local function aggregate()
	local stop = -1
	if maxSize ~= 0 then
		stop = maxSize - 1
	end
	local res = redis.call("ZRANGE", KEYS[1], 0, stop, "WITHSCORES")
	for i=1, table.getn(res)-1, 2 do
		redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
	end
	redis.call("ZREMRANGEBYRANK", KEYS[1], 0, stop)
	redis.call("ZADD", KEYS[3], ARGV[4], KEYS[2])
	if maxSize == 0 or size <= maxSize then
		redis.call("SREM", KEYS[4], ARGV[6])
	end
	return 1
end
if redis.call("SREM", KEYS[5], ARGV[6]) == 1 then
	return aggregate()
end
if maxSize ~= 0 and size >= maxSize then
	return aggregate()
end
local now = tonumber(ARGV[5])
local maxDelay = tonumber(ARGV[2])
if maxDelay ~= 0 then
	local oldest = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
	if tonumber(oldest[2]) <= now - maxDelay then
		return aggregate()
	end
end
local grace = tonumber(ARGV[3])
if grace > 0 and size >= tonumber(ARGV[7]) then
	local latest = redis.call("ZREVRANGE", KEYS[1], 0, 0, "WITHSCORES")
	if tonumber(latest[2]) <= now - grace then
		return aggregate()
	end
end
return 0
`)

// AggregationCheckPolicy checks the group identified by the given queue and group name to see if the tasks in the
// group are ready to be aggregated according to the policy. If so, it moves the tasks to be aggregated to an
// aggregation set and returns the set ID. If not, it returns an empty string for the set ID.
// The time for the grace period and the max delay is computed relative to the time t.
//
// A group whose flush was requested (see RequestGroupFlush) is aggregated regardless of the policy.
func (r *RDB) AggregationCheckPolicy(qname, gname string, t time.Time, policy base.GroupPolicy) (string, error) {
	var op errors.Op = "RDB.AggregationCheck"
	aggregationSetID := uuid.NewString()
	expireTime := r.clock.Now().Add(aggregationTimeout)
	keys := []string{
		base.GroupKey(qname, gname),
		base.AggregationSetKey(qname, gname, aggregationSetID),
		base.AllAggregationSets(qname),
		base.AllGroups(qname),
		base.FlushGroupsKey(qname),
	}
	argv := []interface{}{
		max(policy.MaxSize, 0),
		max(policy.MaxDelaySec, 0),
		policy.GracePeriodSec,
		expireTime.Unix(),
		t.Unix(),
		gname,
		max(policy.MinSize, 0),
	}
	n, err := r.runScriptWithErrorCode(context.Background(), op, aggregationCheckPolicyCmd, keys, argv...)
	if err != nil {
		return "", err
	}
	switch n {
	case 0:
		return "", nil
	case 1:
		return aggregationSetID, nil
	default:
		return "", errors.E(op, errors.Internal, fmt.Sprintf("unexpected return value from lua script: %d", n))
	}
}

// RequestGroupFlush requests the group to be aggregated at the next aggregation check, whatever its policy.
func (r *RDB) RequestGroupFlush(qname, gname string) error {
	var op errors.Op = "RDB.RequestGroupFlush"
	ctx := context.Background()
	n, err := r.client.ZCard(ctx, base.GroupKey(qname, gname)).Result()
	if err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zcard", Err: err})
	}
	if n == 0 {
		return errors.E(op, errors.NotFound, fmt.Sprintf("group %q is empty or does not exist in queue %q", gname, qname))
	}
	if err := r.client.SAdd(ctx, base.FlushGroupsKey(qname), gname).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
	}
	return nil
}

// groupPoliciesTTL is how long the published group policies are kept without being refreshed by a server.
const groupPoliciesTTL = 24 * time.Hour

// PublishGroupPolicies records the aggregation policies used for the groups of the queue, so that tools can show
// how far a group is from being aggregated.
func (r *RDB) PublishGroupPolicies(qname string, policies map[string]base.GroupPolicy) error {
	var op errors.Op = "RDB.PublishGroupPolicies"
	if len(policies) == 0 {
		return nil
	}
	values := make([]interface{}, 0, 2*len(policies))
	for gname, p := range policies {
		data, err := json.Marshal(p)
		if err != nil {
			return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode group policy: %v", err))
		}
		values = append(values, gname, data)
	}
	ctx := context.Background()
	key := base.GroupPoliciesKey(qname)
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, values...)
	pipe.Expire(ctx, key, groupPoliciesTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	return nil
}

// KEYS[1] -> asynq:{<qname>}:t:<task_id>
// KEYS[2] -> asynq:{<qname>}:archived
// -------
// ARGV[1] -> task message data
// ARGV[2] -> current unix time
// ARGV[3] -> task ID
//
// Output:
// Returns 1 if successfully added
// Returns 0 if task ID already exists
var addToArchiveCmd = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return 0
end
redis.call("HSET", KEYS[1], "msg", ARGV[1], "state", "archived")
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
return 1
`)

// AddToArchive adds the given task message, which must not exist yet, straight to the archive.
// It is used for a member of an aggregated batch that failed for good.
func (r *RDB) AddToArchive(ctx context.Context, msg *base.TaskMessage) error {
	var op errors.Op = "rdb.AddToArchive"
	encoded, err := base.EncodeMessage(msg)
	if err != nil {
		return errors.E(op, errors.Unknown, fmt.Sprintf("cannot encode message: %v", err))
	}
	if _, found := r.queuesPublished.Load(msg.Queue); !found {
		if err := r.client.SAdd(ctx, base.AllQueues, msg.Queue).Err(); err != nil {
			return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "sadd", Err: err})
		}
		r.queuesPublished.Store(msg.Queue, true)
	}
	keys := []string{base.TaskKey(msg.Queue, msg.ID), base.ArchivedKey(msg.Queue)}
	n, err := r.runScriptWithErrorCode(ctx, op, addToArchiveCmd, keys, encoded, r.clock.Now().Unix(), msg.ID)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.E(op, errors.AlreadyExists, errors.ErrTaskIdConflict)
	}
	return nil
}
