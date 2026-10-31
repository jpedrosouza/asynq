// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

// KEYS[1] -> asynq:flowrun:{<run id>}
// -------
// ARGV[1] -> encoded run meta, or an empty string
// ARGV[2] -> TTL in seconds
// ARGV[3:] -> encoded nodes
//
// The meta is only written if the run has none yet (or to set the flow name of a run that has none). Nodes are merged field by field into the existing ones,
// except that the state of an existing node is never set back to pending.
var updateFlowRunCmd = redis.NewScript(`
if ARGV[1] ~= "" then
	if redis.call("HSETNX", KEYS[1], "meta", ARGV[1]) == 0 then
		-- A run started by hand (EnqueueChild) gets the name of the flow whose step runs in it.
		local cur = cjson.decode(redis.call("HGET", KEYS[1], "meta"))
		local new = cjson.decode(ARGV[1])
		if (cur["flow"] == nil or cur["flow"] == "") and new["flow"] ~= nil and new["flow"] ~= "" then
			cur["flow"] = new["flow"]
			redis.call("HSET", KEYS[1], "meta", cjson.encode(cur))
		end
	end
end
for i = 3, #ARGV do
	local node = cjson.decode(ARGV[i])
	local field = "n:" .. node["id"]
	local cur = redis.call("HGET", KEYS[1], field)
	if cur then
		local merged = cjson.decode(cur)
		for k, v in pairs(node) do
			-- A task never goes back to pending: a parent that runs again re-records its children.
			if not (k == "state" and v == "pending" and merged["state"] ~= nil) then
				merged[k] = v
			end
		end
		node = merged
	end
	redis.call("HSET", KEYS[1], field, cjson.encode(node))
end
redis.call("EXPIRE", KEYS[1], ARGV[2])
return redis.status_reply("OK")
`)

// UpdateFlowRun creates or updates a flow run: it sets the meta of the run if it has none, and merges the given
// nodes into the nodes of the run.
func (r *RDB) UpdateFlowRun(ctx context.Context, runID string, meta *base.FlowRunMeta, nodes []*base.FlowNode) error {
	var op errors.Op = "rdb.UpdateFlowRun"
	argv := []interface{}{"", int64(base.FlowRunTTL.Seconds())}
	if meta != nil {
		data, err := json.Marshal(meta)
		if err != nil {
			return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode flow run: %v", err))
		}
		argv[0] = data
	}
	now := r.clock.Now()
	for _, n := range nodes {
		n.UpdatedAt = now.UnixMilli()
		data, err := json.Marshal(n)
		if err != nil {
			return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode flow node: %v", err))
		}
		argv = append(argv, data)
	}
	if err := r.runScript(ctx, op, updateFlowRunCmd, []string{base.FlowRunKey(runID)}, argv...); err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.ZAdd(ctx, base.AllFlowRuns, redis.Z{Score: float64(now.Unix()), Member: runID})
	pipe.ZRemRangeByRank(ctx, base.AllFlowRuns, 0, -base.MaxFlowRuns-1)
	pipe.ZRemRangeByScore(ctx, base.AllFlowRuns, "-inf", fmt.Sprint(now.Add(-base.FlowRunTTL).Unix()))
	if _, err := pipe.Exec(ctx); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zadd", Err: err})
	}
	return nil
}

// GetFlowRun returns the flow run with the given ID, or an error wrapping errors.NotFound if it does not exist.
func (r *RDB) GetFlowRun(runID string) (*base.FlowRun, error) {
	var op errors.Op = "rdb.GetFlowRun"
	ctx := context.Background()
	fields, err := r.client.HGetAll(ctx, base.FlowRunKey(runID)).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hgetall", Err: err})
	}
	run, err := parseFlowRun(fields)
	if err != nil {
		return nil, errors.E(op, errors.Internal, err.Error())
	}
	if run == nil {
		return nil, errors.E(op, errors.NotFound, fmt.Sprintf("flow run %q not found", runID))
	}
	if score, err := r.client.ZScore(ctx, base.AllFlowRuns, runID).Result(); err == nil {
		run.UpdatedAt = time.Unix(int64(score), 0)
	}
	return run, nil
}

func parseFlowRun(fields map[string]string) (*base.FlowRun, error) {
	data, ok := fields["meta"]
	if !ok {
		return nil, nil
	}
	var meta base.FlowRunMeta
	if err := json.Unmarshal([]byte(data), &meta); err != nil {
		return nil, fmt.Errorf("cannot decode flow run: %v", err)
	}
	run := &base.FlowRun{Meta: &meta}
	for field, data := range fields {
		if !strings.HasPrefix(field, "n:") {
			continue
		}
		var n base.FlowNode
		if err := json.Unmarshal([]byte(data), &n); err != nil {
			continue // skip bad data
		}
		run.Nodes = append(run.Nodes, &n)
	}
	sort.Slice(run.Nodes, func(i, j int) bool {
		if run.Nodes[i].EnqueuedAt != run.Nodes[j].EnqueuedAt {
			return run.Nodes[i].EnqueuedAt < run.Nodes[j].EnqueuedAt
		}
		return run.Nodes[i].ID < run.Nodes[j].ID
	})
	return run, nil
}

// ListFlowRuns returns the most recently updated flow runs, newest first, without their nodes' results.
// If flow is not empty, only the runs of that flow are returned.
func (r *RDB) ListFlowRuns(flow string, pgn Pagination) ([]*base.FlowRun, error) {
	var op errors.Op = "rdb.ListFlowRuns"
	ctx := context.Background()
	now := r.clock.Now()
	if err := r.client.ZRemRangeByScore(ctx, base.AllFlowRuns, "-inf", fmt.Sprint(now.Add(-base.FlowRunTTL).Unix())).Err(); err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zremrangebyscore", Err: err})
	}
	// Runs are filtered after being read, so read them in chunks until the page is full.
	var res []*base.FlowRun
	skip := int(pgn.start())
	const chunk = 200
	for offset := int64(0); len(res) < pgn.Size; offset += chunk {
		ids, err := r.client.ZRevRangeWithScores(ctx, base.AllFlowRuns, offset, offset+chunk-1).Result()
		if err != nil {
			return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "zrevrange", Err: err})
		}
		if len(ids) == 0 {
			break
		}
		pipe := r.client.Pipeline()
		cmds := make([]*redis.MapStringStringCmd, len(ids))
		for i, z := range ids {
			cmds[i] = pipe.HGetAll(ctx, base.FlowRunKey(z.Member.(string)))
		}
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hgetall", Err: err})
		}
		for i, cmd := range cmds {
			fields, err := cmd.Result()
			if err != nil {
				continue
			}
			run, err := parseFlowRun(fields)
			if err != nil || run == nil {
				continue // expired or bad data
			}
			if flow != "" && run.Meta.Flow != flow {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			for _, n := range run.Nodes {
				n.Result = nil
			}
			run.UpdatedAt = time.Unix(int64(ids[i].Score), 0)
			res = append(res, run)
			if len(res) == pgn.Size {
				break
			}
		}
	}
	return res, nil
}

// PublishFlows records the definitions of the flows run by a server.
func (r *RDB) PublishFlows(defs []*base.FlowDefinition) error {
	var op errors.Op = "rdb.PublishFlows"
	if len(defs) == 0 {
		return nil
	}
	now := r.clock.Now().Unix()
	values := make([]interface{}, 0, 2*len(defs))
	for _, d := range defs {
		d.UpdatedAt = now
		data, err := json.Marshal(d)
		if err != nil {
			return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode flow definition: %v", err))
		}
		values = append(values, d.Name, data)
	}
	if err := r.client.HSet(context.Background(), base.AllFlows, values...).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	return nil
}

// ListFlows returns the flow definitions published by servers, sorted by name.
func (r *RDB) ListFlows() ([]*base.FlowDefinition, error) {
	var op errors.Op = "rdb.ListFlows"
	all, err := r.client.HGetAll(context.Background(), base.AllFlows).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hgetall", Err: err})
	}
	res := make([]*base.FlowDefinition, 0, len(all))
	for _, data := range all {
		var d base.FlowDefinition
		if err := json.Unmarshal([]byte(data), &d); err != nil {
			continue // skip bad data
		}
		res = append(res, &d)
	}
	sort.Slice(res, func(i, j int) bool { return res[i].Name < res[j].Name })
	return res, nil
}

// DeleteFlow deletes the published definition of the flow.
func (r *RDB) DeleteFlow(name string) error {
	var op errors.Op = "rdb.DeleteFlow"
	if err := r.client.HDel(context.Background(), base.AllFlows, name).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hdel", Err: err})
	}
	return nil
}
