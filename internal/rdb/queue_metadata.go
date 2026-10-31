// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package rdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jpedrosouza/asynq/internal/base"
	"github.com/jpedrosouza/asynq/internal/errors"
	"github.com/redis/go-redis/v9"
)

// SetQueueMetadata stores the metadata of the queue, replacing any previous value.
// The queue does not need to exist.
func (r *RDB) SetQueueMetadata(qname string, md *base.QueueMetadata) error {
	var op errors.Op = "rdb.SetQueueMetadata"
	data, err := json.Marshal(md)
	if err != nil {
		return errors.E(op, errors.Internal, fmt.Sprintf("cannot encode queue metadata: %v", err))
	}
	if err := r.client.HSet(context.Background(), base.AllQueueMetadata, qname, data).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hset", Err: err})
	}
	return nil
}

// DeleteQueueMetadata deletes the metadata of the queue.
func (r *RDB) DeleteQueueMetadata(qname string) error {
	var op errors.Op = "rdb.DeleteQueueMetadata"
	if err := r.client.HDel(context.Background(), base.AllQueueMetadata, qname).Err(); err != nil {
		return errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hdel", Err: err})
	}
	return nil
}

// GetQueueMetadata returns the metadata of the queue, or nil if the queue has none.
func (r *RDB) GetQueueMetadata(qname string) (*base.QueueMetadata, error) {
	var op errors.Op = "rdb.GetQueueMetadata"
	data, err := r.client.HGet(context.Background(), base.AllQueueMetadata, qname).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hget", Err: err})
	}
	var md base.QueueMetadata
	if err := json.Unmarshal([]byte(data), &md); err != nil {
		return nil, errors.E(op, errors.Internal, fmt.Sprintf("cannot decode queue metadata: %v", err))
	}
	return &md, nil
}

// ListQueueMetadata returns the metadata of every queue that has some, by queue name.
func (r *RDB) ListQueueMetadata() (map[string]*base.QueueMetadata, error) {
	var op errors.Op = "rdb.ListQueueMetadata"
	all, err := r.client.HGetAll(context.Background(), base.AllQueueMetadata).Result()
	if err != nil {
		return nil, errors.E(op, errors.Unknown, &errors.RedisCommandError{Command: "hgetall", Err: err})
	}
	res := make(map[string]*base.QueueMetadata, len(all))
	for qname, data := range all {
		var md base.QueueMetadata
		if err := json.Unmarshal([]byte(data), &md); err != nil {
			continue // skip bad data
		}
		res[qname] = &md
	}
	return res, nil
}
