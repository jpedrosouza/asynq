// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"strings"

	"github.com/jpedrosouza/asynq/internal/base"
)

// QueueMetadata holds user defined information used to organize queues, e.g. to group them by category in a
// dashboard. It has no effect on how tasks are processed.
type QueueMetadata struct {
	// Category groups related queues, e.g. "Reports" or "Video processing".
	Category string
	// Subcategory optionally divides a category, e.g. "Daily" within "Reports". It requires a Category.
	Subcategory string
	// Description is a free text description of the queue.
	Description string
}

// IsZero reports whether the metadata is empty.
func (md QueueMetadata) IsZero() bool {
	return md.Category == "" && md.Subcategory == "" && md.Description == ""
}

func (md QueueMetadata) normalize() QueueMetadata {
	md.Category = strings.TrimSpace(md.Category)
	md.Subcategory = strings.TrimSpace(md.Subcategory)
	md.Description = strings.TrimSpace(md.Description)
	if md.Category == "" {
		md.Subcategory = ""
	}
	return md
}

// SetQueueMetadata stores the metadata of the queue, replacing any previous value. Setting empty metadata deletes it.
//
// The metadata is stored in redis. The queue does not need to exist, and the metadata is kept if the queue is deleted.
func (i *Inspector) SetQueueMetadata(queue string, md QueueMetadata) error {
	if err := base.ValidateQueueName(queue); err != nil {
		return err
	}
	md = md.normalize()
	if md.IsZero() {
		return i.rdb.DeleteQueueMetadata(queue)
	}
	return i.rdb.SetQueueMetadata(queue, &base.QueueMetadata{
		Category:    md.Category,
		Subcategory: md.Subcategory,
		Description: md.Description,
	})
}

// DeleteQueueMetadata deletes the metadata of the queue.
func (i *Inspector) DeleteQueueMetadata(queue string) error {
	if err := base.ValidateQueueName(queue); err != nil {
		return err
	}
	return i.rdb.DeleteQueueMetadata(queue)
}

// GetQueueMetadata returns the metadata of the queue; it is empty if the queue has none.
func (i *Inspector) GetQueueMetadata(queue string) (QueueMetadata, error) {
	if err := base.ValidateQueueName(queue); err != nil {
		return QueueMetadata{}, err
	}
	md, err := i.rdb.GetQueueMetadata(queue)
	if err != nil || md == nil {
		return QueueMetadata{}, err
	}
	return QueueMetadata{Category: md.Category, Subcategory: md.Subcategory, Description: md.Description}, nil
}

// ListQueueMetadata returns the metadata of every queue that has some, by queue name.
func (i *Inspector) ListQueueMetadata() (map[string]QueueMetadata, error) {
	all, err := i.rdb.ListQueueMetadata()
	if err != nil {
		return nil, err
	}
	res := make(map[string]QueueMetadata, len(all))
	for qname, md := range all {
		res[qname] = QueueMetadata{Category: md.Category, Subcategory: md.Subcategory, Description: md.Description}
	}
	return res, nil
}
