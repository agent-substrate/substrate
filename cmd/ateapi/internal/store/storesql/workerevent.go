// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storesql

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// MarshalWorkerEvent encodes an outbox payload: one event-type byte followed by
// the binary Worker proto. The tag byte is read by other replicas during
// rolling deploys, so store.WorkerEventType values must stay append-only
// stable and fit a byte.
func MarshalWorkerEvent(eventType store.WorkerEventType, worker *ateapipb.Worker) ([]byte, error) {
	b, err := proto.Marshal(worker)
	if err != nil {
		return nil, fmt.Errorf("in proto.Marshal: %w", err)
	}
	return append([]byte{byte(eventType)}, b...), nil
}

// UnmarshalWorkerEvent decodes a payload written by MarshalWorkerEvent.
func UnmarshalWorkerEvent(payload []byte) (store.WorkerEvent, error) {
	if len(payload) == 0 {
		return store.WorkerEvent{}, fmt.Errorf("empty worker event payload")
	}
	// Assert invariants at the boundary. Corrupted payloads or unknown types
	// must fail here to trigger a loud resync, rather than falling through
	// downstream as silent no-ops.
	eventType := store.WorkerEventType(payload[0])
	switch eventType {
	case store.WorkerEventCreated, store.WorkerEventUpdated, store.WorkerEventDeleted:
	default:
		return store.WorkerEvent{}, fmt.Errorf("unknown worker event type byte %d", payload[0])
	}
	worker := &ateapipb.Worker{}
	if err := UnmarshalStored(payload[1:], worker); err != nil {
		return store.WorkerEvent{}, fmt.Errorf("unmarshaling worker event payload: %w", err)
	}
	if worker.GetMetadata().GetName() == "" {
		return store.WorkerEvent{}, fmt.Errorf("worker event payload has no worker name")
	}
	return store.WorkerEvent{Type: eventType, Worker: worker}, nil
}

// Watchers fans committed worker events out to this process's WatchWorkers
// channels. The zero value is ready to use.
type Watchers struct {
	mu       sync.Mutex
	channels map[chan store.WorkerEvent]struct{}
}

// Add enrolls a WatchWorkers channel to receive locally published events.
func (w *Watchers) Add(ch chan store.WorkerEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.channels == nil {
		w.channels = make(map[chan store.WorkerEvent]struct{})
	}
	w.channels[ch] = struct{}{}
}

// Remove unenrolls a channel. The caller must call it before closing the
// channel: once it returns, Publish can no longer send on it.
func (w *Watchers) Remove(ch chan store.WorkerEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.channels, ch)
}

// Publish hands a committed event to this process's watchers a poll interval
// ahead of the outbox, one copy each. Sends are non-blocking: a watcher with a
// full buffer is skipped and gets the event from the outbox.
func (w *Watchers) Publish(ctx context.Context, payload []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.channels) == 0 {
		return
	}
	event, err := UnmarshalWorkerEvent(payload)
	if err != nil {
		slog.ErrorContext(ctx, "decoding locally published worker event failed", slog.Any("err", err))
		return
	}
	for ch := range w.channels {
		select {
		case ch <- store.WorkerEvent{Type: event.Type, Worker: proto.Clone(event.Worker).(*ateapipb.Worker)}:
		default:
		}
	}
}
