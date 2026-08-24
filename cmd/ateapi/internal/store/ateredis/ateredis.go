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

// Package ateredis is an ate storage backend built on Redis.
//
// Actors are stored in keys of the form
// `actor:<atespace>:<name>`.  They are
// stored as DBActor JSON-serialized objects, which lets us manipulate them from
// Redis lua.
//
// Workers are stored in keys of the form
// `worker:<namespace>:<pool-name>:<pod-name>`, holding a DBWorker JSON object.
//
// Note that redis lua scripting has a restriction that informed the data design
// here -- a lua script must predeclare all keys it is going to access.  It
// cannot read one key, then derive another key from the value, and read it.
// This is why we store the worker status inline in the Actor.
//
// Additionally, redis / valkey in cluster mode have a serious restriction that
// informs our data model: it is not possible for a single "action" to touch
// keys that hash to to different cluster slots.  This includes lua scripts. The
// biggest implication here is that it is not possible to atomically mark an
// actor as scheduled on a worker, and the worker as busy.  So we need to be
// very careful about the order in which we take these actions.
//
// Note also (but I cannot find documentation one way or another) that Redis Lua
// is not ACID --- power failure, etc may leave us with half of the effects of a
// script applied.
package ateredis

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// globalAtespace in Substrate is represented by "".
const globalAtespace = ""

type workerPubSubMsg struct {
	Type   int    `json:"t"`
	Worker string `json:"w"` // protojson-encoded Worker
}

type redisClient interface {
	redis.Cmdable
	ForEachMaster(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}

// Persistence is a service that stores information about applications in Redis.
type Persistence struct {
	rdb     redisClient
	lockTTL time.Duration
}

var _ store.Interface = (*Persistence)(nil)

// NewPersistence creates a new Persistence.
func NewPersistence(redisClient *redis.ClusterClient) *Persistence {
	return &Persistence{
		rdb:     redisClient,
		lockTTL: defaultLockTTL,
	}
}

// actorDBKey returns the Redis key an actor is stored under. The encoding is
// "actor:<atespace>:<name>" and must not change: existing databases hold keys
// in this form.
func actorDBKey(actorRef resources.ActorRef) string {
	return "actor:" + actorRef.Atespace + ":" + actorRef.Name
}

// actorScanPattern returns the SCAN match pattern for listing actors. An empty
// atespace lists across all atespaces (actor:*); a non-empty atespace scopes the
// scan to that atespace (actor:<atespace>:*).
func actorScanPattern(atespace string) string {
	if atespace == globalAtespace {
		return "actor:*"
	}
	return "actor:" + atespace + ":*"
}

func actorSnapshotDBKey(atespace, name string) string {
	return "actor-snapshot:" + atespace + ":" + name
}

func actorSnapshotScanPattern(atespace string) string {
	if atespace == globalAtespace {
		return "actor-snapshot:*"
	}
	return "actor-snapshot:" + atespace + ":*"
}

func actorSnapshotTagDBKey(atespace, name string) string {
	return "actor-snapshot-tag:" + atespace + ":" + name
}

func actorSnapshotTagScanPattern(atespace string) string {
	return "actor-snapshot-tag:" + atespace + ":*"
}

func atespaceDBKey(name string) string {
	return "atespace:" + name
}

func (s *Persistence) CreateAtespace(ctx context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error) {
	dbKey := atespaceDBKey(atespace.GetMetadata().GetName())

	dbAtespace := proto.Clone(atespace).(*ateapipb.Atespace)
	// Atespace is global-scoped: identity is the name alone (atespace stays empty).
	dbAtespace.Metadata = newCreateMetadata(globalAtespace, atespace.GetMetadata().GetName())

	dbBytes, err := protojson.Marshal(dbAtespace)
	if err != nil {
		return nil, fmt.Errorf("in protojson.Marshal: %w", err)
	}
	ok, err := s.rdb.SetNX(ctx, dbKey, dbBytes, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("while executing redis set: %w", err)
	}
	if !ok {
		return nil, store.ErrAlreadyExists
	}
	return dbAtespace, nil
}

func (s *Persistence) GetAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	dbKey := atespaceDBKey(name)
	dbBytes, err := s.rdb.Get(ctx, dbKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting atespace key %q: %w", dbKey, err)
	}
	atespace := &ateapipb.Atespace{}
	if err := protojson.Unmarshal(dbBytes, atespace); err != nil {
		return nil, fmt.Errorf("while unmarshaling atespace: %w", err)
	}
	if atespace.GetMetadata().GetName() != name {
		return nil, fmt.Errorf("(impossible) mismatch between stored name and key %q", dbKey)
	}
	return atespace, nil
}

// atespaceExists reports whether the atespace object exists. This is a plain
// EXISTS check and is NOT atomic with respect to a concurrent DeleteAtespace.
func (s *Persistence) atespaceExists(ctx context.Context, name string) (bool, error) {
	n, err := s.rdb.Exists(ctx, atespaceDBKey(name)).Result()
	if err != nil {
		return false, fmt.Errorf("while checking atespace existence: %w", err)
	}
	return n > 0, nil
}

func (s *Persistence) ListAtespaces(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Atespace], error) {
	var result []*ateapipb.Atespace
	nextToken, err := s.listPage(ctx, "atespace:*", opts.PageSize, opts.PageToken, func(ctx context.Context, master *redis.Client, keys []string) (int, error) {
		atespaces, err := fetchProtos(ctx, master, keys, func() *ateapipb.Atespace { return &ateapipb.Atespace{} })
		if err != nil {
			return 0, err
		}
		result = append(result, atespaces...)
		return len(atespaces), nil
	})
	if err != nil {
		return store.ListResponse[*ateapipb.Atespace]{}, err
	}
	return store.ListResponse[*ateapipb.Atespace]{Items: result, NextPageToken: nextToken}, nil
}

// DeleteAtespace deletes an empty atespace. Returns store.ErrNotFound if the
// atespace does not exist, or store.ErrFailedPrecondition if any Actor,
// ActorSnapshotTag, ActorTemplate still lives in it.
func (s *Persistence) DeleteAtespace(ctx context.Context, name string) (*ateapipb.Atespace, error) {
	dbKey := atespaceDBKey(name)

	// Read first, so a missing atespace returns NotFound (not a silent no-op) and
	// so we can return the deleted resource.
	currentVal, err := s.rdb.Get(ctx, dbKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting atespace key %q: %w", dbKey, err)
	}

	deleted := &ateapipb.Atespace{}
	if err := protojson.Unmarshal(currentVal, deleted); err != nil {
		return nil, fmt.Errorf("in protojson.Unmarshal: %w", err)
	}

	// Reject a non-empty atespace.
	actors, err := s.ListActors(ctx, name, store.ListOptions{PageSize: 1})
	if err != nil {
		return nil, fmt.Errorf("while checking atespace emptiness: %w", err)
	}
	if len(actors.Items) > 0 {
		return nil, store.ErrFailedPrecondition
	}
	hasTags, err := s.hasMatching(ctx, actorSnapshotTagScanPattern(name))
	if err != nil {
		return nil, fmt.Errorf("while checking ActorSnapshot tags: %w", err)
	}
	if hasTags {
		return nil, store.ErrFailedPrecondition
	}
	hasTemplates, err := s.hasMatching(ctx, actorTemplateScanPattern(name))
	if err != nil {
		return nil, fmt.Errorf("while checking ActorTemplates: %w", err)
	}
	if hasTemplates {
		return nil, store.ErrFailedPrecondition
	}
	if err := s.rdb.Del(ctx, dbKey).Err(); err != nil {
		return nil, fmt.Errorf("while deleting atespace key %q: %w", dbKey, err)
	}
	return deleted, nil
}

func (s *Persistence) hasMatching(ctx context.Context, pattern string) (bool, error) {
	masters, err := s.getSortedMasters(ctx)
	if err != nil {
		return false, err
	}
	for _, master := range masters {
		for cursor := uint64(0); ; {
			keys, next, err := master.Scan(ctx, cursor, pattern, 1).Result()
			if err != nil {
				return false, err
			}
			if len(keys) > 0 {
				return true, nil
			}
			if cursor = next; cursor == 0 {
				break
			}
		}
	}
	return false, nil
}

func actorTemplateDBKey(templateRef resources.ActorTemplateRef) string {
	return "actor-template:" + templateRef.Atespace + ":" + templateRef.Name
}

func actorTemplateScanPattern(atespace string) string {
	if atespace == globalAtespace {
		return "actor-template:*"
	}
	return "actor-template:" + atespace + ":*"
}

func (s *Persistence) CreateActorTemplate(ctx context.Context, template *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	// The atespace must already exist.  This is non-atomic must be fixed if we
	// keep the redis implementation.
	if exists, err := s.atespaceExists(ctx, template.Metadata.Atespace); err != nil {
		return nil, fmt.Errorf("while checking atespace: %w", err)
	} else if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", template.Metadata.Atespace)
	}

	dbKey := actorTemplateDBKey(resources.ActorTemplateRefFromActorTemplate(template))

	dbTemplate := proto.Clone(template).(*ateapipb.ActorTemplate)
	dbTemplate.Metadata = newCreateMetadata(template.GetMetadata().GetAtespace(), template.GetMetadata().GetName())

	dbBytes, err := protojson.Marshal(dbTemplate)
	if err != nil {
		return nil, fmt.Errorf("in protojson.Marshal: %w", err)
	}
	ok, err := s.rdb.SetNX(ctx, dbKey, dbBytes, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("while executing redis set: %w", err)
	}
	if !ok {
		return nil, store.ErrAlreadyExists
	}
	return dbTemplate, nil
}

func (s *Persistence) GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	dbKey := actorTemplateDBKey(templateRef)
	dbBytes, err := s.rdb.Get(ctx, dbKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting actor template key %q: %w", dbKey, err)
	}
	template := &ateapipb.ActorTemplate{}
	if err := protojson.Unmarshal(dbBytes, template); err != nil {
		return nil, fmt.Errorf("while unmarshaling actor template: %w", err)
	}
	if resources.ActorTemplateRefFromActorTemplate(template) != templateRef {
		return nil, fmt.Errorf("(impossible) mismatch between stored identity and key %q", dbKey)
	}
	return template, nil
}

// ActorTemplateExists reports whether the ActorTemplate exists. This is a
// plain EXISTS check and is NOT atomic with respect to a concurrent
// DeleteActorTemplate.
func (s *Persistence) ActorTemplateExists(ctx context.Context, templateRef resources.ActorTemplateRef) (bool, error) {
	n, err := s.rdb.Exists(ctx, actorTemplateDBKey(templateRef)).Result()
	if err != nil {
		return false, fmt.Errorf("while checking actor template existence: %w", err)
	}
	return n > 0, nil
}

// validateUpdateActorTemplateMutation reports whether a template mutation left
// the fields it does not own alone.
func validateUpdateActorTemplateMutation(storedTemplate, mutatedTemplate *ateapipb.ActorTemplate) error {
	if stored, mutated := storedTemplate.GetMetadata().GetAtespace(), mutatedTemplate.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTemplate.GetMetadata().GetName(), mutatedTemplate.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	return nil
}

func (s *Persistence) UpdateActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(*ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	dbKey := actorTemplateDBKey(templateRef)
	for range updateMaxAttempts {
		var dbTemplate *ateapipb.ActorTemplate
		var abortErr error

		err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
			currentVal, err := tx.Get(ctx, dbKey).Bytes()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					return store.ErrNotFound
				}
				return fmt.Errorf("while getting actor template: %w", err)
			}

			currentTemplate := &ateapipb.ActorTemplate{}
			if err := protojson.Unmarshal(currentVal, currentTemplate); err != nil {
				return fmt.Errorf("in protojson.Unmarshal: %w", err)
			}

			// A failed precondition is final: a retry would re-read the very state that failed it.
			if err := precondition.Check(currentTemplate.GetMetadata()); err != nil {
				abortErr = err
				return err
			}

			// Snapshot the stored state before handing the template to mutate.
			// mutate is free to edit anything it is given.
			templateBeforeMutation := proto.Clone(currentTemplate).(*ateapipb.ActorTemplate)
			if err := mutate(currentTemplate); err != nil {
				abortErr = err
				return err
			}
			if err := validateUpdateActorTemplateMutation(templateBeforeMutation, currentTemplate); err != nil {
				abortErr = err
				return err
			}
			// The stored metadata is authoritative; derive the next metadata
			// from it, discarding whatever mutate made of it.
			currentTemplate.Metadata = newUpdateMetadata(templateBeforeMutation.GetMetadata())

			newVal, err := protojson.Marshal(currentTemplate)
			if err != nil {
				return fmt.Errorf("in protojson.Marshal: %w", err)
			}

			if _, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, dbKey, newVal, 0)
				return nil
			}); err != nil {
				return err
			}
			dbTemplate = currentTemplate
			return nil
		}, dbKey)

		switch {
		case err == nil:
			return dbTemplate, nil
		case abortErr != nil:
			return nil, abortErr
		case errors.Is(err, store.ErrNotFound):
			return nil, store.ErrNotFound
		case errors.Is(err, redis.TxFailedErr):
			// A concurrent write landed between WATCH and EXEC, so mutate never
			// saw it. Re-read and run it against the newer state.
			continue
		default:
			return nil, fmt.Errorf("while executing update actor template transaction: %w", err)
		}
	}

	// Only the TxFailedErr branch continues the loop, so getting here means every
	// attempt lost the race.
	return nil, store.ErrVersionConflict
}

func (s *Persistence) ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error) {
	var result []*ateapipb.ActorTemplate
	nextToken, err := s.listPage(ctx, actorTemplateScanPattern(atespace), opts.PageSize, opts.PageToken, func(ctx context.Context, master *redis.Client, keys []string) (int, error) {
		templates, err := fetchProtos(ctx, master, keys, func() *ateapipb.ActorTemplate { return &ateapipb.ActorTemplate{} })
		if err != nil {
			return 0, err
		}
		result = append(result, templates...)
		return len(templates), nil
	})
	if err != nil {
		return store.ListResponse[*ateapipb.ActorTemplate]{}, err
	}
	return store.ListResponse[*ateapipb.ActorTemplate]{Items: result, NextPageToken: nextToken}, nil
}

// DeleteActorTemplate deletes an ActorTemplate.
// Returns store.ErrNotFound if the template does not exist.
func (s *Persistence) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	dbKey := actorTemplateDBKey(templateRef)

	// Read first, so a missing template returns NotFound (not a silent no-op)
	// and so we can return the deleted resource.
	currentVal, err := s.rdb.Get(ctx, dbKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting actor template key %q: %w", dbKey, err)
	}

	deleted := &ateapipb.ActorTemplate{}
	if err := protojson.Unmarshal(currentVal, deleted); err != nil {
		return nil, fmt.Errorf("in protojson.Unmarshal: %w", err)
	}

	if err := s.rdb.Del(ctx, dbKey).Err(); err != nil {
		return nil, fmt.Errorf("while deleting actor template key %q: %w", dbKey, err)
	}
	return deleted, nil
}

// workerDBKey keys a Worker by its name, which is the Kubernetes pod UID.
// Workers are global-scoped, so there is no atespace component.
func workerDBKey(name string) string {
	return "worker:" + name
}

func marshalWorkerEvent(eventType store.WorkerEventType, worker *ateapipb.Worker) (string, error) {
	workerJSON, err := protojson.Marshal(worker)
	if err != nil {
		return "", fmt.Errorf("in protojson.Marshal: %w", err)
	}
	msg, err := json.Marshal(workerPubSubMsg{Type: int(eventType), Worker: string(workerJSON)})
	if err != nil {
		return "", fmt.Errorf("in json.Marshal: %w", err)
	}
	return string(msg), nil
}

func unmarshalWorkerEvent(payload string) (store.WorkerEvent, error) {
	var msg workerPubSubMsg
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return store.WorkerEvent{}, fmt.Errorf("in json.Unmarshal: %w", err)
	}
	worker := &ateapipb.Worker{}
	if err := protojson.Unmarshal([]byte(msg.Worker), worker); err != nil {
		return store.WorkerEvent{}, fmt.Errorf("in protojson.Unmarshal: %w", err)
	}
	return store.WorkerEvent{Type: store.WorkerEventType(msg.Type), Worker: worker}, nil
}

const workerPubSubChannel = "worker-changes"

// subscribeConfirmTimeout bounds WatchWorkers' wait for the SUBSCRIBE
// confirmation.
const subscribeConfirmTimeout = 5 * time.Second

func (s *Persistence) publishWorkerEvent(ctx context.Context, eventType store.WorkerEventType, worker *ateapipb.Worker) {
	payload, err := marshalWorkerEvent(eventType, worker)
	if err != nil {
		slog.ErrorContext(ctx, "worker event marshal failed", slog.Any("err", err))
		return
	}
	if err := s.rdb.Publish(ctx, workerPubSubChannel, payload).Err(); err != nil {
		slog.ErrorContext(ctx, "worker event publish failed", slog.Any("err", err))
	}
}

func (s *Persistence) WatchWorkers(ctx context.Context) (*store.WorkerWatch, error) {
	// watchCtx scopes the subscription's lifetime: it is cancelled either by the
	// caller via WorkerWatch.Close or when the parent ctx is cancelled.
	watchCtx, cancel := context.WithCancel(ctx)
	pubsub := s.rdb.Subscribe(watchCtx, workerPubSubChannel)
	// Subscribe sends the SUBSCRIBE command asynchronously; wait for the
	// confirmation reply so that events published after WatchWorkers returns
	// are guaranteed to be delivered to this subscription.
	receiveCtx, receiveCancel := context.WithTimeout(watchCtx, subscribeConfirmTimeout)
	defer receiveCancel()
	if _, err := pubsub.Receive(receiveCtx); err != nil {
		pubsub.Close()
		cancel()
		return nil, fmt.Errorf("while confirming worker subscription: %w", err)
	}
	ch := make(chan store.WorkerEvent, 128)
	go func() {
		defer close(ch)
		defer pubsub.Close()
		msgCh := pubsub.Channel()
		for {
			select {
			case <-watchCtx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				event, err := unmarshalWorkerEvent(msg.Payload)
				if err != nil {
					slog.ErrorContext(ctx, "worker event unmarshal failed", slog.Any("err", err))
					continue
				}
				select {
				case ch <- event:
				case <-watchCtx.Done():
					return
				}
			}
		}
	}()
	return store.NewWorkerWatch(ch, cancel), nil
}

// DebugClearAll flushes all data from Redis.
func (s *Persistence) DebugClearAll(ctx context.Context) error {
	// Iterate through every Primary (Master) node in the cluster
	err := s.rdb.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		// Log which shard we are currently flushing (optional but helpful for debugging)
		shardAddr := master.Options().Addr
		fmt.Printf("Flushing shard: %s\n", shardAddr)

		// Execute the flush on this specific shard
		return master.FlushAllAsync(ctx).Err()
	})
	return err
}

func (s *Persistence) GetActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	dbKey := actorDBKey(actorRef)

	dbActorBytes, err := s.rdb.Get(ctx, dbKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting actor key %q: %w", dbKey, err)
	}

	actor := &ateapipb.Actor{}
	if err := protojson.Unmarshal(dbActorBytes, actor); err != nil {
		return nil, fmt.Errorf("while unmarshaling actor: %w", err)
	}

	if resources.ActorRefFromActor(actor) != actorRef {
		return nil, fmt.Errorf("(impossible) mismatch between stored name/atespace and key")
	}

	return actor, nil
}

func (s *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	// The atespace must already exist.  This is non-atomic must be fixed if we
	// keep the redis implementation.
	if exists, err := s.atespaceExists(ctx, actor.Metadata.Atespace); err != nil {
		return nil, fmt.Errorf("while checking atespace: %w", err)
	} else if !exists {
		return nil, status.Errorf(codes.FailedPrecondition, "Atespace %s not found", actor.Metadata.Atespace)
	}

	dbKey := actorDBKey(resources.ActorRefFromActor(actor))

	// Clone so we don't stomp the caller's copy, then attach fresh server-owned
	// metadata carrying the caller-specified identity.
	dbActor := proto.Clone(actor).(*ateapipb.Actor)
	dbActor.Metadata = newCreateMetadata(actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())

	dbActorBytes, err := protojson.Marshal(dbActor)
	if err != nil {
		return nil, fmt.Errorf("in protojson.Marshal: %w", err)
	}

	ok, err := s.rdb.SetNX(ctx, dbKey, dbActorBytes, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("while executing redis set: %w", err)
	}
	if !ok {
		return nil, store.ErrAlreadyExists
	}

	return dbActor, nil
}

func (s *Persistence) CreateActorSnapshot(ctx context.Context, snapshot *ateapipb.ActorSnapshot) (*ateapipb.ActorSnapshot, error) {
	dbKey := actorSnapshotDBKey(snapshot.GetMetadata().GetAtespace(), snapshot.GetMetadata().GetName())
	dbSnapshot := proto.Clone(snapshot).(*ateapipb.ActorSnapshot)
	dbSnapshot.Metadata = newCreateMetadata(snapshot.GetMetadata().GetAtespace(), snapshot.GetMetadata().GetName())
	b, err := protojson.Marshal(dbSnapshot)
	if err != nil {
		return nil, fmt.Errorf("while marshaling actor snapshot: %w", err)
	}
	ok, err := s.rdb.SetNX(ctx, dbKey, b, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("while creating actor snapshot: %w", err)
	}
	if !ok {
		return nil, store.ErrAlreadyExists
	}
	return dbSnapshot, nil
}

func (s *Persistence) GetActorSnapshot(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshot, error) {
	dbKey := actorSnapshotDBKey(atespace, name)
	b, err := s.rdb.Get(ctx, dbKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting actor snapshot key %q: %w", dbKey, err)
	}
	snapshot := &ateapipb.ActorSnapshot{}
	if err := protojson.Unmarshal(b, snapshot); err != nil {
		return nil, fmt.Errorf("while unmarshaling actor snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *Persistence) GetActorSnapshotTag(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshotTag, error) {
	b, err := s.rdb.Get(ctx, actorSnapshotTagDBKey(atespace, name)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	tag := &ateapipb.ActorSnapshotTag{}
	if err := protojson.Unmarshal(b, tag); err != nil {
		return nil, fmt.Errorf("while unmarshaling actor snapshot tag %s/%s: %w", atespace, name, err)
	}
	return tag, nil
}

func (s *Persistence) ListActorSnapshots(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorSnapshot], error) {
	var result []*ateapipb.ActorSnapshot
	nextToken, err := s.listPage(ctx, actorSnapshotScanPattern(atespace), opts.PageSize, opts.PageToken, func(ctx context.Context, master *redis.Client, keys []string) (int, error) {
		cmds, err := master.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, key := range keys {
				pipe.Get(ctx, key)
			}
			return nil
		})
		if err != nil && !errors.Is(err, redis.Nil) {
			return 0, fmt.Errorf("while fetching actor snapshots in shard %s: %w", master.Options().Addr, err)
		}
		collected := 0
		for _, cmd := range cmds {
			getCmd, ok := cmd.(*redis.StringCmd)
			if !ok || errors.Is(getCmd.Err(), redis.Nil) {
				continue
			}
			if getCmd.Err() != nil {
				return 0, fmt.Errorf("while getting actor snapshot: %w", getCmd.Err())
			}
			snapshot := &ateapipb.ActorSnapshot{}
			if err := protojson.Unmarshal([]byte(getCmd.Val()), snapshot); err != nil {
				return 0, fmt.Errorf("while unmarshaling actor snapshot: %w", err)
			}
			result = append(result, snapshot)
			collected++
		}
		return collected, nil
	})
	if err != nil {
		return store.ListResponse[*ateapipb.ActorSnapshot]{}, err
	}
	return store.ListResponse[*ateapipb.ActorSnapshot]{Items: result, NextPageToken: nextToken}, nil
}

func (s *Persistence) CreateActorSnapshotTag(ctx context.Context, atespace, name string, tag *ateapipb.ActorSnapshotTag) (*ateapipb.ActorSnapshotTag, error) {
	if _, err := s.GetActorSnapshot(ctx, atespace, name); err != nil {
		return nil, err
	}
	dbTag := proto.Clone(tag).(*ateapipb.ActorSnapshotTag)
	dbTag.Metadata = newCreateMetadata(tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName())
	dbTag.Snapshot = &ateapipb.ObjectRef{Atespace: atespace, Name: name}
	b, err := protojson.Marshal(dbTag)
	if err != nil {
		return nil, fmt.Errorf("while marshaling actor snapshot tag: %w", err)
	}
	tagKey := actorSnapshotTagDBKey(dbTag.GetMetadata().GetAtespace(), dbTag.GetMetadata().GetName())
	created, err := s.rdb.SetNX(ctx, tagKey, b, 0).Result()
	if err != nil {
		return nil, fmt.Errorf("while creating actor snapshot tag: %w", err)
	}
	if !created {
		existing, err := s.rdb.Get(ctx, tagKey).Bytes()
		if err != nil {
			return nil, fmt.Errorf("while getting actor snapshot tag: %w", err)
		}
		existingTag := &ateapipb.ActorSnapshotTag{}
		if err := protojson.Unmarshal(existing, existingTag); err != nil {
			return nil, fmt.Errorf("while unmarshaling actor snapshot tag: %w", err)
		}
		if existingTag.GetSnapshot().GetAtespace() != atespace || existingTag.GetSnapshot().GetName() != name || existingTag.GetScope() != tag.GetScope() {
			return nil, store.ErrAlreadyExists
		}
		return existingTag, nil
	}
	return dbTag, nil
}

func validateUpdateActorSnapshotTagMutation(storedTag, mutatedTag *ateapipb.ActorSnapshotTag) error {
	if stored, mutated := storedTag.GetMetadata().GetAtespace(), mutatedTag.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetMetadata().GetName(), mutatedTag.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetSnapshot().GetAtespace(), mutatedTag.GetSnapshot().GetAtespace(); stored != mutated {
		return fmt.Errorf("snapshot.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetSnapshot().GetName(), mutatedTag.GetSnapshot().GetName(); stored != mutated {
		return fmt.Errorf("snapshot.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	return nil
}

// updateActorSnapshotTagMaxAttempts bounds how many times UpdateActorSnapshotTag
// re-runs its read-modify-write after a concurrent writer invalidates the
// transaction.
const updateActorSnapshotTagMaxAttempts = 5

func (s *Persistence) UpdateActorSnapshotTag(ctx context.Context, atespace, name string, precondition store.Precondition, mutate func(*ateapipb.ActorSnapshotTag) error) (*ateapipb.ActorSnapshotTag, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	tagKey := actorSnapshotTagDBKey(atespace, name)
	for range updateActorSnapshotTagMaxAttempts {
		var dbTag *ateapipb.ActorSnapshotTag
		var abortErr error

		err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
			currentVal, err := tx.Get(ctx, tagKey).Bytes()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					return store.ErrNotFound
				}
				return fmt.Errorf("while getting actor snapshot tag %s/%s: %w", atespace, name, err)
			}

			currentTag := &ateapipb.ActorSnapshotTag{}
			if err := protojson.Unmarshal(currentVal, currentTag); err != nil {
				return fmt.Errorf("while unmarshaling actor snapshot tag %s/%s: %w", atespace, name, err)
			}

			// A failed precondition is final: a retry would re-read the very state that failed it.
			if err := precondition.Check(currentTag.GetMetadata()); err != nil {
				abortErr = err
				return err
			}

			// Snapshot the stored state before handing the tag to mutate.
			// mutate is free to edit anything it is given.
			tagBeforeMutation := proto.Clone(currentTag).(*ateapipb.ActorSnapshotTag)
			if err := mutate(currentTag); err != nil {
				abortErr = err
				return err
			}
			if err := validateUpdateActorSnapshotTagMutation(tagBeforeMutation, currentTag); err != nil {
				abortErr = fmt.Errorf("%w: %w", store.ErrImmutableField, err)
				return abortErr
			}
			// The stored metadata is authoritative; derive the next metadata
			// from it, discarding whatever mutate made of it.
			currentTag.Metadata = newUpdateMetadata(tagBeforeMutation.GetMetadata())

			newVal, err := protojson.Marshal(currentTag)
			if err != nil {
				return fmt.Errorf("while marshaling actor snapshot tag: %w", err)
			}

			if _, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, tagKey, newVal, 0)
				return nil
			}); err != nil {
				return err
			}
			dbTag = currentTag
			return nil
		}, tagKey)

		switch {
		case err == nil:
			return dbTag, nil
		case abortErr != nil:
			return nil, abortErr
		case errors.Is(err, store.ErrNotFound):
			return nil, store.ErrNotFound
		case errors.Is(err, redis.TxFailedErr):
			// A concurrent write landed before we could commit.
			// Retry.
			continue
		default:
			return nil, fmt.Errorf("while executing update actor snapshot tag transaction: %w", err)
		}
	}

	// Only the TxFailedErr branch continues the loop, so getting here means every
	// attempt lost the race.
	return nil, store.ErrVersionConflict
}

func (s *Persistence) DeleteActorSnapshotTag(ctx context.Context, atespace, name string) (*ateapipb.ActorSnapshotTag, error) {
	tag, err := s.GetActorSnapshotTag(ctx, atespace, name)
	if err != nil {
		return nil, err
	}
	tagKey := actorSnapshotTagDBKey(atespace, name)
	if n, err := s.rdb.Del(ctx, tagKey).Result(); err != nil {
		return nil, fmt.Errorf("while deleting actor snapshot tag: %w", err)
	} else if n == 0 {
		return nil, store.ErrNotFound
	}
	return tag, nil
}

func (s *Persistence) CreateWorker(ctx context.Context, worker *ateapipb.Worker) error {
	dbKey := workerDBKey(worker.GetMetadata().GetName())

	// Clone so we don't stomp the caller's copy, then attach fresh server-owned
	// metadata carrying the caller-specified name. Workers are global-scoped,
	// so the atespace is always empty.
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	dbWorker.Metadata = newCreateMetadata("", worker.GetMetadata().GetName())

	dbWorkerBytes, err := protojson.Marshal(dbWorker)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	ok, err := s.rdb.SetNX(ctx, dbKey, dbWorkerBytes, 0).Result()
	if err != nil {
		return fmt.Errorf("while executing redis set: %w", err)
	}
	if !ok {
		return store.ErrAlreadyExists
	}

	s.publishWorkerEvent(ctx, store.WorkerEventCreated, dbWorker)
	return nil
}

func (s *Persistence) GetWorker(ctx context.Context, name string) (*ateapipb.Worker, error) {
	dbKey := workerDBKey(name)

	dbWorkerBytes, err := s.rdb.Get(ctx, dbKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting worker key %q: %w", dbKey, err)
	}

	worker := &ateapipb.Worker{}
	if err := protojson.Unmarshal(dbWorkerBytes, worker); err != nil {
		return nil, fmt.Errorf("in protojson.Unmarshal: %w", err)
	}

	if worker.GetMetadata().GetName() != name {
		return nil, fmt.Errorf("(impossible) mismatch between stored name and key")
	}

	return worker, nil
}

func (s *Persistence) UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error {
	dbKey := workerDBKey(worker.GetMetadata().GetName())

	// Clone because we will update the metadata, and we don't want to stomp
	// the caller's copy.
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)

	err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
		currentVal, err := tx.Get(ctx, dbKey).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return store.ErrNotFound
			}
			return fmt.Errorf("while getting worker: %w", err)
		}

		currentWorker := &ateapipb.Worker{}
		if err := protojson.Unmarshal(currentVal, currentWorker); err != nil {
			return fmt.Errorf("in protojson.Unmarshal: %w", err)
		}

		if currentWorker.GetMetadata().GetVersion() != expectedVersion {
			return store.ErrVersionConflict
		}
		dbWorker.Metadata = newUpdateMetadata(currentWorker.GetMetadata())
		if currentWorker.GetWorkerNamespace() != dbWorker.GetWorkerNamespace() {
			return fmt.Errorf("worker_namespace is immutable")
		}
		if currentWorker.GetWorkerPool() != dbWorker.GetWorkerPool() {
			return fmt.Errorf("worker_pool is immutable")
		}
		if currentWorker.GetWorkerPod() != dbWorker.GetWorkerPod() {
			return fmt.Errorf("worker_pod is immutable")
		}
		if currentWorker.GetIp() != dbWorker.GetIp() {
			return fmt.Errorf("ip is immutable")
		}
		newVal, err := protojson.Marshal(dbWorker)
		if err != nil {
			return fmt.Errorf("in protojson.Marshal: %w", err)
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, dbKey, newVal, 0)
			return nil
		})
		return err
	}, dbKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.ErrNotFound
		}
		if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, redis.TxFailedErr) {
			return store.ErrVersionConflict
		}
		return fmt.Errorf("while executing update worker transaction: %w", err)
	}

	s.publishWorkerEvent(ctx, store.WorkerEventUpdated, dbWorker)
	return nil
}

func (s *Persistence) DeleteWorker(ctx context.Context, name string) error {
	dbKey := workerDBKey(name)
	err := s.rdb.Del(ctx, dbKey).Err()
	if err != nil {
		return fmt.Errorf("while deleting worker key %q: %w", dbKey, err)
	}
	s.publishWorkerEvent(ctx, store.WorkerEventDeleted, &ateapipb.Worker{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
	})
	return nil
}

func (s *Persistence) DeleteActor(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.Actor, error) {
	dbKey := actorDBKey(actorRef)
	var deleted *ateapipb.Actor
	err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
		currentVal, err := tx.Get(ctx, dbKey).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return store.ErrNotFound
			}
			return fmt.Errorf("while getting actor: %w", err)
		}

		currentActor := &ateapipb.Actor{}
		if err := protojson.Unmarshal(currentVal, currentActor); err != nil {
			return fmt.Errorf("in protojson.Unmarshal: %w", err)
		}

		if currentActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_DELETING {
			return store.ErrFailedPrecondition
		}

		if _, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, dbKey)
			return nil
		}); err != nil {
			return err
		}
		deleted = currentActor
		return nil
	}, dbKey)

	if err != nil {
		if errors.Is(err, redis.TxFailedErr) {
			return nil, store.ErrVersionConflict
		}
		return nil, err
	}

	return deleted, nil
}

// validateUpdateActorMutation reports whether an actor mutation left the fields it does
// not own alone.
func validateUpdateActorMutation(storedActor, mutatedActor *ateapipb.Actor) error {
	if stored, mutated := storedActor.GetMetadata().GetAtespace(), mutatedActor.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetMetadata().GetName(), mutatedActor.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetActorTemplateNamespace(), mutatedActor.GetActorTemplateNamespace(); stored != mutated {
		return fmt.Errorf("actor_template_namespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetActorTemplateName(), mutatedActor.GetActorTemplateName(); stored != mutated {
		return fmt.Errorf("actor_template_name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedActor.GetActorTemplate(), mutatedActor.GetActorTemplate(); !proto.Equal(stored, mutated) {
		return fmt.Errorf("actor_template is immutable: mutation changed it from %v to %v", stored, mutated)
	}
	if stored, mutated := storedActor.GetSourceSnapshotTag(), mutatedActor.GetSourceSnapshotTag(); !proto.Equal(stored, mutated) {
		return fmt.Errorf("source_snapshot_tag is immutable: mutation changed it from %v to %v", stored, mutated)
	}
	return nil
}

// updateMaxAttempts bounds how many times UpdateActor or UpdateActorTemplate re-runs its
// read-modify-write after a concurrent writer invalidates the transaction.
const updateMaxAttempts = 5

func (s *Persistence) UpdateActor(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.Actor) error) (*ateapipb.Actor, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	dbKey := actorDBKey(actorRef)
	for range updateMaxAttempts {
		var dbActor *ateapipb.Actor
		var abortErr error

		err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
			currentVal, err := tx.Get(ctx, dbKey).Bytes()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					return store.ErrNotFound
				}
				return fmt.Errorf("while getting actor: %w", err)
			}

			currentActor := &ateapipb.Actor{}
			if err := protojson.Unmarshal(currentVal, currentActor); err != nil {
				return fmt.Errorf("in protojson.Unmarshal: %w", err)
			}

			// A failed precondition is final: a retry would re-read the very state that failed it.
			if err := precondition.Check(currentActor.GetMetadata()); err != nil {
				abortErr = err
				return err
			}

			// Snapshot the stored state before handing the actor to mutate.
			// mutate is free to edit anything it is given.
			actorBeforeMutation := proto.Clone(currentActor).(*ateapipb.Actor)
			if err := mutate(currentActor); err != nil {
				abortErr = err
				return err
			}
			if err := validateUpdateActorMutation(actorBeforeMutation, currentActor); err != nil {
				abortErr = fmt.Errorf("%w: %w", store.ErrImmutableField, err)
				return abortErr
			}
			// The stored metadata is authoritative; derive the next metadata
			// from it, discarding whatever mutate made of it.
			currentActor.Metadata = newUpdateMetadata(actorBeforeMutation.GetMetadata())

			newVal, err := protojson.Marshal(currentActor)
			if err != nil {
				return fmt.Errorf("in protojson.Marshal: %w", err)
			}

			if _, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, dbKey, newVal, 0)
				return nil
			}); err != nil {
				return err
			}
			dbActor = currentActor
			return nil
		}, dbKey)

		switch {
		case err == nil:
			return dbActor, nil
		case abortErr != nil:
			return nil, abortErr
		case errors.Is(err, store.ErrNotFound):
			return nil, store.ErrNotFound
		case errors.Is(err, redis.TxFailedErr):
			// A concurrent write landed between WATCH and EXEC, so mutate never
			// saw it. Re-read and run it against the newer state.
			continue
		default:
			return nil, fmt.Errorf("while executing update actor transaction: %w", err)
		}
	}

	// Only the TxFailedErr branch continues the loop, so getting here means every
	// attempt lost the race.
	return nil, store.ErrVersionConflict
}

func (s *Persistence) ListWorkers(ctx context.Context, opts store.ListOptions) (store.ListResponse[*ateapipb.Worker], error) {
	var result []*ateapipb.Worker
	nextToken, err := s.listPage(ctx, "worker:*", opts.PageSize, opts.PageToken, func(ctx context.Context, master *redis.Client, keys []string) (int, error) {
		workers, err := fetchProtos(ctx, master, keys, func() *ateapipb.Worker { return &ateapipb.Worker{} })
		if err != nil {
			return 0, err
		}
		result = append(result, workers...)
		return len(workers), nil
	})
	if err != nil {
		return store.ListResponse[*ateapipb.Worker]{}, err
	}
	return store.ListResponse[*ateapipb.Worker]{Items: result, NextPageToken: nextToken}, nil
}

type pageToken struct {
	ShardHash string `json:"shard_hash"`
	Cursor    uint64 `json:"cursor"`
}

func encodePageToken(token pageToken) string {
	b, _ := json.Marshal(token)
	return base64.StdEncoding.EncodeToString(b)
}

func decodePageToken(tokenStr string) (pageToken, error) {
	var token pageToken
	if tokenStr == "" {
		return token, nil
	}
	b, err := base64.StdEncoding.DecodeString(tokenStr)
	if err != nil {
		return token, err
	}
	err = json.Unmarshal(b, &token)
	return token, err
}

func hashShardAddr(addr string) string {
	h := sha256.Sum256([]byte(addr))
	return hex.EncodeToString(h[:])
}

// ListActors lists actors, scoped to the given atespace. An empty atespace lists
// across all atespaces (SCAN actor:*); a non-empty atespace restricts the scan to
// that atespace (SCAN actor:<atespace>:*).
func (s *Persistence) ListActors(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.Actor], error) {
	var result []*ateapipb.Actor
	nextToken, err := s.listPage(ctx, actorScanPattern(atespace), opts.PageSize, opts.PageToken, func(ctx context.Context, master *redis.Client, keys []string) (int, error) {
		actors, err := fetchProtos(ctx, master, keys, func() *ateapipb.Actor { return &ateapipb.Actor{} })
		if err != nil {
			return 0, err
		}
		result = append(result, actors...)
		return len(actors), nil
	})
	if err != nil {
		return store.ListResponse[*ateapipb.Actor]{}, err
	}
	return store.ListResponse[*ateapipb.Actor]{Items: result, NextPageToken: nextToken}, nil
}

// listPage SCANs pattern across the redis masters from the page token, feeding key batches to collect and returns the next-page token.
func (s *Persistence) listPage(ctx context.Context, pattern string, pageSize int32, pageTokenStr string, collect func(ctx context.Context, master *redis.Client, keys []string) (int, error)) (string, error) {
	normalized, err := store.NormalizeListOptions(store.ListOptions{PageSize: pageSize})
	if err != nil {
		return "", err
	}
	pageSize = normalized.PageSize
	token, err := decodePageToken(pageTokenStr)
	if err != nil {
		return "", fmt.Errorf("%w: %v", store.ErrInvalidPageToken, err)
	}

	masters, err := s.getSortedMasters(ctx)
	if err != nil {
		return "", err
	}

	startIndex, err := findStartingShard(masters, token.ShardHash)
	if err != nil {
		return "", err
	}

	i := startIndex
	cursor := token.Cursor
	collected := 0

	for i < len(masters) && collected < int(pageSize) {
		master := masters[i]
		remaining := int(pageSize) - collected

		var keys []string
		keys, cursor, err = master.Scan(ctx, cursor, pattern, int64(remaining)).Result()
		if err != nil {
			return "", fmt.Errorf("while scanning shard %s: %w", master.Options().Addr, err)
		}

		if len(keys) > 0 {
			n, err := collect(ctx, master, keys)
			if err != nil {
				return "", err
			}
			collected += n
		}

		if cursor == 0 {
			i++
		}
	}

	var nextToken string
	if i < len(masters) {
		nextToken = encodePageToken(pageToken{
			ShardHash: hashShardAddr(masters[i].Options().Addr),
			Cursor:    cursor,
		})
	}

	return nextToken, nil
}

func (s *Persistence) getSortedMasters(ctx context.Context) ([]*redis.Client, error) {
	var mu sync.Mutex
	var masters []*redis.Client
	// ForEachMaster invokes the callback concurrently, one goroutine per master.
	err := s.rdb.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		mu.Lock()
		defer mu.Unlock()
		masters = append(masters, master)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while listing redis masters: %w", err)
	}

	sort.Slice(masters, func(i, j int) bool {
		return masters[i].Options().Addr < masters[j].Options().Addr
	})
	return masters, nil
}

func findStartingShard(masters []*redis.Client, shardHash string) (int, error) {
	if shardHash == "" {
		return 0, nil
	}
	for i, m := range masters {
		if hashShardAddr(m.Options().Addr) == shardHash {
			return i, nil
		}
	}
	return 0, fmt.Errorf("topology changed: shard with hash %s not found (aborted)", shardHash)
}

// fetchProtos fetches keys into newMsg-created messages.
func fetchProtos[M proto.Message](ctx context.Context, master *redis.Client, keys []string, newMsg func() M) ([]M, error) {
	cmds, err := master.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, key := range keys {
			pipe.Get(ctx, key)
		}
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("while fetching keys in shard %s: %w", master.Options().Addr, err)
	}

	var out []M
	for _, cmd := range cmds {
		getCmd, ok := cmd.(*redis.StringCmd)
		if !ok {
			continue
		}
		if getCmd.Err() != nil {
			if errors.Is(getCmd.Err(), redis.Nil) {
				continue
			}
			return nil, fmt.Errorf("while getting key: %w", getCmd.Err())
		}

		msg := newMsg()
		if err := protojson.Unmarshal([]byte(getCmd.Val()), msg); err != nil {
			return nil, fmt.Errorf("in protojson.Unmarshal: %w", err)
		}
		out = append(out, msg)
	}
	return out, nil
}

// lockRenewScript extends key's TTL only if it is still owned by ARGV[1],
// atomically. Returns 1 if renewed, 0 if the lock was lost (expired and
// possibly reacquired by someone else, or otherwise deleted).
var lockRenewScript = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("pexpire", KEYS[1], ARGV[2])
	else
		return 0
	end
`)

// lockReleaseScript deletes key only if it is still owned by ARGV[1],
// atomically, so a caller can never release a lock it no longer holds.
var lockReleaseScript = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`)

// defaultLockTTL is how long a lock may go unrenewed before another client
// can reclaim it.
const defaultLockTTL = 30 * time.Second

func (s *Persistence) AcquireLock(ctx context.Context, key string) (*store.Lock, error) {
	ttl := s.lockTTL
	value := uuid.New().String()

	ok, err := s.rdb.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("while acquiring lock for %q: %w", key, err)
	}
	if !ok {
		return nil, store.ErrLockConflict
	}

	// leaseCtx is cancelled either by Close, or by the renewal loop below if it
	// ever stops without Close having been called (i.e. the lease was lost).
	leaseCtx, cancel := context.WithCancel(ctx)
	renewalDone := make(chan struct{})

	go func() {
		defer close(renewalDone)
		defer cancel()
		s.renewLockLoop(leaseCtx, key, value, ttl)
	}()

	closeFn := func() {
		cancel()
		<-renewalDone // wait for the renewal loop to stop before releasing.

		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		if err := s.releaseLock(releaseCtx, key, value); err != nil {
			slog.WarnContext(releaseCtx, "failed to release lock, relying on TTL to reclaim it", "key", key, "error", err)
		}
	}

	return store.NewLock(leaseCtx, closeFn), nil
}

const (
	// renewIntervalDivisor and renewRetryPeriodDivisor set the renewal loop's
	// steady-state cadence and in-failure retry spacing as fractions of ttl:
	// interval = ttl/renewIntervalDivisor, retryPeriod = ttl/renewRetryPeriodDivisor.
	renewIntervalDivisor    = 3
	renewRetryPeriodDivisor = 10
	// renewDeadlineFraction bounds how much of the lock's TTL the renewal loop
	// may spend retrying after its last successful renewal before conceding the
	// lease as lost.
	renewDeadlineFraction = 2.0 / 3.0
)

func (s *Persistence) renewLockLoop(ctx context.Context, key, value string, ttl time.Duration) {
	interval := ttl / renewIntervalDivisor
	renewDeadline := time.Duration(float64(ttl) * renewDeadlineFraction)

	lastRenewed := time.Now()
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			renewCtx, cancel := context.WithDeadline(ctx, lastRenewed.Add(renewDeadline))
			renewed := s.tryRenewLock(renewCtx, key, value, ttl)
			cancel()
			if !renewed {
				return
			}
			lastRenewed = time.Now()
			timer.Reset(interval)
		}
	}
}

func (s *Persistence) tryRenewLock(ctx context.Context, key, value string, ttl time.Duration) bool {
	retryPeriod := ttl / renewRetryPeriodDivisor

	retry := time.NewTimer(0) // first attempt fires immediately.
	defer retry.Stop()

	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				slog.WarnContext(ctx, "failed to renew lock and its renew deadline has elapsed, treating lease as lost", "key", key)
			}
			return false

		case <-retry.C:
			renewed, err := s.renewLock(ctx, key, value, ttl)

			if ctx.Err() != nil {
				return false // deadline elapsed or Close raced with this attempt.
			}

			switch {
			case err == nil && renewed:
				return true

			case err == nil && !renewed:
				slog.WarnContext(ctx, "lock renewal found lease no longer owned", "key", key)
				return false

			default:
				slog.WarnContext(ctx, "failed to renew lock, retrying before its renew deadline elapses", "key", key, "error", err)
				retry.Reset(retryPeriod)
			}
		}
	}
}

func (s *Persistence) renewLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	res, err := lockRenewScript.Run(ctx, s.rdb, []string{key}, value, ttl.Milliseconds()).Result()
	if err != nil {
		return false, fmt.Errorf("while renewing lock for %q: %w", key, err)
	}
	renewed, _ := res.(int64)
	return renewed == 1, nil
}

func (s *Persistence) releaseLock(ctx context.Context, key, value string) error {
	_, err := lockReleaseScript.Run(ctx, s.rdb, []string{key}, value).Result()
	if err != nil {
		return fmt.Errorf("while releasing lock for %q with value %q: %w", key, value, err)
	}
	return nil
}

func newCreateMetadata(atespace, name string) *ateapipb.ResourceMetadata {
	now := timestamppb.Now()
	return &ateapipb.ResourceMetadata{
		Atespace:   atespace,
		Name:       name,
		Uid:        uuid.NewString(),
		Version:    1,
		CreateTime: now,
		UpdateTime: now,
	}
}

func newUpdateMetadata(current *ateapipb.ResourceMetadata) *ateapipb.ResourceMetadata {
	next := proto.Clone(current).(*ateapipb.ResourceMetadata)
	next.Version = current.GetVersion() + 1
	next.UpdateTime = timestamppb.Now()
	return next
}
