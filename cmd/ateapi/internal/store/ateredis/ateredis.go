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
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	rdb redisClient
}

var _ store.Interface = (*Persistence)(nil)

// NewPersistence creates a new Persistence.
func NewPersistence(redisClient *redis.ClusterClient) *Persistence {
	return &Persistence{
		rdb: redisClient,
	}
}

func actorDBKey(atespace, name string) string {
	return "actor:" + atespace + ":" + name
}

// actorScanPattern returns the SCAN match pattern for listing actors. An empty
// atespace lists across all atespaces (actor:*); a non-empty atespace scopes the
// scan to that atespace (actor:<atespace>:*).
func actorScanPattern(atespace string) string {
	if atespace == "" {
		return "actor:*"
	}
	return "actor:" + atespace + ":*"
}

func atespaceDBKey(name string) string {
	return "atespace:" + name
}

func (s *Persistence) CreateAtespace(ctx context.Context, atespace *ateapipb.Atespace) (*ateapipb.Atespace, error) {
	dbKey := atespaceDBKey(atespace.GetMetadata().GetName())

	dbAtespace := proto.Clone(atespace).(*ateapipb.Atespace)
	// Atespace is global-scoped: identity is the name alone (atespace stays empty).
	dbAtespace.Metadata = newCreateMetadata("", atespace.GetMetadata().GetName())

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

// AtespaceExists reports whether the atespace object exists. This is a plain
// EXISTS check and is NOT atomic with respect to a concurrent DeleteAtespace.
func (s *Persistence) AtespaceExists(ctx context.Context, name string) (bool, error) {
	n, err := s.rdb.Exists(ctx, atespaceDBKey(name)).Result()
	if err != nil {
		return false, fmt.Errorf("while checking atespace existence: %w", err)
	}
	return n > 0, nil
}

func (s *Persistence) ListAtespaces(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.Atespace, string, error) {
	var result []*ateapipb.Atespace
	nextToken, err := s.listPage(ctx, "atespace:*", pageSize, pageTokenStr, func(ctx context.Context, master *redis.Client, keys []string) (int, error) {
		atespaces, err := fetchProtos(ctx, master, keys, func() *ateapipb.Atespace { return &ateapipb.Atespace{} })
		if err != nil {
			return 0, err
		}
		result = append(result, atespaces...)
		return len(atespaces), nil
	})
	if err != nil {
		return nil, "", err
	}
	return result, nextToken, nil
}

// DeleteAtespace deletes an empty atespace. Returns store.ErrNotFound if the
// atespace does not exist, or store.ErrFailedPrecondition if any actor still
// lives in it.
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
	actors, _, err := s.ListActors(ctx, name, 1, "")
	if err != nil {
		return nil, fmt.Errorf("while checking atespace emptiness: %w", err)
	}
	if len(actors) > 0 {
		return nil, store.ErrFailedPrecondition
	}

	if err := s.rdb.Del(ctx, dbKey).Err(); err != nil {
		return nil, fmt.Errorf("while deleting atespace key %q: %w", dbKey, err)
	}
	return deleted, nil
}

func workerDBKey(namespace, poolName, podName string) string {
	return "worker:" + namespace + ":" + poolName + ":" + podName
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

func (s *Persistence) GetActor(ctx context.Context, atespace, name string) (*ateapipb.Actor, error) {
	dbKey := actorDBKey(atespace, name)

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

	if actor.GetMetadata().GetName() != name || actor.GetMetadata().GetAtespace() != atespace {
		return nil, fmt.Errorf("(impossible) mismatch between stored name/atespace and key")
	}

	return actor, nil
}

func (s *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	dbKey := actorDBKey(actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())

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

func (s *Persistence) CreateWorker(ctx context.Context, worker *ateapipb.Worker) error {
	dbKey := workerDBKey(worker.GetWorkerNamespace(), worker.GetWorkerPool(), worker.GetWorkerPod())

	// Clone because we will update the version field, and we don't want to
	// stomp the caller's copy.
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)
	dbWorker.Version = 1

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

func (s *Persistence) GetWorker(ctx context.Context, namespace, pool, pod string) (*ateapipb.Worker, error) {
	dbKey := workerDBKey(namespace, pool, pod)

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

	if worker.GetWorkerNamespace() != namespace || worker.GetWorkerPool() != pool || worker.GetWorkerPod() != pod {
		return nil, fmt.Errorf("(impossible) mismatch between stored namespace/pool/pod and key")
	}

	return worker, nil
}

func (s *Persistence) UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error {
	dbKey := workerDBKey(worker.GetWorkerNamespace(), worker.GetWorkerPool(), worker.GetWorkerPod())

	// Clone because we will update the version field, and we don't want to
	// stomp the caller's copy.
	dbWorker := proto.Clone(worker).(*ateapipb.Worker)

	err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
		currentVal, err := tx.Get(ctx, dbKey).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return fmt.Errorf("worker does not exist")
			}
			return fmt.Errorf("while getting worker: %w", err)
		}

		currentWorker := &ateapipb.Worker{}
		if err := protojson.Unmarshal(currentVal, currentWorker); err != nil {
			return fmt.Errorf("in protojson.Unmarshal: %w", err)
		}

		if currentWorker.GetVersion() != expectedVersion {
			return store.ErrPersistenceRetry
		}
		dbWorker.Version = currentWorker.GetVersion() + 1
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
		if errors.Is(err, store.ErrPersistenceRetry) || errors.Is(err, redis.TxFailedErr) {
			return store.ErrPersistenceRetry
		}
		return fmt.Errorf("while executing update worker transaction: %w", err)
	}

	s.publishWorkerEvent(ctx, store.WorkerEventUpdated, dbWorker)
	return nil
}

func (s *Persistence) DeleteWorker(ctx context.Context, namespace, pool, pod string) error {
	dbKey := workerDBKey(namespace, pool, pod)
	err := s.rdb.Del(ctx, dbKey).Err()
	if err != nil {
		return fmt.Errorf("while deleting worker key %q: %w", dbKey, err)
	}
	s.publishWorkerEvent(ctx, store.WorkerEventDeleted, &ateapipb.Worker{
		WorkerNamespace: namespace,
		WorkerPod:       pod,
	})
	return nil
}

func (s *Persistence) DeleteActor(ctx context.Context, atespace, name string) (*ateapipb.Actor, error) {
	dbKey := actorDBKey(atespace, name)
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

		if currentActor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED &&
			currentActor.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
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
			return nil, store.ErrPersistenceRetry
		}
		return nil, err
	}

	return deleted, nil
}

func (s *Persistence) UpdateActor(ctx context.Context, actor *ateapipb.Actor, expectedVersion int64) (*ateapipb.Actor, error) {
	dbKey := actorDBKey(actor.GetMetadata().GetAtespace(), actor.GetMetadata().GetName())

	// Clone because we will update the version field, and we don't want to
	// stomp the caller's copy.
	dbActor := proto.Clone(actor).(*ateapipb.Actor)

	err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
		currentVal, err := tx.Get(ctx, dbKey).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return fmt.Errorf("actor does not exist")
			}
			return fmt.Errorf("while getting actor: %w", err)
		}

		currentActor := &ateapipb.Actor{}
		if err := protojson.Unmarshal(currentVal, currentActor); err != nil {
			return fmt.Errorf("in protojson.Unmarshal: %w", err)
		}

		if currentActor.GetMetadata().GetVersion() != expectedVersion {
			return store.ErrPersistenceRetry
		}
		if currentActor.GetMetadata().GetName() != dbActor.GetMetadata().GetName() {
			return fmt.Errorf("name is immutable")
		}
		if currentActor.GetMetadata().GetAtespace() != dbActor.GetMetadata().GetAtespace() {
			return fmt.Errorf("atespace is immutable")
		}
		if currentActor.GetActorTemplateNamespace() != dbActor.GetActorTemplateNamespace() {
			return fmt.Errorf("actor_template_namespace is immutable")
		}
		if currentActor.GetActorTemplateName() != dbActor.GetActorTemplateName() {
			return fmt.Errorf("actor_template_name is immutable")
		}
		// The stored metadata is authoritative; derive the next metadata from it.
		dbActor.Metadata = newUpdateMetadata(currentActor.GetMetadata())

		newVal, err := protojson.Marshal(dbActor)
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
		if errors.Is(err, store.ErrPersistenceRetry) || errors.Is(err, redis.TxFailedErr) {
			return nil, store.ErrPersistenceRetry
		}
		return nil, fmt.Errorf("while executing update actor transaction: %w", err)
	}

	// dbActor is the persisted state (advanced version and update_time). The
	// caller's input is left unmodified.
	return dbActor, nil
}

func (s *Persistence) ListWorkers(ctx context.Context, pageSize int32, pageTokenStr string) ([]*ateapipb.Worker, string, error) {
	var result []*ateapipb.Worker
	nextToken, err := s.listPage(ctx, "worker:*", pageSize, pageTokenStr, func(ctx context.Context, master *redis.Client, keys []string) (int, error) {
		workers, err := fetchProtos(ctx, master, keys, func() *ateapipb.Worker { return &ateapipb.Worker{} })
		if err != nil {
			return 0, err
		}
		result = append(result, workers...)
		return len(workers), nil
	})
	if err != nil {
		return nil, "", err
	}
	return result, nextToken, nil
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
func (s *Persistence) ListActors(ctx context.Context, atespace string, pageSize int32, pageTokenStr string) ([]*ateapipb.Actor, string, error) {
	var result []*ateapipb.Actor
	nextToken, err := s.listPage(ctx, actorScanPattern(atespace), pageSize, pageTokenStr, func(ctx context.Context, master *redis.Client, keys []string) (int, error) {
		actors, err := fetchProtos(ctx, master, keys, func() *ateapipb.Actor { return &ateapipb.Actor{} })
		if err != nil {
			return 0, err
		}
		result = append(result, actors...)
		return len(actors), nil
	})
	if err != nil {
		return nil, "", err
	}
	return result, nextToken, nil
}

// listPage SCANs pattern across the redis masters from the page token, feeding key batches to collect and returns the next-page token.
func (s *Persistence) listPage(ctx context.Context, pattern string, pageSize int32, pageTokenStr string, collect func(ctx context.Context, master *redis.Client, keys []string) (int, error)) (string, error) {
	token, err := decodePageToken(pageTokenStr)
	if err != nil {
		return "", fmt.Errorf("invalid page token: %w", err)
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

func (s *Persistence) AcquireLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	ok, err := s.rdb.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("while acquiring lock for %q: %w", key, err)
	}
	return ok, nil
}

func (s *Persistence) ReleaseLock(ctx context.Context, key string, value string) error {
	var luaRelease = redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)

	_, err := luaRelease.Run(ctx, s.rdb, []string{key}, value).Result()
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
