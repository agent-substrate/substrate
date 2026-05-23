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
// `actor:<actor-id>`.  They are
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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type redisClient interface {
	redis.Cmdable
	ForEachMaster(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
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

func actorDBKey(id string) string {
	return "actor:" + id
}

func workerDBKey(namespace, poolName, podName string) string {
	return "worker:" + namespace + ":" + poolName + ":" + podName
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

func (s *Persistence) GetActor(ctx context.Context, id string) (*ateapipb.Actor, error) {
	dbKey := actorDBKey(id)

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

	if actor.GetActorId() != id {
		return nil, fmt.Errorf("(impossible) mismatch between stored id and key id")
	}

	return actor, nil
}

func (s *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) error {
	dbKey := actorDBKey(actor.GetActorId())

	// Clone because we will update the version field, and we don't want to
	// stomp the caller's copy.
	dbActor := proto.Clone(actor).(*ateapipb.Actor)
	dbActor.Version = 1

	dbActorBytes, err := protojson.Marshal(dbActor)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	ok, err := s.rdb.SetNX(ctx, dbKey, dbActorBytes, 0).Result()
	if err != nil {
		return fmt.Errorf("while executing redis set: %w", err)
	}
	if !ok {
		return store.ErrAlreadyExists
	}

	return nil
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

	return nil
}

func (s *Persistence) DeleteWorker(ctx context.Context, namespace, pool, pod string) error {
	dbKey := workerDBKey(namespace, pool, pod)
	err := s.rdb.Del(ctx, dbKey).Err()
	if err != nil {
		return fmt.Errorf("while deleting worker key %q: %w", dbKey, err)
	}
	return nil
}

func (s *Persistence) DeleteActor(ctx context.Context, id string) error {
	dbKey := actorDBKey(id)
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

		if currentActor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
			return store.ErrFailedPrecondition
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, dbKey)
			return nil
		})
		return err
	}, dbKey)

	if err != nil {
		if errors.Is(err, redis.TxFailedErr) {
			return store.ErrPersistenceRetry
		}
		return err
	}

	return nil
}

func (s *Persistence) UpdateActor(ctx context.Context, actor *ateapipb.Actor, expectedVersion int64) error {
	dbKey := actorDBKey(actor.GetActorId())

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

		if currentActor.GetVersion() != expectedVersion {
			return store.ErrPersistenceRetry
		}
		dbActor.Version = currentActor.GetVersion() + 1
		if currentActor.GetActorId() != dbActor.GetActorId() {
			return fmt.Errorf("actor_id is immutable")
		}
		if currentActor.GetActorTemplateNamespace() != dbActor.GetActorTemplateNamespace() {
			return fmt.Errorf("actor_template_namespace is immutable")
		}
		if currentActor.GetActorTemplateName() != dbActor.GetActorTemplateName() {
			return fmt.Errorf("actor_template_name is immutable")
		}

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
			return store.ErrPersistenceRetry
		}
		return fmt.Errorf("while executing update actor transaction: %w", err)
	}

	actor.Version = dbActor.Version
	return nil
}

func (s *Persistence) ListWorkers(ctx context.Context) ([]*ateapipb.Worker, error) {
	var result []*ateapipb.Worker
	var mu sync.Mutex

	// Iterate through every Primary (Master) node in the cluster
	err := s.rdb.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		iter := master.Scan(ctx, 0, "worker:*", 0).Iterator()
		for iter.Next(ctx) {
			workerKey := iter.Val()
			parts := strings.Split(workerKey, ":")
			if len(parts) != 4 {
				return fmt.Errorf("bad key format %q", workerKey)
			}

			getCmd := master.Get(ctx, workerKey)
			if getCmd.Err() != nil {
				return fmt.Errorf("while getting worker %q: %w", workerKey, getCmd.Err())
			}

			worker := &ateapipb.Worker{}
			if err := protojson.Unmarshal([]byte(getCmd.Val()), worker); err != nil {
				return fmt.Errorf("in protojson.Unmarshal: %w", err)
			}

			mu.Lock()
			result = append(result, worker)
			mu.Unlock()
		}
		if err := iter.Err(); err != nil {
			return fmt.Errorf("error from iterator: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while iterating all redis master: %w", err)
	}
	return result, nil
}

func (s *Persistence) ListActors(ctx context.Context) ([]*ateapipb.Actor, error) {
	var result []*ateapipb.Actor
	var mu sync.Mutex

	err := s.rdb.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		iter := master.Scan(ctx, 0, "actor:*", 0).Iterator()
		for iter.Next(ctx) {
			actorKey := iter.Val()
			parts := strings.Split(actorKey, ":")
			if len(parts) != 2 {
				return fmt.Errorf("bad key format %q", actorKey)
			}

			getCmd := master.Get(ctx, actorKey)
			if getCmd.Err() != nil {
				return fmt.Errorf("while getting actor %q: %w", actorKey, getCmd.Err())
			}

			actor := &ateapipb.Actor{}
			if err := protojson.Unmarshal([]byte(getCmd.Val()), actor); err != nil {
				return fmt.Errorf("in protojson.Unmarshal: %w", err)
			}

			mu.Lock()
			result = append(result, actor)
			mu.Unlock()
		}
		return iter.Err()
	})

	if err != nil {
		return nil, fmt.Errorf("while iterating all redis master: %w", err)
	}
	return result, nil
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
