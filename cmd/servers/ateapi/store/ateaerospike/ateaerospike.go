//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

// Package ateaerospike is an ate storage backend built on Aerospike.
package ateaerospike

import (
	"context"
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/agent-substrate/substrate/cmd/servers/ateapi/store"
	"github.com/agent-substrate/substrate/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
)

// Persistence is a service that stores information about applications in Aerospike.
type Persistence struct {
	client    *as.Client
	namespace string
	actorSet  string
	workerSet string
	lockSet   string
}

var _ store.Interface = (*Persistence)(nil)

// NewPersistence creates a new Aerospike Persistence store.
func NewPersistence(client *as.Client, namespace string) *Persistence {
	return &Persistence{
		client:    client,
		namespace: namespace,
		actorSet:  "actors",
		workerSet: "workers",
		lockSet:   "locks",
	}
}

func workerKeyStr(namespace, pool, pod string) string {
	return namespace + ":" + pool + ":" + pod
}

func (p *Persistence) GetActor(ctx context.Context, id string) (*ateapipb.Actor, error) {
	var err error
	key, err := as.NewKey(p.namespace, p.actorSet, id)
	if err != nil {
		return nil, fmt.Errorf("failed to create actor key: %w", err)
	}

	policy := as.NewPolicy()
	record, err := p.client.Get(policy, key)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting actor key %q: %w", id, err)
	}

	dataVal, ok := record.Bins["data"].(string)
	if !ok {
		return nil, fmt.Errorf("stored actor data is not a string")
	}

	actor := &ateapipb.Actor{}
	if err := protojson.Unmarshal([]byte(dataVal), actor); err != nil {
		return nil, fmt.Errorf("while unmarshaling actor: %w", err)
	}

	// Map the Aerospike record generation directly to the version
	actor.Version = int64(record.Generation)
	return actor, nil
}

func (p *Persistence) CreateActor(ctx context.Context, actor *ateapipb.Actor) error {
	var err error
	key, err := as.NewKey(p.namespace, p.actorSet, actor.GetActorId())
	if err != nil {
		return fmt.Errorf("failed to create actor key: %w", err)
	}

	dbActorBytes, err := protojson.Marshal(actor)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	policy := as.NewWritePolicy(0, 0)
	policy.RecordExistsAction = as.CREATE_ONLY

	bin := as.NewBin("data", string(dbActorBytes))
	err = p.client.PutBins(policy, key, bin)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.KEY_EXISTS_ERROR {
			return store.ErrAlreadyExists
		}
		return fmt.Errorf("while executing aerospike put: %w", err)
	}

	actor.Version = 1
	return nil
}

func (p *Persistence) UpdateActor(ctx context.Context, actor *ateapipb.Actor, expectedVersion int64) error {
	var err error
	key, err := as.NewKey(p.namespace, p.actorSet, actor.GetActorId())
	if err != nil {
		return fmt.Errorf("failed to create actor key: %w", err)
	}

	dbActorBytes, err := protojson.Marshal(actor)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	policy := as.NewWritePolicy(0, 0)
	policy.RecordExistsAction = as.UPDATE_ONLY
	policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	policy.Generation = uint32(expectedVersion)

	bin := as.NewBin("data", string(dbActorBytes))
	err = p.client.PutBins(policy, key, bin)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok {
			switch asErr.ResultCode {
			case types.KEY_NOT_FOUND_ERROR:
				return store.ErrNotFound
			case types.GENERATION_ERROR:
				return store.ErrPersistenceRetry
			}
		}
		return fmt.Errorf("while executing aerospike put transaction: %w", err)
	}

	actor.Version = expectedVersion + 1
	return nil
}

func (p *Persistence) DeleteActor(ctx context.Context, id string) error {
	var err error
	key, err := as.NewKey(p.namespace, p.actorSet, id)
	if err != nil {
		return fmt.Errorf("failed to create actor key: %w", err)
	}

	record, err := p.client.Get(as.NewPolicy(), key)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
			return store.ErrNotFound
		}
		return fmt.Errorf("while getting actor for deletion: %w", err)
	}

	dataVal, ok := record.Bins["data"].(string)
	if !ok {
		return fmt.Errorf("stored actor data is not a string")
	}

	actor := &ateapipb.Actor{}
	if err := protojson.Unmarshal([]byte(dataVal), actor); err != nil {
		return fmt.Errorf("while unmarshaling actor: %w", err)
	}

	if actor.GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		return store.ErrFailedPrecondition
	}

	// Atomic delete with generation check to prevent race conditions
	policy := as.NewWritePolicy(0, 0)
	policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	policy.Generation = record.Generation

	existed, err := p.client.Delete(policy, key)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.GENERATION_ERROR {
			return store.ErrPersistenceRetry
		}
		return fmt.Errorf("while executing aerospike delete: %w", err)
	}
	if !existed {
		return store.ErrNotFound
	}

	return nil
}

func (p *Persistence) ListActors(ctx context.Context) ([]*ateapipb.Actor, error) {
	var result []*ateapipb.Actor

	policy := as.NewScanPolicy()
	recordset, err := p.client.ScanAll(policy, p.namespace, p.actorSet)
	if err != nil {
		return nil, fmt.Errorf("failed to scan actors: %w", err)
	}

	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("error scanning actors: %w", res.Err)
		}

		dataVal, ok := res.Record.Bins["data"].(string)
		if !ok {
			continue
		}

		actor := &ateapipb.Actor{}
		if err := protojson.Unmarshal([]byte(dataVal), actor); err != nil {
			return nil, fmt.Errorf("in protojson.Unmarshal scanned actor: %w", err)
		}

		actor.Version = int64(res.Record.Generation)
		result = append(result, actor)
	}

	return result, nil
}

func (p *Persistence) GetWorker(ctx context.Context, namespace, pool, pod string) (*ateapipb.Worker, error) {
	var err error
	id := workerKeyStr(namespace, pool, pod)
	key, err := as.NewKey(p.namespace, p.workerSet, id)
	if err != nil {
		return nil, fmt.Errorf("failed to create worker key: %w", err)
	}

	policy := as.NewPolicy()
	record, err := p.client.Get(policy, key)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("while getting worker key %q: %w", id, err)
	}

	dataVal, ok := record.Bins["data"].(string)
	if !ok {
		return nil, fmt.Errorf("stored worker data is not a string")
	}

	worker := &ateapipb.Worker{}
	if err := protojson.Unmarshal([]byte(dataVal), worker); err != nil {
		return nil, fmt.Errorf("while unmarshaling worker: %w", err)
	}

	worker.Version = int64(record.Generation)
	return worker, nil
}

func (p *Persistence) CreateWorker(ctx context.Context, worker *ateapipb.Worker) error {
	var err error
	id := workerKeyStr(worker.GetWorkerNamespace(), worker.GetWorkerPool(), worker.GetWorkerPod())
	key, err := as.NewKey(p.namespace, p.workerSet, id)
	if err != nil {
		return fmt.Errorf("failed to create worker key: %w", err)
	}

	dbWorkerBytes, err := protojson.Marshal(worker)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	policy := as.NewWritePolicy(0, 0)
	policy.RecordExistsAction = as.CREATE_ONLY

	bin := as.NewBin("data", string(dbWorkerBytes))
	err = p.client.PutBins(policy, key, bin)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.KEY_EXISTS_ERROR {
			return store.ErrAlreadyExists
		}
		return fmt.Errorf("while executing aerospike put: %w", err)
	}

	worker.Version = 1
	return nil
}

func (p *Persistence) UpdateWorker(ctx context.Context, worker *ateapipb.Worker, expectedVersion int64) error {
	var err error
	id := workerKeyStr(worker.GetWorkerNamespace(), worker.GetWorkerPool(), worker.GetWorkerPod())
	key, err := as.NewKey(p.namespace, p.workerSet, id)
	if err != nil {
		return fmt.Errorf("failed to create worker key: %w", err)
	}

	dbWorkerBytes, err := protojson.Marshal(worker)
	if err != nil {
		return fmt.Errorf("in protojson.Marshal: %w", err)
	}

	policy := as.NewWritePolicy(0, 0)
	policy.RecordExistsAction = as.UPDATE_ONLY
	policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	policy.Generation = uint32(expectedVersion)

	bin := as.NewBin("data", string(dbWorkerBytes))
	err = p.client.PutBins(policy, key, bin)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok {
			switch asErr.ResultCode {
			case types.KEY_NOT_FOUND_ERROR:
				return store.ErrNotFound
			case types.GENERATION_ERROR:
				return store.ErrPersistenceRetry
			}
		}
		return fmt.Errorf("while executing aerospike put transaction: %w", err)
	}

	worker.Version = expectedVersion + 1
	return nil
}

func (p *Persistence) DeleteWorker(ctx context.Context, namespace, pool, pod string) error {
	var err error
	id := workerKeyStr(namespace, pool, pod)
	key, err := as.NewKey(p.namespace, p.workerSet, id)
	if err != nil {
		return fmt.Errorf("failed to create worker key: %w", err)
	}

	policy := as.NewWritePolicy(0, 0)
	_, err = p.client.Delete(policy, key)
	if err != nil {
		return fmt.Errorf("while executing aerospike delete: %w", err)
	}
	return nil
}

func (p *Persistence) ListWorkers(ctx context.Context) ([]*ateapipb.Worker, error) {
	var result []*ateapipb.Worker

	policy := as.NewScanPolicy()
	recordset, err := p.client.ScanAll(policy, p.namespace, p.workerSet)
	if err != nil {
		return nil, fmt.Errorf("failed to scan workers: %w", err)
	}

	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, fmt.Errorf("error scanning workers: %w", res.Err)
		}

		dataVal, ok := res.Record.Bins["data"].(string)
		if !ok {
			continue
		}

		worker := &ateapipb.Worker{}
		if err := protojson.Unmarshal([]byte(dataVal), worker); err != nil {
			return nil, fmt.Errorf("in protojson.Unmarshal scanned worker: %w", err)
		}

		worker.Version = int64(res.Record.Generation)
		result = append(result, worker)
	}

	return result, nil
}

func (p *Persistence) AcquireLock(ctx context.Context, key string, value string, ttl time.Duration) (bool, error) {
	var err error
	lockKey, err := as.NewKey(p.namespace, p.lockSet, key)
	if err != nil {
		return false, fmt.Errorf("failed to create lock key: %w", err)
	}

	// Set record TTL in seconds
	policy := as.NewWritePolicy(0, uint32(ttl.Seconds()))
	policy.RecordExistsAction = as.CREATE_ONLY

	bin := as.NewBin("val", value)
	err = p.client.PutBins(policy, lockKey, bin)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.KEY_EXISTS_ERROR {
			return false, nil // Lock is already held
		}
		return false, fmt.Errorf("while executing lock creation: %w", err)
	}

	return true, nil
}

func (p *Persistence) ReleaseLock(ctx context.Context, key string, value string) error {
	var err error
	lockKey, err := as.NewKey(p.namespace, p.lockSet, key)
	if err != nil {
		return fmt.Errorf("failed to create lock key: %w", err)
	}

	record, err := p.client.Get(as.NewPolicy(), lockKey)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
			return nil // Lock already released or expired
		}
		return fmt.Errorf("while getting lock for release: %w", err)
	}

	storedVal, ok := record.Bins["val"].(string)
	if !ok {
		return fmt.Errorf("stored lock value is not a string")
	}

	if storedVal != value {
		return nil // Lock is not held by this token
	}

	// Atomic delete with generation check to prevent releasing a re-acquired lock
	policy := as.NewWritePolicy(0, 0)
	policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
	policy.Generation = record.Generation

	_, err = p.client.Delete(policy, lockKey)
	if err != nil {
		if asErr, ok := err.(*as.AerospikeError); ok && asErr.ResultCode == types.GENERATION_ERROR {
			return nil // Lock has changed or already expired
		}
		return fmt.Errorf("while executing lock delete: %w", err)
	}

	return nil
}

func (p *Persistence) DebugClearAll(ctx context.Context) error {
	err := p.client.Truncate(nil, p.namespace, p.actorSet, nil)
	if err != nil {
		return fmt.Errorf("failed to truncate actor set: %w", err)
	}

	err = p.client.Truncate(nil, p.namespace, p.workerSet, nil)
	if err != nil {
		return fmt.Errorf("failed to truncate worker set: %w", err)
	}

	err = p.client.Truncate(nil, p.namespace, p.lockSet, nil)
	if err != nil {
		return fmt.Errorf("failed to truncate locks set: %w", err)
	}

	return nil
}
