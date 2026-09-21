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

package atepg

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

func (p *Persistence) CreateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, policy *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	dbPolicy := proto.Clone(policy).(*ateapipb.EgressPolicy)
	dbPolicy.Metadata = newCreateMetadata(actorRef.Atespace, "default")
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling egress policy: %w", err)
	}
	_, err = p.pool.Exec(ctx, `
		INSERT INTO actor_egress_policies (atespace, actor_name, uid, version, proto)
		VALUES ($1, $2, $3, $4, $5)`, actorRef.Atespace, actorRef.Name, dbPolicy.GetMetadata().GetUid(), dbPolicy.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		if isUniqueViolation(err) {
			return nil, store.ErrAlreadyExists
		}
		if isForeignKeyViolation(err) {
			return nil, store.ErrFailedPrecondition
		}
		return nil, fmt.Errorf("inserting egress policy for %s: %w", actorRef, err)
	}
	return dbPolicy, nil
}

func (p *Persistence) GetEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	return getEgressPolicyRow(ctx, p.pool, `
		SELECT uid, version, proto FROM actor_egress_policies
		WHERE atespace = $1 AND actor_name = $2`, actorRef.Atespace, actorRef.Name)
}

func (p *Persistence) UpdateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.EgressPolicy) error) (*ateapipb.EgressPolicy, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	dbPolicy, err := getEgressPolicyRow(ctx, p.pool, `
		SELECT uid, version, proto FROM actor_egress_policies
		WHERE atespace = $1 AND actor_name = $2`, actorRef.Atespace, actorRef.Name)
	if err != nil {
		return nil, err
	}
	currentUID := dbPolicy.GetMetadata().GetUid()
	currentVersion := dbPolicy.GetMetadata().GetVersion()
	if err := precondition.Check(dbPolicy.GetMetadata()); err != nil {
		return nil, err
	}
	oldMeta := proto.CloneOf(dbPolicy.Metadata)
	if err := mutate(dbPolicy); err != nil {
		return nil, err
	}
	dbPolicy.Metadata = oldMeta
	setUpdateMetadata(dbPolicy.Metadata, oldMeta)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling updated egress policy: %w", err)
	}
	commandTag, err := p.pool.Exec(ctx, `
		UPDATE actor_egress_policies SET version = $1, proto = $2
		WHERE atespace = $3 AND actor_name = $4 AND uid = $5 AND version = $6`,
		dbPolicy.GetMetadata().GetVersion(), protoBytes, actorRef.Atespace, actorRef.Name, currentUID, currentVersion)
	if err != nil {
		return nil, fmt.Errorf("updating egress policy for %s: %w", actorRef, err)
	}
	if commandTag.RowsAffected() == 0 {
		return nil, store.ErrVersionConflict
	}
	if commandTag.RowsAffected() != 1 {
		return nil, fmt.Errorf("updating egress policy for %s affected %d rows, want 1", actorRef, commandTag.RowsAffected())
	}
	return dbPolicy, nil
}

func (p *Persistence) DeleteEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	var version int64
	var uid string
	var protoBytes []byte
	err := p.pool.QueryRow(ctx, `
		DELETE FROM actor_egress_policies
		WHERE atespace = $1 AND actor_name = $2
		RETURNING uid, version, proto`, actorRef.Atespace, actorRef.Name).Scan(&uid, &version, &protoBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("deleting egress policy for %s: %w", actorRef, err)
	}
	return unmarshalEgressPolicy(uid, version, protoBytes)
}

func getEgressPolicyRow(ctx context.Context, q querier, query string, args ...any) (*ateapipb.EgressPolicy, error) {
	var uid string
	var version int64
	var protoBytes []byte
	if err := q.QueryRow(ctx, query, args...).Scan(&uid, &version, &protoBytes); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting egress policy: %w", err)
	}
	return unmarshalEgressPolicy(uid, version, protoBytes)
}

func unmarshalEgressPolicy(uid string, version int64, protoBytes []byte) (*ateapipb.EgressPolicy, error) {
	policy := &ateapipb.EgressPolicy{}
	if err := unmarshalStored(protoBytes, policy); err != nil {
		return nil, fmt.Errorf("unmarshaling egress policy: %w", err)
	}
	if err := validateProtoMetadataMatchesColumns("egress policy", policy.GetMetadata(), uid, version); err != nil {
		return nil, err
	}
	return policy, nil
}
