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

package atemy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storesql"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
)

// CreateEgressPolicy returns ErrFailedPrecondition if the actor does not exist.
func (p *Persistence) CreateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, policy *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	dbPolicy := proto.Clone(policy).(*ateapipb.EgressPolicy)
	storesql.SetCreateMetadata(dbPolicy.Metadata)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling egress policy: %w", err)
	}
	err = p.insertChild(ctx, "egress policy for "+actorRef.String(), func(tx *sql.Tx) error {
		return lockParent(ctx, tx, `SELECT 1 FROM actors WHERE atespace = ? AND name = ? FOR SHARE`, actorRef.Atespace, actorRef.Name)
	}, `
		INSERT INTO actor_egress_policies (atespace, actor_name, uid, version, proto)
		VALUES (?, ?, ?, ?, ?)`, actorRef.Atespace, actorRef.Name, dbPolicy.GetMetadata().GetUid(), dbPolicy.GetMetadata().GetVersion(), protoBytes)
	if err != nil {
		return nil, err
	}
	return dbPolicy, nil
}

func (p *Persistence) GetEgressPolicy(ctx context.Context, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	return getEgressPolicyRow(ctx, p.db, actorRef)
}

func (p *Persistence) UpdateEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.Precondition, mutate func(*ateapipb.EgressPolicy) error) (*ateapipb.EgressPolicy, error) {
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	dbPolicy, err := getEgressPolicyRow(ctx, p.db, actorRef)
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
	storesql.SetUpdateMetadata(dbPolicy.Metadata, oldMeta)
	protoBytes, err := proto.Marshal(dbPolicy)
	if err != nil {
		return nil, fmt.Errorf("marshaling updated egress policy: %w", err)
	}
	if err := updateGuarded(ctx, p.db, "egress policy for "+actorRef.String(), `
		UPDATE actor_egress_policies SET version = ?, proto = ?
		WHERE atespace = ? AND actor_name = ? AND uid = ? AND version = ?`,
		dbPolicy.GetMetadata().GetVersion(), protoBytes, actorRef.Atespace, actorRef.Name, currentUID, currentVersion); err != nil {
		return nil, err
	}
	return dbPolicy, nil
}

func (p *Persistence) DeleteEgressPolicy(ctx context.Context, actorRef resources.ActorRef, precondition store.DeletePreconditions) (*ateapipb.EgressPolicy, error) {
	var (
		uid        string
		version    int64
		protoBytes []byte
	)
	err := inTx(ctx, p.db, func(tx *sql.Tx) error {
		var err error
		uid, version, protoBytes, err = deleteLockedRow(ctx, tx, "actor_egress_policies", "atespace = ? AND actor_name = ?", precondition, actorRef.Atespace, actorRef.Name)
		return err
	})
	if err != nil {
		return nil, err
	}
	return unmarshalEgressPolicy(uid, version, protoBytes)
}

func getEgressPolicyRow(ctx context.Context, q querier, actorRef resources.ActorRef) (*ateapipb.EgressPolicy, error) {
	var uid string
	var version int64
	var protoBytes []byte
	if err := q.QueryRowContext(ctx, `
		SELECT uid, version, proto FROM actor_egress_policies
		WHERE atespace = ? AND actor_name = ?`, actorRef.Atespace, actorRef.Name).Scan(&uid, &version, &protoBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, store.ErrNotFound
		}
		return nil, fmt.Errorf("getting egress policy: %w", err)
	}
	return unmarshalEgressPolicy(uid, version, protoBytes)
}

func unmarshalEgressPolicy(uid string, version int64, protoBytes []byte) (*ateapipb.EgressPolicy, error) {
	policy := &ateapipb.EgressPolicy{}
	if err := storesql.UnmarshalStored(protoBytes, policy); err != nil {
		return nil, fmt.Errorf("unmarshaling egress policy: %w", err)
	}
	if err := storesql.ValidateProtoMetadataMatchesColumns("egress policy", policy.GetMetadata(), uid, version); err != nil {
		return nil, err
	}
	return policy, nil
}
