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
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

// bindActorSharedVolumes commits references with the actor itself. Row locks
// serialize against deletion fencing and other creates; sorted names prevent
// two multi-volume creates from taking locks in opposite orders.
func bindActorSharedVolumes(ctx context.Context, tx pgx.Tx, actor *ateapipb.Actor) error {
	mounts := make(map[string]*ateapipb.ExternalVolumeRef)
	for _, mount := range actor.GetStatus().GetExternalVolumes() {
		if mount.GetSharedVolumeName() != "" {
			mounts[mount.GetSharedVolumeName()] = mount
		}
	}
	names := make([]string, 0, len(mounts))
	for name := range mounts {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		var data []byte
		err := tx.QueryRow(ctx, `SELECT proto FROM shared_volumes WHERE atespace = $1 AND name = $2 FOR UPDATE`, actor.GetMetadata().GetAtespace(), name).Scan(&data)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrFailedPrecondition
		}
		if err != nil {
			return err
		}
		volume := &ateapipb.SharedVolume{}
		if err := unmarshalStored(data, volume); err != nil {
			return err
		}
		mount := mounts[name]
		if volume.GetStatus().GetState() != ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY || volume.GetVolumeId() != mount.GetStorageVolumeId() || volume.GetVolumeType() != mount.GetVolumeType() || len(volume.GetStatus().GetRefs()) >= 256 {
			return fmt.Errorf("shared volume %s is unavailable or changed: %w", name, store.ErrFailedPrecondition)
		}
		oldMeta := proto.CloneOf(volume.Metadata)
		volume.Status.Refs = append(volume.Status.Refs, &ateapipb.ActorRef{ActorUid: actor.GetMetadata().GetUid(), ActorName: actor.GetMetadata().GetName()})
		setUpdateMetadata(volume.Metadata, oldMeta)
		data, err = proto.Marshal(volume)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE shared_volumes SET proto = $1, version = $2 WHERE atespace = $3 AND name = $4`, data, volume.GetMetadata().GetVersion(), actor.GetMetadata().GetAtespace(), name); err != nil {
			return err
		}
	}
	return nil
}
