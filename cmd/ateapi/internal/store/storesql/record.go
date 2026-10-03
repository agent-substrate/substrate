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

// Package storesql holds the backend-neutral pieces shared by the SQL store
// backends, atepg (PostgreSQL) and atemy (MySQL): proto record handling,
// keyset page tokens, the worker event codec and in-process fan-out, the
// lease renewal loop, and the migration runner. The backends supply every SQL
// statement.
package storesql

import (
	"fmt"
	"strings"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// UnmarshalStored decodes a stored proto, dropping fields this binary has no
// descriptor for. This means a newer replica can have written such a field.
// It also backfills defaults, including for resources stored before a field
// with defaults was introduced.
func UnmarshalStored(b []byte, m proto.Message) error {
	if err := (proto.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, m); err != nil {
		return err
	}
	defaults.Apply(m)
	return nil
}

// UnmarshalRow is UnmarshalStored for a row in a listing. A listing fails as a
// whole on one bad row, so the error names the row.
func UnmarshalRow(b []byte, m proto.Message, kind string, id ...string) error {
	if err := UnmarshalStored(b, m); err != nil {
		return fmt.Errorf("unmarshaling %s %s: %w", kind, strings.Join(id, "/"), err)
	}
	return nil
}

// SetCreateMetadata assigns the identity and first revision of a new resource.
func SetCreateMetadata(metadata *ateapipb.ResourceMetadata) {
	metadata.Uid = uuid.NewString()
	metadata.Version = 1
	metadata.CreateTime = timestamppb.Now()
	metadata.UpdateTime = metadata.CreateTime
}

// ValidateProtoMetadataMatchesColumns verifies that the metadata in the database
// matches the metadata in the proto.
func ValidateProtoMetadataMatchesColumns(resource string, metadata *ateapipb.ResourceMetadata, uid string, version int64) error {
	if metadata.GetUid() != uid {
		return fmt.Errorf("%s uid projection %q does not match proto metadata uid %q", resource, uid, metadata.GetUid())
	}
	if metadata.GetVersion() != version {
		return fmt.Errorf("%s version projection %d does not match proto metadata version %d", resource, version, metadata.GetVersion())
	}
	return nil
}

// SetUpdateMetadata derives the next revision of newMeta from the stored oldMeta.
func SetUpdateMetadata(newMeta, oldMeta *ateapipb.ResourceMetadata) {
	newMeta.Uid = oldMeta.Uid
	newMeta.Version = oldMeta.Version + 1
	newMeta.CreateTime = oldMeta.CreateTime
	newMeta.UpdateTime = timestamppb.Now()
}

// ValidateUpdateActorTemplateMutation rejects a mutation that changes an
// actor template's identity.
func ValidateUpdateActorTemplateMutation(storedTemplate, mutatedTemplate *ateapipb.ActorTemplate) error {
	if stored, mutated := storedTemplate.GetMetadata().GetAtespace(), mutatedTemplate.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTemplate.GetMetadata().GetName(), mutatedTemplate.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	return nil
}

// ValidateUpdateTagMutation reports a mutation that changed a field of a tag
// that is immutable for its lifetime.
func ValidateUpdateTagMutation(storedTag, mutatedTag *ateapipb.Tag) error {
	if stored, mutated := storedTag.GetMetadata().GetAtespace(), mutatedTag.GetMetadata().GetAtespace(); stored != mutated {
		return fmt.Errorf("metadata.atespace is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetMetadata().GetName(), mutatedTag.GetMetadata().GetName(); stored != mutated {
		return fmt.Errorf("metadata.name is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetStatus().GetSnapshot(), mutatedTag.GetStatus().GetSnapshot(); stored != nil && !proto.Equal(stored, mutated) {
		return fmt.Errorf("status.snapshot is immutable once set: mutation changed it from %s to %s", stored, mutated)
	}
	if stored, mutated := storedTag.GetStatus().GetStorageLocation(), mutatedTag.GetStatus().GetStorageLocation(); stored != mutated {
		return fmt.Errorf("status.storage_location is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	if stored, mutated := storedTag.GetStatus().GetActorTemplateUid(), mutatedTag.GetStatus().GetActorTemplateUid(); stored != mutated {
		return fmt.Errorf("status.actor_template_uid is immutable: mutation changed it from %q to %q", stored, mutated)
	}
	return nil
}
