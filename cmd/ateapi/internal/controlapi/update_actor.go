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

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// actorMutableFields maps the Actor field paths a client may name in an
// UpdateActor update_mask to the setter that applies them.
// Every other field is either output-only (server-managed), immutable or
// unsupported (e.g. '*'), and naming one is an error.
var actorMutableFields = map[string]func(dst, src *ateapipb.Actor){
	"worker_selector": func(dst, src *ateapipb.Actor) { dst.WorkerSelector = src.GetWorkerSelector() },
}

func (s *Service) UpdateActor(ctx context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.Actor, error) {
	if errs := validateUpdateActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	in := req.GetActor()
	actorRef := resources.ActorRefFromActor(in)
	setSpanActorRefAttributes(ctx, actorRef)

	actor, err := s.persistence.GetActor(ctx, actorRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "Actor %s not found", actorRef)
		}
		return nil, fmt.Errorf("while getting actor: %w", err)
	}

	// UID and version preconditions
	if uid := in.GetMetadata().GetUid(); uid != "" && uid != actor.GetMetadata().GetUid() {
		return nil, status.Errorf(codes.Aborted, "Actor %s has uid %s, not %s", actorRef, actor.GetMetadata().GetUid(), uid)
	}

	expectedVersion := actor.GetMetadata().GetVersion()
	if version := in.GetMetadata().GetVersion(); version != 0 {
		expectedVersion = version
	}

	applyActorUpdateMask(actor, in, req.GetUpdateMask())

	updated, err := s.persistence.UpdateActor(ctx, actor, expectedVersion)
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, status.Error(codes.Aborted, "concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while updating actor: %w", err)
	}

	setSpanActorAttributes(ctx, updated)
	return updated, nil
}

// applyActorUpdateMask copies the masked fields from src onto dst. Fields set on
// src but absent from the mask are ignored, and a masked field that is unset on
// src is cleared on dst.
func applyActorUpdateMask(dst, src *ateapipb.Actor, mask *fieldmaskpb.FieldMask) {
	for _, path := range mask.GetPaths() {
		apply, ok := actorMutableFields[path]
		if ok {
			apply(dst, src)
		}
	}
}

func validateUpdateActorRequest(req *ateapipb.UpdateActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	actor := req.GetActor()
	actorPath := fldPath.Child("actor")
	if actor == nil {
		return field.ErrorList{field.Required(actorPath, "")}
	}

	// atespace and name identify the resource to update; uid and version are
	// optional preconditions.
	metaPath := actorPath.Child("metadata")
	if atespace, p := actor.GetMetadata().GetAtespace(), metaPath.Child("atespace"); atespace == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(atespace, p)...)
	}

	if name, p := actor.GetMetadata().GetName(), metaPath.Child("name"); name == "" {
		errs = append(errs, field.Required(p, ""))
	} else {
		errs = append(errs, resources.ValidateResourceName(name, p)...)
	}

	if uid, p := actor.GetMetadata().GetUid(), metaPath.Child("uid"); uid != "" {
		errs = append(errs, resources.ValidateUUID(uid, p)...)
	}

	if version, p := actor.GetMetadata().GetVersion(), metaPath.Child("version"); version < 0 {
		errs = append(errs, field.Invalid(p, version, "must not be negative"))
	}

	maskPath := fldPath.Child("update_mask")
	if paths := req.GetUpdateMask().GetPaths(); len(paths) == 0 {
		errs = append(errs, field.Required(maskPath, "must name at least one field to update"))
	} else {
		supportedMutableFields := slices.Sorted(maps.Keys(actorMutableFields))
		for _, path := range paths {
			if _, ok := actorMutableFields[path]; !ok {
				errs = append(errs, field.NotSupported(maskPath, path, supportedMutableFields))
			}
		}
	}

	if selector := actor.GetWorkerSelector(); selector != nil {
		errs = append(errs, validateSelector(selector, actorPath.Child("worker_selector"))...)
	}

	return errs
}
