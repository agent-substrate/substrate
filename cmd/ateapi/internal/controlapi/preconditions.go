// Copyright 2026 The Agent Substrate Authors
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
	"errors"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

func toDeletePreconditions(opts *ateapi.DeleteOptions) store.Precondition {
	return store.Precondition{UID: opts.GetUid(), Version: opts.GetVersion()}
}

func toPreconditions(p *ateapi.Preconditions) store.Precondition {
	return store.Precondition{UID: p.GetUid(), Version: p.GetVersion()}
}

// checkPreconditions returns ABORTED if md, the metadata of the resource
// named by what, does not match precondition.
func checkPreconditions(what string, precondition store.Precondition, md *ateapi.ResourceMetadata) error {
	err := precondition.Check(md)
	switch {
	case errors.Is(err, store.ErrUIDConflict):
		return apierror.Aborted("%s does not have uid %s", what, precondition.UID)
	case errors.Is(err, store.ErrVersionConflict):
		return apierror.Aborted("%s is not at version %d", what, precondition.Version)
	}
	return err
}

// checkActorPreconditions returns ABORTED if actor does not match precondition.
func checkActorPreconditions(actorRef resources.ActorRef, precondition store.Precondition, actor *ateapi.Actor) error {
	return checkPreconditions("Actor "+actorRef.String(), precondition, actor.GetMetadata())
}
