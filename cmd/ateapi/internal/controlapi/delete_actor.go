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
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func (s *Service) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (deleted *ateapipb.Actor, err error) {
	if errs := validateDeleteActorRequest(req); len(errs) > 0 {
		return nil, toGRPCStatusError(errs)
	}
	start := time.Now()
	// Template dims only once the record resolved: the request names only the
	// actor, so failures before the load carry none.
	defer func() {
		var attrs []attribute.KeyValue
		if deleted != nil {
			attrs = append(attrs,
				ateattr.TemplateNameKey.String(deleted.GetActorTemplateName()),
				ateattr.TemplateNamespaceKey.String(deleted.GetActorTemplateNamespace()),
			)
		}
		s.instruments.recordLifecycleOp(ctx, ateattr.OperationDelete, start, err, attrs...)
	}()
	actorRef := resources.ActorRefFromObjectRef(req.GetActor())
	setSpanActorRefAttributes(ctx, actorRef)

	deleted, err = s.actorWorkflow.DeleteActor(ctx, req.GetActor().GetAtespace(), req.GetActor().GetName())
	if err != nil {
		return nil, err
	}

	return deleted, nil
}

func validateDeleteActorRequest(req *ateapipb.DeleteActorRequest) field.ErrorList {
	var fldPath *field.Path
	var errs field.ErrorList

	if val, fldPath := req.Actor, fldPath.Child("actor"); val == nil {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, resources.ValidateObjectRef(val, fldPath)...)
	}

	return errs
}
