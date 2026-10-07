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

package admission

import (
	"context"
	"errors"
	"fmt"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/defaults"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/proto"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// CreateActorTemplate validates and persists a new ActorTemplate with an empty initial status.
//
// Returns ErrInvalid if inTemplate fails declarative create validation,
// ErrFailedPrecondition if the referenced SandboxConfig does not exist or its
// class mismatches, store.ErrAlreadyExists if the template name is already
// taken in its atespace, or store.ErrFailedPrecondition if the template's
// atespace does not exist.
func (a *Admission) CreateActorTemplate(ctx context.Context, inTemplate *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	specTemplate := proto.CloneOf(inTemplate)
	specTemplate.Status = nil
	defaults.Apply(specTemplate)

	fldPath := field.NewPath("actor_template")
	if errs := apivalidation.ValidateActorTemplateCreate(ctx, fldPath, specTemplate); len(errs) > 0 {
		return nil, invalidf("%v", errs.ToAggregate())
	}

	if _, err := ResolveTemplateSandboxConfig(a.sandboxConfigLister, specTemplate.GetSandboxConfig()); err != nil {
		return nil, err
	}

	outTemplate := proto.CloneOf(specTemplate)
	outTemplate.Status = &ateapipb.ActorTemplateStatus{}

	if errs := apivalidation.ValidateActorTemplateUpdate(ctx, fldPath, outTemplate, specTemplate); len(errs) > 0 {
		return nil, fmt.Errorf("%v", errs.ToAggregate())
	}

	return a.store.CreateActorTemplate(ctx, outTemplate)
}

// GetActorTemplate retrieves an ActorTemplate by reference.
//
// Returns store.ErrNotFound if the template does not exist.
func (a *Admission) GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	return a.store.GetActorTemplate(ctx, templateRef)
}

// ListActorTemplates lists ActorTemplates in atespace (or across all atespaces if empty).
//
// Returns store.ErrInvalidPageSize or store.ErrInvalidPageToken if pagination
// options are invalid.
func (a *Admission) ListActorTemplates(ctx context.Context, atespace string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error) {
	return a.store.ListActorTemplates(ctx, atespace, opts)
}

// UpdateActorTemplateStatus updates the server-owned status of an ActorTemplate
// and validates the resulting resource before storing it.
//
// Returns store.ErrPreconditionRequired if precondition omits uid or version,
// store.ErrNotFound if the template does not exist, store.ErrUIDConflict or
// store.ErrVersionConflict if precondition does not match the stored template,
// or the error returned by mutate verbatim.
func (a *Admission) UpdateActorTemplateStatus(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.Precondition, mutate func(*ateapipb.ActorTemplateStatus) error) (*ateapipb.ActorTemplate, error) {
	return a.store.UpdateActorTemplate(ctx, templateRef, precondition, func(toUpdate *ateapipb.ActorTemplate) error {
		oldVal := proto.CloneOf(toUpdate)
		if err := mutate(toUpdate.Status); err != nil {
			return err
		}
		if errs := apivalidation.ValidateActorTemplateUpdate(ctx, field.NewPath("actor_template"), toUpdate, oldVal); len(errs) > 0 {
			return fmt.Errorf("%v", errs.ToAggregate())
		}
		return nil
	})
}

// DeleteActorTemplate deletes an ActorTemplate by reference.
//
// Returns store.ErrNotFound if the template does not exist, or
// store.ErrUIDConflict or store.ErrVersionConflict if precondition does not
// match the stored template.
func (a *Admission) DeleteActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef, precondition store.DeletePreconditions) (*ateapipb.ActorTemplate, error) {
	return a.store.DeleteActorTemplate(ctx, templateRef, precondition)
}

// resolveActorTemplate resolves the ActorTemplate that actor's actor_template
// reference names.
func (a *Admission) resolveActorTemplate(ctx context.Context, actor *ateapipb.Actor) (*ateapipb.ActorTemplate, error) {
	templateRef := resources.ActorTemplateRefFromObjectRef(actor.GetActorTemplate())
	template, err := a.store.GetActorTemplate(ctx, templateRef)
	if errors.Is(err, store.ErrNotFound) {
		return nil, failedPreconditionf("actor template not found; ObjectRef: %s ", templateRef)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	return template, nil
}

// ResolveTemplateSandboxConfig resolves the SandboxConfig the ActorTemplate
// names via sandbox_config.config_name and checks that its class matches the
// template's sandbox_class.
//
// Returns ErrFailedPrecondition if the SandboxConfig does not exist or its
// class does not match templateSandbox.
func ResolveTemplateSandboxConfig(
	sandboxConfigLister listersv1alpha1.SandboxConfigLister,
	templateSandbox *ateapipb.SandboxConfig,
) (*atev1alpha1.SandboxConfig, error) {
	name := templateSandbox.GetConfigName()
	sc, err := sandboxConfigLister.Get(name)
	if k8serrors.IsNotFound(err) {
		return nil, failedPreconditionf("SandboxConfig %q not found", name)
	}
	if err != nil {
		return nil, fmt.Errorf("while getting SandboxConfig %q: %w", name, err)
	}
	if class := SandboxClassString(templateSandbox.GetSandboxClass()); string(sc.Spec.SandboxClass) != class {
		return nil, failedPreconditionf(
			"SandboxConfig %q has class %q but sandbox_config.sandbox_class is %q",
			name, sc.Spec.SandboxClass, class)
	}
	return sc, nil
}

// SandboxClassString renders the proto enum in the CRD's lower-case string
// form, which the scheduler and the metric labels share.
func SandboxClassString(in ateapipb.SandboxClass) string {
	switch in {
	case ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR:
		return string(atev1alpha1.SandboxClassGvisor)
	case ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM:
		return string(atev1alpha1.SandboxClassMicroVM)
	default:
		return ""
	}
}
