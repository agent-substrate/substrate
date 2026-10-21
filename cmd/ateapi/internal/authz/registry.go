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

package authz

import (
	"slices"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// check is one OpenFGA permission check: the caller must have relation on object.
type check struct {
	relation string
	object   string
}

// checkExtractor returns the checks a request must pass; the interceptor allows
// the request only if all of them pass. Every resource identifier a check
// needs must be present and a valid resource name, otherwise the extractor
// returns codes.InvalidArgument: a check is never skipped, and OpenFGA never
// sees an object ID it would reject. Any other error (e.g., an unexpected
// request protobuf type) is codes.Internal, and the interceptor fails closed.
type checkExtractor func(req any) ([]check, error)

// checksOf adapts checksFor, written against the concrete request type T, into
// a checkExtractor.
func checksOf[T any](checksFor func(T) ([]check, field.ErrorList)) checkExtractor {
	return func(req any) ([]check, error) {
		r, ok := req.(T)
		if !ok {
			return nil, apierror.Internal("authz: unexpected request type %T", req)
		}
		checks, errs := checksFor(r)
		if len(errs) > 0 {
			return nil, resources.ToAPIError(errs)
		}
		return checks, nil
	}
}

// validateName requires name to be a non-empty, valid resource name.
func validateName(name string, fldPath *field.Path) field.ErrorList {
	if name == "" {
		return field.ErrorList{field.Required(fldPath, "")}
	}
	return resources.ValidateResourceName(name, fldPath)
}

func onGlobal(relation string) ([]check, field.ErrorList) {
	return []check{{relation: relation, object: GlobalRootObject}}, nil
}

func onAtespace(relation, atespace string, fldPath *field.Path) ([]check, field.ErrorList) {
	if errs := validateName(atespace, fldPath); len(errs) > 0 {
		return nil, errs
	}
	return []check{{relation: relation, object: AtespaceObject(atespace)}}, nil
}

// onAtespaceOrGlobal checks relation on the atespace, or on global:root when
// atespace is empty (List RPCs that span all atespaces).
func onAtespaceOrGlobal(relation, atespace string, fldPath *field.Path) ([]check, field.ErrorList) {
	if atespace == "" {
		return onGlobal(relation)
	}
	return onAtespace(relation, atespace, fldPath)
}

func onWorker(relation, name string, fldPath *field.Path) ([]check, field.ErrorList) {
	if errs := validateName(name, fldPath); len(errs) > 0 {
		return nil, errs
	}
	return []check{{relation: relation, object: WorkerObject(name)}}, nil
}

// resourceRef identifies an atespaced resource. Both *ateapipb.ObjectRef and
// *ateapipb.ResourceMetadata satisfy it, including as nil pointers.
type resourceRef interface {
	GetAtespace() string
	GetName() string
}

func validateRef(ref resourceRef, fldPath *field.Path) field.ErrorList {
	return append(
		validateName(ref.GetAtespace(), fldPath.Child("atespace")),
		validateName(ref.GetName(), fldPath.Child("name"))...,
	)
}

func onActor(relation string, ref resourceRef, fldPath *field.Path) ([]check, field.ErrorList) {
	if errs := validateRef(ref, fldPath); len(errs) > 0 {
		return nil, errs
	}
	return []check{{relation: relation, object: ActorObject(ref.GetAtespace(), ref.GetName())}}, nil
}

func onActorTemplate(relation string, ref resourceRef, fldPath *field.Path) ([]check, field.ErrorList) {
	if errs := validateRef(ref, fldPath); len(errs) > 0 {
		return nil, errs
	}
	return []check{{relation: relation, object: ActorTemplateObject(ref.GetAtespace(), ref.GetName())}}, nil
}

func onTag(relation string, ref resourceRef, fldPath *field.Path) ([]check, field.ErrorList) {
	if errs := validateRef(ref, fldPath); len(errs) > 0 {
		return nil, errs
	}
	return []check{{relation: relation, object: TagObject(ref.GetAtespace(), ref.GetName())}}, nil
}

// onSourceTag checks can_use_tag on the tag an actor is created from, if the
// request names one.
func onSourceTag(ref *ateapipb.ObjectRef, fldPath *field.Path) ([]check, field.ErrorList) {
	if ref == nil {
		return nil, nil
	}
	return onTag(RelationCanUseTag, ref, fldPath)
}

// both requires the checks from a and b, reporting the validation errors of
// both.
func both(aChecks []check, aErrs field.ErrorList, bChecks []check, bErrs field.ErrorList) ([]check, field.ErrorList) {
	if errs := append(aErrs, bErrs...); len(errs) > 0 {
		return nil, errs
	}
	return slices.Concat(aChecks, bChecks), nil
}

// rpcRule is the permission rule for one RPC.
type rpcRule struct {
	extract checkExtractor
	// authorizedInHandler, when set, says how the RPC's handler authorizes
	// the caller; the interceptor passes such RPCs through unchecked.
	authorizedInHandler string
}

func rule(extract checkExtractor) rpcRule {
	return rpcRule{extract: extract}
}

// inHandler registers an RPC whose handler authorizes the caller, as how
// describes.
func inHandler(how string) rpcRule {
	return rpcRule{authorizedInHandler: how}
}

// defaultRPCPermissions is the declarative registry mapping gRPC full method
// names to their permission rules. The interceptor denies any RPC missing
// from it.
var defaultRPCPermissions = map[string]rpcRule{
	// Atespaces.
	ateapipb.Control_CreateAtespace_FullMethodName: rule(checksOf(func(*ateapipb.CreateAtespaceRequest) ([]check, field.ErrorList) {
		return onGlobal(RelationCanCreateAtespace)
	})),
	ateapipb.Control_ListAtespaces_FullMethodName: rule(checksOf(func(*ateapipb.ListAtespacesRequest) ([]check, field.ErrorList) {
		return onGlobal(RelationCanListAtespaces)
	})),
	ateapipb.Control_GetAtespace_FullMethodName: rule(checksOf(func(r *ateapipb.GetAtespaceRequest) ([]check, field.ErrorList) {
		return onAtespace(RelationCanGet, r.GetAtespace().GetName(), field.NewPath("atespace", "name"))
	})),
	ateapipb.Control_DeleteAtespace_FullMethodName: rule(checksOf(func(r *ateapipb.DeleteAtespaceRequest) ([]check, field.ErrorList) {
		return onAtespace(RelationCanDelete, r.GetAtespace().GetName(), field.NewPath("atespace", "name"))
	})),

	// Access policies.
	ateapipb.Control_GetGlobalAccessPolicy_FullMethodName: rule(checksOf(func(*ateapipb.GetGlobalAccessPolicyRequest) ([]check, field.ErrorList) {
		return onGlobal(RelationCanGetAccessPolicy)
	})),
	ateapipb.Control_CreateGlobalAccessPolicy_FullMethodName: rule(checksOf(func(*ateapipb.CreateGlobalAccessPolicyRequest) ([]check, field.ErrorList) {
		return onGlobal(RelationCanCreateAccessPolicy)
	})),
	ateapipb.Control_UpdateGlobalAccessPolicy_FullMethodName: rule(checksOf(func(*ateapipb.UpdateGlobalAccessPolicyRequest) ([]check, field.ErrorList) {
		return onGlobal(RelationCanUpdateAccessPolicy)
	})),
	ateapipb.Control_GetAtespaceAccessPolicy_FullMethodName: rule(checksOf(func(r *ateapipb.GetAtespaceAccessPolicyRequest) ([]check, field.ErrorList) {
		return onAtespace(RelationCanGetAccessPolicy, r.GetAtespace().GetName(), field.NewPath("atespace", "name"))
	})),
	ateapipb.Control_CreateAtespaceAccessPolicy_FullMethodName: rule(checksOf(func(r *ateapipb.CreateAtespaceAccessPolicyRequest) ([]check, field.ErrorList) {
		return onAtespace(RelationCanCreateAccessPolicy, r.GetAtespace().GetName(), field.NewPath("atespace", "name"))
	})),
	ateapipb.Control_UpdateAtespaceAccessPolicy_FullMethodName: rule(checksOf(func(r *ateapipb.UpdateAtespaceAccessPolicyRequest) ([]check, field.ErrorList) {
		return onAtespace(RelationCanUpdateAccessPolicy, r.GetAtespace().GetName(), field.NewPath("atespace", "name"))
	})),
	ateapipb.Control_DeleteAtespaceAccessPolicy_FullMethodName: rule(checksOf(func(r *ateapipb.DeleteAtespaceAccessPolicyRequest) ([]check, field.ErrorList) {
		return onAtespace(RelationCanDeleteAccessPolicy, r.GetAtespace().GetName(), field.NewPath("atespace", "name"))
	})),

	// Actor templates.
	ateapipb.Control_CreateActorTemplate_FullMethodName: rule(checksOf(func(r *ateapipb.CreateActorTemplateRequest) ([]check, field.ErrorList) {
		return onAtespace(RelationCanCreateActorTemplate, r.GetActorTemplate().GetMetadata().GetAtespace(), field.NewPath("actor_template", "metadata", "atespace"))
	})),
	ateapipb.Control_GetActorTemplate_FullMethodName: rule(checksOf(func(r *ateapipb.GetActorTemplateRequest) ([]check, field.ErrorList) {
		return onActorTemplate(RelationCanGet, r.GetActorTemplate(), field.NewPath("actor_template"))
	})),
	ateapipb.Control_ListActorTemplates_FullMethodName: rule(checksOf(func(r *ateapipb.ListActorTemplatesRequest) ([]check, field.ErrorList) {
		return onAtespaceOrGlobal(RelationCanListActorTemplates, r.GetAtespace(), field.NewPath("atespace"))
	})),
	ateapipb.Control_DeleteActorTemplate_FullMethodName: rule(checksOf(func(r *ateapipb.DeleteActorTemplateRequest) ([]check, field.ErrorList) {
		return onActorTemplate(RelationCanDelete, r.GetActorTemplate(), field.NewPath("actor_template"))
	})),

	// Actors. Creating or updating an actor also requires can_use_template on
	// the actor template it runs, which may live in another atespace. Creating
	// one from an explicit source tag also requires can_use_tag on that tag;
	// the handler checks the tag's scope, completion and template.
	ateapipb.Control_CreateActor_FullMethodName: rule(checksOf(func(r *ateapipb.CreateActorRequest) ([]check, field.ErrorList) {
		actorChecks, actorErrs := onAtespace(RelationCanCreateActor, r.GetActor().GetMetadata().GetAtespace(), field.NewPath("actor", "metadata", "atespace"))
		templateChecks, templateErrs := onActorTemplate(RelationCanUseTemplate, r.GetActor().GetActorTemplate(), field.NewPath("actor", "actor_template"))
		checks, errs := both(actorChecks, actorErrs, templateChecks, templateErrs)
		tagChecks, tagErrs := onSourceTag(r.GetActor().GetSourceTag(), field.NewPath("actor", "source_tag"))
		return both(checks, errs, tagChecks, tagErrs)
	})),
	ateapipb.Control_GetActor_FullMethodName: rule(checksOf(func(r *ateapipb.GetActorRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanGet, r.GetActor(), field.NewPath("actor"))
	})),
	ateapipb.Control_ListActors_FullMethodName: rule(checksOf(func(r *ateapipb.ListActorsRequest) ([]check, field.ErrorList) {
		return onAtespaceOrGlobal(RelationCanListActors, r.GetAtespace(), field.NewPath("atespace"))
	})),
	ateapipb.Control_UpdateActor_FullMethodName: rule(checksOf(func(r *ateapipb.UpdateActorRequest) ([]check, field.ErrorList) {
		actorChecks, actorErrs := onActor(RelationCanUpdate, r.GetActor().GetMetadata(), field.NewPath("actor", "metadata"))
		templateChecks, templateErrs := onActorTemplate(RelationCanUseTemplate, r.GetActor().GetActorTemplate(), field.NewPath("actor", "actor_template"))
		return both(actorChecks, actorErrs, templateChecks, templateErrs)
	})),
	ateapipb.Control_DeleteActor_FullMethodName: rule(checksOf(func(r *ateapipb.DeleteActorRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanDelete, r.GetActor(), field.NewPath("actor"))
	})),

	// Actor lifecycle.
	ateapipb.Control_ResumeActor_FullMethodName: rule(checksOf(func(r *ateapipb.ResumeActorRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanResume, r.GetActor(), field.NewPath("actor"))
	})),
	ateapipb.Control_SuspendActor_FullMethodName: rule(checksOf(func(r *ateapipb.SuspendActorRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanSuspend, r.GetActor(), field.NewPath("actor"))
	})),
	ateapipb.Control_RevertActor_FullMethodName: rule(checksOf(func(r *ateapipb.RevertActorRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanRevert, r.GetActor(), field.NewPath("actor"))
	})),

	// Actor egress policies.
	ateapipb.Control_GetActorEgressPolicy_FullMethodName: rule(checksOf(func(r *ateapipb.GetActorEgressPolicyRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanGetEgressPolicy, r.GetActor(), field.NewPath("actor"))
	})),
	ateapipb.Control_CreateActorEgressPolicy_FullMethodName: rule(checksOf(func(r *ateapipb.CreateActorEgressPolicyRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanCreateEgressPolicy, r.GetActor(), field.NewPath("actor"))
	})),
	ateapipb.Control_UpdateActorEgressPolicy_FullMethodName: rule(checksOf(func(r *ateapipb.UpdateActorEgressPolicyRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanUpdateEgressPolicy, r.GetActor(), field.NewPath("actor"))
	})),
	ateapipb.Control_DeleteActorEgressPolicy_FullMethodName: rule(checksOf(func(r *ateapipb.DeleteActorEgressPolicyRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanDeleteEgressPolicy, r.GetActor(), field.NewPath("actor"))
	})),

	// Workers.
	ateapipb.Control_CreateWorker_FullMethodName: rule(checksOf(func(*ateapipb.CreateWorkerRequest) ([]check, field.ErrorList) {
		return onGlobal(RelationCanCreateWorker)
	})),
	ateapipb.Control_ListWorkers_FullMethodName: rule(checksOf(func(*ateapipb.ListWorkersRequest) ([]check, field.ErrorList) {
		return onGlobal(RelationCanListWorkers)
	})),
	ateapipb.Control_GetWorker_FullMethodName: rule(checksOf(func(r *ateapipb.GetWorkerRequest) ([]check, field.ErrorList) {
		return onWorker(RelationCanGet, r.GetWorker().GetName(), field.NewPath("worker", "name"))
	})),
	ateapipb.Control_UpdateWorker_FullMethodName: rule(checksOf(func(r *ateapipb.UpdateWorkerRequest) ([]check, field.ErrorList) {
		return onWorker(RelationCanUpdate, r.GetWorker().GetMetadata().GetName(), field.NewPath("worker", "metadata", "name"))
	})),
	ateapipb.Control_DeleteWorker_FullMethodName: rule(checksOf(func(r *ateapipb.DeleteWorkerRequest) ([]check, field.ErrorList) {
		return onWorker(RelationCanDelete, r.GetWorker().GetName(), field.NewPath("worker", "name"))
	})),
	ateapipb.Control_DrainWorker_FullMethodName: rule(checksOf(func(r *ateapipb.DrainWorkerRequest) ([]check, field.ErrorList) {
		return onWorker(RelationCanDrain, r.GetWorker().GetName(), field.NewPath("worker", "name"))
	})),
	ateapipb.Control_MarkWorkerAvailable_FullMethodName: rule(checksOf(func(r *ateapipb.MarkWorkerAvailableRequest) ([]check, field.ErrorList) {
		return onWorker(RelationCanUpdate, r.GetWorker().GetName(), field.NewPath("worker", "name"))
	})),
	ateapipb.Control_MarkWorkerUnavailable_FullMethodName: rule(checksOf(func(r *ateapipb.MarkWorkerUnavailableRequest) ([]check, field.ErrorList) {
		return onWorker(RelationCanUpdate, r.GetWorker().GetName(), field.NewPath("worker", "name"))
	})),
	ateapipb.Control_ListWorkerActorAssignments_FullMethodName: rule(checksOf(func(r *ateapipb.ListWorkerActorAssignmentsRequest) ([]check, field.ErrorList) {
		return onWorker(RelationCanListActorAssignments, r.GetWorker().GetName(), field.NewPath("worker", "name"))
	})),

	// Actor identity credentials.
	ateapipb.Control_MintActorJwt_FullMethodName: rule(checksOf(func(r *ateapipb.MintActorJwtRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanMintActorJWT, r.GetActor(), field.NewPath("actor"))
	})),
	ateapipb.Control_MintActorCertificate_FullMethodName: rule(checksOf(func(r *ateapipb.MintActorCertificateRequest) ([]check, field.ErrorList) {
		return onActor(RelationCanMintActorCertificate, r.GetActor(), field.NewPath("actor"))
	})),

	// Tags. CreateTag stores the tag in its source actor's atespace, so that is
	// the atespace checked. Validation also requires metadata.atespace to
	// match, but it runs after this interceptor.
	//
	// TODO: Creating and publishing tags expose an actor's data; see the TODO
	// on type tag in model.fga.
	ateapipb.Control_CreateTag_FullMethodName: rule(checksOf(func(r *ateapipb.CreateTagRequest) ([]check, field.ErrorList) {
		return onAtespace(RelationCanCreateTag, r.GetTag().GetSourceActor().GetAtespace(), field.NewPath("tag", "source_actor", "atespace"))
	})),
	ateapipb.Control_GetTag_FullMethodName: rule(checksOf(func(r *ateapipb.GetTagRequest) ([]check, field.ErrorList) {
		return onTag(RelationCanGet, r.GetTag(), field.NewPath("tag"))
	})),
	ateapipb.Control_ListTags_FullMethodName: rule(checksOf(func(r *ateapipb.ListTagsRequest) ([]check, field.ErrorList) {
		return onAtespaceOrGlobal(RelationCanListTags, r.GetAtespace(), field.NewPath("atespace"))
	})),
	ateapipb.Control_UpdateTag_FullMethodName: rule(checksOf(func(r *ateapipb.UpdateTagRequest) ([]check, field.ErrorList) {
		return onTag(RelationCanUpdate, r.GetTag().GetMetadata(), field.NewPath("tag", "metadata"))
	})),
	ateapipb.Control_DeleteTag_FullMethodName: rule(checksOf(func(r *ateapipb.DeleteTagRequest) ([]check, field.ErrorList) {
		return onTag(RelationCanDelete, r.GetTag(), field.NewPath("tag"))
	})),

	// WorkerService is served only to atelet, which each handler
	// authenticates by its mTLS SPIFFE ID.
	ateapipb.WorkerService_RegisterWorker_FullMethodName:             inHandler("ateletauth: caller is atelet on the Worker's node"),
	ateapipb.WorkerService_ReportActorCrashed_FullMethodName:         inHandler("ateletauth: caller is atelet on the node of the Worker hosting the Actor"),
	ateapipb.WorkerService_RequestActorSuspend_FullMethodName:        inHandler("ateletauth: caller is atelet on the node of the Worker hosting the Actor"),
	ateapipb.WorkerService_MintWorkerActorCertificate_FullMethodName: inHandler("ateletauth: caller is atelet on the node of the Worker the Actor is assigned to"),
	ateapipb.WorkerService_MintWorkerActorJwt_FullMethodName:         inHandler("ateletauth: caller is atelet on the node hosting the Actor"),
	ateapipb.WorkerService_ReportActorSnapshotState_FullMethodName:   inHandler("ateletauth: caller is atelet on the node the Actor is placed on or holds a local snapshot of it"),
}
