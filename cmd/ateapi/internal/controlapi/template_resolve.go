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

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// crdTemplateFromVersion projects a control-plane (ActorTemplate,
// ActorTemplateVersion) pair onto an in-memory CRD ActorTemplate, so every
// workflow step that consumes a template (workload spec, snapshot scopes,
// scheduling, volumes, golden resume) works unchanged on both paths.
//
// Transitional shim: remove together with the CRD ActorTemplate path.
func crdTemplateFromVersion(at *ateapipb.ActorTemplate, atv *ateapipb.ActorTemplateVersion) (*atev1alpha1.ActorTemplate, error) {
	spec := atv.GetSpec()

	tmpl := &atev1alpha1.ActorTemplate{
		// The UID is the version's: snapshot provenance and clone checks then
		// compare actors against the exact immutable spec they ran.
		ObjectMeta: metav1.ObjectMeta{
			Name: atv.GetMetadata().GetName(),
			UID:  types.UID(atv.GetMetadata().GetUid()),
		},
		Spec: atev1alpha1.ActorTemplateSpec{
			PauseImage:   spec.GetPauseImage(),
			SandboxClass: sandboxClassFromProto(atv.GetStatus().GetResolvedSandbox().GetSandboxClass()),
			SnapshotsConfig: atev1alpha1.SnapshotsConfig{
				Location: spec.GetSnapshotsConfig().GetStorageLocation(),
				OnPause:  snapshotScopeFromProto(spec.GetSnapshotsConfig().GetOnPause()),
				OnCommit: snapshotScopeFromProto(spec.GetSnapshotsConfig().GetOnCommit()),
				OnResume: atev1alpha1.OnResumeConfig{
					FromData: resumeSourceFromProto(spec.GetSnapshotsConfig().GetOnResume().GetFromData()),
				},
			},
		},
		Status: atev1alpha1.ActorTemplateStatus{
			GoldenSnapshot: atv.GetStatus().GetGoldenSnapshot().GetName(),
		},
	}

	if labels := at.GetSpec().GetWorkerSelector().GetMatchLabels(); len(labels) > 0 {
		tmpl.Spec.WorkerSelector = &metav1.LabelSelector{MatchLabels: labels}
	}

	for _, vol := range spec.GetVolumes() {
		out := atev1alpha1.Volume{Name: vol.GetName()}
		switch {
		case vol.GetDurableDir() != nil:
			out.DurableDir = &atev1alpha1.DurableDirVolumeSource{}
		case vol.GetExternalVolumeTemplate() != nil:
			capacity, err := resource.ParseQuantity(vol.GetExternalVolumeTemplate().GetCapacity())
			if err != nil {
				return nil, fmt.Errorf("volume %q has invalid capacity: %w", vol.GetName(), err)
			}
			out.ExternalVolumeTemplate = &atev1alpha1.ExternalVolumeTemplate{
				Capacity:         capacity,
				StorageClassName: vol.GetExternalVolumeTemplate().GetStorageClassName(),
			}
		}
		tmpl.Spec.Volumes = append(tmpl.Spec.Volumes, out)
	}

	for _, ctr := range spec.GetContainers() {
		out := atev1alpha1.Container{
			Name:    ctr.GetName(),
			Image:   ctr.GetImage(),
			Command: ctr.GetCommand(),
			Args:    ctr.GetArgs(),
		}
		for _, env := range ctr.GetEnv() {
			value := env.GetValue()
			out.Env = append(out.Env, atev1alpha1.EnvVar{Name: env.GetName(), Value: &value})
		}
		if readyz := ctr.GetReadyz(); readyz != nil {
			out.Readyz = &atev1alpha1.ContainerReadyz{
				TimeoutSeconds: readyz.GetTimeoutSeconds(),
				HTTPGet: &atev1alpha1.HTTPGetAction{
					Path: readyz.GetHttpGet().GetPath(),
					Port: readyz.GetHttpGet().GetPort(),
				},
			}
		}
		for _, mount := range ctr.GetVolumeMounts() {
			out.VolumeMounts = append(out.VolumeMounts, atev1alpha1.VolumeMount{
				Name:      mount.GetName(),
				MountPath: mount.GetMountPath(),
			})
		}
		tmpl.Spec.Containers = append(tmpl.Spec.Containers, out)
	}

	return tmpl, nil
}

func snapshotScopeFromProto(scope ateapipb.SnapshotContentScope) atev1alpha1.SnapshotScope {
	if scope == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		return atev1alpha1.SnapshotScopeData
	}
	return atev1alpha1.SnapshotScopeFull
}

func resumeSourceFromProto(source ateapipb.ResumeSource) atev1alpha1.ResumeSource {
	if source == ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN {
		return atev1alpha1.ResumeSourceGolden
	}
	return atev1alpha1.ResumeSourceColdBoot
}

// ateletSandboxAssetsFromAPI converts the SandboxAssets frozen into an
// ActorTemplateVersion's status into the proto atelet consumes; the twin of
// sandboxAssetsProto for the pre-resolved case.
func ateletSandboxAssetsFromAPI(in *ateapipb.SandboxAssets) *ateletpb.SandboxAssets {
	out := &ateletpb.SandboxAssets{
		SandboxClass: string(sandboxClassFromProto(in.GetSandboxClass())),
		Assets:       make(map[string]*ateletpb.ArchAssets, len(in.GetAssets())),
	}
	for arch, files := range in.GetAssets() {
		archAssets := &ateletpb.ArchAssets{Files: make(map[string]*ateletpb.AssetFile, len(files.GetFiles()))}
		for name, f := range files.GetFiles() {
			archAssets.Files[name] = &ateletpb.AssetFile{Url: f.GetUrl(), Sha256: f.GetSha256()}
		}
		out.Assets[arch] = archAssets
	}
	return out
}

// templateRefForAtelet returns the template identity recorded on atelet
// requests and snapshots: the CRD namespace/name, or ("", version-name) for
// native actors. Consumers use it for metrics attribution and provenance
// only, never in filesystem paths.
func templateRefForAtelet(actor *ateapipb.Actor) (namespace, name string) {
	if actor.GetActorTemplateVersion() != "" {
		return "", actor.GetActorTemplateVersion()
	}
	return actor.GetActorTemplateNamespace(), actor.GetActorTemplateName()
}

// resolveTemplateForActor loads the template an actor runs from: the
// control-plane (ActorTemplate, ActorTemplateVersion) pair projected onto a
// synthetic CRD template when the actor carries a version pin, or the CRD
// ActorTemplate named by namespace/name otherwise. frozenAssets is non-nil
// only on the native path, where boot resumes must use the version's frozen
// sandbox instead of resolving it from the worker pool.
func resolveTemplateForActor(ctx context.Context, persistence store.Interface, lister listersv1alpha1.ActorTemplateLister, actor *ateapipb.Actor) (tmpl *atev1alpha1.ActorTemplate, frozenAssets *ateletpb.SandboxAssets, err error) {
	if actor.GetActorTemplateVersion() == "" {
		tmpl, err := lister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
		if err != nil {
			return nil, nil, fmt.Errorf("while getting ActorTemplate: %w", err)
		}
		return tmpl, nil, nil
	}

	at, err := persistence.GetActorTemplate(ctx, actor.GetActorTemplate())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, status.Errorf(codes.FailedPrecondition, "ActorTemplate %q not found", actor.GetActorTemplate())
		}
		return nil, nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	atv, err := persistence.GetActorTemplateVersion(ctx, actor.GetActorTemplateVersion())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q not found", actor.GetActorTemplateVersion())
		}
		return nil, nil, fmt.Errorf("while getting ActorTemplateVersion: %w", err)
	}
	if atv.GetActorTemplate().GetName() != at.GetMetadata().GetName() {
		return nil, nil, status.Errorf(codes.FailedPrecondition,
			"ActorTemplateVersion %q belongs to ActorTemplate %q, not %q",
			atv.GetMetadata().GetName(), atv.GetActorTemplate().GetName(), at.GetMetadata().GetName())
	}

	tmpl, err = crdTemplateFromVersion(at, atv)
	if err != nil {
		return nil, nil, err
	}
	return tmpl, ateletSandboxAssetsFromAPI(atv.GetStatus().GetResolvedSandbox()), nil
}
