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
	"sort"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// actorTemplateGetter is the storage subset template resolution needs.
type actorTemplateGetter interface {
	GetActorTemplate(ctx context.Context, templateRef resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error)
}

// errActorTemplateNotFound matches (via errors.Is) resolution failures where
// the actor names a template that does not exist. Most callers return the
// error as is — it already carries FailedPrecondition — while delete
// tolerates it and cleans up without the template.
var errActorTemplateNotFound = status.New(codes.FailedPrecondition, "actor template not found").Err()

// resolveActorTemplate resolves an actor's template from whichever reference
// form it carries: the substrate ActorTemplate resource when actor_template
// is set, the ActorTemplate CRD otherwise. A missing template surfaces as a
// templateNotFoundError either way.
func resolveActorTemplate(ctx context.Context, st actorTemplateGetter, lister listersv1alpha1.ActorTemplateLister, actor *ateapipb.Actor) (*ateapipb.ActorTemplate, error) {
	if ref := actor.GetActorTemplate(); ref != nil {
		templateRef := resources.ActorTemplateRefFromObjectRef(ref)
		template, err := st.GetActorTemplate(ctx, templateRef)
		if errors.Is(err, store.ErrNotFound) {
			return nil, fmt.Errorf("%w; ObjectRef: %s ", errActorTemplateNotFound, templateRef)
		}
		if err != nil {
			return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
		}
		return template, nil
	}
	// TODO: remove this fallback when we cut over to substrate resources.
	crd, err := lister.ActorTemplates(actor.GetActorTemplateNamespace()).Get(actor.GetActorTemplateName())
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil, fmt.Errorf("%w; CRD %s/%s ", errActorTemplateNotFound, actor.GetActorTemplateNamespace(), actor.GetActorTemplateName())
		}
		return nil, fmt.Errorf("while getting ActorTemplate: %w", err)
	}
	return actorTemplateFromCRD(crd)
}

// actorTemplateObjectRef returns a fresh copy of the actor's substrate
// template reference, or nil for CRD-backed actors — fresh so records built
// from it never alias the actor message.
func actorTemplateObjectRef(actor *ateapipb.Actor) *ateapipb.ObjectRef {
	ref := actor.GetActorTemplate()
	if ref == nil {
		return nil
	}
	return &ateapipb.ObjectRef{Atespace: ref.GetAtespace(), Name: ref.GetName()}
}

// actorTemplateFromCRD projects an ActorTemplate CRD onto the substrate
// ActorTemplate proto, so the workflows handle a single template type while
// both the CRD and the substrate resource exist during the migration.
// It refuses a template whose workerSelector uses matchExpressions: the
// substrate Selector supports equality matching only, and dropping the
// expressions would schedule actors onto a wider pool set than declared.
func actorTemplateFromCRD(t *atev1alpha1.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	if t == nil {
		return nil, status.Error(codes.Internal, "nil ActorTemplate")
	}
	if sel := t.Spec.WorkerSelector; sel != nil && len(sel.MatchExpressions) > 0 {
		return nil, status.Errorf(codes.FailedPrecondition,
			"ActorTemplate %s/%s workerSelector uses matchExpressions; only matchLabels is supported",
			t.Namespace, t.Name)
	}
	out := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			// Using the t.Namespace as Atespace is ok because this is used in memory only,
			// never persists to store.
			Atespace: t.Namespace,
			Name:     t.Name,
			Uid:      string(t.GetUID()),
		},
		WorkerSelector: selectorFromLabelSelector(t.Spec.WorkerSelector),
		SnapshotsConfig: &ateapipb.SnapshotsConfig{
			StorageLocation: t.Spec.SnapshotsConfig.Location,
			OnPause:         toActorSnapshotContentScope(t.Spec.SnapshotsConfig.OnPause),
			OnCommit:        toActorSnapshotContentScope(t.Spec.SnapshotsConfig.OnCommit),
			OnResume: &ateapipb.OnResumeConfig{
				FromData: resumeSourceFromCRD(t.Spec.SnapshotsConfig.OnResume.FromData),
			},
		},
		// The CRD names no SandboxConfig object: CRD-backed actors resolve
		// sandbox assets from their worker pool instead.
		SandboxConfig: &ateapipb.SandboxConfig{
			SandboxClass: sandboxClassFromCRD(t.Spec.SandboxClass),
		},
		Resources: resourcesFromCRD(t.Spec.Resources),
		Status:    templateStatusFromCRD(t.Status),
	}
	for i := range t.Spec.Containers {
		out.Containers = append(out.Containers, containerFromCRD(&t.Spec.Containers[i]))
	}
	for i := range t.Spec.Volumes {
		out.Volumes = append(out.Volumes, volumeFromCRD(&t.Spec.Volumes[i]))
	}
	return out, nil
}

// selectorFromLabelSelector converts the equality half of the CRD selector;
// actorTemplateFromCRD has already rejected any matchExpressions.
func selectorFromLabelSelector(sel *metav1.LabelSelector) *ateapipb.Selector {
	if sel == nil {
		return nil
	}
	out := &ateapipb.Selector{}
	if len(sel.MatchLabels) > 0 {
		out.MatchLabels = maps.Clone(sel.MatchLabels)
	}
	return out
}

func resumeSourceFromCRD(in atev1alpha1.ResumeSource) ateapipb.ResumeSource {
	// Unset defaults to ColdBoot, mirroring the CRD's kubebuilder default.
	if in == atev1alpha1.ResumeSourceGolden {
		return ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN
	}
	return ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT
}

func sandboxClassFromCRD(in atev1alpha1.SandboxClass) ateapipb.SandboxClass {
	switch in {
	case atev1alpha1.SandboxClassGvisor:
		return ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR
	case atev1alpha1.SandboxClassMicroVM:
		return ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM
	default:
		return ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED
	}
}

func resourcesFromCRD(in *corev1.ResourceRequirements) *ateapipb.Resources {
	if in == nil {
		return nil
	}
	return limitsFromCRD(in.Limits)
}

func limitsFromCRD(limits map[corev1.ResourceName]resource.Quantity) *ateapipb.Resources {
	if len(limits) == 0 {
		return nil
	}
	names := make([]string, 0, len(limits))
	for name := range limits {
		names = append(names, string(name))
	}
	// Map iteration is unordered; sort for a deterministic proto.
	sort.Strings(names)
	out := &ateapipb.Resources{}
	for _, name := range names {
		q := limits[corev1.ResourceName(name)]
		out.Limits = append(out.Limits, &ateapipb.Limits{Name: name, Quantity: q.String()})
	}
	return out
}

func containerFromCRD(c *atev1alpha1.Container) *ateapipb.Container {
	out := &ateapipb.Container{
		Name:    c.Name,
		Image:   c.Image,
		Command: append([]string(nil), c.Command...),
		Args:    append([]string(nil), c.Args...),
	}
	for _, env := range c.Env {
		out.Env = append(out.Env, &ateapipb.EnvVar{Name: env.Name, Value: env.Value})
	}
	if r := c.Readyz; r != nil {
		out.Readyz = &ateapipb.ContainerReadyz{TimeoutSeconds: r.TimeoutSeconds}
		if r.HTTPGet != nil {
			out.Readyz.HttpGet = &ateapipb.HTTPGetAction{Path: r.HTTPGet.Path, Port: r.HTTPGet.Port}
		}
	}
	for _, m := range c.VolumeMounts {
		out.VolumeMounts = append(out.VolumeMounts, &ateapipb.VolumeMount{Name: m.Name, MountPath: m.MountPath})
	}
	if sc := c.SecurityContext; sc != nil && sc.Capabilities != nil {
		out.SecurityContext = &ateapipb.SecurityContext{
			Capabilities: &ateapipb.Capabilities{
				Add:  capabilityNames(sc.Capabilities.Add),
				Drop: capabilityNames(sc.Capabilities.Drop),
			},
		}
	}
	if r := c.Resources; r != nil {
		out.Resources = limitsFromCRD(r.Limits)
	}
	return out
}

func capabilityNames(in []atev1alpha1.Capability) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for _, c := range in {
		out = append(out, string(c))
	}
	return out
}

func volumeFromCRD(v *atev1alpha1.Volume) *ateapipb.Volume {
	out := &ateapipb.Volume{Name: v.Name}
	switch {
	case v.DurableDir != nil:
		out.Type = "DurableDir"
		out.DurableDir = &ateapipb.DurableDirVolumeSource{}
	case v.ExternalVolumeTemplate != nil:
		out.Type = "ExternalVolumeTemplate"
		out.ExternalVolumeTemplate = &ateapipb.ExternalVolumeTemplate{
			Capacity:         v.ExternalVolumeTemplate.Capacity.String(),
			StorageClassName: v.ExternalVolumeTemplate.StorageClassName,
		}
	case v.Image != nil:
		out.Type = "Image"
		out.Image = &ateapipb.ImageVolumeSource{Reference: v.Image.Reference}
	case v.SystemInfo != nil:
		out.Type = "SystemInfo"
		out.SystemInfo = systemInfoFromCRD(v.SystemInfo)
	}
	return out
}

func systemInfoFromCRD(in *atev1alpha1.SystemInfoVolumeSource) *ateapipb.SystemInfoVolumeSource {
	out := &ateapipb.SystemInfoVolumeSource{}
	for _, ds := range in.DataSources {
		switch {
		case ds.ActorMetadata != nil:
			meta := &ateapipb.ActorMetadataDataSource{}
			for _, item := range ds.ActorMetadata.Items {
				meta.Items = append(meta.Items, &ateapipb.ActorMetadataItem{
					Field: actorMetadataFieldFromCRD(item.Field),
					Path:  item.Path,
				})
			}
			out.DataSources = append(out.DataSources, &ateapipb.SystemInfoDataSource{ActorMetadata: meta})
		case ds.TrustBundle != nil:
			out.DataSources = append(out.DataSources, &ateapipb.SystemInfoDataSource{
				TrustBundle: &ateapipb.TrustBundleDataSource{Name: ds.TrustBundle.Name, Path: ds.TrustBundle.Path},
			})
		}
	}
	return out
}

func actorMetadataFieldFromCRD(in atev1alpha1.ActorMetadataField) ateapipb.ActorMetadataField {
	switch in {
	case atev1alpha1.ActorMetadataFieldName:
		return ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME
	case atev1alpha1.ActorMetadataFieldAtespace:
		return ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE
	case atev1alpha1.ActorMetadataFieldUID:
		return ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UID
	default:
		return ateapipb.ActorMetadataField_ACTOR_METADATA_FIELD_UNSPECIFIED
	}
}

// templateStatusFromCRD maps only what the substrate proto models: the CRD's
// phase machinery belongs to the CRD controller, while converted templates
// are consumed by the actor workflows, which need the golden snapshot ref.
func templateStatusFromCRD(in atev1alpha1.ActorTemplateStatus) *ateapipb.ActorTemplateStatus {
	out := &ateapipb.ActorTemplateStatus{}
	if in.GoldenSnapshot != "" {
		// The CRD stores only the snapshot name; golden snapshots always live
		// in the reserved golden atespace.
		out.GoldenSnapshotStatus = &ateapipb.GoldenSnapshotStatus{
			GoldenSnapshot: &ateapipb.ObjectRef{
				Atespace: resources.GoldenActorAtespace,
				Name:     in.GoldenSnapshot,
			},
		}
	}
	return out
}
