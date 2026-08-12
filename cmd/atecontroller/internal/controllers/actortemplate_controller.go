// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	GoldenSnapshotCreationReason = "GoldenSnapshotCreation"

	// actorTemplateControllerName is the source component recorded on Events
	// emitted for ActorTemplate reconciliation.
	actorTemplateControllerName = "actortemplate-controller"

	// goldenSnapshotWarmup is the default wall-clock delay between resuming
	// the golden actor and taking its snapshot, used as a coarse "give the
	// workload time to finish initializing" fallback for templates without
	// a readiness probe. Templates whose containers all declare readyz skip
	// this wait — ResumeActor only returns once readyz reports 200, so the
	// workload is already initialized by the time we get here.
	goldenSnapshotWarmup = 20 * time.Second
)

type ActorTemplateReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	AteClient ateapipb.ControlClient

	// Recorder emits Kubernetes Events against the reconciled ActorTemplate so
	// golden-actor lifecycle progress and failures surface in
	// `kubectl describe`. Wired in SetupWithManager; may be left unset (nil) in
	// unit tests that construct the reconciler directly — such tests inject a
	// record.NewFakeRecorder so Reconcile does not panic.
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=ate.dev,resources=actortemplates,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ate.dev,resources=actortemplates/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ate.dev,resources=actortemplates/finalizers,verbs=update
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch,namespace=ate-system

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *ActorTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Fetch actor template
	at := &atev1alpha1.ActorTemplate{}
	if err := r.Get(ctx, req.NamespacedName, at); err != nil {
		if k8errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get actor template %q: %w", req.NamespacedName, err)
	}

	// Handle deletion
	if !at.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, nil
	}

	switch at.Status.Phase {
	case atev1alpha1.PhaseInitial:
		actorName := string(at.UID)

		// Golden actors live in the reserved ate-golden system atespace.
		_, err := r.AteClient.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
			Atespace: &ateapipb.Atespace{
				Metadata: &ateapipb.ResourceMetadata{
					Name: resources.GoldenActorAtespace,
				},
			},
		})
		if err != nil && status.Code(err) != codes.AlreadyExists {
			r.Recorder.Eventf(at, corev1.EventTypeWarning, "GoldenActorCreateFailed",
				"Failed to ensure golden atespace %q: %v", resources.GoldenActorAtespace, err)
			return ctrl.Result{}, fmt.Errorf("while ensuring atespace %q: %w", resources.GoldenActorAtespace, err)
		}

		createReq := &ateapipb.CreateActorRequest{
			Actor: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace: resources.GoldenActorAtespace,
					Name:     actorName,
				},
				ActorTemplateNamespace: at.ObjectMeta.Namespace,
				ActorTemplateName:      at.ObjectMeta.Name,
			},
		}
		_, err = r.AteClient.CreateActor(ctx, createReq)
		if err != nil && status.Code(err) != codes.AlreadyExists {
			r.Recorder.Eventf(at, corev1.EventTypeWarning, "GoldenActorCreateFailed",
				"Failed to create golden actor %q: %v", actorName, err)
			return ctrl.Result{}, fmt.Errorf("while creating golden actor: %w", err)
		}

		at.Status.Phase = atev1alpha1.PhaseResumeGoldenActor
		at.Status.GoldenActorID = actorName
		if err := r.Status().Update(ctx, at); err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Eventf(at, corev1.EventTypeNormal, "GoldenActorCreated",
			"Created golden actor %q in atespace %q", actorName, resources.GoldenActorAtespace)
		return ctrl.Result{}, nil

	case atev1alpha1.PhaseResumeGoldenActor:

		// TODO(ateom): If resumption fails because the ateom or atelet is not
		// quite ready, we can end up leaking a worker that thinks it's assigned
		// to the golden actor.  We should persist the golden actor ID first,
		// then drive resume as a separate step.

		// Resuming when the ActorTemplate has no golden snapshot results in the
		// workload being freshly booted.
		//
		// TODO: Maybe this should go through a different RPC dedicated to
		// booting an actor from scratch.
		resumeReq := &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: at.Status.GoldenActorID},
		}
		_, err := r.AteClient.ResumeActor(ctx, resumeReq)
		if err != nil {
			r.Recorder.Eventf(at, corev1.EventTypeWarning, "GoldenActorResumeFailed",
				"Failed to resume golden actor %q: %v", at.Status.GoldenActorID, err)
			return ctrl.Result{}, fmt.Errorf("while resuming golden actor: %w", err)
		}

		at.Status.Phase = atev1alpha1.PhaseWaitGoldenActor
		at.Status.TakeGoldenSnapshotAt = metav1.NewTime(time.Now().Add(goldenSnapshotWarmupFor(at)))
		if err := r.Status().Update(ctx, at); err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Eventf(at, corev1.EventTypeNormal, "GoldenActorResumed",
			"Resumed golden actor %q; will snapshot at %s", at.Status.GoldenActorID, at.Status.TakeGoldenSnapshotAt.Format(time.RFC3339))
		return ctrl.Result{}, nil

	case atev1alpha1.PhaseWaitGoldenActor:
		// Wait until the snapshot time.
		rem := time.Until(at.Status.TakeGoldenSnapshotAt.Time)
		if rem >= 0 {
			return ctrl.Result{RequeueAfter: rem}, nil
		}

		// TODO: Need to be more resilient --- if suspendactor tells us
		// conflict, we should fetch the suspended actor and read the snapshot
		// from it.

		req := &ateapipb.SuspendActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: at.Status.GoldenActorID},
		}
		resp, err := r.AteClient.SuspendActor(ctx, req)
		if err != nil {
			r.Recorder.Eventf(at, corev1.EventTypeWarning, "GoldenSnapshotFailed",
				"Failed to suspend golden actor %q for snapshot: %v", at.Status.GoldenActorID, err)
			return ctrl.Result{}, fmt.Errorf("while suspending golden actor: %w", err)
		}

		snapshot := resp.GetActor().GetLatestSnapshot()
		if snapshot == nil {
			r.Recorder.Eventf(at, corev1.EventTypeWarning, "GoldenSnapshotFailed",
				"Suspending golden actor %q returned no ActorSnapshot", at.Status.GoldenActorID)
			return ctrl.Result{}, fmt.Errorf("suspending golden actor returned no ActorSnapshot")
		}

		// Transition to PhaseReady
		at.Status.GoldenSnapshot = snapshot.GetName()
		at.Status.Phase = atev1alpha1.PhaseReady
		meta.SetStatusCondition(&at.Status.Conditions, metav1.Condition{
			Type:    "Ready",
			Status:  metav1.ConditionTrue,
			Reason:  "Ready",
			Message: "Actor template is ready for use",
		})
		if err := r.Status().Update(ctx, at); err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Eventf(at, corev1.EventTypeNormal, "Ready",
			"Golden snapshot %q captured; ActorTemplate is ready for use", snapshot.GetName())

		return ctrl.Result{}, nil
	case atev1alpha1.PhaseReady:
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, fmt.Errorf("unrecognized phase %q", at.Status.Phase)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ActorTemplateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor(actorTemplateControllerName)
	}
	return ctrl.NewControllerManagedBy(mgr).For(&atev1alpha1.ActorTemplate{}).Complete(r)
}

// goldenSnapshotWarmupFor returns 0 when every container in the template has
// a readyz probe (so ResumeActor already blocked until the workload reported
// 200), and the default warmup otherwise. A template with no containers
// keeps the default — there is nothing to gate on.
func goldenSnapshotWarmupFor(at *atev1alpha1.ActorTemplate) time.Duration {
	containers := at.Spec.Containers
	if len(containers) == 0 {
		return goldenSnapshotWarmup
	}
	for i := range containers {
		if containers[i].Readyz == nil {
			return goldenSnapshotWarmup
		}
	}
	return 0
}
