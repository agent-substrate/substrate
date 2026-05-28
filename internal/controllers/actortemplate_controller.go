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

	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	GoldenSnapshotCreationReason = "GoldenSnapshotCreation"

	// ActorTemplateFinalizer ensures the golden actor is suspended and deleted
	// from the ateom before the ActorTemplate resource is garbage-collected.
	ActorTemplateFinalizer = "ate.dev/actortemplate-finalizer"
)

type ActorTemplateReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	AteClient ateapipb.ControlClient
}

//+kubebuilder:rbac:groups=ate.dev,resources=actortemplates,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=ate.dev,resources=actortemplates/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=ate.dev,resources=actortemplates/finalizers,verbs=update
//+kubebuilder:rbac:groups=ate.dev,resources=workerpools,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

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

	// Handle deletion before any other work — once a DeletionTimestamp is
	// set, we must drive cleanup and remove the finalizer; the phase switch
	// would otherwise keep creating or driving the golden actor.
	if !at.GetDeletionTimestamp().IsZero() {
		return r.reconcileDelete(ctx, at)
	}

	// Ensure the finalizer is present before the phase switch creates a
	// golden actor. If we crashed between CreateActor and the finalizer add,
	// a kubectl delete would orphan the actor on the ateom.
	if !controllerutil.ContainsFinalizer(at, ActorTemplateFinalizer) {
		controllerutil.AddFinalizer(at, ActorTemplateFinalizer)
		if err := r.Update(ctx, at); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add finalizer: %w", err)
		}
		return ctrl.Result{}, nil
	}

	switch at.Status.Phase {
	case atev1alpha1.PhaseInitial:
		actorID := uuid.NewString()

		createReq := &ateapipb.CreateActorRequest{
			ActorId:                actorID,
			ActorTemplateNamespace: at.ObjectMeta.Namespace,
			ActorTemplateName:      at.ObjectMeta.Name,
		}
		_, err := r.AteClient.CreateActor(ctx, createReq)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("while creating golden actor: %w", err)
		}

		at.Status.Phase = atev1alpha1.PhaseResumeGoldenActor
		at.Status.GoldenActorID = actorID
		if err := r.Status().Update(ctx, at); err != nil {
			return ctrl.Result{}, err
		}
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
			ActorId: at.Status.GoldenActorID,
		}
		_, err := r.AteClient.ResumeActor(ctx, resumeReq)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("while resuming golden actor: %w", err)
		}

		at.Status.Phase = atev1alpha1.PhaseWaitGoldenActor
		at.Status.TakeGoldenSnapshotAt = metav1.NewTime(time.Now().Add(20 * time.Second))
		if err := r.Status().Update(ctx, at); err != nil {
			return ctrl.Result{}, err
		}
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
			ActorId: at.Status.GoldenActorID,
		}
		resp, err := r.AteClient.SuspendActor(ctx, req)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("while suspending golden actor: %w", err)
		}

		// Transition to PhaseReady
		at.Status.GoldenSnapshot = resp.GetActor().GetLastSnapshot()
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

		return ctrl.Result{}, nil
	case atev1alpha1.PhaseReady:
		return ctrl.Result{}, nil
	default:
		return ctrl.Result{}, fmt.Errorf("unrecognized phase %q", at.Status.Phase)
	}
}

// reconcileDelete drains the golden actor from the ateom and removes the
// finalizer. Suspend + Delete are both idempotent (workflow-level guards on
// IsComplete) and NotFound is treated as success so a retry after partial
// progress converges.
func (r *ActorTemplateReconciler) reconcileDelete(ctx context.Context, at *atev1alpha1.ActorTemplate) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(at, ActorTemplateFinalizer) {
		return ctrl.Result{}, nil
	}

	actorID := at.Status.GoldenActorID
	if actorID != "" {
		if _, err := r.AteClient.SuspendActor(ctx, &ateapipb.SuspendActorRequest{ActorId: actorID}); err != nil {
			if !isNotFound(err) {
				log.Error(err, "failed to suspend golden actor on deletion", "actorID", actorID)
				return ctrl.Result{}, fmt.Errorf("suspend before delete: %w", err)
			}
			log.Info("golden actor already absent on suspend", "actorID", actorID)
		}
		if _, err := r.AteClient.DeleteActor(ctx, &ateapipb.DeleteActorRequest{ActorId: actorID}); err != nil {
			if !isNotFound(err) {
				log.Error(err, "failed to delete golden actor", "actorID", actorID)
				return ctrl.Result{}, fmt.Errorf("delete golden actor: %w", err)
			}
			log.Info("golden actor already absent on delete", "actorID", actorID)
		}
	} else {
		log.Info("no golden actor ID recorded; removing finalizer without RPC calls")
	}

	controllerutil.RemoveFinalizer(at, ActorTemplateFinalizer)
	if err := r.Update(ctx, at); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// isNotFound reports whether the gRPC error carries codes.NotFound, so the
// caller can treat the ateom-side resource as already gone.
func isNotFound(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.NotFound
}

// SetupWithManager sets up the controller with the Manager.
func (r *ActorTemplateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&atev1alpha1.ActorTemplate{}).Complete(r)
}
