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

package harness

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/internal/resources"
)

// retryInterval is how long ResumeActor and RetryAborted wait between attempts.
const retryInterval = 20 * time.Millisecond

const (
	// StorageLocation is where ate-api keeps snapshots.
	StorageLocation = "gs://harness-snapshots"
	// Image is the image of the container harness templates run.
	Image = "main@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	// TemplateCPUMillicores and TemplateMemoryBytes are the resource limits
	// of NewTemplate's actors.
	TemplateCPUMillicores = 1000
	TemplateMemoryBytes   = 512 << 20
)

// TemplateOption customizes a template created by NewTemplate.
type TemplateOption func(*ateapi.ActorTemplate)

// WithExternalVolume gives the template an external volume, provisioned by
// the harness's volume plugin and mounted at mountPath in its container.
func WithExternalVolume(name, mountPath string) TemplateOption {
	return func(tmpl *ateapi.ActorTemplate) {
		tmpl.Volumes = append(tmpl.Volumes, &ateapi.Volume{
			Name: name,
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
				StorageClassName: StorageClass,
				CapacityBytes:    1 << 30,
			}},
		})
		c := tmpl.Containers[0]
		c.VolumeMounts = append(c.VolumeMounts, &ateapi.VolumeMount{Name: name, MountPath: mountPath})
	}
}

// CreateAtespace creates the atespace name.
func (h *Harness) CreateAtespace(name string) string {
	h.t.Helper()
	if _, err := h.Control.CreateAtespace(h.ctx, &ateapi.CreateAtespaceRequest{Atespace: &ateapi.Atespace{
		Metadata: &ateapi.ResourceMetadata{Name: name},
	}}); err != nil {
		h.t.Fatalf("creating atespace %s: %v", name, err)
	}
	return name
}

// NewTemplate creates an ActorTemplate in atespace that places its actors on
// pool, and waits for ate-api to publish its golden snapshot. The pool needs a
// worker with room for the golden actor. An option that sets a preview field
// needs a harness WithPreviewClient.
func (h *Harness) NewTemplate(atespace string, pool *WorkerPool, opts ...TemplateOption) *ateapi.ActorTemplate {
	h.t.Helper()
	tmpl := &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: fmt.Sprintf("tmpl-%d", h.next())},
		Containers: []*ateapi.Container{{
			Name:    "main",
			Image:   Image,
			Command: []string{"/main"},
			// A wakeup probe on every container lets the golden snapshot be
			// taken as soon as the golden actor runs, with no warmup wait.
			WakeupProbe: &ateapi.WakeupProbe{
				HttpGet: &ateapi.HttpGetAction{Path: "/healthz", Port: 8080},
			},
		}},
		WorkerSelector: &ateapi.Selector{MatchLabels: pool.Labels},
		Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(TemplateCPUMillicores, TemplateMemoryBytes)},
	}
	for _, opt := range opts {
		opt(tmpl)
	}
	created, err := h.Control.CreateActorTemplate(h.ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: tmpl})
	if err != nil {
		h.t.Fatalf("creating template %s: %v", tmpl.GetMetadata().GetName(), err)
	}
	ref := &ateapi.ObjectRef{Atespace: created.GetMetadata().GetAtespace(), Name: created.GetMetadata().GetName()}

	var ready *ateapi.ActorTemplate
	h.Eventually(fmt.Sprintf("the golden snapshot of template %s", ref.GetName()), func() bool {
		got, err := h.Control.GetActorTemplate(h.ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: ref})
		if err != nil {
			return false
		}
		if msg := got.GetStatus().GetGoldenSnapshotStatus().GetErrorMessage(); msg != "" {
			h.t.Fatalf("golden snapshot of template %s failed: %s", ref.GetName(), msg)
		}
		ready = got
		return got.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil
	})
	return ready
}

// ActorOption customizes an actor created by NewActor.
type ActorOption func(*ateapi.Actor)

// NewActor creates a SUSPENDED actor from tmpl in atespace.
func (h *Harness) NewActor(atespace string, tmpl *ateapi.ActorTemplate, opts ...ActorOption) *ateapi.Actor {
	h.t.Helper()
	actor := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: fmt.Sprintf("actor-%d", h.next())},
		ActorTemplate: &ateapi.ObjectRef{
			Atespace: tmpl.GetMetadata().GetAtespace(),
			Name:     tmpl.GetMetadata().GetName(),
		},
	}
	for _, opt := range opts {
		opt(actor)
	}
	created, err := h.Control.CreateActor(h.ctx, &ateapi.CreateActorRequest{Actor: actor})
	if err != nil {
		h.t.Fatalf("creating actor %s: %v", actor.GetMetadata().GetName(), err)
	}
	return created
}

// RunningActor creates an actor from tmpl in atespace and resumes it.
func (h *Harness) RunningActor(atespace string, tmpl *ateapi.ActorTemplate, opts ...ActorOption) *ateapi.Actor {
	h.t.Helper()
	actor := h.NewActor(atespace, tmpl, opts...)
	resp, err := h.ResumeActor(h.ctx, &ateapi.ResumeActorRequest{Actor: ActorRef(actor)})
	if err != nil {
		h.t.Fatalf("resuming actor %s: %v", actor.GetMetadata().GetName(), err)
	}
	return resp.GetActor()
}

// SuspendedActor creates an actor from tmpl in atespace, resumes it, and
// suspends it synchronously, so it holds a durable snapshot of its own.
func (h *Harness) SuspendedActor(atespace string, tmpl *ateapi.ActorTemplate, opts ...ActorOption) *ateapi.Actor {
	h.t.Helper()
	running := h.RunningActor(atespace, tmpl, opts...)
	resp, err := h.Control.SuspendActor(h.ctx, &ateapi.SuspendActorRequest{
		Actor:   ActorRef(running),
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		h.t.Fatalf("suspending actor %s: %v", running.GetMetadata().GetName(), err)
	}
	return resp.GetActor()
}

// AsyncSuspendedActor creates an actor from tmpl in atespace, resumes it, and
// suspends it asynchronously, so its latest snapshot is local to its node and
// not uploaded.
func (h *Harness) AsyncSuspendedActor(atespace string, tmpl *ateapi.ActorTemplate, opts ...ActorOption) *ateapi.Actor {
	h.t.Helper()
	running := h.RunningActor(atespace, tmpl, opts...)
	resp, err := h.Control.SuspendActor(h.ctx, &ateapi.SuspendActorRequest{
		Actor:   ActorRef(running),
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS},
	})
	if err != nil {
		h.t.Fatalf("suspending actor %s: %v", running.GetMetadata().GetName(), err)
	}
	return resp.GetActor()
}

// CrashedActor creates an actor from tmpl in atespace, resumes it, and crashes
// it by failing the checkpoint of a suspend.
func (h *Harness) CrashedActor(atespace string, tmpl *ateapi.ActorTemplate, opts ...ActorOption) *ateapi.Actor {
	h.t.Helper()
	running := h.RunningActor(atespace, tmpl, opts...)
	w := h.WorkerOf(running)
	w.Node.Atelet.On(AteletCheckpoint).FailNext(ErrSandboxFailed)
	if _, err := h.Control.SuspendActor(h.ctx, &ateapi.SuspendActorRequest{Actor: ActorRef(running)}); err == nil {
		h.t.Fatalf("suspending actor %s succeeded despite a failing checkpoint", running.GetMetadata().GetName())
	}
	crashed := h.GetActor(ActorRef(running))
	if got := crashed.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_CRASHED {
		h.t.Fatalf("actor %s is %v after a failed checkpoint, want CRASHED", running.GetMetadata().GetName(), got)
	}
	return crashed
}

// ResumeActor calls Control.ResumeActor, retrying as a client does while
// ate-api has not yet caught up with workers and atelets the test just set up,
// released or stopped: it may report no free worker, no atelet on the worker's
// node, or fail to reach an atelet the test stopped. Any other error is
// returned at once.
func (h *Harness) ResumeActor(ctx context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	deadline := time.Now().Add(waitTimeout)
	for {
		resp, err := h.Control.ResumeActor(ctx, req)
		if err == nil || !catchingUp(err) || time.Now().After(deadline) {
			return resp, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(retryInterval):
		}
	}
}

// RetryAborted calls call, retrying as a client does while it fails with
// ABORTED, such as while another operation holds the resource's lease. Any
// other result is returned at once.
func RetryAborted(ctx context.Context, call func() error) error {
	deadline := time.Now().Add(waitTimeout)
	for {
		err := call()
		if status.Code(err) != codes.Aborted || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(retryInterval):
		}
	}
}

// catchingUp reports whether err is ate-api answering from caches that have
// not yet seen what the test set up: its scheduler's view of workers, or its
// index of atelets.
func catchingUp(err error) bool {
	return status.Code(err) == codes.ResourceExhausted ||
		strings.Contains(err.Error(), controlapi.ErrNoAteletOnNode.Error()) ||
		strings.Contains(err.Error(), errNoAtelet.Error())
}

// GetActor reads the actor through the Control API, failing the test if it
// cannot.
func (h *Harness) GetActor(ref *ateapi.ObjectRef) *ateapi.Actor {
	h.t.Helper()
	actor, err := h.Control.GetActor(h.ctx, &ateapi.GetActorRequest{Actor: ref})
	if err != nil {
		h.t.Fatalf("getting actor %s/%s: %v", ref.GetAtespace(), ref.GetName(), err)
	}
	return actor
}

// GetWorker reads the worker through the Control API, failing the test if it
// cannot.
func (h *Harness) GetWorker(w *Worker) *ateapi.Worker {
	h.t.Helper()
	worker, err := h.Control.GetWorker(h.ctx, &ateapi.GetWorkerRequest{Worker: w.Ref()})
	if err != nil {
		h.t.Fatalf("getting worker %s: %v", w.Pod, err)
	}
	return worker
}

// WorkerOf returns the worker the actor is assigned to, as its status says,
// failing the test if it has none.
func (h *Harness) WorkerOf(actor *ateapi.Actor) *Worker {
	h.t.Helper()
	name := actor.GetStatus().GetWorkerAssignment().GetWorker().GetName()
	w := h.worker(name)
	if w == nil {
		h.t.Fatalf("actor %s is assigned to worker %q, which the test did not add", actor.GetMetadata().GetName(), name)
	}
	return w
}

// ActorRef is the actor's reference in the Control API.
func ActorRef(actor *ateapi.Actor) *ateapi.ObjectRef {
	return &ateapi.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
}
