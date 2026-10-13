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
	"testing"
	"testing/synctest"
	"time"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

func TestReconcileOne_InterruptedCaptureRestartsWarmup(t *testing.T) {
	for _, state := range []ateapipb.ActorState{
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := newFakeTemplateStore(testTemplate(withoutWakeupProbe))
				control := &fakeGoldenControl{exists: true, goldenState: state, goldenSnapshot: "gs://bucket/interrupted"}
				r := newTestTemplateReconciler(st, control)
				defer r.queue.ShutDown()
				delay, err := r.reconcileOne(t.Context(), testTemplateRef)
				if err != nil {
					t.Fatal(err)
				}
				if delay != goldenSnapshotWarmup || st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag() != nil {
					t.Fatalf("interrupted capture returned delay %s and tag %v, want a fresh warmup", delay, st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag())
				}
			})
		})
	}
}

type interferingGoldenControl struct {
	*fakeGoldenControl
	beforeCapture     bool
	beforePublication bool
}

func (c *interferingGoldenControl) suspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest, precondition *store.Precondition) (*ateapipb.SuspendActorResponse, error) {
	if c.beforeCapture {
		c.beforeCapture = false
		if _, err := c.fakeGoldenControl.PauseActor(ctx, &ateapipb.PauseActorRequest{Actor: req.GetActor()}); err != nil {
			return nil, err
		}
	}
	return c.fakeGoldenControl.suspendActor(ctx, req, precondition)
}

func (c *interferingGoldenControl) createTag(ctx context.Context, req *ateapipb.CreateTagRequest, precondition *store.Precondition) (*ateapipb.Tag, error) {
	if c.beforePublication {
		c.beforePublication = false
		ref := req.GetTag().GetSourceActor()
		if _, err := c.fakeGoldenControl.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
			return nil, err
		}
		if _, err := c.fakeGoldenControl.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref}); err != nil {
			return nil, err
		}
	}
	return c.fakeGoldenControl.createTag(ctx, req, precondition)
}

func TestReconcileOne_CapturePublicationRacesAreRejected(t *testing.T) {
	for _, phase := range []string{"capture", "publication"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := newFakeTemplateStore(testTemplate(withoutWakeupProbe))
				control := &interferingGoldenControl{
					fakeGoldenControl: &fakeGoldenControl{snapshot: "gs://bucket/snapshot"},
					beforeCapture:     phase == "capture",
					beforePublication: phase == "publication",
				}
				r := newTestTemplateReconciler(st, control)
				defer r.queue.ShutDown()
				if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
					t.Fatal(err)
				}
				time.Sleep(goldenSnapshotWarmup)
				if _, err := r.reconcileOne(t.Context(), testTemplateRef); apierror.Code(err) != codes.Aborted {
					t.Fatalf("%s race returned %v, want Aborted", phase, err)
				}
				if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag() != nil {
					t.Fatal("published a snapshot after a conflicting lifecycle operation")
				}
			})
		})
	}
}

func TestReconcileOne_HiddenLifecycleRestartsWarmup(t *testing.T) {
	for _, operation := range []string{"pause", "suspend", "revert", "replace"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := newFakeTemplateStore(testTemplate(withoutWakeupProbe))
				control := &fakeGoldenControl{snapshot: "gs://bucket/snapshot"}
				r := newTestTemplateReconciler(st, control)
				defer r.queue.ShutDown()
				if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
					t.Fatal(err)
				}
				ref := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID}
				time.Sleep(5 * time.Second)
				var err error
				switch operation {
				case "pause":
					_, err = control.PauseActor(t.Context(), &ateapipb.PauseActorRequest{Actor: ref})
				case "suspend":
					_, err = control.SuspendActor(t.Context(), &ateapipb.SuspendActorRequest{Actor: ref})
				case "revert":
					_, err = control.RevertActor(t.Context(), &ateapipb.RevertActorRequest{Actor: ref})
				case "replace":
					control.actorUID = "22222222-2222-4222-8222-222222222222"
				}
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(20 * time.Second)
				if _, err := control.ResumeActor(t.Context(), &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
					t.Fatal(err)
				}
				delay, err := r.reconcileOne(t.Context(), testTemplateRef)
				if err != nil {
					t.Fatal(err)
				}
				if delay != goldenSnapshotWarmup || st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag() != nil {
					t.Fatalf("%s inherited old warmup: delay %s, tag %v", operation, delay, st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag())
				}
			})
		})
	}
}

type slowGoldenResume struct {
	*fakeGoldenControl
	resumedAt time.Time
	taggedAt  time.Time
}

func (c *slowGoldenResume) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	time.Sleep(25 * time.Second)
	resp, err := c.fakeGoldenControl.ResumeActor(ctx, req)
	c.resumedAt = time.Now()
	return resp, err
}

func (c *slowGoldenResume) createTag(ctx context.Context, req *ateapipb.CreateTagRequest, precondition *store.Precondition) (*ateapipb.Tag, error) {
	c.taggedAt = time.Now()
	return c.fakeGoldenControl.createTag(ctx, req, precondition)
}

func TestReconcileOne_WarmupAfterSlowResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := newFakeTemplateStore(testTemplate(withoutWakeupProbe))
		control := &slowGoldenResume{fakeGoldenControl: &fakeGoldenControl{snapshot: "gs://bucket/snapshot"}}
		r := newTestTemplateReconciler(st, control)
		defer r.queue.ShutDown()
		delay, err := r.reconcileOne(t.Context(), testTemplateRef)
		if err != nil {
			t.Fatal(err)
		}
		if !control.taggedAt.IsZero() {
			t.Fatalf("snapshot tagged %s after Resume returned; want a full %s warmup", control.taggedAt.Sub(control.resumedAt), goldenSnapshotWarmup)
		}
		if delay < goldenSnapshotWarmup {
			t.Fatalf("remaining warmup = %s, want >= %s", delay, goldenSnapshotWarmup)
		}
		time.Sleep(delay)
		if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
			t.Fatal(err)
		}
		if control.taggedAt.Sub(control.resumedAt) < goldenSnapshotWarmup {
			t.Fatal("resume time was counted as warmup")
		}
	})
}

func TestReconcileOne_PublicationRetry(t *testing.T) {
	for _, failure := range []string{"copy", "cleanup"} {
		t.Run(failure, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := newFakeTemplateStore(testTemplate(withoutWakeupProbe))
				control := &fakeGoldenControl{snapshot: "gs://bucket/snapshot"}
				r := newTestTemplateReconciler(st, control)
				defer r.queue.ShutDown()
				if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
					t.Fatal(err)
				}
				time.Sleep(goldenSnapshotWarmup)
				if failure == "copy" {
					control.tagErr = errors.New("copy failed")
				} else {
					control.deleteErr = errors.New("cleanup failed")
				}
				if _, err := r.reconcileOne(t.Context(), testTemplateRef); err == nil {
					t.Fatal("injected failure succeeded")
				}
				control.tagErr, control.deleteErr = nil, nil
				delay, err := r.reconcileOne(t.Context(), testTemplateRef)
				if err != nil {
					t.Fatal(err)
				}
				wantCalls := 1
				if failure == "copy" {
					wantCalls = 2
					if delay != goldenSnapshotWarmup || st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag() != nil {
						t.Fatal("failed copy reused approval from a previous pass")
					}
					time.Sleep(delay)
					if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
						t.Fatal(err)
					}
				} else if delay != 0 {
					t.Fatal("completed tag restarted warmup")
				}
				if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag() == nil {
					t.Fatal("retry did not publish the tag")
				}
				if _, resumes, suspends := control.callCounts(); resumes != wantCalls || suspends != wantCalls {
					t.Fatalf("retry resumed %d and suspended %d times, want %d each", resumes, suspends, wantCalls)
				}
			})
		})
	}
}

func TestReconcileOne_ChangedTemplateFailsBuild(t *testing.T) {
	st := newFakeTemplateStore(testTemplate())
	control := &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING, actorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "other-template"}}
	r := newTestTemplateReconciler(st, control)
	defer r.queue.ShutDown()
	if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
		t.Fatal(err)
	}
	if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetErrorMessage() == "" || len(control.suspendReqs) != 0 || len(control.tagReqs) != 0 {
		t.Fatal("captured or published an actor that uses another template")
	}
}
