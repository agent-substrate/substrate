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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeTemplateStore is an in-memory templateReconcilerStore.
type fakeTemplateStore struct {
	mu        sync.Mutex
	templates map[resources.ActorTemplateRef]*ateapipb.ActorTemplate

	// lockErr, when set, is returned by AcquireLock.
	lockErr error
	// forcePageSize, when > 0, overrides the requested page size so tests
	// can exercise the resync pagination loop.
	forcePageSize int
}

func newFakeTemplateStore(templates ...*ateapipb.ActorTemplate) *fakeTemplateStore {
	s := &fakeTemplateStore{templates: map[resources.ActorTemplateRef]*ateapipb.ActorTemplate{}}
	for _, tmpl := range templates {
		s.templates[resources.ActorTemplateRefFromActorTemplate(tmpl)] = proto.Clone(tmpl).(*ateapipb.ActorTemplate)
	}
	return s
}

func (s *fakeTemplateStore) GetActorTemplate(_ context.Context, ref resources.ActorTemplateRef) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tmpl, ok := s.templates[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	return proto.Clone(tmpl).(*ateapipb.ActorTemplate), nil
}

func (s *fakeTemplateStore) ListActorTemplates(_ context.Context, _ string, opts store.ListOptions) (store.ListResponse[*ateapipb.ActorTemplate], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	refs := make([]resources.ActorTemplateRef, 0, len(s.templates))
	for ref := range s.templates {
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].Name < refs[j].Name })

	pageSize := int(opts.PageSize)
	if s.forcePageSize > 0 {
		pageSize = s.forcePageSize
	}
	start := 0
	if opts.PageToken != "" {
		var err error
		if start, err = strconv.Atoi(opts.PageToken); err != nil {
			return store.ListResponse[*ateapipb.ActorTemplate]{}, err
		}
	}
	end := min(start+pageSize, len(refs))
	var resp store.ListResponse[*ateapipb.ActorTemplate]
	for _, ref := range refs[start:end] {
		resp.Items = append(resp.Items, proto.Clone(s.templates[ref]).(*ateapipb.ActorTemplate))
	}
	if end < len(refs) {
		resp.NextPageToken = strconv.Itoa(end)
	}
	return resp, nil
}

func (s *fakeTemplateStore) UpdateActorTemplate(_ context.Context, ref resources.ActorTemplateRef, mutate func(*ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tmpl, ok := s.templates[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	updated := proto.Clone(tmpl).(*ateapipb.ActorTemplate)
	if err := mutate(updated); err != nil {
		return nil, err
	}
	s.templates[ref] = updated
	return proto.Clone(updated).(*ateapipb.ActorTemplate), nil
}

func (s *fakeTemplateStore) AcquireLock(ctx context.Context, _ string) (*store.Lock, error) {
	if s.lockErr != nil {
		return nil, s.lockErr
	}
	lockCtx, cancel := context.WithCancel(ctx)
	return store.NewLock(lockCtx, cancel), nil
}

// storedStatus returns the persisted status for ref, for assertions.
func (s *fakeTemplateStore) storedStatus(t *testing.T, ref resources.ActorTemplateRef) *ateapipb.ActorTemplateStatus {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	tmpl, ok := s.templates[ref]
	if !ok {
		t.Fatalf("template %v not in store", ref)
	}
	return proto.Clone(tmpl).(*ateapipb.ActorTemplate).GetStatus()
}

// fakeGoldenControl is an in-memory goldenActorControl recording every call.
type fakeGoldenControl struct {
	mu sync.Mutex

	createErr  error
	resumeErr  error
	suspendErr error
	getErr     error
	// goldenState is the actor state GetActor reports.
	goldenState ateapipb.ActorState
	// snapshot is what SuspendActor reports as the latest snapshot; nil
	// simulates a suspend that produced no ActorSnapshot.
	snapshot *ateapipb.ObjectRef

	createReqs  []*ateapipb.CreateActorRequest
	resumeReqs  []*ateapipb.ResumeActorRequest
	suspendReqs []*ateapipb.SuspendActorRequest
}

func (c *fakeGoldenControl) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createReqs = append(c.createReqs, req)
	if c.createErr != nil {
		return nil, c.createErr
	}
	return req.GetActor(), nil
}

func (c *fakeGoldenControl) GetActor(_ context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, c.getErr
	}
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: req.GetActor().GetAtespace(), Name: req.GetActor().GetName()},
		Status:   &ateapipb.ActorStatus{State: c.goldenState},
	}, nil
}

func (c *fakeGoldenControl) ResumeActor(_ context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeReqs = append(c.resumeReqs, req)
	if c.resumeErr != nil {
		return nil, c.resumeErr
	}
	return &ateapipb.ResumeActorResponse{}, nil
}

func (c *fakeGoldenControl) SuspendActor(_ context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.suspendReqs = append(c.suspendReqs, req)
	if c.suspendErr != nil {
		return nil, c.suspendErr
	}
	return &ateapipb.SuspendActorResponse{
		Actor: &ateapipb.Actor{Status: &ateapipb.ActorStatus{LatestSnapshot: c.snapshot}},
	}, nil
}

func (c *fakeGoldenControl) callCounts() (creates, resumes, suspends int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.createReqs), len(c.resumeReqs), len(c.suspendReqs)
}

const (
	testTemplateName = "tmpl-1"
	testTemplateUID  = "tmpl-uid-1"
)

var testTemplateRef = resources.ActorTemplateRef{Atespace: testAtespace, Name: testTemplateName}

// testTemplate builds a template in the given phase whose single container
// has a readyz probe, so goldenSnapshotWarmupFor returns 0 and reconcileOne
// runs the state machine to completion without waiting for a warmup window.
func testTemplate(phase ateapipb.ActorTemplatePhase, opts ...func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	tmpl := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testTemplateName, Uid: testTemplateUID},
		Containers: []*ateapipb.Container{
			{Name: "main", Image: "img", Readyz: &ateapipb.ContainerReadyz{}},
		},
		Status: &ateapipb.ActorTemplateStatus{Phase: phase},
	}
	for _, opt := range opts {
		opt(tmpl)
	}
	return tmpl
}

func withoutReadyz(tmpl *ateapipb.ActorTemplate) {
	for _, container := range tmpl.Containers {
		container.Readyz = nil
	}
}

func withSnapshotDeadline(at time.Time) func(*ateapipb.ActorTemplate) {
	return func(tmpl *ateapipb.ActorTemplate) {
		tmpl.Status.TakeGoldenSnapshotAt = timestamppb.New(at)
	}
}

func newTestTemplateReconciler(persistence templateReconcilerStore, control goldenActorControl) *ActorTemplateReconciler {
	// The SandboxConfig lister is not used by reconcileOne or resync.
	return NewActorTemplateReconciler(persistence, control, nil)
}

func TestGoldenSnapshotWarmupFor(t *testing.T) {
	readyz := &ateapipb.ContainerReadyz{}
	tests := []struct {
		name       string
		containers []*ateapipb.Container
		want       time.Duration
	}{
		{"no containers", nil, goldenSnapshotWarmup},
		{"all containers have readyz", []*ateapipb.Container{{Readyz: readyz}, {Readyz: readyz}}, 0},
		{"one container missing readyz", []*ateapipb.Container{{Readyz: readyz}, {}}, goldenSnapshotWarmup},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := goldenSnapshotWarmupFor(tt.containers); got != tt.want {
				t.Errorf("goldenSnapshotWarmupFor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileOne(t *testing.T) {
	snapshot := &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "snap-1"}
	tests := []struct {
		name     string
		template *ateapipb.ActorTemplate // nil: template absent from the store
		lockErr  error
		control  *fakeGoldenControl

		wantErr bool
		// requeueAfter must land in [wantRequeueMin, wantRequeueMax];
		// both zero means exactly 0.
		wantRequeueMin time.Duration
		wantRequeueMax time.Duration
		// wantPhase is the stored phase afterwards; checked when template
		// is seeded.
		wantPhase ateapipb.ActorTemplatePhase
		// wantMessage, when non-empty, must be a substring of status.message.
		wantMessage string
		// wantSnapshot, when non-nil, must equal the stored golden snapshot.
		wantSnapshot *ateapipb.ObjectRef
		wantCreates  int
		wantResumes  int
		wantSuspends int
	}{
		{
			name:         "happy path runs INITIAL to GOLDEN_SNAPSHOT_READY",
			template:     testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL),
			control:      &fakeGoldenControl{snapshot: snapshot},
			wantPhase:    ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_SNAPSHOT_READY,
			wantSnapshot: snapshot,
			wantCreates:  1,
			wantResumes:  1,
			wantSuspends: 1,
		},
		{
			name:           "warmup without readyz stops at RUNNING and requeues",
			template:       testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL, withoutReadyz),
			control:        &fakeGoldenControl{snapshot: snapshot},
			wantRequeueMin: goldenSnapshotWarmup - time.Second,
			wantRequeueMax: goldenSnapshotWarmup,
			wantPhase:      ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_RUNNING,
			wantCreates:    1,
			wantResumes:    1,
		},
		{
			name: "RUNNING waits for a future snapshot deadline",
			template: testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_RUNNING,
				withSnapshotDeadline(time.Now().Add(time.Hour))),
			control:        &fakeGoldenControl{snapshot: snapshot},
			wantRequeueMin: 59 * time.Minute,
			wantRequeueMax: time.Hour,
			wantPhase:      ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_RUNNING,
		},
		{
			name: "RUNNING takes the snapshot once the deadline passed",
			template: testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_RUNNING,
				withSnapshotDeadline(time.Now().Add(-time.Minute))),
			control:      &fakeGoldenControl{snapshot: snapshot},
			wantPhase:    ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_SNAPSHOT_READY,
			wantSnapshot: snapshot,
			wantSuspends: 1,
		},
		{
			name: "suspend without a snapshot errors",
			template: testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_RUNNING,
				withSnapshotDeadline(time.Now().Add(-time.Minute))),
			control:      &fakeGoldenControl{snapshot: nil},
			wantErr:      true,
			wantPhase:    ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_RUNNING,
			wantSuspends: 1,
		},
		{
			name:     "create AlreadyExists proceeds",
			template: testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_ASSETS_FINALIZED),
			control: &fakeGoldenControl{
				createErr: status.Error(codes.AlreadyExists, "golden actor exists"),
				snapshot:  snapshot,
			},
			wantPhase:    ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_SNAPSHOT_READY,
			wantSnapshot: snapshot,
			wantCreates:  1,
			wantResumes:  1,
			wantSuspends: 1,
		},
		{
			name:        "create InvalidArgument fails the template",
			template:    testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_ASSETS_FINALIZED),
			control:     &fakeGoldenControl{createErr: status.Error(codes.InvalidArgument, "bad spec")},
			wantPhase:   ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_FAILED,
			wantMessage: "creating golden actor",
			wantCreates: 1,
		},
		{
			name:        "create retriable error requeues",
			template:    testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_ASSETS_FINALIZED),
			control:     &fakeGoldenControl{createErr: status.Error(codes.Unavailable, "workers busy")},
			wantErr:     true,
			wantPhase:   ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_ASSETS_FINALIZED,
			wantCreates: 1,
		},
		{
			name:     "resume failure with crashed golden actor fails the template",
			template: testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_CREATED),
			control: &fakeGoldenControl{
				resumeErr:   status.Error(codes.Internal, "resume failed"),
				goldenState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			},
			wantPhase:   ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_FAILED,
			wantMessage: "crashed",
			wantResumes: 1,
		},
		{
			name:     "resume failure without crash requeues",
			template: testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_CREATED),
			control: &fakeGoldenControl{
				resumeErr: status.Error(codes.Unavailable, "no workers"),
				getErr:    status.Error(codes.NotFound, "still scheduling"),
			},
			wantErr:     true,
			wantPhase:   ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_CREATED,
			wantResumes: 1,
		},
		{
			name:      "lock conflict yields without error",
			template:  testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL),
			lockErr:   store.ErrLockConflict,
			control:   &fakeGoldenControl{},
			wantPhase: ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL,
		},
		{
			name:      "lock error propagates",
			template:  testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL),
			lockErr:   errors.New("store unavailable"),
			control:   &fakeGoldenControl{},
			wantErr:   true,
			wantPhase: ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL,
		},
		{
			name:    "deleted template is a noop",
			control: &fakeGoldenControl{},
		},
		{
			name:      "terminal GOLDEN_SNAPSHOT_READY is a noop",
			template:  testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_SNAPSHOT_READY),
			control:   &fakeGoldenControl{},
			wantPhase: ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_SNAPSHOT_READY,
		},
		{
			name:      "terminal FAILED is a noop",
			template:  testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_FAILED),
			control:   &fakeGoldenControl{},
			wantPhase: ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_FAILED,
		},
		{
			name:      "terminal UNSPECIFIED is a noop",
			template:  testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_UNSPECIFIED),
			control:   &fakeGoldenControl{},
			wantPhase: ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_UNSPECIFIED,
		},
		{
			name:      "unrecognized phase errors",
			template:  testTemplate(ateapipb.ActorTemplatePhase(99)),
			control:   &fakeGoldenControl{},
			wantErr:   true,
			wantPhase: ateapipb.ActorTemplatePhase(99),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newFakeTemplateStore()
			if tt.template != nil {
				st = newFakeTemplateStore(tt.template)
			}
			st.lockErr = tt.lockErr
			r := newTestTemplateReconciler(st, tt.control)

			requeueAfter, err := r.reconcileOne(context.Background(), testTemplateRef)
			if (err != nil) != tt.wantErr {
				t.Fatalf("reconcileOne error = %v, wantErr %v", err, tt.wantErr)
			}
			if requeueAfter < tt.wantRequeueMin || requeueAfter > tt.wantRequeueMax {
				t.Errorf("requeueAfter = %v, want in [%v, %v]", requeueAfter, tt.wantRequeueMin, tt.wantRequeueMax)
			}
			if creates, resumes, suspends := tt.control.callCounts(); creates != tt.wantCreates || resumes != tt.wantResumes || suspends != tt.wantSuspends {
				t.Errorf("control calls = create:%d resume:%d suspend:%d, want create:%d resume:%d suspend:%d",
					creates, resumes, suspends, tt.wantCreates, tt.wantResumes, tt.wantSuspends)
			}
			if tt.template == nil {
				return
			}
			tmplStatus := st.storedStatus(t, testTemplateRef)
			if got := tmplStatus.GetPhase(); got != tt.wantPhase {
				t.Errorf("stored phase = %v, want %v", got, tt.wantPhase)
			}
			if tt.wantMessage != "" && !strings.Contains(tmplStatus.GetMessage(), tt.wantMessage) {
				t.Errorf("stored message = %q, want it to contain %q", tmplStatus.GetMessage(), tt.wantMessage)
			}
			if tt.wantSnapshot != nil && !proto.Equal(tmplStatus.GetGoldenSnapshot(), tt.wantSnapshot) {
				t.Errorf("stored golden snapshot = %v, want %v", tmplStatus.GetGoldenSnapshot(), tt.wantSnapshot)
			}
		})
	}
}

// TestReconcileOne_GoldenActorRequests pins the shape of the control-plane
// requests the happy path issues: the golden actor is named after the
// template UID so recreated templates with the same name never collide.
func TestReconcileOne_GoldenActorRequests(t *testing.T) {
	ctx := context.Background()
	st := newFakeTemplateStore(testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL))
	control := &fakeGoldenControl{snapshot: &ateapipb.ObjectRef{Atespace: "ate-golden", Name: "snap-1"}}
	r := newTestTemplateReconciler(st, control)

	if _, err := r.reconcileOne(ctx, testTemplateRef); err != nil {
		t.Fatalf("reconcileOne failed: %v", err)
	}

	created := control.createReqs[0].GetActor()
	if got := created.GetMetadata().GetName(); got != testTemplateUID {
		t.Errorf("golden actor name = %q, want template UID %q", got, testTemplateUID)
	}
	if got := created.GetMetadata().GetAtespace(); got != testAtespace {
		t.Errorf("golden actor atespace = %q, want %q", got, testAtespace)
	}
	if got := created.GetActorTemplate().GetName(); got != testTemplateName {
		t.Errorf("golden actor template ref = %q, want %q", got, testTemplateName)
	}
	if got := control.resumeReqs[0].GetActor().GetName(); got != testTemplateUID {
		t.Errorf("resumed actor = %q, want %q", got, testTemplateUID)
	}
	if got := control.suspendReqs[0].GetActor().GetName(); got != testTemplateUID {
		t.Errorf("suspended actor = %q, want %q", got, testTemplateUID)
	}
	if st.storedStatus(t, testTemplateRef).GetTakeGoldenSnapshotAt() == nil {
		t.Error("stored take_golden_snapshot_at is nil, want set")
	}
}

func TestCheckpoint_StalePhaseErrors(t *testing.T) {
	ctx := context.Background()
	st := newFakeTemplateStore(testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_ASSETS_FINALIZED))
	r := newTestTemplateReconciler(st, &fakeGoldenControl{})

	// Precondition the checkpoint on INITIAL while the stored template is
	// already ASSETS_FINALIZED, as if another writer advanced it.
	_, err := r.checkpoint(ctx, testTemplateRef, ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL, func(templateStatus *ateapipb.ActorTemplateStatus) {
		templateStatus.Phase = ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_CREATED
	})
	if err == nil {
		t.Fatal("checkpoint succeeded, want error for concurrently advanced phase")
	}
	if got := st.storedStatus(t, testTemplateRef).GetPhase(); got != ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_ASSETS_FINALIZED {
		t.Errorf("stored phase = %v, want ASSETS_FINALIZED (unchanged)", got)
	}
}

// drainQueue shuts the reconciler queue down and returns every ref it held.
func drainQueue(r *ActorTemplateReconciler) []resources.ActorTemplateRef {
	r.queue.ShutDown()
	var refs []resources.ActorTemplateRef
	for {
		ref, quit := r.queue.Get()
		if quit {
			return refs
		}
		refs = append(refs, ref)
		r.queue.Done(ref)
	}
}

func TestResync_QueuesOnlyActionablePhases(t *testing.T) {
	tests := []struct {
		phase      ateapipb.ActorTemplatePhase
		wantQueued bool
	}{
		{ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_UNSPECIFIED, false},
		{ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL, true},
		{ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_ASSETS_FINALIZED, true},
		{ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_CREATED, true},
		{ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_ACTOR_RUNNING, true},
		{ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_GOLDEN_SNAPSHOT_READY, false},
		{ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_FAILED, false},
	}

	// Seed one template per phase, resync once, and check membership per row.
	templateName := func(phase ateapipb.ActorTemplatePhase) string {
		return "tmpl-" + strings.ToLower(phase.String())
	}
	templates := make([]*ateapipb.ActorTemplate, 0, len(tests))
	for _, tt := range tests {
		tmpl := testTemplate(tt.phase)
		tmpl.Metadata.Name = templateName(tt.phase)
		templates = append(templates, tmpl)
	}
	r := newTestTemplateReconciler(newFakeTemplateStore(templates...), &fakeGoldenControl{})

	r.resync(context.Background())

	queued := map[resources.ActorTemplateRef]bool{}
	for _, ref := range drainQueue(r) {
		queued[ref] = true
	}
	for _, tt := range tests {
		t.Run(tt.phase.String(), func(t *testing.T) {
			ref := resources.ActorTemplateRef{Atespace: testAtespace, Name: templateName(tt.phase)}
			if queued[ref] != tt.wantQueued {
				t.Errorf("phase %v queued = %v, want %v", tt.phase, queued[ref], tt.wantQueued)
			}
		})
	}
}

func TestResync_FollowsPagination(t *testing.T) {
	st := newFakeTemplateStore(
		testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL, func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Name = "tmpl-a" }),
		testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL, func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Name = "tmpl-b" }),
		testTemplate(ateapipb.ActorTemplatePhase_ACTOR_TEMPLATE_PHASE_INITIAL, func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Name = "tmpl-c" }),
	)
	st.forcePageSize = 1
	r := newTestTemplateReconciler(st, &fakeGoldenControl{})

	r.resync(context.Background())

	if got := len(drainQueue(r)); got != 3 {
		t.Errorf("queued %d templates, want 3 (all pages walked)", got)
	}
}
