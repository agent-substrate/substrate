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

// fakeGoldenControl is a stateful in-memory goldenActorControl: the golden
// actor it simulates moves through the real lifecycle (absent -> SUSPENDED on
// create -> RUNNING on resume -> SUSPENDED with a snapshot on suspend), and
// GetActor reports the current state, since the reconciler derives every step
// from that observation. Tests seed mid-lifecycle states via exists /
// goldenState / goldenSnapshot.
type fakeGoldenControl struct {
	mu sync.Mutex

	createErr  error
	resumeErr  error
	suspendErr error
	getErr     error

	// exists seeds whether the golden actor pre-exists; goldenState and
	// goldenSnapshot are its observed state and latest snapshot while it
	// does.
	exists         bool
	goldenState    ateapipb.ActorState
	goldenSnapshot *ateapipb.ObjectRef
	// snapshot is what a completed suspend produces as the latest snapshot;
	// nil simulates a suspend that produced no ActorSnapshot.
	snapshot *ateapipb.ObjectRef

	createReqs  []*ateapipb.CreateActorRequest
	resumeReqs  []*ateapipb.ResumeActorRequest
	suspendReqs []*ateapipb.SuspendActorRequest
}

func (c *fakeGoldenControl) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createReqs = append(c.createReqs, req)
	// AlreadyExists means the actor does exist (e.g. a racing creation).
	if c.createErr == nil || status.Code(c.createErr) == codes.AlreadyExists {
		c.exists = true
		c.goldenState = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	}
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
	if !c.exists {
		return nil, status.Error(codes.NotFound, "no such actor")
	}
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: req.GetActor().GetAtespace(), Name: req.GetActor().GetName()},
		Status:   &ateapipb.ActorStatus{State: c.goldenState, LatestSnapshot: c.goldenSnapshot},
	}, nil
}

func (c *fakeGoldenControl) ResumeActor(_ context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeReqs = append(c.resumeReqs, req)
	if c.resumeErr != nil {
		return nil, c.resumeErr
	}
	c.goldenState = ateapipb.ActorState_ACTOR_STATE_RUNNING
	return &ateapipb.ResumeActorResponse{}, nil
}

func (c *fakeGoldenControl) SuspendActor(_ context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.suspendReqs = append(c.suspendReqs, req)
	if c.suspendErr != nil {
		return nil, c.suspendErr
	}
	c.goldenState = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	if c.goldenSnapshot == nil {
		c.goldenSnapshot = c.snapshot
	}
	return &ateapipb.SuspendActorResponse{
		Actor: &ateapipb.Actor{Status: &ateapipb.ActorStatus{LatestSnapshot: c.goldenSnapshot}},
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

// testTemplate builds a template with empty conditions whose single container
// has a readyz probe, so goldenSnapshotWarmupFor returns 0 and reconcileOne
// drives the golden actor to a snapshot without waiting for a warmup window.
func testTemplate(opts ...func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	tmpl := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testTemplateName, Uid: testTemplateUID},
		Containers: []*ateapipb.Container{
			{Name: "main", Image: "img", Readyz: &ateapipb.ContainerReadyz{}},
		},
		Status: &ateapipb.ActorTemplateStatus{},
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

func withReady() func(*ateapipb.ActorTemplate) {
	return func(tmpl *ateapipb.ActorTemplate) {
		setCondition(&tmpl.Status.Conditions, conditionReady, ateapipb.ConditionStatus_CONDITION_STATUS_TRUE, conditionReady, "")
	}
}

func withFailed(reason string) func(*ateapipb.ActorTemplate) {
	return func(tmpl *ateapipb.ActorTemplate) {
		setCondition(&tmpl.Status.Conditions, conditionFailed, ateapipb.ConditionStatus_CONDITION_STATUS_TRUE, reason, "seeded failure")
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
		// wantTrue / wantNotTrue name conditions that must (not) be true in
		// the stored status afterwards; checked when template is seeded.
		wantTrue    []string
		wantNotTrue []string
		// wantFailedReason, when non-empty, must equal the Failed condition's
		// reason; wantMessage must be a substring of its message.
		wantFailedReason string
		wantMessage      string
		// wantSnapshot, when non-nil, must equal the stored golden snapshot.
		wantSnapshot *ateapipb.ObjectRef
		// wantDeadline asserts whether take_golden_snapshot_at is set.
		wantDeadline bool
		wantCreates  int
		wantResumes  int
		wantSuspends int
	}{
		{
			name:         "happy path creates, resumes, and snapshots the golden actor",
			template:     testTemplate(),
			control:      &fakeGoldenControl{snapshot: snapshot},
			wantTrue:     []string{conditionReady},
			wantNotTrue:  []string{conditionFailed},
			wantSnapshot: snapshot,
			wantDeadline: true,
			wantCreates:  1,
			wantResumes:  1,
			wantSuspends: 1,
		},
		{
			name:           "warmup without readyz stops after resume and requeues",
			template:       testTemplate(withoutReadyz),
			control:        &fakeGoldenControl{snapshot: snapshot},
			wantRequeueMin: goldenSnapshotWarmup - time.Second,
			wantRequeueMax: goldenSnapshotWarmup,
			wantNotTrue:    []string{conditionReady, conditionFailed},
			wantDeadline:   true,
			wantCreates:    1,
			wantResumes:    1,
		},
		{
			name: "running golden actor waits for a future deadline",
			template: testTemplate(withoutReadyz,
				withSnapshotDeadline(time.Now().Add(time.Hour))),
			control:        &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			wantRequeueMin: 59 * time.Minute,
			wantRequeueMax: time.Hour,
			wantNotTrue:    []string{conditionReady},
		},
		{
			name: "running golden actor is snapshotted once the deadline passed",
			template: testTemplate(
				withSnapshotDeadline(time.Now().Add(-time.Minute))),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING, snapshot: snapshot},
			wantTrue:     []string{conditionReady},
			wantSnapshot: snapshot,
			wantSuspends: 1,
		},
		{
			name:           "running golden actor with a lost deadline restarts the warmup",
			template:       testTemplate(withoutReadyz),
			control:        &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			wantRequeueMin: goldenSnapshotWarmup - time.Second,
			wantRequeueMax: goldenSnapshotWarmup,
			wantNotTrue:    []string{conditionReady},
			wantDeadline:   true,
		},
		{
			name: "suspend returning no snapshot errors",
			template: testTemplate(
				withSnapshotDeadline(time.Now().Add(-time.Minute))),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING, snapshot: nil},
			wantErr:      true,
			wantNotTrue:  []string{conditionReady},
			wantSuspends: 1,
		},
		{
			name:         "suspending golden actor is completed and recorded",
			template:     testTemplate(),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, snapshot: snapshot},
			wantTrue:     []string{conditionReady},
			wantSnapshot: snapshot,
			wantSuspends: 1,
		},
		{
			name:         "suspended golden actor with a snapshot is recorded without more control calls",
			template:     testTemplate(),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, goldenSnapshot: snapshot},
			wantTrue:     []string{conditionReady},
			wantSnapshot: snapshot,
		},
		{
			name:         "create AlreadyExists proceeds",
			template:     testTemplate(),
			control:      &fakeGoldenControl{createErr: status.Error(codes.AlreadyExists, "golden actor exists"), snapshot: snapshot},
			wantTrue:     []string{conditionReady},
			wantSnapshot: snapshot,
			wantCreates:  1,
			wantResumes:  1,
			wantSuspends: 1,
		},
		{
			name:             "create InvalidArgument fails the template",
			template:         testTemplate(),
			control:          &fakeGoldenControl{createErr: status.Error(codes.InvalidArgument, "bad spec")},
			wantTrue:         []string{conditionFailed},
			wantFailedReason: reasonGoldenActorInvalid,
			wantMessage:      "creating golden actor",
			wantCreates:      1,
		},
		{
			name:        "create retriable error requeues",
			template:    testTemplate(),
			control:     &fakeGoldenControl{createErr: status.Error(codes.Unavailable, "workers busy")},
			wantErr:     true,
			wantNotTrue: []string{conditionFailed},
			wantCreates: 1,
		},
		{
			name:             "crashed golden actor fails the template",
			template:         testTemplate(),
			control:          &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_CRASHED},
			wantTrue:         []string{conditionFailed},
			wantFailedReason: reasonGoldenActorCrashed,
			wantMessage:      "crashed",
		},
		{
			name:        "resume failure requeues without failing",
			template:    testTemplate(),
			control:     &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, resumeErr: status.Error(codes.Unavailable, "no workers")},
			wantErr:     true,
			wantNotTrue: []string{conditionFailed},
			wantResumes: 1,
		},
		{
			name:        "get failure requeues",
			template:    testTemplate(),
			control:     &fakeGoldenControl{getErr: status.Error(codes.Unavailable, "control plane down")},
			wantErr:     true,
			wantNotTrue: []string{conditionFailed},
		},
		{
			name:             "deleting golden actor fails the template",
			template:         testTemplate(),
			control:          &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_DELETING},
			wantTrue:         []string{conditionFailed},
			wantFailedReason: reasonUnexpectedState,
			wantMessage:      "unexpected state",
		},
		{
			name:           "unspecified actor state checks back later",
			template:       testTemplate(),
			control:        &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED},
			wantRequeueMin: templateResyncInterval,
			wantRequeueMax: templateResyncInterval,
			wantNotTrue:    []string{conditionReady, conditionFailed},
		},
		{
			name:        "lock conflict yields without error",
			template:    testTemplate(),
			lockErr:     store.ErrLockConflict,
			control:     &fakeGoldenControl{},
			wantNotTrue: []string{conditionReady, conditionFailed},
		},
		{
			name:        "lock error propagates",
			template:    testTemplate(),
			lockErr:     errors.New("store unavailable"),
			control:     &fakeGoldenControl{},
			wantErr:     true,
			wantNotTrue: []string{conditionReady, conditionFailed},
		},
		{
			name:    "deleted template is a noop",
			control: &fakeGoldenControl{},
		},
		{
			name:        "terminal Ready is a noop",
			template:    testTemplate(withReady()),
			control:     &fakeGoldenControl{},
			wantTrue:    []string{conditionReady},
			wantNotTrue: []string{conditionFailed},
		},
		{
			name:             "terminal Failed is a noop",
			template:         testTemplate(withFailed(reasonGoldenActorCrashed)),
			control:          &fakeGoldenControl{},
			wantTrue:         []string{conditionFailed},
			wantNotTrue:      []string{conditionReady},
			wantFailedReason: reasonGoldenActorCrashed,
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
			for _, cond := range tt.wantTrue {
				if !conditionIsTrue(tmplStatus.GetConditions(), cond) {
					t.Errorf("condition %s not true, want true (conditions: %s)", cond, conditionsSummary(tmplStatus.GetConditions()))
				}
			}
			for _, cond := range tt.wantNotTrue {
				if conditionIsTrue(tmplStatus.GetConditions(), cond) {
					t.Errorf("condition %s is true, want not true", cond)
				}
			}
			if tt.wantFailedReason != "" || tt.wantMessage != "" {
				failed := findCondition(tmplStatus.GetConditions(), conditionFailed)
				if tt.wantFailedReason != "" && failed.GetReason() != tt.wantFailedReason {
					t.Errorf("Failed reason = %q, want %q", failed.GetReason(), tt.wantFailedReason)
				}
				if tt.wantMessage != "" && !strings.Contains(failed.GetMessage(), tt.wantMessage) {
					t.Errorf("Failed message = %q, want it to contain %q", failed.GetMessage(), tt.wantMessage)
				}
			}
			if tt.wantSnapshot != nil && !proto.Equal(tmplStatus.GetGoldenSnapshot(), tt.wantSnapshot) {
				t.Errorf("stored golden snapshot = %v, want %v", tmplStatus.GetGoldenSnapshot(), tt.wantSnapshot)
			}
			if tt.wantDeadline && tmplStatus.GetTakeGoldenSnapshotAt() == nil {
				t.Error("stored take_golden_snapshot_at is nil, want set")
			}
		})
	}
}

// TestReconcileOne_GoldenActorRequests pins the shape of the control-plane
// requests the happy path issues: the golden actor is named after the
// template UID so recreated templates with the same name never collide.
func TestReconcileOne_GoldenActorRequests(t *testing.T) {
	ctx := context.Background()
	st := newFakeTemplateStore(testTemplate())
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

func TestCheckpoint_TerminalConditionErrors(t *testing.T) {
	ctx := context.Background()
	for _, seed := range []struct {
		name string
		opt  func(*ateapipb.ActorTemplate)
	}{
		{"ready", withReady()},
		{"failed", withFailed(reasonGoldenActorCrashed)},
	} {
		t.Run(seed.name, func(t *testing.T) {
			st := newFakeTemplateStore(testTemplate(seed.opt))
			r := newTestTemplateReconciler(st, &fakeGoldenControl{})

			// Checkpoint against a template a concurrent writer already
			// drove to a terminal condition.
			_, err := r.checkpoint(ctx, testTemplateRef, func(templateStatus *ateapipb.ActorTemplateStatus) {
				templateStatus.TakeGoldenSnapshotAt = timestamppb.New(time.Now())
			})
			if err == nil {
				t.Fatal("checkpoint succeeded, want error for terminal template")
			}
			if st.storedStatus(t, testTemplateRef).GetTakeGoldenSnapshotAt() != nil {
				t.Error("take_golden_snapshot_at set, want store unchanged")
			}
		})
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

func TestResync_QueuesOnlyActionableTemplates(t *testing.T) {
	tests := []struct {
		name       string
		opts       []func(*ateapipb.ActorTemplate)
		wantQueued bool
	}{
		{"empty status", nil, true},
		{"mid warmup", []func(*ateapipb.ActorTemplate){withSnapshotDeadline(time.Now().Add(time.Hour))}, true},
		{"ready", []func(*ateapipb.ActorTemplate){withReady()}, false},
		{"failed", []func(*ateapipb.ActorTemplate){withFailed(reasonGoldenActorCrashed)}, false},
	}

	// Seed one template per row, resync once, and check membership per row.
	templateName := func(name string) string {
		return "tmpl-" + strings.ReplaceAll(name, " ", "-")
	}
	templates := make([]*ateapipb.ActorTemplate, 0, len(tests))
	for _, tt := range tests {
		tmpl := testTemplate(tt.opts...)
		tmpl.Metadata.Name = templateName(tt.name)
		templates = append(templates, tmpl)
	}
	r := newTestTemplateReconciler(newFakeTemplateStore(templates...), &fakeGoldenControl{})

	r.resync(context.Background())

	queued := map[resources.ActorTemplateRef]bool{}
	for _, ref := range drainQueue(r) {
		queued[ref] = true
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref := resources.ActorTemplateRef{Atespace: testAtespace, Name: templateName(tt.name)}
			if queued[ref] != tt.wantQueued {
				t.Errorf("%s queued = %v, want %v", tt.name, queued[ref], tt.wantQueued)
			}
		})
	}
}

func TestResync_FollowsPagination(t *testing.T) {
	st := newFakeTemplateStore(
		testTemplate(func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Name = "tmpl-a" }),
		testTemplate(func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Name = "tmpl-b" }),
		testTemplate(func(tmpl *ateapipb.ActorTemplate) { tmpl.Metadata.Name = "tmpl-c" }),
	)
	st.forcePageSize = 1
	r := newTestTemplateReconciler(st, &fakeGoldenControl{})

	r.resync(context.Background())

	if got := len(drainQueue(r)); got != 3 {
		t.Errorf("queued %d templates, want 3 (all pages walked)", got)
	}
}
