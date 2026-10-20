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
	"unicode/utf8"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeTemplateStore is an in-memory templateReconcilerStore.
type fakeTemplateStore struct {
	mu        sync.Mutex
	templates map[resources.ActorTemplateRef]*ateapi.ActorTemplate

	// leaseErr, when set, is returned by AcquireLease.
	leaseErr error
	// forcePageSize, when > 0, overrides the requested page size so tests
	// can exercise the resync pagination loop.
	forcePageSize int
}

func newFakeTemplateStore(templates ...*ateapi.ActorTemplate) *fakeTemplateStore {
	s := &fakeTemplateStore{templates: map[resources.ActorTemplateRef]*ateapi.ActorTemplate{}}
	for _, tmpl := range templates {
		s.templates[resources.ActorTemplateRefFromActorTemplate(tmpl)] = proto.Clone(tmpl).(*ateapi.ActorTemplate)
	}
	return s
}

func (s *fakeTemplateStore) GetActorTemplate(_ context.Context, ref resources.ActorTemplateRef) (*ateapi.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tmpl, ok := s.templates[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	return proto.Clone(tmpl).(*ateapi.ActorTemplate), nil
}

func (s *fakeTemplateStore) ListActorTemplates(_ context.Context, _ string, opts store.ListOptions) (store.ListResponse[*ateapi.ActorTemplate], error) {
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
			return store.ListResponse[*ateapi.ActorTemplate]{}, err
		}
	}
	end := min(start+pageSize, len(refs))
	var resp store.ListResponse[*ateapi.ActorTemplate]
	for _, ref := range refs[start:end] {
		resp.Items = append(resp.Items, proto.Clone(s.templates[ref]).(*ateapi.ActorTemplate))
	}
	if end < len(refs) {
		resp.NextPageToken = strconv.Itoa(end)
	}
	return resp, nil
}

func (s *fakeTemplateStore) UpdateActorTemplate(_ context.Context, ref resources.ActorTemplateRef, precondition store.Precondition, mutate func(*ateapi.ActorTemplate) error) (*ateapi.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := precondition.Validate(); err != nil {
		return nil, err
	}
	tmpl, ok := s.templates[ref]
	if !ok {
		return nil, store.ErrNotFound
	}
	if err := precondition.Check(tmpl.GetMetadata()); err != nil {
		return nil, err
	}
	updated := proto.Clone(tmpl).(*ateapi.ActorTemplate)
	if err := mutate(updated); err != nil {
		return nil, err
	}
	updated.Metadata.Version++
	s.templates[ref] = updated
	return proto.Clone(updated).(*ateapi.ActorTemplate), nil
}

func (s *fakeTemplateStore) AcquireLease(ctx context.Context, _ string) (*store.Lease, error) {
	if s.leaseErr != nil {
		return nil, s.leaseErr
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	return store.NewLease(leaseCtx, cancel), nil
}

// storedStatus returns the persisted status for ref, for assertions.
func (s *fakeTemplateStore) storedStatus(t *testing.T, ref resources.ActorTemplateRef) *ateapi.ActorTemplateStatus {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	tmpl, ok := s.templates[ref]
	if !ok {
		t.Fatalf("template %v not in store", ref)
	}
	return proto.Clone(tmpl).(*ateapi.ActorTemplate).GetStatus()
}

// fakeGoldenControl is a stateful in-memory goldenActorControl: the golden
// actor it simulates moves through the real lifecycle (absent -> SUSPENDED on
// create -> RUNNING on resume -> SUSPENDED with a snapshot on suspend), and
// GetActor reports the current state, since the reconciler derives every step
// from that observation. Tests seed mid-lifecycle states via exists /
// goldenState / goldenSnapshot.
type fakeGoldenControl struct {
	mu           sync.Mutex
	tag          *ateapi.Tag
	tagErr       error
	deleteErr    error
	deleteTagErr error
	tagReqs      []*ateapi.CreateTagRequest
	deleteReqs   []*ateapi.DeleteActorRequest

	createErr  error
	resumeErr  error
	suspendErr error
	getErr     error

	// exists seeds whether the golden actor pre-exists; goldenState and
	// goldenSnapshot are its observed state and external snapshot while it
	// does.
	exists         bool
	goldenState    ateapi.ActorState
	goldenSnapshot string
	// snapshot is the external snapshot a completed suspend produces; empty
	// simulates a suspend that wrote none.
	snapshot string

	createReqs   []*ateapi.CreateActorRequest
	resumeReqs   []*ateapi.ResumeActorRequest
	suspendReqs  []*ateapi.SuspendActorRequest
	atespaceReqs []*ateapi.CreateAtespaceRequest
}

func (c *fakeGoldenControl) CreateAtespace(_ context.Context, req *ateapi.CreateAtespaceRequest) (*ateapi.Atespace, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.atespaceReqs = append(c.atespaceReqs, req)
	return req.GetAtespace(), nil
}

func (c *fakeGoldenControl) goldenActorStatus() *ateapi.ActorStatus {
	st := &ateapi.ActorStatus{State: c.goldenState}
	if c.goldenSnapshot != "" {
		st.LastAssignedGeneration = 1
		st.Snapshots = []*ateapi.Snapshot{
			newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, testTemplateUID, "golden-snapshot", c.goldenSnapshot, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
	}
	return st
}

func (c *fakeGoldenControl) CreateActor(_ context.Context, req *ateapi.CreateActorRequest) (*ateapi.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createReqs = append(c.createReqs, req)
	// AlreadyExists means the actor does exist (e.g. a racing creation).
	if c.createErr == nil || apierror.Code(c.createErr) == codes.AlreadyExists {
		c.exists = true
		c.goldenState = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	}
	if c.createErr != nil {
		return nil, c.createErr
	}
	// Like the real Control service, return the stored actor: metadata plus
	// a status observing the initial SUSPENDED state.
	return &ateapi.Actor{
		Metadata: req.GetActor().GetMetadata(),
		Status:   c.goldenActorStatus(),
	}, nil
}

func (c *fakeGoldenControl) GetActor(_ context.Context, req *ateapi.GetActorRequest) (*ateapi.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, c.getErr
	}
	if !c.exists {
		return nil, apierror.NotFound("no such actor")
	}
	return &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: req.GetActor().GetAtespace(), Name: req.GetActor().GetName()},
		Status:   c.goldenActorStatus(),
	}, nil
}

func (c *fakeGoldenControl) ResumeActor(_ context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeReqs = append(c.resumeReqs, req)
	if c.resumeErr != nil {
		return nil, c.resumeErr
	}
	c.goldenState = ateapi.ActorState_ACTOR_STATE_RUNNING
	return &ateapi.ResumeActorResponse{}, nil
}

func (c *fakeGoldenControl) CreateActorEgressPolicy(context.Context, *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	return nil, errors.New("unexpected golden egress policy creation")
}

func (c *fakeGoldenControl) GetActorEgressPolicy(context.Context, *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	return nil, errors.New("unexpected golden egress policy lookup")
}

func (c *fakeGoldenControl) SuspendActor(_ context.Context, req *ateapi.SuspendActorRequest) (*ateapi.SuspendActorResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.suspendReqs = append(c.suspendReqs, req)
	if c.suspendErr != nil {
		return nil, c.suspendErr
	}
	c.goldenState = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	if c.goldenSnapshot == "" {
		c.goldenSnapshot = c.snapshot
	}
	return &ateapi.SuspendActorResponse{
		Actor: &ateapi.Actor{Status: c.goldenActorStatus()},
	}, nil
}

func (c *fakeGoldenControl) GetTag(_ context.Context, _ *ateapi.GetTagRequest) (*ateapi.Tag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tag == nil {
		return nil, apierror.NotFound("no tag")
	}
	return proto.CloneOf(c.tag), nil
}

func (c *fakeGoldenControl) CreateTag(_ context.Context, req *ateapi.CreateTagRequest) (*ateapi.Tag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tagReqs = append(c.tagReqs, req)
	if c.tagErr != nil {
		return nil, c.tagErr
	}
	c.tag = proto.CloneOf(req.GetTag())
	c.tag.Status = &ateapi.TagStatus{
		ActorTemplateUid: testTemplateUID,
		Snapshot:         newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, testTemplateUID, "golden-snapshot", c.goldenSnapshot, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
	}
	return proto.CloneOf(c.tag), nil
}

func (c *fakeGoldenControl) DeleteTag(_ context.Context, _ *ateapi.DeleteTagRequest) (*ateapi.Tag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deleteTagErr != nil {
		return nil, c.deleteTagErr
	}
	tag := c.tag
	c.tag = nil
	return tag, nil
}

func (c *fakeGoldenControl) DeleteActor(_ context.Context, req *ateapi.DeleteActorRequest) (*ateapi.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteReqs = append(c.deleteReqs, req)
	if c.deleteErr != nil {
		return nil, c.deleteErr
	}
	if !c.exists {
		return nil, apierror.NotFound("no actor")
	}
	c.exists = false
	return &ateapi.Actor{}, nil
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

// testTemplate builds a template with an empty status whose single container
// has a wakeup probe, so goldenSnapshotWarmupFor returns 0 and reconcileOne
// drives the golden actor to a snapshot without waiting for a warmup window.
func testTemplate(opts ...func(*ateapi.ActorTemplate)) *ateapi.ActorTemplate {
	tmpl := &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: testTemplateName, Uid: testTemplateUID, Version: 1},
		Containers: []*ateapi.Container{
			{Name: "main", Image: "img", WakeupProbe: &ateapi.WakeupProbe{}},
		},
		SnapshotConfig: &ateapi.SnapshotConfig{
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		},
		Status: &ateapi.ActorTemplateStatus{},
	}
	for _, opt := range opts {
		opt(tmpl)
	}
	return tmpl
}

func withoutWakeupProbe(tmpl *ateapi.ActorTemplate) {
	for _, container := range tmpl.Containers {
		container.WakeupProbe = nil
	}
}

func withoutGoldenSnapshot(tmpl *ateapi.ActorTemplate) {
	tmpl.SnapshotConfig.GoldenSnapshotConfig.Mode = ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED
}

// seededGoldenStatus returns the template's golden snapshot status, allocating
// it so the with* options below can mutate it.
func seededGoldenStatus(tmpl *ateapi.ActorTemplate) *ateapi.GoldenSnapshotStatus {
	if tmpl.Status.GoldenSnapshotStatus == nil {
		tmpl.Status.GoldenSnapshotStatus = &ateapi.GoldenSnapshotStatus{}
	}
	return tmpl.Status.GoldenSnapshotStatus
}

func withSnapshotDeadline(at time.Time) func(*ateapi.ActorTemplate) {
	return func(tmpl *ateapi.ActorTemplate) {
		seededGoldenStatus(tmpl).SnapshotTime = timestamppb.New(at)
	}
}

func withGoldenTag() func(*ateapi.ActorTemplate) {
	return func(tmpl *ateapi.ActorTemplate) {
		seededGoldenStatus(tmpl).GoldenTag = &ateapi.ObjectRef{Atespace: testTemplateRef.Atespace, Name: testTemplateUID}
	}
}

func withFailed(reason string) func(*ateapi.ActorTemplate) {
	return func(tmpl *ateapi.ActorTemplate) {
		seededGoldenStatus(tmpl).ErrorMessage = reason + ": seeded failure"
	}
}

func newTestTemplateReconciler(persistence templateReconcilerStore, control goldenActorControl) *ActorTemplateReconciler {
	return NewActorTemplateReconciler(persistence, control, 7*time.Second)
}

func TestGoldenSnapshotWarmupFor(t *testing.T) {
	probe := &ateapi.WakeupProbe{}
	tests := []struct {
		name       string
		containers []*ateapi.Container
		want       time.Duration
	}{
		{"no containers", nil, goldenSnapshotWarmup},
		{"all containers have wakeup probe", []*ateapi.Container{{WakeupProbe: probe}, {WakeupProbe: probe}}, 0},
		{"one container missing wakeup probe", []*ateapi.Container{{WakeupProbe: probe}, {}}, goldenSnapshotWarmup},
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
	goldenSnapshot := "gs://bucket/root/atespaces/ns/actors/" + someActorUID + "/snapshots/snap-1"
	tests := []struct {
		name     string
		template *ateapi.ActorTemplate // nil: template absent from the store
		leaseErr error
		control  *fakeGoldenControl

		wantErr bool
		// requeueAfter must land in [wantRequeueMin, wantRequeueMax];
		// both zero means exactly 0.
		wantRequeueMin time.Duration
		wantRequeueMax time.Duration
		// wantFailedReason and wantMessage, when non-empty, must be
		// substrings of the stored error message; when both are empty the
		// stored error message must be empty. Checked when template is seeded.
		wantFailedReason string
		wantMessage      string
		// wantTag indicates that the golden tag should be recorded.
		wantTag bool
		// wantDeadline asserts whether snapshot_time is set.
		wantDeadline  bool
		wantCreates   int
		wantResumes   int
		wantSuspends  int
		wantUnchanged bool
	}{
		{
			name:          "golden snapshot disabled is a noop",
			template:      testTemplate(withoutGoldenSnapshot),
			control:       &fakeGoldenControl{},
			wantUnchanged: true,
		},
		{
			name:         "happy path creates, resumes, and snapshots the golden actor",
			template:     testTemplate(),
			control:      &fakeGoldenControl{snapshot: goldenSnapshot},
			wantTag:      true,
			wantDeadline: true,
			wantCreates:  1,
			wantResumes:  1,
			wantSuspends: 1,
		},
		{
			name:           "warmup without wakeup probe stops after resume and requeues",
			template:       testTemplate(withoutWakeupProbe),
			control:        &fakeGoldenControl{snapshot: goldenSnapshot},
			wantRequeueMin: goldenSnapshotWarmup - time.Second,
			wantRequeueMax: goldenSnapshotWarmup,
			wantDeadline:   true,
			wantCreates:    1,
			wantResumes:    1,
		},
		{
			name: "running golden actor waits for a future deadline",
			template: testTemplate(withoutWakeupProbe,
				withSnapshotDeadline(time.Now().Add(time.Hour))),
			control:        &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_RUNNING},
			wantRequeueMin: 59 * time.Minute,
			wantRequeueMax: time.Hour,
		},
		{
			name: "running golden actor is snapshotted once the deadline passed",
			template: testTemplate(
				withSnapshotDeadline(time.Now().Add(-time.Minute))),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_RUNNING, snapshot: goldenSnapshot},
			wantTag:      true,
			wantSuspends: 1,
		},
		{
			name:           "running golden actor with a lost deadline restarts the warmup",
			template:       testTemplate(withoutWakeupProbe),
			control:        &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_RUNNING},
			wantRequeueMin: goldenSnapshotWarmup - time.Second,
			wantRequeueMax: goldenSnapshotWarmup,
			wantDeadline:   true,
		},
		{
			name: "suspend returning no snapshot errors",
			template: testTemplate(
				withSnapshotDeadline(time.Now().Add(-time.Minute))),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_RUNNING, snapshot: ""},
			wantErr:      true,
			wantSuspends: 1,
		},
		{
			name:         "suspending golden actor is completed and recorded",
			template:     testTemplate(),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_SUSPENDING, snapshot: goldenSnapshot},
			wantTag:      true,
			wantSuspends: 1,
		},
		{
			name:     "suspended golden actor with a snapshot is recorded without more control calls",
			template: testTemplate(),
			control:  &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_SUSPENDED, goldenSnapshot: goldenSnapshot},
			wantTag:  true,
		},
		{
			name:        "create AlreadyExists requeues for the retry to observe",
			template:    testTemplate(),
			control:     &fakeGoldenControl{createErr: apierror.AlreadyExists("golden actor exists")},
			wantErr:     true,
			wantCreates: 1,
		},
		{
			name:             "create InvalidArgument fails the template",
			template:         testTemplate(),
			control:          &fakeGoldenControl{createErr: apierror.InvalidArgument("bad spec")},
			wantFailedReason: reasonGoldenActorInvalid,
			wantMessage:      "creating golden actor",
			wantCreates:      1,
		},
		{
			name:        "create retriable error requeues",
			template:    testTemplate(),
			control:     &fakeGoldenControl{createErr: apierror.Unavailable("workers busy")},
			wantErr:     true,
			wantCreates: 1,
		},
		{
			name:             "crashed golden actor fails the template",
			template:         testTemplate(),
			control:          &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_CRASHED},
			wantFailedReason: reasonGoldenActorCrashed,
			wantMessage:      "crashed",
		},
		{
			name:        "resume failure requeues without failing",
			template:    testTemplate(),
			control:     &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_SUSPENDED, resumeErr: apierror.Unavailable("no workers")},
			wantErr:     true,
			wantResumes: 1,
		},
		{
			name:     "get failure requeues",
			template: testTemplate(),
			control:  &fakeGoldenControl{getErr: apierror.Unavailable("control plane down")},
			wantErr:  true,
		},
		{
			name:             "deleting golden actor fails the template",
			template:         testTemplate(),
			control:          &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_DELETING},
			wantFailedReason: reasonUnexpectedState,
			wantMessage:      "unexpected state",
		},
		{
			name:           "unspecified actor state checks back later",
			template:       testTemplate(),
			control:        &fakeGoldenControl{exists: true, goldenState: ateapi.ActorState_ACTOR_STATE_UNSPECIFIED},
			wantRequeueMin: 7 * time.Second,
			wantRequeueMax: 7 * time.Second,
		},
		{
			name:     "lease conflict yields without error",
			template: testTemplate(),
			leaseErr: store.ErrLeaseConflict,
			control:  &fakeGoldenControl{},
		},
		{
			name:     "lease error propagates",
			template: testTemplate(),
			leaseErr: errors.New("store unavailable"),
			control:  &fakeGoldenControl{},
			wantErr:  true,
		},
		{
			name:    "deleted template is a noop",
			control: &fakeGoldenControl{},
		},
		{
			name:     "terminal golden snapshot is a noop",
			template: testTemplate(withGoldenTag()),
			control:  &fakeGoldenControl{},
			wantTag:  true,
		},
		{
			name:             "terminal error message is a noop",
			template:         testTemplate(withFailed(reasonGoldenActorCrashed)),
			control:          &fakeGoldenControl{},
			wantFailedReason: reasonGoldenActorCrashed,
			wantMessage:      "seeded failure",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newFakeTemplateStore()
			if tt.template != nil {
				st = newFakeTemplateStore(tt.template)
			}
			st.leaseErr = tt.leaseErr
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
			if tt.wantUnchanged {
				got, err := st.GetActorTemplate(t.Context(), testTemplateRef)
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(tt.template, got) {
					t.Fatalf("template was modified: %v", got)
				}
			}
			snapshotStatus := st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus()
			errorMessage := snapshotStatus.GetErrorMessage()
			if tt.wantFailedReason == "" && tt.wantMessage == "" {
				if errorMessage != "" {
					t.Errorf("stored error message = %q, want empty", errorMessage)
				}
			} else {
				if !strings.Contains(errorMessage, tt.wantFailedReason) {
					t.Errorf("stored error message = %q, want it to contain reason %q", errorMessage, tt.wantFailedReason)
				}
				if !strings.Contains(errorMessage, tt.wantMessage) {
					t.Errorf("stored error message = %q, want it to contain %q", errorMessage, tt.wantMessage)
				}
			}
			if got := snapshotStatus.GetGoldenTag(); (got != nil) != tt.wantTag {
				t.Errorf("stored golden tag = %v, want tag %v", got, tt.wantTag)
			}
			if tt.wantDeadline && snapshotStatus.GetSnapshotTime() == nil {
				t.Error("stored snapshot_time is nil, want set")
			}
		})
	}
}

type goldenEgressControl struct {
	*RPCService
	resume       func(context.Context, *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error)
	createPolicy func(context.Context, *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error)
	getPolicy    func(context.Context, *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error)
}

func (c *goldenEgressControl) ResumeActor(ctx context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
	return c.resume(ctx, req)
}

func (c *goldenEgressControl) CreateActorEgressPolicy(ctx context.Context, req *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	if c.createPolicy != nil {
		return c.createPolicy(ctx, req)
	}
	return c.RPCService.CreateActorEgressPolicy(ctx, req)
}

func (c *goldenEgressControl) GetActorEgressPolicy(ctx context.Context, req *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
	if c.getPolicy != nil {
		return c.getPolicy(ctx, req)
	}
	return c.RPCService.GetActorEgressPolicy(ctx, req)
}

func TestReconcileOne_GoldenEgressPolicyBeforeResume(t *testing.T) {
	preview.InitForTest(t, "GoldenEgressPolicy")
	for _, tt := range []struct {
		name   string
		policy *ateapi.EgressPolicyTemplate
	}{
		{name: "absent"},
		{name: "configured", policy: &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{{Https: &ateapi.HttpsRule{Hostnames: []string{"api.example.com"}}}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			impl := newServiceImpl(persistence, nil)
			impl.clusterSnapshotLocation = "gs://cluster-bucket/"
			svc := &RPCService{impl: impl}
			storetest.MustCreateAtespace(t, ctx, persistence, "templates")
			tmpl, err := svc.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
				tmpl.Metadata = &ateapi.ResourceMetadata{Atespace: "templates", Name: "tmpl"}
				tmpl.GoldenEgressPolicy = tt.policy
			})})
			if err != nil {
				t.Fatal(err)
			}
			stop := errors.New("stop before running workload")
			control := &goldenEgressControl{RPCService: svc, resume: func(ctx context.Context, req *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
				ref := req.GetActor()
				if ref.GetAtespace() != tmpl.GetMetadata().GetAtespace() || ref.GetName() != tmpl.GetMetadata().GetUid() {
					t.Fatalf("unexpected golden actor: %v", ref)
				}
				policy, err := svc.GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{Actor: ref, Name: resources.SingletonName})
				if tt.policy == nil {
					if apierror.Code(err) != codes.NotFound {
						t.Fatalf("golden without configured policy: %v, want NotFound", err)
					}
					return nil, stop
				}
				if err != nil {
					t.Fatalf("golden policy must exist before resume: %v", err)
				}
				want := &ateapi.EgressPolicy{Metadata: policy.Metadata, Rules: tmpl.GetGoldenEgressPolicy().GetRules()}
				if policy.GetMetadata().GetAtespace() != ref.GetAtespace() || policy.GetMetadata().GetName() != "default" || !proto.Equal(policy, want) {
					t.Fatalf("golden policy = %v, want rules %v in %s", policy, want.Rules, ref.GetAtespace())
				}
				if md := policy.GetMetadata(); md.GetUid() == "" || md.GetVersion() != 1 || md.GetCreateTime() == nil || md.GetUpdateTime() == nil {
					t.Errorf("unexpected policy metadata: %v", md)
				}
				return nil, stop
			}}
			r := newTestTemplateReconciler(persistence, control)
			defer r.queue.ShutDown()
			// Turning the gate off must not drop an existing template's policy.
			preview.InitForTest(t)
			for range 2 {
				if _, err := r.reconcileOne(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl)); !errors.Is(err, stop) {
					t.Fatalf("reconcile = %v, want to reach resume with the policy present", err)
				}
			}
		})
	}
}

func TestReconcileOne_GoldenEgressPolicyRecovery(t *testing.T) {
	preview.InitForTest(t, "GoldenEgressPolicy")
	for _, scenario := range []string{"create fails", "create response lost", "lookup fails", "conflicting policy"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			impl := newServiceImpl(persistence, nil)
			impl.clusterSnapshotLocation = "gs://cluster-bucket/"
			svc := &RPCService{impl: impl}
			storetest.MustCreateAtespace(t, ctx, persistence, "templates")
			tmpl, err := svc.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{ActorTemplate: validActorTemplate(func(tmpl *ateapi.ActorTemplate) {
				tmpl.Metadata = &ateapi.ResourceMetadata{Atespace: "templates", Name: "tmpl"}
				tmpl.GoldenEgressPolicy = &ateapi.EgressPolicyTemplate{Rules: []*ateapi.EgressRule{
					{Https: &ateapi.HttpsRule{Hostnames: []string{"api.example.com"}}},
				}}
			})})
			if err != nil {
				t.Fatal(err)
			}
			ref := &ateapi.ObjectRef{Atespace: tmpl.GetMetadata().GetAtespace(), Name: tmpl.GetMetadata().GetUid()}
			transient := errors.New("policy service unavailable")
			stop := errors.New("stop before running workload")
			resumeCalls := 0
			control := &goldenEgressControl{RPCService: svc, resume: func(context.Context, *ateapi.ResumeActorRequest) (*ateapi.ResumeActorResponse, error) {
				resumeCalls++
				return nil, stop
			}}
			control.createPolicy = func(ctx context.Context, req *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
				if scenario == "create fails" {
					return nil, transient
				}
				if scenario == "conflicting policy" {
					req.EgressPolicy.Rules[0].Https.Hostnames = []string{"other.example.com"}
				}
				if _, err := svc.CreateActorEgressPolicy(ctx, req); err != nil {
					t.Fatal(err)
				}
				if scenario == "create response lost" {
					return nil, transient
				}
				return nil, apierror.AlreadyExists("policy exists")
			}
			if scenario == "lookup fails" {
				control.getPolicy = func(context.Context, *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
					return nil, transient
				}
			}
			var actorUID string
			var previousPolicy *ateapi.EgressPolicy
			for pass := range 2 {
				// A fresh reconciler must recover from persisted state alone.
				r := newTestTemplateReconciler(persistence, control)
				after, err := r.reconcileOne(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl))
				r.queue.ShutDown()
				switch {
				case scenario == "conflicting policy":
					if err != nil || after != 0 {
						t.Fatalf("reconcile = (%v, %v), want terminal failure without retry", after, err)
					}
				case pass == 0:
					if !errors.Is(err, transient) {
						t.Fatalf("reconcile = %v, want policy service error", err)
					}
				default:
					if !errors.Is(err, stop) {
						t.Fatalf("reconcile = %v, want to reach resume", err)
					}
				}
				updated, err := persistence.GetActorTemplate(ctx, resources.ActorTemplateRefFromActorTemplate(tmpl))
				if err != nil {
					t.Fatal(err)
				}
				wantError := ""
				if scenario == "conflicting policy" {
					wantError = "GoldenEgressPolicyConflict: golden actor egress policy does not match the template"
				}
				snapshotStatus := updated.GetStatus().GetGoldenSnapshotStatus()
				if snapshotStatus.GetErrorMessage() != wantError || snapshotStatus.GetGoldenTag() != nil {
					t.Fatalf("unexpected golden snapshot status: %v", snapshotStatus)
				}
				wantResumes := 0
				if pass == 1 && scenario != "conflicting policy" {
					wantResumes = 1
				}
				if resumeCalls != wantResumes {
					t.Fatalf("resume calls = %d, want %d", resumeCalls, wantResumes)
				}
				actor, err := svc.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
				if err != nil {
					t.Fatal(err)
				}
				if actorUID != "" && actor.GetMetadata().GetUid() != actorUID {
					t.Fatal("retry recreated the golden actor")
				}
				actorUID = actor.GetMetadata().GetUid()
				if actor.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
					t.Fatalf("actor state = %v, want SUSPENDED", actor.GetStatus().GetState())
				}
				policy, err := svc.GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{Actor: ref, Name: resources.SingletonName})
				if pass == 0 && scenario == "create fails" {
					if apierror.Code(err) != codes.NotFound {
						t.Fatalf("policy lookup = %v, want NotFound", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if previousPolicy != nil && !proto.Equal(policy, previousPolicy) {
						t.Fatal("reconcile changed an existing policy")
					}
					wantHost := "api.example.com"
					if scenario == "conflicting policy" {
						wantHost = "other.example.com"
					}
					if got := policy.GetRules()[0].GetHttps().GetHostnames(); len(got) != 1 || got[0] != wantHost {
						t.Fatalf("policy hostnames = %v, want %s", got, wantHost)
					}
					previousPolicy = policy
				}
				control.createPolicy = nil
				control.getPolicy = nil
				if scenario == "conflicting policy" {
					control.createPolicy = func(context.Context, *ateapi.CreateActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
						t.Fatal("retried policy creation after terminal failure")
						return nil, nil
					}
					control.getPolicy = func(context.Context, *ateapi.GetActorEgressPolicyRequest) (*ateapi.EgressPolicy, error) {
						t.Fatal("retried policy lookup after terminal failure")
						return nil, nil
					}
				}
			}
		})
	}
}

// TestReconcileOne_GoldenActorRequests pins the shape of the control-plane
// requests the happy path issues: the golden actor is named after the
// template UID so recreated templates with the same name never collide, and
// lives in the template's atespace under ordinary tenant policies.
func TestReconcileOne_GoldenActorRequests(t *testing.T) {
	ctx := context.Background()
	st := newFakeTemplateStore(testTemplate())

	goldenSnapshot := "gs://bucket/root/atespaces/ns/actors/" + someActorUID + "/snapshots/snap-1"
	control := &fakeGoldenControl{snapshot: goldenSnapshot}
	r := newTestTemplateReconciler(st, control)

	if _, err := r.reconcileOne(ctx, testTemplateRef); err != nil {
		t.Fatalf("reconcileOne failed: %v", err)
	}

	created := control.createReqs[0].GetActor()
	if got := created.GetMetadata().GetName(); got != testTemplateUID {
		t.Errorf("golden actor name = %q, want template UID %q", got, testTemplateUID)
	}
	if got := created.GetMetadata().GetAtespace(); got != testTemplateRef.Atespace {
		t.Errorf("golden actor atespace = %q, want %q", got, testTemplateRef.Atespace)
	}
	if got := created.GetActorTemplate().GetName(); got != testTemplateName {
		t.Errorf("golden actor template ref = %q, want %q", got, testTemplateName)
	}
	if len(control.atespaceReqs) != 0 {
		t.Errorf("unexpected atespace creation requests: %v", control.atespaceReqs)
	}
	if got := control.resumeReqs[0].GetActor().GetName(); got != testTemplateUID {
		t.Errorf("resumed actor = %q, want %q", got, testTemplateUID)
	}
	if got := control.suspendReqs[0].GetActor().GetName(); got != testTemplateUID {
		t.Errorf("suspended actor = %q, want %q", got, testTemplateUID)
	}
	if got := control.suspendReqs[0].GetOptions().GetMode(); got != ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS {
		t.Errorf("golden suspend mode = %v, want %v", got, ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS)
	}
	if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetSnapshotTime() == nil {
		t.Error("stored snapshot_time is nil, want set")
	}
}

func TestCheckpoint_TerminalStateErrors(t *testing.T) {
	ctx := context.Background()

	for _, seed := range []struct {
		name string
		opt  func(*ateapi.ActorTemplate)
	}{
		{"golden snapshot taken", withGoldenTag()},
		{"failed", withFailed(reasonGoldenActorCrashed)},
	} {
		t.Run(seed.name, func(t *testing.T) {
			st := newFakeTemplateStore(testTemplate(seed.opt))
			r := newTestTemplateReconciler(st, &fakeGoldenControl{})

			// Checkpoint against a template a concurrent writer already
			// drove to a terminal state.
			_, err := r.checkpoint(ctx, testTemplate(seed.opt), func(snapshotStatus *ateapi.GoldenSnapshotStatus) {
				snapshotStatus.SnapshotTime = timestamppb.New(time.Now())
			})
			if err == nil {
				t.Fatal("checkpoint succeeded, want error for terminal template")
			}
			if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetSnapshotTime() != nil {
				t.Error("snapshot_time set, want store unchanged")
			}
		})
	}
}

func TestTruncateUTF8(t *testing.T) {
	tests := []struct {
		name string
		s    string
		n    int
		want string
	}{
		{
			name: "shorter than the bound",
			s:    "abc",
			n:    4,
			want: "abc",
		},
		{
			name: "exactly the bound",
			s:    "abcd",
			n:    4,
			want: "abcd",
		},
		{
			name: "ASCII over the bound",
			s:    "abcdef",
			n:    4,
			want: "abcd",
		},
		{
			// "é" is 2 bytes, so the cut at 4 would land inside it.
			name: "backs off a split rune",
			s:    "abcéf",
			n:    4,
			want: "abc",
		},
		{
			name: "cut on a rune boundary keeps the rune",
			s:    "abéf",
			n:    4,
			want: "abé",
		},
		{
			name: "zero bound",
			s:    "abc",
			n:    0,
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := truncateUTF8(tt.s, tt.n); got != tt.want {
				t.Errorf("truncateUTF8(%q, %d) = %q, want %q", tt.s, tt.n, got, tt.want)
			}
		})
	}
}

// TestFail_TruncatesErrorMessage pins that an oversized failure detail is
// stored cut to error_message's maxLength, as valid UTF-8 that still passes
// validation.
func TestFail_TruncatesErrorMessage(t *testing.T) {
	ctx := context.Background()
	st := newFakeTemplateStore(testTemplate())
	r := newTestTemplateReconciler(st, &fakeGoldenControl{})

	if err := r.fail(ctx, testTemplate(), reasonGoldenActorInvalid, strings.Repeat("é", maxGoldenErrorMessageLen)); err != nil {
		t.Fatalf("fail() error = %v", err)
	}

	golden := st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus()
	msg := golden.GetErrorMessage()
	if len(msg) > maxGoldenErrorMessageLen {
		t.Errorf("error_message is %d bytes, want at most %d", len(msg), maxGoldenErrorMessageLen)
	}
	if !strings.HasPrefix(msg, reasonGoldenActorInvalid+": ") {
		t.Errorf("error_message = %.40q..., want the %q reason prefix", msg, reasonGoldenActorInvalid)
	}
	if !utf8.ValidString(msg) {
		t.Error("error_message is not valid UTF-8")
	}
	op := apivalidation.MakeUpdateOp(ctx)
	if errs := apivalidation.Validate_GoldenSnapshotStatus(ctx, op, nil, golden, &ateapi.GoldenSnapshotStatus{}); len(errs) != 0 {
		t.Errorf("stored status fails validation: %v", errs)
	}
}

// TestCheckpoint_StaleObservationConflicts pins that checkpoint guards its
// write with the observed template's uid and version: once a concurrent write
// advances the stored version, the stale observation is rejected.
func TestCheckpoint_StaleObservationConflicts(t *testing.T) {
	ctx := context.Background()
	st := newFakeTemplateStore(testTemplate())
	r := newTestTemplateReconciler(st, &fakeGoldenControl{})

	// A concurrent writer advances the stored version past the observation.
	if _, err := st.UpdateActorTemplate(ctx, testTemplateRef, store.PreconditionFrom(testTemplate()), func(*ateapi.ActorTemplate) error { return nil }); err != nil {
		t.Fatalf("seeding concurrent update failed: %v", err)
	}

	_, err := r.checkpoint(ctx, testTemplate(), func(snapshotStatus *ateapi.GoldenSnapshotStatus) {
		snapshotStatus.SnapshotTime = timestamppb.New(time.Now())
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("checkpoint error = %v, want ErrVersionConflict", err)
	}
	if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetSnapshotTime() != nil {
		t.Error("snapshot_time set, want store unchanged")
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
		opts       []func(*ateapi.ActorTemplate)
		wantQueued bool
	}{
		{"empty status", nil, true},
		{"golden snapshot disabled", []func(*ateapi.ActorTemplate){withoutGoldenSnapshot}, false},
		{"mid warmup", []func(*ateapi.ActorTemplate){withSnapshotDeadline(time.Now().Add(time.Hour))}, true},
		{"golden snapshot taken", []func(*ateapi.ActorTemplate){withGoldenTag()}, false},
		{"failed", []func(*ateapi.ActorTemplate){withFailed(reasonGoldenActorCrashed)}, false},
	}

	// Seed one template per row, resync once, and check membership per row.
	templateName := func(name string) string {
		return "tmpl-" + strings.ReplaceAll(name, " ", "-")
	}
	templates := make([]*ateapi.ActorTemplate, 0, len(tests))
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
		testTemplate(func(tmpl *ateapi.ActorTemplate) { tmpl.Metadata.Name = "tmpl-a" }),
		testTemplate(func(tmpl *ateapi.ActorTemplate) { tmpl.Metadata.Name = "tmpl-b" }),
		testTemplate(func(tmpl *ateapi.ActorTemplate) { tmpl.Metadata.Name = "tmpl-c" }),
	)
	st.forcePageSize = 1
	r := newTestTemplateReconciler(st, &fakeGoldenControl{})

	r.resync(context.Background())

	if got := len(drainQueue(r)); got != 3 {
		t.Errorf("queued %d templates, want 3 (all pages walked)", got)
	}
}

func TestReconcileOne_GoldenTagRecovery(t *testing.T) {
	ref := &ateapi.ObjectRef{Atespace: testTemplateRef.Atespace, Name: testTemplateUID}
	completed := &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
		SourceActor: ref,
		Scope:       ateapi.TagScope_TAG_SCOPE_PUBLISHED,
		Status: &ateapi.TagStatus{
			ActorTemplateUid: testTemplateUID,
			Snapshot:         newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, testTemplateUID, "tag-snapshot", "gs://bucket/tag-snapshot", ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		},
	}
	incomplete := proto.CloneOf(completed)
	incomplete.Status.Snapshot = nil
	tests := []struct {
		name string
		// tag is the golden tag an earlier pass left behind, if any.
		tag *ateapi.Tag
		// actorDeleted seeds a pass that died after deleting the golden actor.
		actorDeleted bool
		// These errors fail one step of the first pass; the retry succeeds.
		createTagErr   error
		deleteActorErr error
		deleteTagErr   error
		// wantCreateTags counts copy attempts across both passes.
		wantCreateTags int
	}{
		{name: "completed tag", tag: completed, wantCreateTags: 0},
		{name: "actor already deleted", tag: completed, actorDeleted: true, wantCreateTags: 0},
		{name: "incomplete tag", tag: incomplete, wantCreateTags: 1},
		{name: "copy failure", createTagErr: errors.New("copy interrupted"), wantCreateTags: 2},
		{name: "actor deletion failure", deleteActorErr: errors.New("storage unavailable"), wantCreateTags: 1},
		{name: "tag deletion failure", tag: incomplete, deleteTagErr: errors.New("storage unavailable"), wantCreateTags: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			control := &fakeGoldenControl{
				tag: proto.CloneOf(tt.tag), exists: !tt.actorDeleted,
				goldenState: ateapi.ActorState_ACTOR_STATE_SUSPENDED, goldenSnapshot: "gs://bucket/actor-snapshot",
				tagErr: tt.createTagErr, deleteErr: tt.deleteActorErr, deleteTagErr: tt.deleteTagErr,
			}
			st := newFakeTemplateStore(testTemplate())
			r := newTestTemplateReconciler(st, control)
			defer r.queue.ShutDown()
			_, err := r.reconcileOne(t.Context(), testTemplateRef)
			wantErr := tt.createTagErr != nil || tt.deleteActorErr != nil || tt.deleteTagErr != nil
			if (err != nil) != wantErr {
				t.Fatalf("reconcile = %v, want error %v", err, wantErr)
			}
			if wantErr {
				if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag() != nil {
					t.Fatal("marked ready before cleanup completed")
				}
				if !control.exists {
					t.Fatal("deleted actor after tag failure")
				}
				control.tagErr, control.deleteErr, control.deleteTagErr = nil, nil, nil
				if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
					t.Fatal(err)
				}
			}
			if control.exists {
				t.Fatal("golden actor still exists")
			}
			if !proto.Equal(st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetGoldenTag(), ref) {
				t.Fatal("golden tag not recorded")
			}
			if tagDurableSnapshotURI(control.tag, ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED) == "" {
				t.Fatal("golden tag has no snapshot")
			}
			if len(control.createReqs) != 0 || len(control.resumeReqs) != 0 || len(control.suspendReqs) != 0 {
				t.Fatal("repeated golden actor warmup")
			}
			if got := len(control.tagReqs); got != tt.wantCreateTags {
				t.Fatalf("CreateTag calls = %d, want %d", got, tt.wantCreateTags)
			}
			for _, req := range control.tagReqs {
				if !proto.Equal(req.Tag.SourceActor, ref) || req.Tag.Scope != ateapi.TagScope_TAG_SCOPE_PUBLISHED || req.Tag.Metadata.Name != ref.Name {
					t.Fatalf("incorrect golden tag request: %v", req)
				}
			}
		})
	}
}

func TestReconcileOne_GoldenTagConflict(t *testing.T) {
	tests := []struct {
		name        string
		templateUID string
		sourceActor *ateapi.ObjectRef
	}{
		{name: "template", templateUID: "another-template", sourceActor: &ateapi.ObjectRef{Atespace: testTemplateRef.Atespace, Name: testTemplateUID}},
		{name: "actor name", templateUID: testTemplateUID, sourceActor: &ateapi.ObjectRef{Atespace: testTemplateRef.Atespace, Name: "another-actor"}},
		{name: "actor atespace", templateUID: testTemplateUID, sourceActor: &ateapi.ObjectRef{Atespace: "another-atespace", Name: testTemplateUID}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag := &ateapi.Tag{
				SourceActor: tt.sourceActor,
				Status:      &ateapi.TagStatus{ActorTemplateUid: tt.templateUID},
			}
			control := &fakeGoldenControl{tag: proto.CloneOf(tag), exists: true}
			st := newFakeTemplateStore(testTemplate())
			r := newTestTemplateReconciler(st, control)
			defer r.queue.ShutDown()
			for range 2 {
				after, err := r.reconcileOne(t.Context(), testTemplateRef)
				if err != nil || after != 0 {
					t.Fatalf("reconcile = (%v, %v), want terminal failure without retry", after, err)
				}
				snapshotStatus := st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus()
				if snapshotStatus.GetErrorMessage() != reasonGoldenTagConflict+": golden tag belongs to another actor or template" || snapshotStatus.GetGoldenTag() != nil {
					t.Fatalf("unexpected golden snapshot status: %v", snapshotStatus)
				}
				if !proto.Equal(control.tag, tag) || !control.exists || len(control.tagReqs) != 0 || len(control.deleteReqs) != 0 {
					t.Fatal("modified golden resources after ownership conflict")
				}
			}
			r.resync(t.Context())
			if r.queue.Len() != 0 {
				t.Fatal("resync queued a terminally failed template")
			}
		})
	}
}
