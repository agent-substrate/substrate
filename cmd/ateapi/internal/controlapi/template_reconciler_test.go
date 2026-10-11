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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/apivalidation"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/snapshotlayout"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/operation"
)

// fakeTemplateStore is an in-memory templateReconcilerStore.
type fakeTemplateStore struct {
	mu        sync.Mutex
	templates map[resources.ActorTemplateRef]*ateapipb.ActorTemplate

	// leaseErr, when set, is returned by AcquireLease.
	leaseErr error
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

func (s *fakeTemplateStore) UpdateActorTemplate(_ context.Context, ref resources.ActorTemplateRef, precondition store.Precondition, mutate func(*ateapipb.ActorTemplate) error) (*ateapipb.ActorTemplate, error) {
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
	updated := proto.Clone(tmpl).(*ateapipb.ActorTemplate)
	if err := mutate(updated); err != nil {
		return nil, err
	}
	updated.Metadata.Version++
	s.templates[ref] = updated
	return proto.Clone(updated).(*ateapipb.ActorTemplate), nil
}

func (s *fakeTemplateStore) AcquireLease(ctx context.Context, _ string) (*store.Lease, error) {
	if s.leaseErr != nil {
		return nil, s.leaseErr
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	return store.NewLease(leaseCtx, cancel), nil
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
	// tags are the tags of the reserved atespace, by name: the golden tag
	// named after the template UID and, for a golden taken again, the one
	// named after the UID and the layout.
	tags          map[string]*ateapipb.Tag
	tagErr        error
	deleteErr     error
	deleteTagErr  error
	tagReqs       []*ateapipb.CreateTagRequest
	deleteReqs    []*ateapipb.DeleteActorRequest
	deleteTagReqs []*ateapipb.DeleteTagRequest

	createErr  error
	resumeErr  error
	suspendErr error
	getErr     error

	// exists seeds whether the golden actor pre-exists; goldenState and
	// goldenSnapshot are its observed state and external snapshot while it
	// does.
	exists         bool
	goldenState    ateapipb.ActorState
	goldenSnapshot string
	// snapshot is the external snapshot a completed suspend produces; empty
	// simulates a suspend that wrote none.
	snapshot string

	createReqs   []*ateapipb.CreateActorRequest
	resumeReqs   []*ateapipb.ResumeActorRequest
	suspendReqs  []*ateapipb.SuspendActorRequest
	atespaceReqs []*ateapipb.CreateAtespaceRequest
}

func (c *fakeGoldenControl) CreateAtespace(_ context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.atespaceReqs = append(c.atespaceReqs, req)
	return req.GetAtespace(), nil
}

func (c *fakeGoldenControl) goldenActorStatus() *ateapipb.ActorStatus {
	st := &ateapipb.ActorStatus{State: c.goldenState}
	if c.goldenSnapshot != "" {
		st.LastAssignedGeneration = 1
		st.Snapshots = []*ateapipb.Snapshot{
			newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, testTemplateUID, c.goldenSnapshot, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		}
	}
	return st
}

func (c *fakeGoldenControl) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createReqs = append(c.createReqs, req)
	// AlreadyExists means the actor does exist (e.g. a racing creation).
	if c.createErr == nil || apierror.Code(c.createErr) == codes.AlreadyExists {
		c.exists = true
		c.goldenState = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	}
	if c.createErr != nil {
		return nil, c.createErr
	}
	// Like the real Control service, return the stored actor: metadata plus
	// a status observing the initial SUSPENDED state.
	return &ateapipb.Actor{
		Metadata: req.GetActor().GetMetadata(),
		Status:   c.goldenActorStatus(),
	}, nil
}

func (c *fakeGoldenControl) GetActor(_ context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.getErr != nil {
		return nil, c.getErr
	}
	if !c.exists {
		return nil, apierror.NotFound("no such actor")
	}
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: req.GetActor().GetAtespace(), Name: req.GetActor().GetName()},
		Status:   c.goldenActorStatus(),
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
	if c.goldenSnapshot == "" {
		c.goldenSnapshot = c.snapshot
	}
	return &ateapipb.SuspendActorResponse{
		Actor: &ateapipb.Actor{Status: c.goldenActorStatus()},
	}, nil
}

func (c *fakeGoldenControl) GetTag(_ context.Context, req *ateapipb.GetTagRequest) (*ateapipb.Tag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tag, ok := c.tags[req.GetTag().GetName()]
	if !ok {
		return nil, apierror.NotFound("no tag")
	}
	return proto.CloneOf(tag), nil
}

func (c *fakeGoldenControl) CreateTag(_ context.Context, req *ateapipb.CreateTagRequest) (*ateapipb.Tag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tagReqs = append(c.tagReqs, req)
	if c.tagErr != nil {
		return nil, c.tagErr
	}
	tag := proto.CloneOf(req.GetTag())
	tag.Status = &ateapipb.TagStatus{
		ActorTemplateUid: testTemplateUID,
		Snapshot:         newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, testTemplateUID, c.goldenSnapshot, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
	}
	if c.tags == nil {
		c.tags = map[string]*ateapipb.Tag{}
	}
	c.tags[tag.GetMetadata().GetName()] = tag
	return proto.CloneOf(tag), nil
}

func (c *fakeGoldenControl) DeleteTag(_ context.Context, req *ateapipb.DeleteTagRequest) (*ateapipb.Tag, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteTagReqs = append(c.deleteTagReqs, req)
	if c.deleteTagErr != nil {
		return nil, c.deleteTagErr
	}
	name := req.GetTag().GetName()
	tag, ok := c.tags[name]
	if !ok {
		return nil, apierror.NotFound("no tag")
	}
	delete(c.tags, name)
	return tag, nil
}

// DeleteActor releases the golden actor with its snapshot: one created again
// starts without.

func (c *fakeGoldenControl) DeleteActor(_ context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
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
	c.goldenSnapshot = ""
	return &ateapipb.Actor{}, nil
}

// goldenTags keys tags by name for a fakeGoldenControl, skipping nil ones.
func goldenTags(tags ...*ateapipb.Tag) map[string]*ateapipb.Tag {
	byName := map[string]*ateapipb.Tag{}
	for _, tag := range tags {
		if tag != nil {
			byName[tag.GetMetadata().GetName()] = proto.CloneOf(tag)
		}
	}
	return byName
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
func testTemplate(opts ...func(*ateapipb.ActorTemplate)) *ateapipb.ActorTemplate {
	tmpl := &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: testTemplateName, Uid: testTemplateUID, Version: 1},
		Containers: []*ateapipb.Container{
			{Name: "main", Image: "img", WakeupProbe: &ateapipb.ContainerWakeupProbe{}},
		},
		SnapshotConfig: &ateapipb.SnapshotConfig{
			GoldenSnapshotConfig: &ateapipb.GoldenSnapshotConfig{Mode: ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
		},
		Status: &ateapipb.ActorTemplateStatus{},
	}
	for _, opt := range opts {
		opt(tmpl)
	}
	return tmpl
}

func withoutWakeupProbe(tmpl *ateapipb.ActorTemplate) {
	for _, container := range tmpl.Containers {
		container.WakeupProbe = nil
	}
}

func withoutGoldenSnapshot(tmpl *ateapipb.ActorTemplate) {
	tmpl.SnapshotConfig.GoldenSnapshotConfig.Mode = ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED
}

// seededGoldenStatus returns the template's golden snapshot status, allocating
// it so the with* options below can mutate it.
func seededGoldenStatus(tmpl *ateapipb.ActorTemplate) *ateapipb.GoldenSnapshotStatus {
	if tmpl.Status.GoldenSnapshotStatus == nil {
		tmpl.Status.GoldenSnapshotStatus = &ateapipb.GoldenSnapshotStatus{}
	}
	return tmpl.Status.GoldenSnapshotStatus
}

func withSnapshotDeadline(at time.Time) func(*ateapipb.ActorTemplate) {
	return func(tmpl *ateapipb.ActorTemplate) {
		seededGoldenStatus(tmpl).TakeGoldenSnapshotAt = timestamppb.New(at)
	}
}

// withGoldenTag records the golden tag named after the template UID, taken in
// the layout the workers write: the golden is done.
func withGoldenTag() func(*ateapipb.ActorTemplate) {
	return withGoldenTagInLayout(snapshotlayout.Current)
}

// withGoldenTagInLayout records the golden tag named after the template UID,
// taken in layout: snapshotlayout.Unrecorded for one a release before layouts
// were recorded left.
func withGoldenTagInLayout(layout int32) func(*ateapipb.ActorTemplate) {
	return func(tmpl *ateapipb.ActorTemplate) {
		snapshotStatus := seededGoldenStatus(tmpl)
		snapshotStatus.GoldenTag = &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID}
		snapshotStatus.SnapshotLayout = layout
	}
}

// withRetakenGoldenTag records the golden tag of a golden taken again in the
// layout the workers write, named after the template UID and the layout.
func withRetakenGoldenTag() func(*ateapipb.ActorTemplate) {
	return func(tmpl *ateapipb.ActorTemplate) {
		snapshotStatus := seededGoldenStatus(tmpl)
		snapshotStatus.GoldenTag = &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: retakenGoldenTagName(testTemplateUID)}
		snapshotStatus.SnapshotLayout = snapshotlayout.Current
	}
}

func withFailed(reason string) func(*ateapipb.ActorTemplate) {
	return func(tmpl *ateapipb.ActorTemplate) {
		seededGoldenStatus(tmpl).ErrorMessage = reason + ": seeded failure"
	}
}

func newTestTemplateReconciler(persistence templateReconcilerStore, control goldenActorControl) *ActorTemplateReconciler {
	return NewActorTemplateReconciler(persistence, control, 7*time.Second)
}

func TestGoldenSnapshotWarmupFor(t *testing.T) {
	probe := &ateapipb.ContainerWakeupProbe{}
	tests := []struct {
		name       string
		containers []*ateapipb.Container
		want       time.Duration
	}{
		{"no containers", nil, goldenSnapshotWarmup},
		{"all containers have wakeup probe", []*ateapipb.Container{{WakeupProbe: probe}, {WakeupProbe: probe}}, 0},
		{"one container missing wakeup probe", []*ateapipb.Container{{WakeupProbe: probe}, {}}, goldenSnapshotWarmup},
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
	goldenSnapshot := "gs://bucket/root/atespaces/ate-golden/actors/" + someActorUID + "/snapshots/snap-1"
	tests := []struct {
		name     string
		template *ateapipb.ActorTemplate // nil: template absent from the store
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
		// wantDeadline asserts whether take_golden_snapshot_at is set.
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
			control:        &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			wantRequeueMin: 59 * time.Minute,
			wantRequeueMax: time.Hour,
		},
		{
			name: "running golden actor is snapshotted once the deadline passed",
			template: testTemplate(
				withSnapshotDeadline(time.Now().Add(-time.Minute))),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING, snapshot: goldenSnapshot},
			wantTag:      true,
			wantSuspends: 1,
		},
		{
			name:           "running golden actor with a lost deadline restarts the warmup",
			template:       testTemplate(withoutWakeupProbe),
			control:        &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING},
			wantRequeueMin: goldenSnapshotWarmup - time.Second,
			wantRequeueMax: goldenSnapshotWarmup,
			wantDeadline:   true,
		},
		{
			name: "suspend returning no snapshot errors",
			template: testTemplate(
				withSnapshotDeadline(time.Now().Add(-time.Minute))),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_RUNNING, snapshot: ""},
			wantErr:      true,
			wantSuspends: 1,
		},
		{
			name:         "suspending golden actor is completed and recorded",
			template:     testTemplate(),
			control:      &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDING, snapshot: goldenSnapshot},
			wantTag:      true,
			wantSuspends: 1,
		},
		{
			name:     "suspended golden actor with a snapshot is recorded without more control calls",
			template: testTemplate(),
			control:  &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, goldenSnapshot: goldenSnapshot},
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
			control:          &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_CRASHED},
			wantFailedReason: reasonGoldenActorCrashed,
			wantMessage:      "crashed",
		},
		{
			name:        "resume failure requeues without failing",
			template:    testTemplate(),
			control:     &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, resumeErr: apierror.Unavailable("no workers")},
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
			control:          &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_DELETING},
			wantFailedReason: reasonUnexpectedState,
			wantMessage:      "unexpected state",
		},
		{
			name:           "unspecified actor state checks back later",
			template:       testTemplate(),
			control:        &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_UNSPECIFIED},
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
			if tt.wantDeadline && snapshotStatus.GetTakeGoldenSnapshotAt() == nil {
				t.Error("stored take_golden_snapshot_at is nil, want set")
			}
		})
	}
}

// TestReconcileOne_GoldenActorRequests pins the shape of the control-plane
// requests the happy path issues: the golden actor is named after the
// template UID so recreated templates with the same name never collide, and
// lives in the reserved ate-golden atespace so the suspend workflow commits
// it Full regardless of the template's preferredFidelity scope.
func TestReconcileOne_GoldenActorRequests(t *testing.T) {
	ctx := context.Background()
	st := newFakeTemplateStore(testTemplate())

	goldenSnapshot := "gs://bucket/root/atespaces/ate-golden/actors/" + someActorUID + "/snapshots/snap-1"
	control := &fakeGoldenControl{snapshot: goldenSnapshot}
	r := newTestTemplateReconciler(st, control)

	if _, err := r.reconcileOne(ctx, testTemplateRef); err != nil {
		t.Fatalf("reconcileOne failed: %v", err)
	}

	created := control.createReqs[0].GetActor()
	if got := created.GetMetadata().GetName(); got != testTemplateUID {
		t.Errorf("golden actor name = %q, want template UID %q", got, testTemplateUID)
	}
	if got := created.GetMetadata().GetAtespace(); got != resources.GoldenActorAtespace {
		t.Errorf("golden actor atespace = %q, want %q", got, resources.GoldenActorAtespace)
	}
	if got := created.GetActorTemplate().GetName(); got != testTemplateName {
		t.Errorf("golden actor template ref = %q, want %q", got, testTemplateName)
	}
	if len(control.atespaceReqs) != 1 || control.atespaceReqs[0].GetAtespace().GetMetadata().GetName() != resources.GoldenActorAtespace {
		t.Errorf("atespace ensure requests = %v, want one for %q", control.atespaceReqs, resources.GoldenActorAtespace)
	}
	if got := control.resumeReqs[0].GetActor().GetName(); got != testTemplateUID {
		t.Errorf("resumed actor = %q, want %q", got, testTemplateUID)
	}
	if got := control.suspendReqs[0].GetActor().GetName(); got != testTemplateUID {
		t.Errorf("suspended actor = %q, want %q", got, testTemplateUID)
	}
	if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetTakeGoldenSnapshotAt() == nil {
		t.Error("stored take_golden_snapshot_at is nil, want set")
	}
}

func TestCheckpoint_TerminalStateErrors(t *testing.T) {
	ctx := context.Background()

	for _, seed := range []struct {
		name string
		opt  func(*ateapipb.ActorTemplate)
	}{
		{"golden snapshot taken", withGoldenTag()},
		{"failed", withFailed(reasonGoldenActorCrashed)},
	} {
		t.Run(seed.name, func(t *testing.T) {
			st := newFakeTemplateStore(testTemplate(seed.opt))
			r := newTestTemplateReconciler(st, &fakeGoldenControl{})

			// Checkpoint against a template a concurrent writer already
			// drove to a terminal state.
			_, err := r.checkpoint(ctx, testTemplate(seed.opt), func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
				snapshotStatus.TakeGoldenSnapshotAt = timestamppb.New(time.Now())
			})
			if err == nil {
				t.Fatal("checkpoint succeeded, want error for terminal template")
			}
			if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetTakeGoldenSnapshotAt() != nil {
				t.Error("take_golden_snapshot_at set, want store unchanged")
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
	op := operation.Operation{Type: operation.Update}
	if errs := apivalidation.Validate_GoldenSnapshotStatus(ctx, op, nil, golden, &ateapipb.GoldenSnapshotStatus{}); len(errs) != 0 {
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
	if _, err := st.UpdateActorTemplate(ctx, testTemplateRef, store.PreconditionFrom(testTemplate()), func(*ateapipb.ActorTemplate) error { return nil }); err != nil {
		t.Fatalf("seeding concurrent update failed: %v", err)
	}

	_, err := r.checkpoint(ctx, testTemplate(), func(snapshotStatus *ateapipb.GoldenSnapshotStatus) {
		snapshotStatus.TakeGoldenSnapshotAt = timestamppb.New(time.Now())
	})
	if !errors.Is(err, store.ErrVersionConflict) {
		t.Fatalf("checkpoint error = %v, want ErrVersionConflict", err)
	}
	if st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetTakeGoldenSnapshotAt() != nil {
		t.Error("take_golden_snapshot_at set, want store unchanged")
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
		{"golden snapshot disabled", []func(*ateapipb.ActorTemplate){withoutGoldenSnapshot}, false},
		{"mid warmup", []func(*ateapipb.ActorTemplate){withSnapshotDeadline(time.Now().Add(time.Hour))}, true},
		{"golden snapshot taken", []func(*ateapipb.ActorTemplate){withGoldenTag()}, false},
		{"golden snapshot recorded without its layout", []func(*ateapipb.ActorTemplate){withGoldenTagInLayout(snapshotlayout.Unrecorded)}, true},
		{"golden snapshot in an older layout", []func(*ateapipb.ActorTemplate){withGoldenTagInLayout(snapshotlayout.SingleDurableTar)}, true},
		{"golden snapshot taken again in the current layout", []func(*ateapipb.ActorTemplate){withRetakenGoldenTag()}, false},
		{"failed", []func(*ateapipb.ActorTemplate){withFailed(reasonGoldenActorCrashed)}, false},
		{"failed re-take", []func(*ateapipb.ActorTemplate){withGoldenTagInLayout(snapshotlayout.SingleDurableTar), withFailed(reasonGoldenActorCrashed)}, false},
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

func TestReconcileOne_GoldenTagRecovery(t *testing.T) {
	ref := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID}
	completed := &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
		SourceActor: ref,
		Scope:       ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
		Status: &ateapipb.TagStatus{
			ActorTemplateUid: testTemplateUID,
			Snapshot:         newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, testTemplateUID, "gs://bucket/tag-snapshot", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
		},
	}
	incomplete := proto.CloneOf(completed)
	incomplete.Status.Snapshot = nil
	tests := []struct {
		name string
		// tag is the golden tag an earlier pass left behind, if any.
		tag *ateapipb.Tag
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
				tags: goldenTags(tt.tag), exists: !tt.actorDeleted,
				goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, goldenSnapshot: "gs://bucket/actor-snapshot",
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
			if tagDurableSnapshotURI(control.tags[ref.Name], ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED) == "" {
				t.Fatal("golden tag has no snapshot")
			}
			if got := st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus().GetSnapshotLayout(); got != snapshotlayout.Current {
				t.Fatalf("recorded snapshot layout = %d, want the current %d", got, snapshotlayout.Current)
			}
			if len(control.createReqs) != 0 || len(control.resumeReqs) != 0 || len(control.suspendReqs) != 0 {
				t.Fatal("repeated golden actor warmup")
			}
			if got := len(control.tagReqs); got != tt.wantCreateTags {
				t.Fatalf("CreateTag calls = %d, want %d", got, tt.wantCreateTags)
			}
			for _, req := range control.tagReqs {
				if !proto.Equal(req.Tag.SourceActor, ref) || req.Tag.Scope != ateapipb.TagScope_TAG_SCOPE_PUBLISHED || req.Tag.Metadata.Name != ref.Name {
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
		sourceActor *ateapipb.ObjectRef
	}{
		{name: "template", templateUID: "another-template", sourceActor: &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID}},
		{name: "actor name", templateUID: testTemplateUID, sourceActor: &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: "another-actor"}},
		{name: "actor atespace", templateUID: testTemplateUID, sourceActor: &ateapipb.ObjectRef{Atespace: "another-atespace", Name: testTemplateUID}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag := &ateapipb.Tag{
				Metadata:    &ateapipb.ResourceMetadata{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID},
				SourceActor: tt.sourceActor,
				Status:      &ateapipb.TagStatus{ActorTemplateUid: tt.templateUID},
			}
			control := &fakeGoldenControl{tags: goldenTags(tag), exists: true}
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
				if !proto.Equal(control.tags[testTemplateUID], tag) || !control.exists || len(control.tagReqs) != 0 || len(control.deleteReqs) != 0 {
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

func TestGoldenSnapshotLayout(t *testing.T) {
	tag := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID}
	tests := []struct {
		name   string
		status *ateapipb.GoldenSnapshotStatus
		// wantStale: the golden is in an older layout or recorded without
		// one. wantPending: it is being taken again. wantDone: the build is
		// terminal.
		wantStale, wantPending, wantDone bool
	}{
		{name: "no status"},
		{name: "no golden tag", status: &ateapipb.GoldenSnapshotStatus{}},
		{name: "golden tag recorded without its layout", status: &ateapipb.GoldenSnapshotStatus{GoldenTag: tag}, wantStale: true, wantPending: true},
		{name: "golden tag in an older layout", status: &ateapipb.GoldenSnapshotStatus{GoldenTag: tag, SnapshotLayout: snapshotlayout.SingleDurableTar}, wantStale: true, wantPending: true},
		{name: "golden tag in the current layout", status: &ateapipb.GoldenSnapshotStatus{GoldenTag: tag, SnapshotLayout: snapshotlayout.Current}, wantDone: true},
		{name: "golden tag in a newer layout", status: &ateapipb.GoldenSnapshotStatus{GoldenTag: tag, SnapshotLayout: snapshotlayout.Current + 1}, wantDone: true},
		{name: "failed re-take", status: &ateapipb.GoldenSnapshotStatus{GoldenTag: tag, SnapshotLayout: snapshotlayout.SingleDurableTar, ErrorMessage: reasonGoldenActorCrashed + ": crashed"}, wantStale: true, wantDone: true},
		{name: "failed first take", status: &ateapipb.GoldenSnapshotStatus{ErrorMessage: reasonGoldenActorCrashed + ": crashed"}, wantDone: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := staleGoldenSnapshotLayout(tt.status); got != tt.wantStale {
				t.Errorf("staleGoldenSnapshotLayout() = %v, want %v", got, tt.wantStale)
			}
			if got := goldenSnapshotRetakePending(tt.status); got != tt.wantPending {
				t.Errorf("goldenSnapshotRetakePending() = %v, want %v", got, tt.wantPending)
			}
			if got := goldenSnapshotDone(tt.status); got != tt.wantDone {
				t.Errorf("goldenSnapshotDone() = %v, want %v", got, tt.wantDone)
			}
		})
	}
}

// TestGoldenTagRefs pins the names of the tags the golden flow makes and
// deletes: the first golden's after the template UID, a re-take's after the
// UID and the layout, beside the one it replaces.
func TestGoldenTagRefs(t *testing.T) {
	retaken := testTemplateUID + "-" + strconv.Itoa(int(snapshotlayout.Current))
	tests := []struct {
		name string
		opts []func(*ateapipb.ActorTemplate)
		// wantNext names the tag the next take records; empty when the
		// golden is done.
		wantNext string
		// wantRefs names every tag the template's deletion removes.
		wantRefs []string
	}{
		{name: "no golden", wantNext: testTemplateUID, wantRefs: []string{testTemplateUID}},
		{name: "golden recorded without its layout", opts: []func(*ateapipb.ActorTemplate){withGoldenTagInLayout(snapshotlayout.Unrecorded)}, wantNext: retaken, wantRefs: []string{testTemplateUID, retaken}},
		{name: "golden in an older layout", opts: []func(*ateapipb.ActorTemplate){withGoldenTagInLayout(snapshotlayout.SingleDurableTar)}, wantNext: retaken, wantRefs: []string{testTemplateUID, retaken}},
		{name: "failed re-take", opts: []func(*ateapipb.ActorTemplate){withGoldenTagInLayout(snapshotlayout.SingleDurableTar), withFailed(reasonGoldenActorCrashed)}, wantRefs: []string{testTemplateUID, retaken}},
		{name: "golden in the current layout", opts: []func(*ateapipb.ActorTemplate){withGoldenTag()}, wantRefs: []string{testTemplateUID}},
		{name: "golden taken again in the current layout", opts: []func(*ateapipb.ActorTemplate){withRetakenGoldenTag()}, wantRefs: []string{testTemplateUID, retaken}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl := testTemplate(tt.opts...)
			snapshotStatus := tmpl.GetStatus().GetGoldenSnapshotStatus()
			if tt.wantNext != "" {
				next := goldenTagRef(tmpl, snapshotStatus)
				if next.GetAtespace() != resources.GoldenActorAtespace || next.GetName() != tt.wantNext {
					t.Errorf("goldenTagRef() = %v, want %s/%s", next, resources.GoldenActorAtespace, tt.wantNext)
				}
			}
			var got []string
			for _, ref := range goldenTagRefs(tmpl) {
				if ref.Atespace != resources.GoldenActorAtespace {
					t.Errorf("goldenTagRefs() names %v outside %s", ref, resources.GoldenActorAtespace)
				}
				got = append(got, ref.Name)
			}
			if strings.Join(got, ",") != strings.Join(tt.wantRefs, ",") {
				t.Errorf("goldenTagRefs() = %v, want %v", got, tt.wantRefs)
			}
		})
	}
}

// goldenTestTag is a golden tag of the test template's golden actor: complete
// with snapshotURI, unfinished without.
func goldenTestTag(ref *ateapipb.ObjectRef, snapshotURI string) *ateapipb.Tag {
	tag := &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
		SourceActor: &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID},
		Scope:       ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
		Status:      &ateapipb.TagStatus{ActorTemplateUid: testTemplateUID},
	}
	if snapshotURI != "" {
		tag.Status.Snapshot = newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, testTemplateUID, snapshotURI, ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED)
	}
	return tag
}

// TestReconcileOne_RetakesGoldenInOlderLayout pins the re-take of a golden
// written in an older snapshot layout, or recorded without one: a golden
// actor is booted and snapshotted again, its tag made beside the recorded
// one, which stays the template's golden until the new one is complete, and
// recorded with the current layout, the replaced tag deleted. The failure
// reasons and bounds of a first take apply, and a failed or interrupted
// re-take keeps the old golden recorded.
func TestReconcileOne_RetakesGoldenInOlderLayout(t *testing.T) {
	oldRef := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID}
	newRef := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: retakenGoldenTagName(testTemplateUID)}
	oldSnapshot := "s3://bucket/root/atespaces/ate-golden/tags/old-tag-uid"
	newSnapshot := "s3://bucket/root/atespaces/ate-golden/actors/" + testTemplateUID + "/snapshots/snap-2"
	tests := []struct {
		name    string
		control func() *fakeGoldenControl
		// wantRetaken: the new tag is recorded in the current layout and the
		// old one deleted. wantFailure: the template fails with this reason,
		// the old golden kept beside the failure. Neither: the pass errors
		// and the old golden stays recorded for the retry.
		wantRetaken                                            bool
		wantFailure                                            string
		wantCreates, wantResumes, wantSuspends, wantTagCreates int
	}{
		{
			name: "boots a golden actor, takes the golden in the current layout and swaps the tag",
			control: func() *fakeGoldenControl {
				return &fakeGoldenControl{snapshot: newSnapshot, tags: goldenTags(goldenTestTag(oldRef, oldSnapshot))}
			},
			wantRetaken: true, wantCreates: 1, wantResumes: 1, wantSuspends: 1, wantTagCreates: 1,
		},
		{
			name: "records a finished tag an earlier pass made",
			control: func() *fakeGoldenControl {
				return &fakeGoldenControl{tags: goldenTags(goldenTestTag(oldRef, oldSnapshot), goldenTestTag(newRef, newSnapshot))}
			},
			wantRetaken: true,
		},
		{
			name: "makes an unfinished tag again",
			control: func() *fakeGoldenControl {
				return &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED, goldenSnapshot: newSnapshot,
					tags: goldenTags(goldenTestTag(oldRef, oldSnapshot), goldenTestTag(newRef, ""))}
			},
			wantRetaken: true, wantTagCreates: 1,
		},
		{
			name: "a crashed golden actor fails the template, the old golden kept",
			control: func() *fakeGoldenControl {
				return &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_CRASHED,
					tags: goldenTags(goldenTestTag(oldRef, oldSnapshot))}
			},
			wantFailure: reasonGoldenActorCrashed,
		},
		{
			name: "a resume that fails keeps the old golden for the retry",
			control: func() *fakeGoldenControl {
				return &fakeGoldenControl{exists: true, goldenState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
					resumeErr: apierror.Unavailable("no workers"), tags: goldenTags(goldenTestTag(oldRef, oldSnapshot))}
			},
			wantResumes: 1,
		},
		{
			name: "a replaced tag that cannot be deleted keeps the record on it for the retry",
			control: func() *fakeGoldenControl {
				return &fakeGoldenControl{deleteTagErr: errors.New("storage unavailable"),
					tags: goldenTags(goldenTestTag(oldRef, oldSnapshot), goldenTestTag(newRef, newSnapshot))}
			},
		},
	}
	for _, layout := range []int32{snapshotlayout.Unrecorded, snapshotlayout.SingleDurableTar} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("layout %d/%s", layout, tt.name), func(t *testing.T) {
				control := tt.control()
				st := newFakeTemplateStore(testTemplate(withGoldenTagInLayout(layout)))
				r := newTestTemplateReconciler(st, control)
				defer r.queue.ShutDown()

				wantErr := !tt.wantRetaken && tt.wantFailure == ""
				after, err := r.reconcileOne(t.Context(), testTemplateRef)
				if (err != nil) != wantErr || after != 0 {
					t.Fatalf("reconcileOne = (%v, %v), want error %v without requeue", after, err, wantErr)
				}
				snapshotStatus := st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus()
				_, oldKept := control.tags[oldRef.GetName()]
				switch {
				case tt.wantRetaken:
					if !proto.Equal(snapshotStatus.GetGoldenTag(), newRef) || snapshotStatus.GetSnapshotLayout() != snapshotlayout.Current || snapshotStatus.GetErrorMessage() != "" {
						t.Errorf("stored status = %v, want the re-taken tag %v in layout %d", snapshotStatus, newRef, snapshotlayout.Current)
					}
					if !goldenSnapshotDone(snapshotStatus) {
						t.Error("re-taken golden is not done")
					}
					if oldKept {
						t.Error("the replaced golden tag was not deleted")
					}
					if tagDurableSnapshotURI(control.tags[newRef.GetName()], ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED) == "" {
						t.Error("the re-taken golden tag has no snapshot")
					}
					if control.exists {
						t.Error("golden actor still exists after its snapshot became the golden tag")
					}
				case tt.wantFailure != "":
					if !strings.HasPrefix(snapshotStatus.GetErrorMessage(), tt.wantFailure+": ") {
						t.Errorf("stored error message = %q, want reason %q", snapshotStatus.GetErrorMessage(), tt.wantFailure)
					}
					if !proto.Equal(snapshotStatus.GetGoldenTag(), oldRef) || snapshotStatus.GetSnapshotLayout() != layout {
						t.Errorf("stored status = %v, want the old golden kept beside the failure", snapshotStatus)
					}
					if !oldKept {
						t.Error("the failed re-take deleted the old golden tag")
					}
					if goldenSnapshotRetakePending(snapshotStatus) || !goldenSnapshotDone(snapshotStatus) {
						t.Error("failed re-take is not terminal")
					}
				default:
					if !proto.Equal(snapshotStatus.GetGoldenTag(), oldRef) || snapshotStatus.GetSnapshotLayout() != layout || snapshotStatus.GetErrorMessage() != "" {
						t.Errorf("stored status = %v, want the old golden kept for the retry", snapshotStatus)
					}
					if !oldKept {
						t.Error("the interrupted re-take deleted the old golden tag")
					}
					if !goldenSnapshotRetakePending(snapshotStatus) {
						t.Error("interrupted re-take is not pending")
					}
				}
				if creates, resumes, suspends := control.callCounts(); creates != tt.wantCreates || resumes != tt.wantResumes || suspends != tt.wantSuspends {
					t.Errorf("control calls = create:%d resume:%d suspend:%d, want create:%d resume:%d suspend:%d",
						creates, resumes, suspends, tt.wantCreates, tt.wantResumes, tt.wantSuspends)
				}
				if got := len(control.tagReqs); got != tt.wantTagCreates {
					t.Errorf("CreateTag calls = %d, want %d", got, tt.wantTagCreates)
				}
				for _, req := range control.tagReqs {
					if !proto.Equal(req.GetTag().GetSourceActor(), oldRef) || req.GetTag().GetScope() != ateapipb.TagScope_TAG_SCOPE_PUBLISHED || req.GetTag().GetMetadata().GetName() != newRef.GetName() {
						t.Errorf("CreateTag request = %v, want a published tag %v of golden actor %v", req.GetTag(), newRef.GetName(), oldRef)
					}
				}
				for _, req := range control.deleteTagReqs {
					if name := req.GetTag().GetName(); name != oldRef.GetName() && name != newRef.GetName() {
						t.Errorf("DeleteTag request names %v, want only the replaced or the unfinished re-take tag", req.GetTag())
					}
				}

				switch {
				case tt.wantRetaken:
					// Done: the next pass and the resync leave the template alone.
					if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
						t.Fatalf("second reconcileOne: %v", err)
					}
					if !proto.Equal(st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus(), snapshotStatus) {
						t.Error("second pass changed the re-taken golden snapshot status")
					}
					r.resync(t.Context())
					if r.queue.Len() != 0 {
						t.Error("resync queued a re-taken template")
					}
				case tt.wantFailure != "":
					r.resync(t.Context())
					if r.queue.Len() != 0 {
						t.Error("resync queued a failed template")
					}
				default:
					// The retry finishes the re-take.
					r.resync(t.Context())
					if r.queue.Len() != 1 {
						t.Error("resync did not queue the template for the retry")
					}
					control.resumeErr, control.deleteTagErr = nil, nil
					if control.goldenSnapshot == "" {
						control.snapshot = newSnapshot
					}
					if _, err := r.reconcileOne(t.Context(), testTemplateRef); err != nil {
						t.Fatalf("retry reconcileOne: %v", err)
					}
					retried := st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus()
					if !proto.Equal(retried.GetGoldenTag(), newRef) || retried.GetSnapshotLayout() != snapshotlayout.Current {
						t.Errorf("status after the retry = %v, want the re-taken tag %v in layout %d", retried, newRef, snapshotlayout.Current)
					}
					if _, kept := control.tags[oldRef.GetName()]; kept {
						t.Error("the replaced golden tag was not deleted by the retry")
					}
				}
			})
		}
	}
}

// serviceCreateControl is a fakeGoldenControl whose CreateActor is the Control
// service's own against the store the template lives in, the fake recording
// the golden actor it simulates once the service created it: the reconciler's
// golden-actor create then meets every check a create on the template meets.
type serviceCreateControl struct {
	*fakeGoldenControl
	service *RPCService
	created []*ateapipb.Actor
}

func (c *serviceCreateControl) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	created, err := c.service.CreateActor(ctx, proto.CloneOf(req))
	if err != nil {
		return nil, err
	}
	c.created = append(c.created, created)
	return c.fakeGoldenControl.CreateActor(ctx, req)
}

// TestReconcileOne_RetakeCreatesGoldenActorThroughControl drives a template
// whose golden is in an older layout through reconcileOne with the golden
// actor created through the Control service, as in production: a create on
// the template is refused while the re-take is pending, and the golden actor's
// own create is not such a create, nor does it start from the golden being
// replaced.
func TestReconcileOne_RetakeCreatesGoldenActorThroughControl(t *testing.T) {
	for _, layout := range []int32{snapshotlayout.Unrecorded, snapshotlayout.SingleDurableTar} {
		t.Run(fmt.Sprintf("layout %d", layout), func(t *testing.T) {
			ctx := t.Context()
			persistence := newTestPersistence(t)
			storetest.MustCreateAtespace(t, ctx, persistence, resources.GoldenActorAtespace)
			created, err := persistence.CreateActorTemplate(ctx, &ateapipb.ActorTemplate{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "tmpl"},
				SnapshotConfig: &ateapipb.SnapshotConfig{
					StorageLocation:      "gs://ate-snapshots/team-a/",
					GoldenSnapshotConfig: &ateapipb.GoldenSnapshotConfig{Mode: ateapipb.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_ENABLED},
				},
				SandboxConfig: &ateapipb.SandboxConfig{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ConfigName: "gvisor"},
				// With a wakeup probe on every container the golden is
				// snapshotted right after the resume, without a warmup.
				Containers: []*ateapipb.Container{{Name: "main", Image: "img", WakeupProbe: &ateapipb.ContainerWakeupProbe{}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			ref := resources.ActorTemplateRefFromActorTemplate(created)
			tmpl, err := persistence.GetActorTemplate(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			oldRef := goldenActorRefFor(tmpl)
			oldTag := &ateapipb.Tag{
				Metadata:    &ateapipb.ResourceMetadata{Atespace: oldRef.GetAtespace(), Name: oldRef.GetName()},
				SourceActor: oldRef,
				Scope:       ateapipb.TagScope_TAG_SCOPE_PUBLISHED,
				Status: &ateapipb.TagStatus{
					ActorTemplateUid: tmpl.GetMetadata().GetUid(),
					Snapshot:         newDurableSnapshot(1, ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, tmpl.GetMetadata().GetUid(), "gs://ate-snapshots/team-a/atespaces/ate-golden/tags/old-tag-uid", ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED),
				},
			}
			if _, err := persistence.CreateTag(ctx, oldTag); err != nil {
				t.Fatal(err)
			}
			if tmpl, err = persistence.UpdateActorTemplate(ctx, ref, store.PreconditionFrom(tmpl), func(db *ateapipb.ActorTemplate) error {
				db.Status = &ateapipb.ActorTemplateStatus{GoldenSnapshotStatus: &ateapipb.GoldenSnapshotStatus{GoldenTag: oldRef, SnapshotLayout: layout}}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			service := &RPCService{impl: newServiceImpl(persistence, nil)}

			// A create on the template waits for the re-take.
			_, err = service.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
				Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor"},
				ActorTemplate: ref.ToObjectRef(),
			}})
			if apierror.Code(err) != codes.Unavailable {
				t.Fatalf("CreateActor on the template during the re-take = %v, want %v", err, codes.Unavailable)
			}

			newSnapshot := "gs://ate-snapshots/team-a/atespaces/ate-golden/actors/" + tmpl.GetMetadata().GetUid() + "/snapshots/snap-2"
			control := &serviceCreateControl{
				fakeGoldenControl: &fakeGoldenControl{snapshot: newSnapshot, tags: goldenTags(oldTag)},
				service:           service,
			}
			r := newTestTemplateReconciler(persistence, control)
			defer r.queue.ShutDown()

			after, err := r.reconcileOne(ctx, ref)
			if err != nil || after != 0 {
				t.Fatalf("reconcileOne = (%v, %v), want the golden taken again without requeue", after, err)
			}
			if len(control.created) != 1 {
				t.Fatalf("golden actor creates through the service = %d, want 1", len(control.created))
			}
			golden := control.created[0]
			if resources.ActorRefFromActor(golden) != resources.ActorRefFromObjectRef(oldRef) {
				t.Errorf("created actor = %v, want the golden actor %v", resources.ActorRefFromActor(golden), oldRef)
			}
			if snapshots := golden.GetStatus().GetSnapshots(); len(snapshots) != 0 {
				t.Errorf("golden actor created from source snapshots %v, want a fresh boot", snapshots)
			}
			stored, err := persistence.GetActorTemplate(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			snapshotStatus := stored.GetStatus().GetGoldenSnapshotStatus()
			newRef := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: retakenGoldenTagName(tmpl.GetMetadata().GetUid())}
			if !proto.Equal(snapshotStatus.GetGoldenTag(), newRef) || snapshotStatus.GetSnapshotLayout() != snapshotlayout.Current || snapshotStatus.GetErrorMessage() != "" {
				t.Errorf("stored status = %v, want the re-taken tag %v in layout %d", snapshotStatus, newRef, snapshotlayout.Current)
			}
			if !goldenSnapshotDone(snapshotStatus) || goldenSnapshotRetakePending(snapshotStatus) {
				t.Error("re-taken golden is not done")
			}
			if _, kept := control.tags[oldRef.GetName()]; kept {
				t.Error("the replaced golden tag was not deleted")
			}
		})
	}
}

// TestReconcileOne_RetakeRestartsWarmup pins that the deadline of the golden's
// first take does not end the re-taken golden actor's warmup: it is cleared
// before the resume, which records a new one.
func TestReconcileOne_RetakeRestartsWarmup(t *testing.T) {
	oldRef := &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: testTemplateUID}
	st := newFakeTemplateStore(testTemplate(withoutWakeupProbe,
		withGoldenTagInLayout(snapshotlayout.Unrecorded),
		withSnapshotDeadline(time.Now().Add(-time.Hour))))
	control := &fakeGoldenControl{snapshot: "s3://bucket/root/atespaces/ate-golden/actors/" + testTemplateUID + "/snapshots/snap-2",
		tags: goldenTags(goldenTestTag(oldRef, "s3://bucket/root/atespaces/ate-golden/tags/old-tag-uid"))}
	r := newTestTemplateReconciler(st, control)
	defer r.queue.ShutDown()

	after, err := r.reconcileOne(t.Context(), testTemplateRef)
	if err != nil {
		t.Fatalf("reconcileOne: %v", err)
	}
	if after < goldenSnapshotWarmup-time.Second || after > goldenSnapshotWarmup {
		t.Errorf("requeueAfter = %v, want the warmup %v", after, goldenSnapshotWarmup)
	}
	if creates, resumes, suspends := control.callCounts(); creates != 1 || resumes != 1 || suspends != 0 {
		t.Errorf("control calls = create:%d resume:%d suspend:%d, want create:1 resume:1 suspend:0", creates, resumes, suspends)
	}
	snapshotStatus := st.storedStatus(t, testTemplateRef).GetGoldenSnapshotStatus()
	if deadline := snapshotStatus.GetTakeGoldenSnapshotAt(); deadline == nil || deadline.AsTime().Before(time.Now()) {
		t.Errorf("stored take_golden_snapshot_at = %v, want a new warmup deadline ahead", deadline)
	}
	if !proto.Equal(snapshotStatus.GetGoldenTag(), oldRef) || !goldenSnapshotRetakePending(snapshotStatus) {
		t.Errorf("stored status = %v, want the old golden kept while the re-take warms up", snapshotStatus)
	}
}
