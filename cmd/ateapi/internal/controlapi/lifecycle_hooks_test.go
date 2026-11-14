// Copyright 2026 The Agent Substrate Authors
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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/api/hook/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/lifecyclehook"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/principal"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const testHookPrefix = "id.example.com/"

var testHookCABundle = func() []byte {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}()

type fakeHookClient struct {
	mu       sync.Mutex
	onCreate func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error)
	onDelete func(context.Context, *hook.OnDeleteActorRequest) (*hook.OnDeleteActorResponse, error)
	creates  []*hook.OnCreateActorRequest
	deletes  []*hook.OnDeleteActorRequest
}

func (f *fakeHookClient) OnCreateActor(ctx context.Context, in *hook.OnCreateActorRequest, _ ...grpc.CallOption) (*hook.OnCreateActorResponse, error) {
	f.mu.Lock()
	f.creates = append(f.creates, in)
	fn := f.onCreate
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
	return &hook.OnCreateActorResponse{Allowed: true}, nil
}

func (f *fakeHookClient) OnDeleteActor(ctx context.Context, in *hook.OnDeleteActorRequest, _ ...grpc.CallOption) (*hook.OnDeleteActorResponse, error) {
	f.mu.Lock()
	f.deletes = append(f.deletes, in)
	fn := f.onDelete
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
	return &hook.OnDeleteActorResponse{}, nil
}

func (f *fakeHookClient) counts() (creates, deletes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.creates), len(f.deletes)
}

func newTestHooks(t *testing.T, client *fakeHookClient) lifecyclehook.Hooks {
	t.Helper()
	d, err := lifecyclehook.NewDispatcher(&lifecyclehook.Config{
		MaxWaitDuration: "50ms",
		Hooks: []lifecyclehook.HookTarget{{
			Name:                      "hook-a",
			Address:                   "dns:///hook-a:8443",
			AllowedAnnotationPrefixes: []string{testHookPrefix},
			TLS:                       lifecyclehook.HookTLSConfig{CABundle: testHookCABundle, ServerName: "hook-a"},
		}},
	}, lifecyclehook.WithHookClient("hook-a", client))
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func allow(annotations map[string]string) func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
	return func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
		return &hook.OnCreateActorResponse{Allowed: true, MutatedAnnotations: annotations}, nil
	}
}

func TestCreateActorLifecycleHooks(t *testing.T) {
	manyAnnotations := make(map[string]string, 17)
	for i := range 17 {
		manyAnnotations[testHookPrefix+string(rune('a'+i))] = "v"
	}

	tests := []struct {
		name         string
		gateOn       bool
		onCreate     func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error)
		wantCode     codes.Code
		wantAnnot    map[string]string
		wantCreates  int
		wantRollback int
	}{
		{
			name:   "gate off skips hooks",
			gateOn: false,
			onCreate: func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
				return &hook.OnCreateActorResponse{Allowed: false}, nil
			},
			wantCode: codes.OK,
		},
		{
			name:        "gate on stamps annotations and preserves pre-assigned uid and create_time",
			gateOn:      true,
			onCreate:    allow(map[string]string{testHookPrefix + "spiffe": "spiffe://example.com/agent"}),
			wantCode:    codes.OK,
			wantAnnot:   map[string]string{testHookPrefix + "spiffe": "spiffe://example.com/agent"},
			wantCreates: 1,
		},
		{
			name:   "hook denial aborts before store write",
			gateOn: true,
			onCreate: func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
				return &hook.OnCreateActorResponse{Allowed: false, StatusMessage: "denied by policy"}, nil
			},
			wantCode:    codes.PermissionDenied,
			wantCreates: 1,
		},
		{
			name:         "invalid mutated annotation key triggers Internal and compensating rollback",
			gateOn:       true,
			onCreate:     allow(map[string]string{testHookPrefix + "bad key": "val"}),
			wantCode:     codes.Internal,
			wantCreates:  1,
			wantRollback: 1,
		},
		{
			name:         "post-hook annotation count overflow triggers InvalidArgument and compensating rollback",
			gateOn:       true,
			onCreate:     allow(manyAnnotations),
			wantCode:     codes.InvalidArgument,
			wantCreates:  1,
			wantRollback: 1,
		},
		{
			name:   "hook timeout is skipped and the actor is created",
			gateOn: true,
			onCreate: func(ctx context.Context, _ *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
			wantCode:    codes.OK,
			wantCreates: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.gateOn {
				preview.InitForTest(t, "LifecycleHooks")
			} else {
				preview.InitForTest(t)
			}
			ctx := principal.InjectContext(t.Context(), principal.PrincipalInfo{ID: "user:alice"})
			persistence := newTestPersistence(t)
			impl := newServiceImpl(persistence, nil)
			impl.clusterSnapshotLocation = "gs://cluster-bucket/"
			client := &fakeHookClient{onCreate: tc.onCreate}
			impl.hooks = newTestHooks(t, client)
			svc := &RPCService{impl: impl}
			storetest.MustCreateAtespace(t, ctx, persistence, testAtespace)

			tmpl, err := svc.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
				ActorTemplate: validActorTemplate(func(at *ateapi.ActorTemplate) {
					at.Metadata = &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "tmpl"}
				}),
			})
			if err != nil {
				t.Fatalf("CreateActorTemplate: %v", err)
			}

			actorRef := resources.ActorRef{Atespace: testAtespace, Name: "actor-1"}
			created, err := svc.CreateActor(ctx, &ateapi.CreateActorRequest{
				Actor: &ateapi.Actor{
					Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: actorRef.Name},
					ActorTemplate: resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef(),
				},
			})
			if got := apierror.Code(err); got != tc.wantCode {
				t.Fatalf("CreateActor code = %v (err: %v), want %v", got, err, tc.wantCode)
			}
			creates, deletes := client.counts()
			if creates != tc.wantCreates {
				t.Errorf("OnCreateActor calls = %d, want %d", creates, tc.wantCreates)
			}
			if deletes != tc.wantRollback {
				t.Errorf("compensating OnDeleteActor calls = %d, want %d", deletes, tc.wantRollback)
			}

			stored, getErr := persistence.GetActor(ctx, actorRef)
			if tc.wantCode == codes.OK && getErr != nil {
				t.Fatalf("actor not stored after successful create: %v", getErr)
			}
			if tc.wantCode != codes.OK && !errors.Is(getErr, store.ErrNotFound) {
				t.Fatalf("actor present after failed create (err: %v)", getErr)
			}
			if tc.wantCode != codes.OK || !tc.gateOn {
				return
			}
			if diff := cmp.Diff(tc.wantAnnot, created.GetMetadata().GetAnnotations()); diff != "" {
				t.Errorf("annotations mismatch (-want +got):\n%s", diff)
			}
			req := client.creates[0]
			if req.GetCallerPrincipal() != "user:alice" {
				t.Errorf("caller_principal = %q, want user:alice", req.GetCallerPrincipal())
			}
			if got := req.GetActor().GetMetadata().GetUid(); got == "" || got != created.GetMetadata().GetUid() {
				t.Errorf("hook saw uid %q, stored uid %q; want matching non-empty uid", got, created.GetMetadata().GetUid())
			}
			hookCreateTime := req.GetActor().GetMetadata().GetCreateTime()
			if hookCreateTime == nil || !hookCreateTime.AsTime().Equal(created.GetMetadata().GetCreateTime().AsTime()) || !hookCreateTime.AsTime().Equal(stored.GetMetadata().GetCreateTime().AsTime()) {
				t.Errorf("hook saw create_time %v, created %v, stored %v; want equal", hookCreateTime.AsTime(), created.GetMetadata().GetCreateTime().AsTime(), stored.GetMetadata().GetCreateTime().AsTime())
			}
			if req.GetActorTemplate().GetMetadata().GetUid() != tmpl.GetMetadata().GetUid() {
				t.Errorf("hook saw template uid %q, want %q", req.GetActorTemplate().GetMetadata().GetUid(), tmpl.GetMetadata().GetUid())
			}
		})
	}
}

func TestDeleteAndGoldenBypassLifecycleHooks(t *testing.T) {
	preview.InitForTest(t, "LifecycleHooks")
	ctx := t.Context()
	persistence := newTestPersistence(t)
	w := newTestActorWorkflow(t, persistence, testAtespace, "tmpl-1")
	client := &fakeHookClient{}
	hooks := newTestHooks(t, client)
	w.hooks = hooks
	impl := newServiceImpl(persistence, nil)
	impl.clusterSnapshotLocation = "gs://cluster-bucket/"
	impl.hooks = hooks
	svc := &RPCService{impl: impl, actorWorkflow: w}

	tmpl, err := svc.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
		ActorTemplate: validActorTemplate(func(at *ateapi.ActorTemplate) {
			at.Metadata = &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "tmpl-2"}
		}),
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	tmplRef := resources.ActorTemplateRefFromActorTemplate(tmpl)

	// Internal golden actor create must bypass hooks.
	if _, err := svc.CreateActor(withInternalGoldenActor(ctx), &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: tmpl.GetMetadata().GetUid()},
			ActorTemplate: tmplRef.ToObjectRef(),
		},
	}); err != nil {
		t.Fatalf("CreateActor golden: %v", err)
	}
	if creates, _ := client.counts(); creates != 0 {
		t.Fatalf("golden CreateActor invoked hook %d times, want 0", creates)
	}

	actor, err := svc.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "user-actor"},
			ActorTemplate: tmplRef.ToObjectRef(),
		},
	})
	if err != nil {
		t.Fatalf("CreateActor user: %v", err)
	}
	if creates, _ := client.counts(); creates != 1 {
		t.Fatalf("user CreateActor hook calls = %d, want 1", creates)
	}

	// A failing delete hook does not block deletion.
	client.onDelete = func(context.Context, *hook.OnDeleteActorRequest) (*hook.OnDeleteActorResponse, error) {
		return nil, errors.New("hook down")
	}
	actorRef := resources.ActorRef{Atespace: testAtespace, Name: "user-actor"}
	deleteCtx := principal.InjectContext(ctx, principal.PrincipalInfo{ID: "spiffe://cluster.local/ns/default/sa/deleter"})
	if _, err := w.DeleteActor(deleteCtx, actorRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteActor with failing hook: %v", err)
	}
	if _, err := persistence.GetActor(ctx, actorRef); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("actor after delete = present (err: %v), want ErrNotFound", err)
	}
	if _, deletes := client.counts(); deletes != 1 {
		t.Fatalf("OnDeleteActor calls = %d, want 1", deletes)
	}
	if got := client.deletes[0]; got.GetUid() != actor.GetMetadata().GetUid() || got.GetCallerPrincipal() != "spiffe://cluster.local/ns/default/sa/deleter" {
		t.Errorf("OnDeleteActor request = %v, want uid %s from the deleter", got, actor.GetMetadata().GetUid())
	}

	// DeleteActorTemplate deletes the golden actor without calling the hook.
	if _, err := w.DeleteActorTemplate(deleteCtx, tmplRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteActorTemplate: %v", err)
	}
	if _, deletes := client.counts(); deletes != 1 {
		t.Errorf("DeleteActorTemplate triggered OnDeleteActor for the golden actor: %v", client.deletes)
	}
}
