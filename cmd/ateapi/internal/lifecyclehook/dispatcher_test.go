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

package lifecyclehook

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
	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var testCABundle = mustGenerateTestCABundle()

func mustGenerateTestCABundle() []byte {
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
}

type fakeHookClient struct {
	mu            sync.Mutex
	createActorFn func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error)
	deleteActorFn func(context.Context, *hook.OnDeleteActorRequest) (*hook.OnDeleteActorResponse, error)
	createActors  []*hook.OnCreateActorRequest
	deleteActors  []*hook.OnDeleteActorRequest
}

func (f *fakeHookClient) OnCreateActor(ctx context.Context, in *hook.OnCreateActorRequest, _ ...grpc.CallOption) (*hook.OnCreateActorResponse, error) {
	f.mu.Lock()
	f.createActors = append(f.createActors, in)
	fn := f.createActorFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
	return &hook.OnCreateActorResponse{Allowed: true}, nil
}

func (f *fakeHookClient) OnDeleteActor(ctx context.Context, in *hook.OnDeleteActorRequest, _ ...grpc.CallOption) (*hook.OnDeleteActorResponse, error) {
	f.mu.Lock()
	f.deleteActors = append(f.deleteActors, in)
	fn := f.deleteActorFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, in)
	}
	return &hook.OnDeleteActorResponse{}, nil
}

func (f *fakeHookClient) deletes() []*hook.OnDeleteActorRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*hook.OnDeleteActorRequest(nil), f.deleteActors...)
}

type createFn = func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error)

func allowWith(annotations map[string]string) createFn {
	return func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
		return &hook.OnCreateActorResponse{Allowed: true, MutatedAnnotations: annotations}, nil
	}
}

func hangUntilDeadline(ctx context.Context, _ *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func twoHookConfig() *Config {
	return &Config{
		MaxWaitDuration: "60ms",
		Hooks: []HookTarget{
			{
				Name:                      "hook-a",
				Address:                   "dns:///hook-a:8443",
				AllowedAnnotationPrefixes: []string{"a.example.com/"},
				TLS:                       HookTLSConfig{CABundle: testCABundle, ServerName: "hook-a"},
			},
			{
				Name:                      "hook-b",
				Address:                   "dns:///hook-b:8443",
				AllowedAnnotationPrefixes: []string{"b.example.com/"},
				TLS:                       HookTLSConfig{CABundle: testCABundle, ServerName: "hook-b"},
			},
		},
	}
}

func TestDispatcherOnCreateActor(t *testing.T) {
	tests := []struct {
		name           string
		hookAFn        createFn
		hookBFn        createFn
		callerRollback bool
		wantCode       codes.Code
		wantAnnot      map[string]string
		wantRollbackA  int
		wantRollbackB  int
	}{
		{
			name:     "parallel hooks mutate under their prefixes",
			hookAFn:  allowWith(map[string]string{"a.example.com/id": "spiffe-1", "a.example.com/drop": ""}),
			hookBFn:  allowWith(map[string]string{"b.example.com/reg": "registered"}),
			wantCode: codes.OK,
			wantAnnot: map[string]string{
				"user-key":          "keep",
				"a.example.com/id":  "spiffe-1",
				"b.example.com/reg": "registered",
			},
		},
		{
			name:    "denial rolls back the hook that allowed",
			hookAFn: allowWith(nil),
			hookBFn: func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
				return &hook.OnCreateActorResponse{Allowed: false, StatusMessage: "policy denied"}, nil
			},
			wantCode:      codes.PermissionDenied,
			wantRollbackA: 1,
		},
		{
			name:          "unpermitted annotation prefix fails closed and rolls back both",
			hookAFn:       allowWith(nil),
			hookBFn:       allowWith(map[string]string{"forbidden.example.com/x": "1"}),
			wantCode:      codes.Internal,
			wantRollbackA: 1,
			wantRollbackB: 1,
		},
		{
			name:          "malformed annotation key syntax fails with Internal and rolls back both",
			hookAFn:       allowWith(map[string]string{"a.example.com/bad key": "1"}),
			hookBFn:       allowWith(nil),
			wantCode:      codes.Internal,
			wantRollbackA: 1,
			wantRollbackB: 1,
		},
		{
			name:     "timeout is skipped and the healthy hook's mutations are kept",
			hookAFn:  allowWith(map[string]string{"a.example.com/id": "ok"}),
			hookBFn:  hangUntilDeadline,
			wantCode: codes.OK,
			wantAnnot: map[string]string{
				"user-key":           "keep",
				"a.example.com/drop": "old",
				"a.example.com/id":   "ok",
			},
		},
		{
			name:    "denial after a sibling timed out rolls back the timed-out hook too",
			hookAFn: hangUntilDeadline,
			hookBFn: func(context.Context, *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
				return &hook.OnCreateActorResponse{Allowed: false}, nil
			},
			wantCode:      codes.PermissionDenied,
			wantRollbackA: 1,
		},
		{
			name:           "caller rollback after store failure notifies every hook",
			callerRollback: true,
			wantCode:       codes.OK,
			wantRollbackA:  1,
			wantRollbackB:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientA := &fakeHookClient{createActorFn: tc.hookAFn}
			clientB := &fakeHookClient{createActorFn: tc.hookBFn}
			d, err := NewDispatcher(twoHookConfig(), WithHookClient("hook-a", clientA), WithHookClient("hook-b", clientB))
			if err != nil {
				t.Fatalf("NewDispatcher: %v", err)
			}
			defer d.Close()

			actor := &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{
					Atespace:    "ns1",
					Name:        "actor-1",
					Uid:         "uid-123",
					Annotations: map[string]string{"user-key": "keep", "a.example.com/drop": "old"},
				},
			}
			tmpl := &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: "ns1", Name: "tmpl-1", Uid: "tmpl-uid"},
			}

			outcome, err := d.OnCreateActor(context.Background(), actor, tmpl, "user:alice")
			if got := apierror.Code(err); got != tc.wantCode {
				t.Fatalf("OnCreateActor code = %v (err: %v), want %v", got, err, tc.wantCode)
			}
			if err != nil || tc.callerRollback {
				outcome.Rollback(context.Background())
				// A second Rollback call must be a no-op.
				outcome.Rollback(context.Background())
			} else if diff := cmp.Diff(tc.wantAnnot, actor.GetMetadata().GetAnnotations()); diff != "" {
				t.Errorf("annotations mismatch (-want +got):\n%s", diff)
			}

			for _, c := range []struct {
				name   string
				client *fakeHookClient
				want   int
			}{{"hook-a", clientA, tc.wantRollbackA}, {"hook-b", clientB, tc.wantRollbackB}} {
				got := c.client.deletes()
				if len(got) != c.want {
					t.Errorf("%s compensating deletes = %d, want %d", c.name, len(got), c.want)
					continue
				}
				if c.want > 0 && (got[0].GetUid() != "uid-123" || got[0].GetCallerPrincipal() != "user:alice") {
					t.Errorf("%s compensating delete = %v, want uid-123 from user:alice", c.name, got[0])
				}
			}
		})
	}
}

func TestDispatcherOnDeleteActor(t *testing.T) {
	clientA := &fakeHookClient{}
	clientB := &fakeHookClient{
		deleteActorFn: func(context.Context, *hook.OnDeleteActorRequest) (*hook.OnDeleteActorResponse, error) {
			return nil, errors.New("boom")
		},
	}
	d, err := NewDispatcher(twoHookConfig(), WithHookClient("hook-a", clientA), WithHookClient("hook-b", clientB))
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	defer d.Close()

	d.OnDeleteActor(context.Background(), "ns1", "a1", "auid1", "user:charlie")

	for name, c := range map[string]*fakeHookClient{"hook-a": clientA, "hook-b": clientB} {
		got := c.deletes()
		if len(got) != 1 || got[0].GetUid() != "auid1" || got[0].GetCallerPrincipal() != "user:charlie" {
			t.Errorf("%s OnDeleteActor calls = %v, want one for auid1 from user:charlie", name, got)
		}
	}
}

func BenchmarkDispatcherOnCreateActor(b *testing.B) {
	for _, numHooks := range []int{1, 3} {
		name := "OneHook"
		if numHooks == 3 {
			name = "ThreeHooks"
		}
		b.Run(name, func(b *testing.B) {
			cfg := &Config{MaxWaitDuration: "100ms"}
			var opts []Option
			for i := range numHooks {
				hName := "hook-" + string(rune('a'+i))
				prefix := hName + ".example.com/"
				cfg.Hooks = append(cfg.Hooks, HookTarget{
					Name:                      hName,
					Address:                   "dns:///" + hName + ":8443",
					AllowedAnnotationPrefixes: []string{prefix},
					TLS:                       HookTLSConfig{CABundle: testCABundle, ServerName: hName},
				})
				opts = append(opts, WithHookClient(hName, &fakeHookClient{createActorFn: allowWith(map[string]string{prefix + "k": "v"})}))
			}
			d, err := NewDispatcher(cfg, opts...)
			if err != nil {
				b.Fatal(err)
			}
			defer d.Close()

			ctx := context.Background()
			tmpl := &ateapi.ActorTemplate{Metadata: &ateapi.ResourceMetadata{Atespace: "ns", Name: "t", Uid: "tu"}}
			b.ResetTimer()
			for range b.N {
				actor := &ateapi.Actor{
					Metadata: &ateapi.ResourceMetadata{
						Atespace:    "ns",
						Name:        "a",
						Uid:         "au",
						Annotations: map[string]string{"init": "1"},
					},
				}
				if _, err := d.OnCreateActor(ctx, actor, tmpl, "user:bench"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
