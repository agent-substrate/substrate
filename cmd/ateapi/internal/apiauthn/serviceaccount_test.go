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

package apiauthn

import (
	"context"
	"errors"
	"testing"

	"go.ate.dev/substrate/internal/oidcjwt"
	"go.ate.dev/substrate/internal/principal"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	corev1listers "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestServiceAccountChecker(t *testing.T) {
	const (
		issuer  = "https://issuer.example"
		subject = "system:serviceaccount:team-a:zed"
		oldUID  = "00000000-0000-4000-8000-000000000001"
		newUID  = "00000000-0000-4000-8000-000000000002"
	)
	zed := func(uid types.UID) *corev1.ServiceAccount {
		return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "zed", UID: uid}}
	}
	deleting := func(sa *corev1.ServiceAccount) *corev1.ServiceAccount {
		sa.DeletionTimestamp = &metav1.Time{}
		sa.Finalizers = []string{"example.com/hold"}
		return sa
	}
	saClaims := func(uid string) *oidcjwt.Claims {
		return &oidcjwt.Claims{
			Subject:          subject,
			KubernetesClaims: oidcjwt.KubernetesClaims{Namespace: "team-a", ServiceAccountName: "zed", ServiceAccountUID: uid},
		}
	}
	tests := []struct {
		name   string
		claims *oidcjwt.Claims
		// cached is what the informer holds; nil with unsynced set means
		// the informer has not listed yet.
		cached   []*corev1.ServiceAccount
		unsynced bool
		// live is what the API server holds.
		live []*corev1.ServiceAccount
		// getErr fails the API server read.
		getErr      error
		rateLimited bool
		wantCode    codes.Code
		wantMsg     string
		wantGet     bool
		// twice authenticates a second time, which must get the same
		// result without another API server read.
		twice bool
	}{
		{
			name:     "ServiceAccount exists",
			claims:   saClaims(oldUID),
			cached:   []*corev1.ServiceAccount{zed(oldUID)},
			live:     []*corev1.ServiceAccount{zed(oldUID)},
			wantCode: codes.OK,
		},
		{
			name:     "ServiceAccount deleted",
			claims:   saClaims(oldUID),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid bearer token",
			wantGet:  true,
			twice:    true,
		},
		{
			name:     "ServiceAccount being deleted",
			claims:   saClaims(oldUID),
			cached:   []*corev1.ServiceAccount{deleting(zed(oldUID))},
			live:     []*corev1.ServiceAccount{deleting(zed(oldUID))},
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid bearer token",
			wantGet:  true,
		},
		{
			name:     "ServiceAccount recreated",
			claims:   saClaims(oldUID),
			cached:   []*corev1.ServiceAccount{zed(newUID)},
			live:     []*corev1.ServiceAccount{zed(newUID)},
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid bearer token",
			wantGet:  true,
			twice:    true,
		},
		{
			name:     "ServiceAccount recreated, cache holds the old one",
			claims:   saClaims(newUID),
			cached:   []*corev1.ServiceAccount{zed(oldUID)},
			live:     []*corev1.ServiceAccount{zed(newUID)},
			wantCode: codes.OK,
			wantGet:  true,
		},
		{
			name:     "ServiceAccount created after the cache's last update",
			claims:   saClaims(oldUID),
			live:     []*corev1.ServiceAccount{zed(oldUID)},
			wantCode: codes.OK,
			wantGet:  true,
		},
		{
			name:     "informer not synced",
			claims:   saClaims(oldUID),
			unsynced: true,
			live:     []*corev1.ServiceAccount{zed(oldUID)},
			wantCode: codes.OK,
			wantGet:  true,
		},
		{
			name:     "informer not synced, ServiceAccount deleted",
			claims:   saClaims(oldUID),
			unsynced: true,
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid bearer token",
			wantGet:  true,
		},
		{
			name:     "not a ServiceAccount token",
			claims:   &oidcjwt.Claims{Subject: "alice@example.com"},
			wantCode: codes.OK,
		},
		{
			name: "subject names another ServiceAccount than the claims",
			claims: &oidcjwt.Claims{
				Subject:          "system:serviceaccount:team-b:zed",
				KubernetesClaims: oidcjwt.KubernetesClaims{Namespace: "team-a", ServiceAccountName: "zed", ServiceAccountUID: oldUID},
			},
			cached:   []*corev1.ServiceAccount{zed(oldUID)},
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid bearer token",
		},
		{
			name:     "ServiceAccount subject without Kubernetes claims",
			claims:   &oidcjwt.Claims{Subject: subject},
			cached:   []*corev1.ServiceAccount{zed(oldUID)},
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid bearer token",
		},
		{
			name:     "ServiceAccount subject without a name",
			claims:   &oidcjwt.Claims{Subject: "system:serviceaccount:team-a"},
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid bearer token",
		},
		{
			name:        "cache miss over the read limit",
			claims:      saClaims(oldUID),
			live:        []*corev1.ServiceAccount{zed(oldUID)},
			rateLimited: true,
			wantCode:    codes.Unavailable,
			wantMsg:     "cannot verify ServiceAccount, retry later",
		},
		{
			name:     "API server read fails",
			claims:   saClaims(oldUID),
			getErr:   errors.New("connection refused"),
			wantCode: codes.Unavailable,
			wantMsg:  "cannot verify ServiceAccount, retry later",
			wantGet:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objs []runtime.Object
			for _, sa := range tt.live {
				objs = append(objs, sa)
			}
			client := fake.NewClientset(objs...)
			if tt.getErr != nil {
				client.PrependReactor("get", "serviceaccounts", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tt.getErr
				})
			}
			var lister corev1listers.ServiceAccountLister
			if tt.unsynced {
				// Never started, so it has not listed anything.
				lister = informers.NewSharedInformerFactory(client, 0).Core().V1().ServiceAccounts().Lister()
			} else {
				indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
				for _, sa := range tt.cached {
					// Store what the informer stores in ateapi.
					trimmed, err := TrimServiceAccount(sa)
					if err != nil {
						t.Fatal(err)
					}
					if err := indexer.Add(trimmed); err != nil {
						t.Fatal(err)
					}
				}
				lister = corev1listers.NewServiceAccountLister(indexer)
			}
			checker := NewServiceAccountChecker(lister, client.CoreV1())
			if tt.rateLimited {
				checker.limiter = rate.NewLimiter(0, 0)
			}
			auth := jwtServerAuthenticator{
				providers: []JWTProvider{{Name: "test", Issuer: issuer, Verify: func(context.Context, string) (*oidcjwt.Claims, error) {
					return tt.claims, nil
				}}},
				serviceAccounts: checker,
			}

			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+testGoodToken))
			newCtx, err := auth.authenticate(ctx)
			st := status.Convert(err)
			if st.Code() != tt.wantCode || st.Message() != tt.wantMsg {
				t.Fatalf("authenticate: got %v %q, want %v %q", st.Code(), st.Message(), tt.wantCode, tt.wantMsg)
			}
			if tt.twice {
				_, err := auth.authenticate(ctx)
				if st := status.Convert(err); st.Code() != tt.wantCode || st.Message() != tt.wantMsg {
					t.Fatalf("second authenticate: got %v %q, want %v %q", st.Code(), st.Message(), tt.wantCode, tt.wantMsg)
				}
			}
			var gets int
			for _, a := range client.Actions() {
				if a.Matches("get", "serviceaccounts") {
					gets++
				}
			}
			if want := map[bool]int{false: 0, true: 1}[tt.wantGet]; gets != want {
				t.Errorf("API server reads = %d, want %d", gets, want)
			}
			if tt.wantCode != codes.OK {
				return
			}
			got, _ := principal.FromContext(newCtx)
			want := principal.PrincipalInfo{ID: tt.claims.Subject, Kind: principal.KindJWT, Issuer: issuer}
			if got != want {
				t.Errorf("principal = %+v, want %+v", got, want)
			}
		})
	}
}
