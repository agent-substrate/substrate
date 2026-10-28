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
	"log/slog"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/oidcjwt"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/lru"
)

// serviceAccountPrefix starts the subject of a Kubernetes ServiceAccount
// token: system:serviceaccount:<namespace>:<name>.
const serviceAccountPrefix = "system:serviceaccount:"

// ServiceAccountChecker rejects a ServiceAccount token unless its
// ServiceAccount exists, and is not being deleted, with the UID the token
// names. Access policies keep naming the ServiceAccount by name, so a
// ServiceAccount recreated with the same name keeps them, but tokens minted
// for the deleted one stop working.
type ServiceAccountChecker struct {
	lister corev1listers.ServiceAccountLister
	client corev1client.ServiceAccountsGetter
	// limiter bounds the reads that bypass lister. It stays under
	// client-go's default client-side limit, which client shares with
	// ateapi's other core API calls. All callers share it, so a flood of
	// tokens for many deleted ServiceAccounts can make other cache misses
	// fail with Unavailable.
	limiter *rate.Limiter
	// limitedLog throttles the warning for reads over limiter.
	limitedLog rate.Sometimes
	// staleUIDs holds the UIDs of ServiceAccounts a read found gone. A UID
	// is never reused, so a token naming one is rejected without another
	// read.
	staleUIDs *lru.Cache
}

// NewServiceAccountChecker returns a checker that reads ServiceAccounts from
// lister and, when lister has none with the token's UID, from client.
func NewServiceAccountChecker(lister corev1listers.ServiceAccountLister, client corev1client.ServiceAccountsGetter) *ServiceAccountChecker {
	return &ServiceAccountChecker{
		lister:     lister,
		client:     client,
		limiter:    rate.NewLimiter(2, 5),
		limitedLog: rate.Sometimes{Interval: time.Minute},
		staleUIDs:  lru.New(1024),
	}
}

// check returns nil when claims are not a ServiceAccount's, or when the
// ServiceAccount they name is current. It returns Unauthenticated when it is
// not, and Unavailable when it cannot tell.
func (c *ServiceAccountChecker) check(ctx context.Context, claims *oidcjwt.Claims) error {
	nsName, ok := strings.CutPrefix(claims.Subject, serviceAccountPrefix)
	if !ok {
		return nil
	}
	namespace, name, _ := strings.Cut(nsName, ":")
	if namespace == "" || name == "" || claims.Namespace != namespace || claims.ServiceAccountName != name || claims.ServiceAccountUID == "" {
		slog.DebugContext(ctx, "ServiceAccount token claims do not match its subject", slog.String("subject", claims.Subject))
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	if sa, err := c.lister.ServiceAccounts(namespace).Get(name); err == nil && current(sa, claims.ServiceAccountUID) {
		return nil
	}
	if _, ok := c.staleUIDs.Get(claims.ServiceAccountUID); ok {
		slog.DebugContext(ctx, "ServiceAccount of token is gone", slog.String("namespace", namespace), slog.String("name", name))
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}
	// The cache lags the API server, so it can miss a ServiceAccount created
	// moments ago or still hold the one a recreated ServiceAccount replaced.
	if !c.limiter.Allow() {
		c.limitedLog.Do(func() {
			slog.WarnContext(ctx, "ServiceAccount lookups over the rate limit, failing tokens with Unavailable", slog.String("namespace", namespace), slog.String("name", name))
		})
		return status.Error(codes.Unavailable, "cannot verify ServiceAccount, retry later")
	}
	sa, err := c.client.ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		slog.WarnContext(ctx, "ServiceAccount lookup failed", slog.String("namespace", namespace), slog.String("name", name), slog.Any("err", err))
		return status.Error(codes.Unavailable, "cannot verify ServiceAccount, retry later")
	}
	if err == nil && current(sa, claims.ServiceAccountUID) {
		return nil
	}
	c.staleUIDs.Add(claims.ServiceAccountUID, struct{}{})
	slog.DebugContext(ctx, "ServiceAccount of token is gone", slog.String("namespace", namespace), slog.String("name", name))
	return status.Error(codes.Unauthenticated, "invalid bearer token")
}

// TrimServiceAccount is an informer transform that keeps only the fields
// ServiceAccountChecker and the informer read.
func TrimServiceAccount(obj any) (any, error) {
	sa, ok := obj.(*corev1.ServiceAccount)
	if !ok {
		return obj, nil
	}
	return &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{
		Namespace:         sa.Namespace,
		Name:              sa.Name,
		UID:               sa.UID,
		ResourceVersion:   sa.ResourceVersion,
		DeletionTimestamp: sa.DeletionTimestamp,
	}}, nil
}

// current reports whether sa has uid and is not being deleted.
func current(sa *corev1.ServiceAccount, uid string) bool {
	return string(sa.UID) == uid && sa.DeletionTimestamp == nil
}
