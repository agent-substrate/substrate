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

package authz

import (
	"context"
	"maps"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/principal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetrics(t *testing.T) {
	pool := startPostgres(t)
	ctx := context.Background()

	fgaServer, err := NewOpenFGAServer(pool)
	if err != nil {
		t.Fatalf("NewOpenFGAServer failed: %v", err)
	}
	t.Cleanup(fgaServer.Close)

	_, seed, err := New(ctx, pool, fgaServer, nil, testSystemPrincipals(), noop.Meter{})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	writeTestTuple(t, ctx, pool, seed, "alice@example.com", RoleOwner, GlobalRootObject)

	jwtCtx := func(id string) context.Context {
		return principal.InjectContext(ctx, principal.PrincipalInfo{ID: id, Kind: principal.KindJWT})
	}
	controllerCtx := principal.InjectContext(ctx, principal.PrincipalInfo{
		ID:   installdefaults.ControllerSPIFFEID(installdefaults.SystemNamespace),
		Kind: principal.KindMTLS,
	})
	canceledCtx, cancel := context.WithCancel(jwtCtx("alice@example.com"))
	cancel()

	checkAttrs := func(relation, objType, kind string, outcome attribute.KeyValue) attribute.Set {
		return attribute.NewSet(
			ateattr.AuthzRelationKey.String(relation),
			ateattr.AuthzObjectTypeKey.String(objType),
			ateattr.PrincipalKindKey.String(kind),
			outcome,
		)
	}
	allowed := ateattr.AuthzDecisionKey.String(ateattr.AuthzDecisionAllowed)
	denied := ateattr.AuthzDecisionKey.String(ateattr.AuthzDecisionDenied)

	tests := []struct {
		name string
		// run makes the calls under test and returns the attribute sets it
		// expects on ate.authz.check.duration and ate.authz.write.duration,
		// one record each.
		run func(t *testing.T, a *Authorizer, pm *PolicyManager) (wantCheck, wantWrite []attribute.Set)
	}{
		{
			name: "check allowed",
			run: func(t *testing.T, a *Authorizer, _ *PolicyManager) ([]attribute.Set, []attribute.Set) {
				if err := a.Check(jwtCtx("alice@example.com"), RelationCanCreateAtespace, GlobalRootObject); err != nil {
					t.Fatalf("Check failed: %v", err)
				}
				return []attribute.Set{checkAttrs(RelationCanCreateAtespace, ateattr.AuthzObjectTypeGlobal, principal.KindJWT, allowed)}, nil
			},
		},
		{
			name: "check denied",
			run: func(t *testing.T, a *Authorizer, _ *PolicyManager) ([]attribute.Set, []attribute.Set) {
				if err := a.Check(jwtCtx("bob@example.com"), RelationCanGet, ActorObject("team-a", "runner")); err == nil {
					t.Fatal("Check succeeded, want PermissionDenied")
				}
				return []attribute.Set{checkAttrs(RelationCanGet, ateattr.AuthzObjectTypeActor, principal.KindJWT, denied)}, nil
			},
		},
		{
			name: "check by an mTLS system principal",
			run: func(t *testing.T, a *Authorizer, _ *PolicyManager) ([]attribute.Set, []attribute.Set) {
				if err := a.Check(controllerCtx, RelationCanListWorkers, GlobalRootObject); err != nil {
					t.Fatalf("Check failed: %v", err)
				}
				return []attribute.Set{checkAttrs(RelationCanListWorkers, ateattr.AuthzObjectTypeGlobal, principal.KindMTLS, allowed)}, nil
			},
		},
		{
			name: "check error",
			run: func(t *testing.T, a *Authorizer, _ *PolicyManager) ([]attribute.Set, []attribute.Set) {
				err := a.Check(canceledCtx, RelationCanCreateAtespace, GlobalRootObject)
				if err == nil {
					t.Fatal("Check with a canceled context succeeded")
				}
				errType := ateattr.ErrorTypeKey.String(apierror.Code(err).String())
				return []attribute.Set{checkAttrs(RelationCanCreateAtespace, ateattr.AuthzObjectTypeGlobal, principal.KindJWT, errType)}, nil
			},
		},
		{
			name: "check without a principal is not recorded",
			run: func(t *testing.T, a *Authorizer, _ *PolicyManager) ([]attribute.Set, []attribute.Set) {
				if err := a.Check(ctx, RelationCanCreateAtespace, GlobalRootObject); err == nil {
					t.Fatal("Check succeeded, want Unauthenticated")
				}
				return nil, nil
			},
		},
		{
			name: "bypassed check is not recorded",
			run: func(t *testing.T, a *Authorizer, _ *PolicyManager) ([]attribute.Set, []attribute.Set) {
				if err := a.Check(WithBypass(ctx), RelationCanCreateAtespace, GlobalRootObject); err != nil {
					t.Fatalf("Check failed: %v", err)
				}
				return nil, nil
			},
		},
		{
			name: "write",
			run: func(t *testing.T, _ *Authorizer, pm *PolicyManager) ([]attribute.Set, []attribute.Set) {
				if err := reconcileInTx(ctx, pool, pm, "user:carol@example.com"); err != nil {
					t.Fatalf("ReconcileAtespaceBindings failed: %v", err)
				}
				return nil, []attribute.Set{attribute.NewSet(ateattr.AuthzObjectTypeKey.String(ateattr.AuthzObjectTypeAtespace))}
			},
		},
		{
			name: "write error",
			run: func(t *testing.T, _ *Authorizer, pm *PolicyManager) ([]attribute.Set, []attribute.Set) {
				if err := reconcileInTx(ctx, pool, pm, "carol@example.com"); err == nil {
					t.Fatal("ReconcileAtespaceBindings with a member without the user: prefix succeeded")
				}
				return nil, []attribute.Set{attribute.NewSet(
					ateattr.AuthzObjectTypeKey.String(ateattr.AuthzObjectTypeAtespace),
					ateattr.ErrorTypeKey.String("Internal"),
				)}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("ateapi")
			authorizer, policyManager, err := New(ctx, pool, fgaServer, nil, testSystemPrincipals(), meter)
			if err != nil {
				t.Fatalf("New failed: %v", err)
			}
			wantCheck, wantWrite := tc.run(t, authorizer, policyManager)

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(ctx, &rm); err != nil {
				t.Fatalf("Collect failed: %v", err)
			}
			assertRecords(t, rm, checkDurationMetric, wantCheck)
			assertRecords(t, rm, writeDurationMetric, wantWrite)
		})
	}

}

// reconcileInTx reconciles a viewer binding for member on atespace team-a, and
// rolls the transaction back.
func reconcileInTx(ctx context.Context, pool *pgxpool.Pool, pm *PolicyManager, member string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return pm.ReconcileAtespaceBindings(ctx, tx, "team-a", []*ateapi.Binding{
		{Role: RoleViewer, Members: []string{member}},
	})
}

// assertRecords checks that the histogram name in rm has one record for each
// set in want and no other records.
func assertRecords(t *testing.T, rm metricdata.ResourceMetrics, name string, want []attribute.Set) {
	t.Helper()
	wantCounts := map[attribute.Distinct]uint64{}
	for _, s := range want {
		wantCounts[s.Equivalent()]++
	}
	got := map[attribute.Distinct]uint64{}
	var gotSets []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			if m.Unit != "s" {
				t.Errorf("%s unit = %q, want s", name, m.Unit)
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("%s data = %T, want metricdata.Histogram[float64]", name, m.Data)
			}
			for _, dp := range hist.DataPoints {
				got[dp.Attributes.Equivalent()] = dp.Count
				gotSets = append(gotSets, dp.Attributes.Encoded(attribute.DefaultEncoder()))
			}
		}
	}
	if !maps.Equal(got, wantCounts) {
		var wantSets []string
		for _, s := range want {
			wantSets = append(wantSets, s.Encoded(attribute.DefaultEncoder()))
		}
		t.Errorf("%s records = %v, want one each of %v", name, gotSets, wantSets)
	}
}
