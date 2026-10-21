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
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// testSecurityRegression checks one case per threat, each in a subtest
// named for it. Every negative check has a positive control showing the
// request could have succeeded, because a denial that stops exercising its
// boundary keeps passing.
func testSecurityRegression(t *testing.T, ctx context.Context, f *fixture) {
	t.Run("authorization is enforced by default", func(t *testing.T) { testEnforcedByDefault(t, ctx, f) })
	t.Run("invalid credentials are rejected", func(t *testing.T) { testInvalidCredentials(t, ctx, f) })
	t.Run("no access across atespaces", func(t *testing.T) { testTenantCrossing(t, ctx, f) })
	t.Run("editor cannot grant itself owner", func(t *testing.T) { testPrivilegeEscalation(t, ctx, f) })
	t.Run("revocation takes effect", func(t *testing.T) { testRevocation(t, ctx, f) })
	t.Run("recreated atespace drops old grants", func(t *testing.T) { testAtespaceRecreated(t, ctx, f) })
	t.Run("recreated AccessPolicy rejects stale updates", func(t *testing.T) { testAccessPolicyRecreated(t, ctx, f) })
	t.Run("WorkerService accepts only atelet", func(t *testing.T) { testWorkerServiceRequiresAtelet(t, ctx, f) })
}

// testEnforcedByDefault checks that a default install enforces
// authorization: a ServiceAccount no policy names gets nothing. An install
// that left it off would pass every unit test.
func testEnforcedByDefault(t *testing.T, ctx context.Context, f *fixture) {
	nobody := newPrincipal(t, ctx, f.clients, f.atespace, "boundary-nobody")
	expectAll(t, ctx, []call{
		{"GetActor", f.bootstrap, getActor(f.actor), allowed},
		{"GetActor", nobody, getActor(f.actor), denied},
		{"CreateActor", nobody, createActorUnnamed(f.atespace, f.template), denied},
	})
}

// testInvalidCredentials checks that a bearer token ateapi can't verify is
// refused rather than treated as some caller.
func testInvalidCredentials(t *testing.T, ctx context.Context, f *fixture) {
	forged := &principal{name: "invalid-token", api: clientWithToken(t, ctx, "not-a-token")}
	expectAll(t, ctx, []call{
		{"GetActor", f.bootstrap, getActor(f.actor), allowed},
		{"GetActor", forged, getActor(f.actor), rejected},
	})
}

// testTenantCrossing checks that an editor of one atespace can't reach into
// another.
func testTenantCrossing(t *testing.T, ctx context.Context, f *fixture) {
	other := f.atespace + "-other"
	createAtespace(t, ctx, f.clients, other)
	grace := newPrincipal(t, ctx, f.clients, f.atespace, "boundary-grace")
	setAtespacePolicy(t, ctx, f.clients, other, binding("editor", grace))

	expectAll(t, ctx, []call{
		// Control: grace's grant on her own atespace works.
		{"ListActors in her atespace", grace, listActors(other), allowed},
		{"GetActor in another atespace", grace, getActor(f.actor), denied},
		{"CreateActor in another atespace", grace, createActorUnnamed(f.atespace, f.template), denied},
	})
}

// testPrivilegeEscalation checks that an editor can't rewrite the
// AccessPolicy to make itself owner, while an owner can update it.
func testPrivilegeEscalation(t *testing.T, ctx context.Context, f *fixture) {
	owner := newPrincipal(t, ctx, f.clients, f.atespace, "escalate-owner")
	editor := newPrincipal(t, ctx, f.clients, f.atespace, "escalate-editor")
	current := []*ateapipb.Binding{binding("owner", owner), binding("editor", editor)}
	setAtespacePolicy(t, ctx, f.clients, f.atespace, current...)
	cleanupAtespacePolicy(t, f.clients, f.atespace)

	// updatePolicy rewrites the policy's bindings, guarded by the uid and
	// version the bootstrap owner reads just before, so the update is
	// otherwise valid.
	ref := &ateapipb.ObjectRef{Name: f.atespace}
	updatePolicy := func(bindings ...*ateapipb.Binding) rpc {
		return func(ctx context.Context, api *ateclient.Client) error {
			cur, err := f.clients.SubstrateAPI.GetAtespaceAccessPolicy(ctx, &ateapipb.GetAtespaceAccessPolicyRequest{Atespace: ref, Name: resources.SingletonName})
			if err != nil {
				return err
			}
			_, err = api.UpdateAtespaceAccessPolicy(ctx, &ateapipb.UpdateAtespaceAccessPolicyRequest{
				Atespace:     ref,
				AccessPolicy: &ateapipb.AccessPolicy{Metadata: cur.GetMetadata(), Bindings: bindings},
			})
			return err
		}
	}

	// Wait for both grants to take effect, so the editor's denial below is
	// for its role and not for having none yet.
	expectAll(t, ctx, []call{
		{"GetActor", owner, getActor(f.actor), allowed},
		{"GetActor", editor, getActor(f.actor), allowed},
	})
	if t.Failed() {
		return
	}
	expectAll(t, ctx, []call{
		{"UpdateAtespaceAccessPolicy to make itself owner", editor, updatePolicy(binding("owner", owner, editor)), denied},
		// Control: the same request with the bindings unchanged, as owner.
		{"UpdateAtespaceAccessPolicy unchanged", owner, updatePolicy(current...), allowed},
	})
}

// testRevocation checks that removing a binding, downgrading a role and
// deleting the AccessPolicy each take effect on every ateapi replica, with
// no cached grant outliving the change. Steps run in order.
func testRevocation(t *testing.T, ctx context.Context, f *fixture) {
	carol := newPrincipal(t, ctx, f.clients, f.atespace, "revoke-carol")
	dan := newPrincipal(t, ctx, f.clients, f.atespace, "revoke-dan")
	erin := newPrincipal(t, ctx, f.clients, f.atespace, "revoke-erin")
	cleanupAtespacePolicy(t, f.clients, f.atespace)
	createActor := createActorUnnamed(f.atespace, f.template)

	for _, step := range []struct {
		name   string
		change func(t *testing.T)
		expect []call
	}{
		{
			name: "granted",
			change: func(t *testing.T) {
				setAtespacePolicy(t, ctx, f.clients, f.atespace, binding("editor", carol, dan), binding("viewer", erin))
			},
			expect: []call{
				{"CreateActor", carol, createActor, passedCheck},
				{"CreateActor", dan, createActor, passedCheck},
				{"CreateActor", erin, createActor, denied},
			},
		},
		{
			name: "binding removed",
			change: func(t *testing.T) {
				setAtespacePolicy(t, ctx, f.clients, f.atespace, binding("editor", dan), binding("viewer", erin))
			},
			expect: []call{
				{"CreateActor", carol, createActor, denied},
				{"GetActor", carol, getActor(f.actor), denied},
				{"CreateActor", dan, createActor, passedCheck},
			},
		},
		{
			name: "editor downgraded to viewer",
			change: func(t *testing.T) {
				setAtespacePolicy(t, ctx, f.clients, f.atespace, binding("viewer", dan, erin))
			},
			expect: []call{
				{"CreateActor", dan, createActor, denied},
				{"GetActor", dan, getActor(f.actor), allowed},
			},
		},
		{
			name: "AccessPolicy deleted",
			change: func(t *testing.T) {
				if err := deleteAtespacePolicy(ctx, f.clients, f.atespace); err != nil {
					t.Fatalf("DeleteAtespaceAccessPolicy: %v", err)
				}
			},
			expect: []call{
				{"GetActor", dan, getActor(f.actor), denied},
				{"GetActor", erin, getActor(f.actor), denied},
				{"GetActor", f.bootstrap, getActor(f.actor), allowed},
			},
		},
	} {
		if !t.Run(step.name, func(t *testing.T) {
			step.change(t)
			expectAll(t, ctx, step.expect)
		}) {
			// Later steps assume this one's change took effect.
			t.FailNow()
		}
	}
}

// testAtespaceRecreated checks that grants on an atespace go with it:
// deleting an atespace and creating another with the same name doesn't
// hand the new one's access to the old one's members.
func testAtespaceRecreated(t *testing.T, ctx context.Context, f *fixture) {
	name := f.atespace + "-aba"
	frank := newPrincipal(t, ctx, f.clients, f.atespace, "aba-frank")
	createAtespace(t, ctx, f.clients, name)
	setAtespacePolicy(t, ctx, f.clients, name, binding("owner", frank))
	expectAll(t, ctx, []call{{"GetAtespace", frank, getAtespace(name), allowed}})
	if t.Failed() {
		return
	}

	if err := deleteAtespace(ctx, f.clients, name); err != nil {
		t.Fatalf("DeleteAtespace %q: %v", name, err)
	}
	createAtespace(t, ctx, f.clients, name)
	expectAll(t, ctx, []call{
		{"GetAtespace", frank, getAtespace(name), denied},
		{"GetAtespace", f.bootstrap, getAtespace(name), allowed},
	})
}

// testAccessPolicyRecreated checks that a deleted and recreated AccessPolicy
// is a new object: it carries none of the old one's bindings, and an update
// guarded by the old one's uid is rejected rather than applied to it.
func testAccessPolicyRecreated(t *testing.T, ctx context.Context, f *fixture) {
	gina := newPrincipal(t, ctx, f.clients, f.atespace, "aba-gina")
	setAtespacePolicy(t, ctx, f.clients, f.atespace, binding("viewer", gina))
	cleanupAtespacePolicy(t, f.clients, f.atespace)
	expectAll(t, ctx, []call{{"GetActor", gina, getActor(f.actor), allowed}})
	if t.Failed() {
		return
	}

	ref := &ateapipb.ObjectRef{Name: f.atespace}
	old, err := f.clients.SubstrateAPI.GetAtespaceAccessPolicy(ctx, &ateapipb.GetAtespaceAccessPolicyRequest{Atespace: ref, Name: resources.SingletonName})
	if err != nil {
		t.Fatalf("GetAtespaceAccessPolicy: %v", err)
	}
	if err := deleteAtespacePolicy(ctx, f.clients, f.atespace); err != nil {
		t.Fatalf("DeleteAtespaceAccessPolicy: %v", err)
	}
	if _, err := f.clients.SubstrateAPI.CreateAtespaceAccessPolicy(ctx, &ateapipb.CreateAtespaceAccessPolicyRequest{
		Atespace:     ref,
		AccessPolicy: &ateapipb.AccessPolicy{Metadata: &ateapipb.ResourceMetadata{Name: resources.SingletonName}},
	}); err != nil {
		t.Fatalf("CreateAtespaceAccessPolicy: %v", err)
	}

	// The new policy has none of the old one's bindings.
	expectAll(t, ctx, []call{{"GetActor", gina, getActor(f.actor), denied}})

	// An update guarded by the old policy's uid and version must not apply
	// to the new one.
	_, err = f.clients.SubstrateAPI.UpdateAtespaceAccessPolicy(ctx, &ateapipb.UpdateAtespaceAccessPolicyRequest{
		Atespace:     ref,
		AccessPolicy: &ateapipb.AccessPolicy{Metadata: old.GetMetadata(), Bindings: []*ateapipb.Binding{binding("owner", gina)}},
	})
	if status.Code(err) != codes.Aborted {
		t.Errorf("UpdateAtespaceAccessPolicy with the deleted policy's uid: got %v, want %v", err, codes.Aborted)
	}
	cur, err := f.clients.SubstrateAPI.GetAtespaceAccessPolicy(ctx, &ateapipb.GetAtespaceAccessPolicyRequest{Atespace: ref, Name: resources.SingletonName})
	if err != nil {
		t.Fatalf("GetAtespaceAccessPolicy: %v", err)
	}
	if len(cur.GetBindings()) != 0 {
		t.Errorf("recreated AccessPolicy bindings = %v, want none", cur.GetBindings())
	}
}

// testWorkerServiceRequiresAtelet checks that WorkerService rejects every
// caller but atelet. Its RPCs skip the permission checks; each handler
// authenticates atelet by its mTLS certificate, so that is all that guards
// them. A bearer token is rejected even when it is atelet's own
// ServiceAccount's or the bootstrap owner's.
func testWorkerServiceRequiresAtelet(t *testing.T, ctx context.Context, f *fixture) {
	ns := e2e.SystemNamespace()
	sa := installdefaults.AteletServiceAccount
	// Every install creates atelet's ServiceAccount, so a missing one fails
	// the test rather than skipping these checks.
	if !serviceAccountExists(t, ctx, f.clients, ns, sa) {
		t.Fatalf("ServiceAccount %s/%s not found", ns, sa)
	}
	ateletToken := principalFor(t, ctx, f.clients, ns, sa)

	// The worker named is never created: a handler that let the caller
	// through would answer NotFound, not reject it.
	worker := &ateapipb.ObjectRef{Name: "authz-missing"}
	registerWorker := func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.WorkerService().RegisterWorker(ctx, &ateapipb.RegisterWorkerRequest{Worker: worker})
		return err
	}
	requestSuspend := func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.WorkerService().RequestActorSuspend(ctx, &ateapipb.RequestActorSuspendRequest{Worker: worker, Actor: f.actor})
		return err
	}
	// This handler validates the request before authenticating the caller,
	// so the request must be valid. The CSR is parsed only after
	// authentication, so it needn't be a real one.
	mintWorkerCert := func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.WorkerService().MintWorkerActorCertificate(ctx, &ateapipb.MintWorkerActorCertificateRequest{
			Worker:                    worker,
			Actor:                     f.actor,
			ActorUid:                  "00000000-0000-4000-8000-000000000000",
			CertificateSigningRequest: []byte("not a CSR"),
		})
		return err
	}
	// This handler also validates the request before authenticating the
	// caller, so the request must be valid.
	mintWorkerJWT := func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.WorkerService().MintWorkerActorJwt(ctx, &ateapipb.MintWorkerActorJwtRequest{
			Actor:     f.actor,
			ActorUid:  "00000000-0000-4000-8000-000000000000",
			Audiences: []string{"authz-e2e"},
		})
		return err
	}
	reportSnapshotState := func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.WorkerService().ReportActorSnapshotState(ctx, &ateapipb.ReportActorSnapshotStateRequest{
			Actor:    f.actor,
			ActorUid: "00000000-0000-4000-8000-000000000000",
			State:    &ateapipb.ActorSnapshotState{},
		})
		return err
	}

	expectAll(t, ctx, []call{
		{"RegisterWorker", f.bootstrap, registerWorker, rejected},
		{"RegisterWorker", ateletToken, registerWorker, rejected},
		{"RequestActorSuspend", f.bootstrap, requestSuspend, rejected},
		{"RequestActorSuspend", ateletToken, requestSuspend, rejected},
		{"MintWorkerActorCertificate", f.bootstrap, mintWorkerCert, rejected},
		{"MintWorkerActorCertificate", ateletToken, mintWorkerCert, rejected},
		{"MintWorkerActorJwt", f.bootstrap, mintWorkerJWT, rejected},
		{"MintWorkerActorJwt", ateletToken, mintWorkerJWT, rejected},
		{"ReportActorSnapshotState", f.bootstrap, reportSnapshotState, rejected},
		{"ReportActorSnapshotState", ateletToken, reportSnapshotState, rejected},
	})
}
