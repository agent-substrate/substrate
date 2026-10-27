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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
)

const (
	actorName = "authz-actor"
	// propagationTimeout bounds how long a policy change may take to reach
	// every ateapi replica's checks.
	propagationTimeout = 30 * time.Second
)

// fixture is what every test shares: an atespace holding the probe template
// and one suspended actor from it, all created by the bootstrap owner.
type fixture struct {
	clients   *e2e.Clients
	bootstrap *principal
	atespace  string
	template  *ateapi.ObjectRef
	actor     *ateapi.ObjectRef
}

// outcome is the set of status codes a call may return to pass.
type outcome struct {
	name string
	ok   func(codes.Code) bool
}

func exactly(c codes.Code) outcome {
	return outcome{name: c.String(), ok: func(got codes.Code) bool { return got == c }}
}

var (
	allowed = exactly(codes.OK)
	denied  = exactly(codes.PermissionDenied)
	// passedCheck is any outcome but a permission or authentication failure.
	// The permission check runs before the handler validates or looks
	// anything up, so a request the handler then rejects (an invalid
	// request, a missing object) shows the caller got past the check
	// without changing anything.
	//
	// The check also rejects a malformed identifier with InvalidArgument, so
	// every request that expects passedCheck is paired with a caller that
	// expects denied on the same request: were its identifiers malformed,
	// that caller would get InvalidArgument and fail.
	passedCheck = outcome{name: "anything but PermissionDenied or Unauthenticated", ok: func(c codes.Code) bool {
		return c != codes.PermissionDenied && c != codes.Unauthenticated
	}}
	// rejected is a caller refused for who it is, whether by the permission
	// check or by a handler that authenticates the caller itself.
	rejected = outcome{name: "PermissionDenied or Unauthenticated", ok: func(c codes.Code) bool {
		return c == codes.PermissionDenied || c == codes.Unauthenticated
	}}
)

// rpc is one request, made with whichever client a call supplies.
type rpc func(ctx context.Context, api *ateclient.Client) error

// call is an rpc made as who, and the outcome it must have.
type call struct {
	name string
	who  *principal
	do   rpc
	want outcome
}

// expectAll polls calls until each has its outcome, since a policy change is
// not guaranteed to reach every ateapi replica at once. The calls share one
// deadline, so a step costs at most propagationTimeout however many it has.
func expectAll(t *testing.T, ctx context.Context, calls []call) {
	t.Helper()
	deadline := time.Now().Add(propagationTimeout)
	pending := calls
	for {
		lastErr := map[int]error{}
		var still []call
		for _, c := range pending {
			err := c.do(ctx, c.who.api)
			if !c.want.ok(status.Code(err)) {
				lastErr[len(still)] = err
				still = append(still, c)
			}
		}
		if len(still) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for i, c := range still {
				t.Errorf("%s %s: got %v, want %s", c.who.name, c.name, lastErr[i], c.want.name)
			}
			return
		}
		pending = still
		time.Sleep(time.Second)
	}
}

// principal is an identity the tests make calls as.
type principal struct {
	name   string
	member string // AccessPolicy member: "user:" + the token subject
	api    *ateclient.Client
}

// newPrincipal creates a ServiceAccount in namespace and returns a principal
// that authenticates with its token.
func newPrincipal(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, name string) *principal {
	t.Helper()
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if _, err := clients.K8s.CoreV1().ServiceAccounts(namespace).Create(ctx, sa, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating ServiceAccount %s/%s: %v", namespace, name, err)
	}
	return principalFor(t, ctx, clients, namespace, name)
}

// principalFor returns a principal that authenticates with a token minted
// for the existing ServiceAccount namespace/name.
func principalFor(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, name string) *principal {
	t.Helper()
	return &principal{
		name:   name,
		member: fmt.Sprintf("user:system:serviceaccount:%s:%s", namespace, name),
		api:    clientWithToken(t, ctx, mintToken(t, ctx, clients, namespace, name)),
	}
}

// mintToken returns an ateapi token for the ServiceAccount namespace/name.
func mintToken(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, name string) string {
	t.Helper()
	token, _, err := ateclient.MintServiceAccountToken(ctx, clients.K8s, namespace, name, ateclient.DefaultAudience(), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// clientWithToken returns an ateapi client that sends token as its bearer
// token.
func clientWithToken(t *testing.T, ctx context.Context, token string) *ateclient.Client {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatalf("writing token: %v", err)
	}
	return dial(t, ctx, ateclient.DialOptions{TokenFile: tokenFile})
}

// dial returns an ateapi client for the cluster under test, authenticated as
// opts describes.
func dial(t *testing.T, ctx context.Context, opts ateclient.DialOptions) *ateclient.Client {
	t.Helper()
	opts.Kubeconfig, opts.KubeContext = e2e.KubeConfig, e2e.KubeContext
	connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	api, err := ateclient.Dial(connectCtx, opts)
	if err != nil {
		t.Fatalf("ateclient.Dial: %v", err)
	}
	t.Cleanup(api.Close)
	return api
}

// serviceAccountExists reports whether namespace/name exists, failing the
// test on any other error.
func serviceAccountExists(t *testing.T, ctx context.Context, clients *e2e.Clients, namespace, name string) bool {
	t.Helper()
	_, err := clients.K8s.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatalf("getting ServiceAccount %s/%s: %v", namespace, name, err)
	}
	return true
}

func binding(role string, ps ...*principal) *ateapi.Binding {
	b := &ateapi.Binding{Role: role}
	for _, p := range ps {
		b.Members = append(b.Members, p.member)
	}
	return b
}

// setAtespacePolicy replaces atespace's AccessPolicy bindings, creating the
// policy if it doesn't exist yet. The caller deletes it, with
// cleanupAtespacePolicy at the scope the policy must last for.
func setAtespacePolicy(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace string, bindings ...*ateapi.Binding) {
	t.Helper()
	ref := &ateapi.ObjectRef{Name: atespace}
	policy := &ateapi.AccessPolicy{Metadata: &ateapi.ResourceMetadata{Name: resources.SingletonName}, Bindings: bindings}
	current, err := clients.SubstrateAPI.GetAtespaceAccessPolicy(ctx, &ateapi.GetAtespaceAccessPolicyRequest{Atespace: ref, Name: resources.SingletonName})
	switch {
	case status.Code(err) == codes.NotFound:
		_, err = clients.SubstrateAPI.CreateAtespaceAccessPolicy(ctx, &ateapi.CreateAtespaceAccessPolicyRequest{Atespace: ref, AccessPolicy: policy})
	case err == nil:
		policy.Metadata = current.GetMetadata()
		_, err = clients.SubstrateAPI.UpdateAtespaceAccessPolicy(ctx, &ateapi.UpdateAtespaceAccessPolicyRequest{Atespace: ref, AccessPolicy: policy})
	}
	if err != nil {
		t.Fatalf("setting the AccessPolicy of atespace %q: %v", atespace, err)
	}
}

// cleanupAtespacePolicy deletes atespace's AccessPolicy when t ends.
func cleanupAtespacePolicy(t *testing.T, clients *e2e.Clients, atespace string) {
	t.Cleanup(func() {
		if err := deleteAtespacePolicy(context.Background(), clients, atespace); err != nil {
			t.Logf("cleanup: deleting the AccessPolicy of atespace %q: %v", atespace, err)
		}
	})
}

// deleteAtespacePolicy deletes atespace's AccessPolicy; one already gone is
// not an error.
func deleteAtespacePolicy(ctx context.Context, clients *e2e.Clients, atespace string) error {
	_, err := clients.SubstrateAPI.DeleteAtespaceAccessPolicy(ctx, &ateapi.DeleteAtespaceAccessPolicyRequest{
		Atespace: &ateapi.ObjectRef{Name: atespace},
		Name:     resources.SingletonName,
	})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// createAtespace creates an empty atespace as the bootstrap owner and deletes
// it when t ends. Deleting it also deletes its AccessPolicy.
func createAtespace(t *testing.T, ctx context.Context, clients *e2e.Clients, name string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{
		Atespace: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: name}},
	}); err != nil {
		t.Fatalf("CreateAtespace %q: %v", name, err)
	}
	t.Cleanup(func() {
		if err := deleteAtespace(context.Background(), clients, name); err != nil {
			t.Logf("cleanup: DeleteAtespace %q: %v", name, err)
		}
	})
}

// deleteAtespace deletes an atespace; one already gone is not an error.
func deleteAtespace(ctx context.Context, clients *e2e.Clients, name string) error {
	_, err := clients.SubstrateAPI.DeleteAtespace(ctx, &ateapi.DeleteAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: name}})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

// deployFixture deploys the probe template into a new atespace and creates
// one actor from it, all as the bootstrap owner. The actor is never resumed:
// the tests only need an actor that exists.
func deployFixture(t *testing.T, ctx context.Context, bucket string) *fixture {
	t.Helper()
	clients := e2e.GetClients()
	atespace, _ := e2e.DeployProbe(t, bucket, "authz")
	actor := &ateapi.ObjectRef{Atespace: atespace, Name: actorName}
	template := &ateapi.ObjectRef{Atespace: atespace, Name: e2e.ProbeName}
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: atespace, Name: actorName},
		ActorTemplate: template,
	}}); err != nil {
		t.Fatalf("CreateActor %s/%s: %v", atespace, actorName, err)
	}
	t.Cleanup(func() {
		if _, err := clients.SubstrateAPI.DeleteActor(context.Background(), &ateapi.DeleteActorRequest{Actor: actor}); err != nil {
			t.Logf("cleanup: DeleteActor %s/%s: %v", atespace, actorName, err)
		}
	})
	return &fixture{
		clients:   clients,
		bootstrap: &principal{name: "bootstrap-owner", api: clients.SubstrateAPI},
		atespace:  atespace,
		template:  template,
		actor:     actor,
	}
}

// waitForActorState polls actor, as the bootstrap owner, until it is in want.
func waitForActorState(t *testing.T, ctx context.Context, clients *e2e.Clients, actor *ateapi.ObjectRef, want ateapi.ActorState) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := clients.SubstrateAPI.GetActor(ctx, &ateapi.GetActorRequest{Actor: actor})
		if err == nil && resp.GetStatus().GetState() == want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for actor %s/%s to reach %v", actor.GetAtespace(), actor.GetName(), want)
}

// The requests the tests make, shared so that each test pairs callers on
// the same request.

func getAtespace(name string) rpc {
	return func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.GetAtespace(ctx, &ateapi.GetAtespaceRequest{Atespace: &ateapi.ObjectRef{Name: name}})
		return err
	}
}

func getActor(ref *ateapi.ObjectRef) rpc {
	return func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
		return err
	}
}

func listActors(atespace string) rpc {
	return func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.ListActors(ctx, &ateapi.ListActorsRequest{Atespace: atespace})
		return err
	}
}

func suspendActor(ref *ateapi.ObjectRef) rpc {
	return func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: ref})
		return err
	}
}

// createActorUnnamed asks for an actor with no name: the check reads only
// its atespace and template, and validation then rejects it, so nothing is
// created.
func createActorUnnamed(atespace string, template *ateapi.ObjectRef) rpc {
	return func(ctx context.Context, api *ateclient.Client) error {
		_, err := api.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: atespace},
			ActorTemplate: template,
		}})
		return err
	}
}
