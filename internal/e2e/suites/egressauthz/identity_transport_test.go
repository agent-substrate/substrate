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

package egressauthz

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGatewayCertificateMetadataTransport(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	origin := e2e.DeployServerPod(t, ctx, e2e.ServerPod{Name: "egress-origin", ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver", Args: []string{"http"}, Port: 8080})
	var liveUID string
	var liveName = "live-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	probe := startProbeWithProvision(t, ctx, func(ns string) {
		at := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, ns, e2e.SubstrateTemplateOptions{Atespace: ns, Name: "counter", PoolName: "counter", PoolReplicas: 1, Labels: map[string]string{"egressauthz": ns}})
		ref := &ateapipb.ObjectRef{Atespace: ns, Name: liveName}
		if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: ns, Name: liveName}, ActorTemplate: e2e.TemplateRef(at)}}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
			_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
		})
		bits := 128
		if ip := net.ParseIP(origin.ClusterIP); ip != nil && ip.To4() != nil {
			bits = 32
		}
		e2e.EnsureEgressPolicy(t, ctx, clients, ref, e2e.EgressAllowCIDRs(origin.ClusterIP+"/"+strconv.Itoa(bits)))
		if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			got, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
			if err == nil && got.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
				liveUID = got.GetMetadata().GetUid()
				break
			}
			time.Sleep(time.Second)
		}
		if liveUID == "" {
			t.Fatal("actor did not reach RUNNING")
		}
		identity := &substratex509.ActorIdentity{Atespace: ns, ActorName: liveName, ActorUid: liveUID, Purpose: substratex509.ActorIdentityPurposeAtunnel}
		ca := actorIdentityCA(t, ctx)
		writeCredentialSecret(t, ctx, ns, "egressprobe-live-actor", mintActorCredential(t, ca, identity))
		writeCredentialSecret(t, ctx, ns, "egressprobe-live-actor-chain", mintActorCredentialWithIntermediate(t, ca, identity))
	})
	destination := origin.Address()
	result := probe.connectAs(t, ctx, destination, "/run/actor-identity-live/credential-bundle.pem", "")
	if result.Stage != "" || result.ConnectStatus != http.StatusOK {
		t.Fatalf("direct-root credential: got stage %q status %d: %s", result.Stage, result.ConnectStatus, result.Error)
	}
	t.Run("leaf-plus-intermediate (Envoy scope)", func(t *testing.T) {
		if os.Getenv(e2e.AtenetDataplaneEnv) == "agentgateway" {
			t.Skip("chain transport proof is scoped to the Envoy egress implementation")
		}
		result := probe.connectAs(t, ctx, destination, "/run/actor-identity-live-chain/credential-bundle.pem", "")
		if os.Getenv("E2E_EGRESS_CERTIFICATE_LEAF_ONLY") == "1" {
			if result.Stage != stageConnect || result.ConnectStatus != http.StatusForbidden {
				t.Fatalf("leaf-only chain credential: got stage %q status %d: %s", result.Stage, result.ConnectStatus, result.Error)
			}
			return
		}
		if result.Stage != "" || result.ConnectStatus != http.StatusOK {
			t.Fatalf("chain credential: got stage %q status %d: %s", result.Stage, result.ConnectStatus, result.Error)
		}
	})
}

func TestGatewayIgnoresSpoofedXFCC(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()
	origin := e2e.DeployServerPod(t, ctx, e2e.ServerPod{Name: "spoof-origin", ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver", Args: []string{"http"}, Port: 8080})
	var uid, name, livePEM string
	var unknownPEM string
	probe := startProbeWithProvision(t, ctx, func(ns string) {
		name = "spoof-live-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
		at := e2e.CreateSubstrateCounterTemplate(ctx, t, clients, ns, e2e.SubstrateTemplateOptions{Atespace: ns, Name: "counter", PoolName: "counter", PoolReplicas: 1, Labels: map[string]string{"egressauthz": ns}})
		ref := &ateapipb.ObjectRef{Atespace: ns, Name: name}
		if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: ns, Name: name}, ActorTemplate: e2e.TemplateRef(at)}}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
			_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
		})
		bits := 128
		if ip := net.ParseIP(origin.ClusterIP); ip != nil && ip.To4() != nil {
			bits = 32
		}
		e2e.EnsureEgressPolicy(t, ctx, clients, ref, e2e.EgressAllowCIDRs(origin.ClusterIP+"/"+strconv.Itoa(bits)))
		if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{Actor: ref}); err != nil {
			t.Fatal(err)
		}
		for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); {
			got, err := clients.SubstrateAPI.GetActor(ctx, &ateapipb.GetActorRequest{Actor: ref})
			if err == nil && got.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
				uid = got.GetMetadata().GetUid()
				break
			}
			time.Sleep(time.Second)
		}
		if uid == "" {
			t.Fatal("actor did not reach RUNNING")
		}
		ca := actorIdentityCA(t, ctx)
		live := &substratex509.ActorIdentity{Atespace: ns, ActorName: name, ActorUid: uid, Purpose: substratex509.ActorIdentityPurposeAtunnel}
		wrong := *live
		wrong.ActorUid = "00000000-0000-0000-0000-000000000000"
		liveBundle := mintActorCredential(t, ca, live)
		livePEM = certificatePEM(liveBundle)
		writeCredentialSecret(t, ctx, ns, "egressprobe-live-actor", liveBundle)
		writeCredentialSecret(t, ctx, ns, "egressprobe-wrong-uid", mintActorCredential(t, ca, &wrong))
		secret, err := clients.K8s.CoreV1().Secrets(ns).Get(ctx, unknownActorCredentialSecret, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		unknownPEM = certificatePEM(secret.Data[credentialBundleKey])
		if unknownPEM == "" {
			t.Fatal("unknown certificate PEM is empty")
		}
	})
	if livePEM == "" {
		t.Fatal("live certificate PEM is empty")
	}
	valid := `Chain="` + url.PathEscape(livePEM) + `"`
	if result := probe.connectAs(t, ctx, origin.Address(), "/run/actor-identity-live/credential-bundle.pem", ""); result.ConnectStatus != http.StatusOK {
		t.Fatalf("live actor got stage %q status %d: %s", result.Stage, result.ConnectStatus, result.Error)
	}
	if result := probe.connectAs(t, ctx, origin.Address(), "/run/actor-identity-live/credential-bundle.pem", "malformed"); result.ConnectStatus != http.StatusOK {
		t.Fatalf("malformed XFCC changed live authorization: stage %q status %d: %s", result.Stage, result.ConnectStatus, result.Error)
	}
	if result := probe.connectAs(t, ctx, origin.Address(), "/run/actor-identity-live/credential-bundle.pem", `Chain="`+url.PathEscape(unknownPEM)+`"`); result.ConnectStatus != http.StatusOK {
		t.Fatalf("live actor with unknown valid XFCC got stage %q status %d: %s", result.Stage, result.ConnectStatus, result.Error)
	}
	for _, credential := range []string{"/run/actor-identity-unknown/credential-bundle.pem", "/run/actor-identity-wrong-uid/credential-bundle.pem"} {
		result := probe.connectAs(t, ctx, origin.Address(), credential, valid)
		if result.Stage != stageConnect || result.ConnectStatus != http.StatusForbidden {
			t.Fatalf("spoofed identity with %s got stage %q status %d: %s", credential, result.Stage, result.ConnectStatus, result.Error)
		}
	}
}

func certificatePEM(bundle []byte) string {
	for rest := bundle; ; {
		block, tail := pem.Decode(rest)
		if block == nil {
			return ""
		}
		rest = tail
		if block.Type == "CERTIFICATE" {
			return string(pem.EncodeToMemory(block))
		}
	}
}
