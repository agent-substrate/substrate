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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
	"google.golang.org/protobuf/proto"
)

const macTestImage = "registry.example/mac@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestHostRuntimeTLSConfigReloadsIdentityAndTrust(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, caPath := filepath.Join(dir, "client.pem"), filepath.Join(dir, "client-key.pem"), filepath.Join(dir, "ca.pem")
	ca1, ca1Key := writeTestCA(t, caPath, 1)
	writeTestLeaf(t, certPath, keyPath, ca1, ca1Key, 11, "client")
	cfg, err := hostRuntimeTLSConfig(certPath, keyPath, caPath)
	if err != nil {
		t.Fatal(err)
	}
	first, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := certSerial(t, first); got != 11 {
		t.Fatalf("initial client serial = %d, want 11", got)
	}

	ca2, ca2Key := writeTestCA(t, caPath, 2)
	writeTestLeaf(t, certPath, keyPath, ca2, ca2Key, 22, "client")
	second, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := certSerial(t, second); got != 22 {
		t.Fatalf("rotated client serial = %d, want 22", got)
	}

	serverCert, _ := writeTestLeaf(t, filepath.Join(dir, "server.pem"), filepath.Join(dir, "server-key.pem"), ca2, ca2Key, 32, "runtime.test")
	if err := cfg.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{serverCert}, ServerName: "runtime.test"}); err != nil {
		t.Fatalf("rotated server CA was not trusted: %v", err)
	}
	oldServer, _ := writeTestLeaf(t, filepath.Join(dir, "old.pem"), filepath.Join(dir, "old-key.pem"), ca1, ca1Key, 31, "runtime.test")
	if err := cfg.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{oldServer}, ServerName: "runtime.test"}); err == nil {
		t.Fatal("server signed by removed CA was trusted")
	}
}

func writeTestCA(t *testing.T, path string, serial int64) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func writeTestLeaf(t *testing.T, certPath, keyPath string, ca *x509.Certificate, caKey *rsa.PrivateKey, serial int64, dns string) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: dns}, DNSNames: []string{dns}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert, key
}

func certSerial(t *testing.T, cert *tls.Certificate) int64 {
	t.Helper()
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return parsed.SerialNumber.Int64()
}

type capturingHostRuntime struct {
	endpoint          string
	request           *hostruntimepb.ActivateRequest
	pauseRequest      *hostruntimepb.PauseRequest
	checkpointRequest *hostruntimepb.CheckpointRequest
	discardRequest    *hostruntimepb.DiscardRequest
	response          *hostruntimepb.ActivateResponse
}

func (r *capturingHostRuntime) Activate(_ context.Context, endpoint string, req *hostruntimepb.ActivateRequest) (*hostruntimepb.ActivateResponse, error) {
	r.endpoint = endpoint
	r.request = proto.Clone(req).(*hostruntimepb.ActivateRequest)
	return r.response, nil
}

func (r *capturingHostRuntime) Pause(_ context.Context, endpoint string, req *hostruntimepb.PauseRequest) error {
	r.endpoint = endpoint
	r.pauseRequest = proto.Clone(req).(*hostruntimepb.PauseRequest)
	return nil
}

func (r *capturingHostRuntime) Checkpoint(_ context.Context, endpoint string, req *hostruntimepb.CheckpointRequest) error {
	r.endpoint = endpoint
	r.checkpointRequest = proto.Clone(req).(*hostruntimepb.CheckpointRequest)
	return nil
}

func (r *capturingHostRuntime) Discard(_ context.Context, endpoint string, req *hostruntimepb.DiscardRequest) error {
	r.endpoint = endpoint
	r.discardRequest = proto.Clone(req).(*hostruntimepb.DiscardRequest)
	return nil
}

func (*capturingHostRuntime) Terminate(context.Context, string, *hostruntimepb.TerminateRequest) error {
	return nil
}

func TestEnsureMacActivatedDispatchesAndPersistsEndpoint(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "mac-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "mac-template"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "mac-worker-1"},
				RuntimeEndpoint: "dns:///mac-worker-1.example:9443",
			},
		},
	})
	tmpl := &ateapipb.ActorTemplate{
		MacVm: &ateapipb.MacVMWorkload{
			Image: "registry.example/mac-base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			WakeupProbe: &ateapipb.ContainerWakeupProbe{
				HttpGet:        &ateapipb.HTTPGetAction{Path: "/ready", Port: 8123},
				TimeoutSeconds: 120,
			},
		},
		Resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
			{Name: "cpu", Quantity: "6"},
			{Name: "memory", Quantity: "12Gi"},
		}},
	}
	runtime := &capturingHostRuntime{response: &hostruntimepb.ActivateResponse{
		Endpoint: &hostruntimepb.ActorEndpoint{Host: "192.168.64.17", Port: 8123},
	}}
	w := &ActorWorkflow{store: persistence, hostRuntime: runtime}
	actorRef := resources.ActorRefFromActor(actor)

	if _, err := w.ensureMacActivated(ctx, actorRef, actor, tmpl, resumeSnapshotSource{}); err != nil {
		t.Fatalf("ensureMacActivated: %v", err)
	}
	if runtime.endpoint != "dns:///mac-worker-1.example:9443" {
		t.Errorf("Activate endpoint = %q", runtime.endpoint)
	}
	wantRequest := &hostruntimepb.ActivateRequest{
		ActorUid:    actor.GetMetadata().GetUid(),
		Image:       tmpl.GetMacVm().GetImage(),
		CpuMilli:    6000,
		MemoryBytes: 12 * 1024 * 1024 * 1024,
		ReadinessProbe: &hostruntimepb.HTTPReadinessProbe{
			Port: 8123, Path: "/ready", TimeoutSeconds: 120,
		},
	}
	if !proto.Equal(runtime.request, wantRequest) {
		t.Errorf("Activate request = %v, want %v", runtime.request, wantRequest)
	}
	updated, err := persistence.GetActor(ctx, actorRef)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	wantEndpoint := &ateapipb.ActorEndpoint{Host: "192.168.64.17", Port: 8123}
	if got := updated.GetStatus().GetWorkerAssignment().GetActorEndpoint(); !proto.Equal(got, wantEndpoint) {
		t.Errorf("persisted endpoint = %v, want %v", got, wantEndpoint)
	}
}

func TestEnsureMacActivatedRejectsInvalidProviderEndpoint(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "mac-1"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "team-a", Name: "mac-template"},
		Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker: &ateapipb.ObjectRef{Name: "mac-worker-1"}, RuntimeEndpoint: "dns:///mac-worker-1.example:9443",
			}},
	})
	runtime := &capturingHostRuntime{response: &hostruntimepb.ActivateResponse{Endpoint: &hostruntimepb.ActorEndpoint{Port: 8123}}}
	w := &ActorWorkflow{store: persistence, hostRuntime: runtime}
	tmpl := &ateapipb.ActorTemplate{MacVm: &ateapipb.MacVMWorkload{Image: "registry.example/mac-base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}

	if _, err := w.ensureMacActivated(ctx, resources.ActorRefFromActor(actor), actor, tmpl, resumeSnapshotSource{}); err == nil {
		t.Fatal("ensureMacActivated succeeded with an empty provider host")
	}
	updated, err := persistence.GetActor(ctx, resources.ActorRefFromActor(actor))
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got := updated.GetStatus().GetWorkerAssignment().GetActorEndpoint(); got != nil {
		t.Errorf("persisted endpoint = %v, want nil", got)
	}
}

func TestEnsureVolumesAttachedAllowsExternalWorkerWithoutVolumes(t *testing.T) {
	w := &ActorWorkflow{}
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "mac-1"},
		Status:   &ateapipb.ActorStatus{},
	}
	worker := &ateapipb.Worker{
		Metadata:     &ateapipb.ResourceMetadata{Name: "mac-worker-1"},
		ExternalHost: &ateapipb.ExternalWorkerHost{RuntimeEndpoint: "dns:///mac-worker-1.example:9443"},
	}

	if err := w.ensureVolumesAttached(context.Background(), actor, worker, &ateapipb.ActorTemplate{}); err != nil {
		t.Fatalf("ensureVolumesAttached: %v", err)
	}
}

func TestMacLifecycleDispatchesPauseAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	runtime := &capturingHostRuntime{}
	w := &ActorWorkflow{hostRuntime: runtime}
	tmpl := &ateapipb.ActorTemplate{MacVm: &ateapipb.MacVMWorkload{}}
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "mac-1", Uid: "actor-uid"},
		Status: &ateapipb.ActorStatus{
			WorkerAssignment:            &ateapipb.WorkerAssignment{RuntimeEndpoint: "mac.example:9443"},
			InProgressLocalSnapshotName: "local-snapshot",
			InProgressSnapshotUri:       "gs://bucket/actor/snapshot",
		},
	}

	if scope, err := w.ensureAteletPaused(ctx, resources.ActorRefFromActor(actor), actor, tmpl); err != nil || scope != ateattr.SnapshotScopeDisk {
		t.Fatalf("ensureAteletPaused() = %q, %v", scope, err)
	}
	wantPause := &hostruntimepb.PauseRequest{ActorUid: "actor-uid", LocalSnapshotName: "local-snapshot"}
	if runtime.endpoint != "mac.example:9443" || !proto.Equal(runtime.pauseRequest, wantPause) {
		t.Fatalf("Pause(%q, %v), want endpoint and %v", runtime.endpoint, runtime.pauseRequest, wantPause)
	}

	if scope, err := w.ensureAteletSuspended(ctx, resources.ActorRefFromActor(actor), actor, tmpl); err != nil || scope != ateattr.SnapshotScopeDisk {
		t.Fatalf("ensureAteletSuspended() = %q, %v", scope, err)
	}
	wantCheckpoint := &hostruntimepb.CheckpointRequest{ActorUid: "actor-uid", ExternalSnapshotUri: "gs://bucket/actor/snapshot"}
	if !proto.Equal(runtime.checkpointRequest, wantCheckpoint) {
		t.Fatalf("Checkpoint request = %v, want %v", runtime.checkpointRequest, wantCheckpoint)
	}
}

func TestEnsureMacActivatedSelectsSnapshotSource(t *testing.T) {
	for _, tc := range []struct {
		name     string
		local    *ateapipb.LocalSnapshot
		external string
		want     *hostruntimepb.ActivateRequest
	}{
		{name: "local", local: &ateapipb.LocalSnapshot{SnapshotName: "local-1"}, want: &hostruntimepb.ActivateRequest{LocalSnapshotName: proto.String("local-1")}},
		{name: "external", external: "gs://bucket/actor/snapshot", want: &hostruntimepb.ActivateRequest{ExternalSnapshotUri: proto.String("gs://bucket/actor/snapshot")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			actor := storetest.MustCreateActor(t, ctx, persistence, &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "mac-1"},
				Status: &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RESUMING, LocalSnapshot: tc.local,
					WorkerAssignment: &ateapipb.WorkerAssignment{RuntimeEndpoint: "mac.example:9443"}},
			})
			runtime := &capturingHostRuntime{response: &hostruntimepb.ActivateResponse{Endpoint: &hostruntimepb.ActorEndpoint{Host: "mac.example", Port: 1234}}}
			w := &ActorWorkflow{store: persistence, hostRuntime: runtime}
			tmpl := &ateapipb.ActorTemplate{MacVm: &ateapipb.MacVMWorkload{Image: macTestImage}}
			src := resumeSnapshotSource{}
			if tc.external != "" {
				var err error
				src.SnapshotURI, err = resources.ParseSnapshotURI(tc.external)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.ensureMacActivated(ctx, resources.ActorRefFromActor(actor), actor, tmpl, src); err != nil {
				t.Fatal(err)
			}
			if tc.want.LocalSnapshotName != nil && runtime.request.GetLocalSnapshotName() != tc.want.GetLocalSnapshotName() {
				t.Fatalf("local source = %q", runtime.request.GetLocalSnapshotName())
			}
			if tc.want.ExternalSnapshotUri != nil && runtime.request.GetExternalSnapshotUri() != tc.want.GetExternalSnapshotUri() {
				t.Fatalf("external source = %q", runtime.request.GetExternalSnapshotUri())
			}
		})
	}
}
