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

package functionaltest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/api/hook/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/cmd/ateapi/internal/lifecyclehook"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

const testClientSPIFFEID = "spiffe://podcert.ate.dev/ns/ate-system/sa/ate-api-server"

type recordedHookCall struct {
	method         string
	atespace       string
	name           string
	uid            string
	createTime     *timestamppb.Timestamp
	templateUID    string
	clientSPIFFEID string
	annotations    map[string]string
}

type mTLSHookServer struct {
	hook.UnimplementedLifecycleHookServer

	name   string
	prefix string
	addr   string

	mu               sync.Mutex
	calls            []recordedHookCall
	denyCreate       bool
	denyMessage      string
	createDelay      time.Duration
	deleteDelay      time.Duration
	deleteErr        error
	extraAnnotations map[string]string
}

func (s *mTLSHookServer) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = nil
	s.denyCreate = false
	s.denyMessage = ""
	s.createDelay = 0
	s.deleteDelay = 0
	s.deleteErr = nil
	s.extraAnnotations = nil
}

func (s *mTLSHookServer) recorded() []recordedHookCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]recordedHookCall, len(s.calls))
	copy(out, s.calls)
	return out
}

func peerSPIFFEID(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing peer")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing verified client certificate")
	}
	for _, u := range tlsInfo.State.VerifiedChains[0][0].URIs {
		if u.Scheme == "spiffe" {
			return u.String(), nil
		}
	}
	return "", status.Error(codes.Unauthenticated, "client certificate has no SPIFFE URI SAN")
}

func (s *mTLSHookServer) OnCreateActor(ctx context.Context, req *hook.OnCreateActorRequest) (*hook.OnCreateActorResponse, error) {
	clientID, err := peerSPIFFEID(ctx)
	if err != nil {
		return nil, err
	}
	if clientID != testClientSPIFFEID {
		return nil, status.Errorf(codes.Unauthenticated, "client SPIFFE ID = %q, want %q", clientID, testClientSPIFFEID)
	}
	meta := req.GetActor().GetMetadata()
	s.mu.Lock()
	s.calls = append(s.calls, recordedHookCall{
		method:         "OnCreateActor",
		atespace:       meta.GetAtespace(),
		name:           meta.GetName(),
		uid:            meta.GetUid(),
		createTime:     meta.GetCreateTime(),
		templateUID:    req.GetActorTemplate().GetMetadata().GetUid(),
		clientSPIFFEID: clientID,
		annotations:    maps.Clone(meta.GetAnnotations()),
	})
	deny := s.denyCreate
	denyMsg := s.denyMessage
	delay := s.createDelay
	extra := maps.Clone(s.extraAnnotations)
	s.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if deny {
		if denyMsg == "" {
			denyMsg = "denied by " + s.name
		}
		return &hook.OnCreateActorResponse{Allowed: false, StatusMessage: denyMsg}, nil
	}
	var mutations map[string]string
	if s.prefix != "" || len(extra) > 0 {
		mutations = make(map[string]string)
		if s.prefix != "" {
			mutations[s.prefix+"hook"] = s.name
			mutations[s.prefix+"uid"] = meta.GetUid()
		}
		maps.Copy(mutations, extra)
	}
	return &hook.OnCreateActorResponse{
		Allowed:            true,
		MutatedAnnotations: mutations,
	}, nil
}

func (s *mTLSHookServer) OnDeleteActor(ctx context.Context, req *hook.OnDeleteActorRequest) (*hook.OnDeleteActorResponse, error) {
	clientID, err := peerSPIFFEID(ctx)
	if err != nil {
		return nil, err
	}
	if clientID != testClientSPIFFEID {
		return nil, status.Errorf(codes.Unauthenticated, "client SPIFFE ID = %q, want %q", clientID, testClientSPIFFEID)
	}
	s.mu.Lock()
	s.calls = append(s.calls, recordedHookCall{
		method:         "OnDeleteActor",
		atespace:       req.GetAtespace(),
		name:           req.GetName(),
		uid:            req.GetUid(),
		clientSPIFFEID: clientID,
	})
	delay := s.deleteDelay
	delErr := s.deleteErr
	s.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	if delErr != nil {
		return nil, delErr
	}
	return &hook.OnDeleteActorResponse{}, nil
}

type hookPKI struct {
	caPEM            []byte
	caFile           string
	clientBundleFile string
	caCert           *x509.Certificate
	caKey            *ecdsa.PrivateKey
	caPool           *x509.CertPool
}

func newHookPKI(tb testing.TB) *hookPKI {
	tb.Helper()
	dir := tb.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "hook-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		tb.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		tb.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		tb.Fatal(err)
	}
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caPEM)

	cliURI, err := url.Parse(testClientSPIFFEID)
	if err != nil {
		tb.Fatal(err)
	}
	cliKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	cliTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "ate-api-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{cliURI},
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTmpl, caCert, &cliKey.PublicKey, caKey)
	if err != nil {
		tb.Fatal(err)
	}
	cliKeyDER, err := x509.MarshalPKCS8PrivateKey(cliKey)
	if err != nil {
		tb.Fatal(err)
	}
	var bundle []byte
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: cliKeyDER})...)
	bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cliDER})...)
	clientBundleFile := filepath.Join(dir, "client-bundle.pem")
	if err := os.WriteFile(clientBundleFile, bundle, 0o600); err != nil {
		tb.Fatal(err)
	}
	return &hookPKI{
		caPEM:            caPEM,
		caFile:           caFile,
		clientBundleFile: clientBundleFile,
		caCert:           caCert,
		caKey:            caKey,
		caPool:           caPool,
	}
}

func (p *hookPKI) startHookServer(tb testing.TB, name, prefix string) *mTLSHookServer {
	tb.Helper()
	dnsName := name + ".ate-system.svc"
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{dnsName},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		tb.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		tb.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		tb.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = lis.Close() })

	srv := &mTLSHookServer{
		name:   name,
		prefix: prefix,
		addr:   "tcp://" + lis.Addr().String(),
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    p.caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	grpcSrv := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsCfg)))
	hook.RegisterLifecycleHookServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()
	tb.Cleanup(grpcSrv.Stop)
	return srv
}

func TestLifecycleHooksEndToEnd(t *testing.T) {
	preview.InitForTest(t, string(preview.GateLifecycleHooks))
	ns := namespaceForTest("lifecycle-hooks")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := t.Context()

	pki := newHookPKI(t)
	hookA := pki.startHookServer(t, "hook-a", "a.example.com/")
	hookB := pki.startHookServer(t, "hook-b", "b.example.com/")
	hookC := pki.startHookServer(t, "hook-c", "")

	configYAML := fmt.Sprintf(`apiVersion: ate.dev/v1
kind: LifecycleHookConfig
metadata:
  name: default
spec:
  maxWaitDuration: 80ms
  hooks:
    - name: hook-a
      address: %s
      allowedAnnotationPrefixes:
        - a.example.com/
      tls:
        caCertFile: %s
        serverName: hook-a.ate-system.svc
        clientAuthMode: MTLS
    - name: hook-b
      address: %s
      allowedAnnotationPrefixes:
        - b.example.com/
      tls:
        caCertFile: %s
        serverName: hook-b.ate-system.svc
        clientAuthMode: MTLS
    - name: hook-c
      address: %s
      tls:
        caBundle: %s
        serverName: hook-c.ate-system.svc
        clientAuthMode: MTLS
`, hookA.addr, pki.caFile, hookB.addr, pki.caFile, hookC.addr, base64.StdEncoding.EncodeToString(pki.caPEM))

	cfgPath := filepath.Join(t.TempDir(), "hooks.yaml")
	if err := os.WriteFile(cfgPath, []byte(configYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	hooksCfg, err := lifecyclehook.LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	dispatcher, err := lifecyclehook.NewDispatcher(hooksCfg, lifecyclehook.WithTLSOptions(lifecyclehook.TLSOptions{
		DefaultClientCredBundlePath: pki.clientBundleFile,
	}))
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	defer dispatcher.Close()
	tc.service.SetLifecycleHooks(dispatcher)

	tmpl, err := tc.client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
		ActorTemplate: &ateapi.ActorTemplate{
			Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "no-golden-tmpl"},
			SnapshotConfig: &ateapi.SnapshotConfig{
				GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED},
			},
			Containers: []*ateapi.Container{{Name: "main", Image: "main@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Command: []string{"/main"}}},
			Resources:  &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 512<<20)},
		},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}
	tmplRef := resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
	if calls := hookA.recorded(); len(calls) != 0 {
		t.Fatalf("CreateActorTemplate triggered hook calls: %+v", calls)
	}

	var createdActor *ateapi.Actor

	t.Run("ParallelMergeAndMetadataFidelity", func(t *testing.T) {
		hookA.reset()
		hookB.reset()
		hookC.reset()

		var err error
		createdActor, err = tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{
			Actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{
					Atespace:    testAtespace,
					Name:        "hooked-actor",
					Annotations: map[string]string{"user.example.com/tag": "v1"},
				},
				ActorTemplate: tmplRef,
			},
		})
		if err != nil {
			t.Fatalf("CreateActor: %v", err)
		}
		stored, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "hooked-actor"},
		})
		if err != nil {
			t.Fatalf("GetActor: %v", err)
		}
		uid := createdActor.GetMetadata().GetUid()
		createTime := createdActor.GetMetadata().GetCreateTime().AsTime()
		if !stored.GetMetadata().GetCreateTime().AsTime().Equal(createTime) {
			t.Errorf("stored create_time %v != returned create_time %v", stored.GetMetadata().GetCreateTime().AsTime(), createTime)
		}

		annots := stored.GetMetadata().GetAnnotations()
		if annots["user.example.com/tag"] != "v1" ||
			annots["a.example.com/hook"] != "hook-a" || annots["a.example.com/uid"] != uid ||
			annots["b.example.com/hook"] != "hook-b" || annots["b.example.com/uid"] != uid {
			t.Errorf("merged annotations = %v, want user + hook-a + hook-b keys for uid %s", annots, uid)
		}

		for _, h := range []*mTLSHookServer{hookA, hookB, hookC} {
			calls := h.recorded()
			if len(calls) != 1 || calls[0].method != "OnCreateActor" {
				t.Fatalf("%s calls = %+v, want 1 OnCreateActor", h.name, calls)
			}
			c := calls[0]
			if c.uid != uid || !c.createTime.AsTime().Equal(createTime) || c.templateUID != tmpl.GetMetadata().GetUid() || c.clientSPIFFEID != testClientSPIFFEID {
				t.Errorf("%s saw call %+v, want uid=%s createTime=%v tmplUID=%s clientSPIFFE=%s",
					h.name, c, uid, createTime, tmpl.GetMetadata().GetUid(), testClientSPIFFEID)
			}
		}
	})

	t.Run("HookDenialAndCompensatingRollback", func(t *testing.T) {
		hookA.reset()
		hookB.reset()
		hookC.reset()
		hookB.mu.Lock()
		hookB.denyCreate = true
		hookB.denyMessage = "quota exceeded"
		hookB.mu.Unlock()
		defer hookB.reset()

		_, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{
			Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "denied-actor"},
				ActorTemplate: tmplRef,
			},
		})
		assertGrpcErrorRegex(t, err, codes.PermissionDenied, `lifecycle hook "hook-b" denied request: quota exceeded`)

		if _, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "denied-actor"},
		}); status.Code(err) != codes.NotFound {
			t.Fatalf("GetActor(denied-actor) = %v, want NotFound", err)
		}

		for _, h := range []*mTLSHookServer{hookA, hookC} {
			calls := h.recorded()
			if len(calls) != 2 || calls[0].method != "OnCreateActor" || calls[1].method != "OnDeleteActor" || calls[0].uid != calls[1].uid {
				t.Errorf("%s calls = %+v, want OnCreateActor followed by compensating OnDeleteActor with matching uid", h.name, calls)
			}
		}
		if callsB := hookB.recorded(); len(callsB) != 1 || callsB[0].method != "OnCreateActor" {
			t.Errorf("hook-b calls = %+v, want only OnCreateActor (no rollback to denying hook)", callsB)
		}
	})

	t.Run("PostHookOverflowAndUnpermittedPrefixRollback", func(t *testing.T) {
		hookA.reset()
		hookB.reset()
		hookC.reset()

		userAnnots := make(map[string]string, 14)
		for i := range 14 {
			userAnnots[fmt.Sprintf("user.example.com/k-%02d", i)] = "v"
		}
		_, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{
			Actor: &ateapi.Actor{
				Metadata: &ateapi.ResourceMetadata{
					Atespace:    testAtespace,
					Name:        "overflow-actor",
					Annotations: userAnnots,
				},
				ActorTemplate: tmplRef,
			},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("CreateActor overflow err = %v, want InvalidArgument", err)
		}
		if _, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "overflow-actor"},
		}); status.Code(err) != codes.NotFound {
			t.Fatalf("GetActor(overflow-actor) = %v, want NotFound", err)
		}
		for _, h := range []*mTLSHookServer{hookA, hookB, hookC} {
			calls := h.recorded()
			if len(calls) != 2 || calls[1].method != "OnDeleteActor" {
				t.Errorf("%s calls after overflow = %+v, want compensating OnDeleteActor", h.name, calls)
			}
		}

		hookA.reset()
		hookB.reset()
		hookC.reset()
		hookC.mu.Lock()
		hookC.extraAnnotations = map[string]string{"c.example.com/unpermitted": "1"}
		hookC.mu.Unlock()
		defer hookC.reset()

		_, err = tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{
			Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "bad-prefix-actor"},
				ActorTemplate: tmplRef,
			},
		})
		if status.Code(err) != codes.Internal {
			t.Fatalf("CreateActor unpermitted prefix err = %v, want Internal", err)
		}
		for _, h := range []*mTLSHookServer{hookA, hookB, hookC} {
			calls := h.recorded()
			if len(calls) != 2 || calls[1].method != "OnDeleteActor" {
				t.Errorf("%s calls after unpermitted prefix = %+v, want compensating OnDeleteActor", h.name, calls)
			}
		}
	})

	t.Run("HookTimeoutSkippedOnCreateAndRolledBackOnSiblingDenial", func(t *testing.T) {
		hookA.reset()
		hookB.reset()
		hookC.reset()
		hookC.mu.Lock()
		hookC.createDelay = 200 * time.Millisecond
		hookC.mu.Unlock()
		defer hookC.reset()

		slowActor, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{
			Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "slow-hook-actor"},
				ActorTemplate: tmplRef,
			},
		})
		if err != nil {
			t.Fatalf("CreateActor with timed-out hook-c: %v", err)
		}
		if got := slowActor.GetMetadata().GetAnnotations(); got["a.example.com/hook"] != "hook-a" || got["b.example.com/hook"] != "hook-b" {
			t.Errorf("annotations after hook-c timeout = %v, want hook-a and hook-b annotations", got)
		}
		if callsA := hookA.recorded(); len(callsA) != 1 {
			t.Errorf("hook-a calls = %+v, want 1 OnCreateActor without rollback", callsA)
		}

		// Now combine hook-c timeout with hook-b denial: both hook-a (allowed)
		// and hook-c (unanswered) must receive compensating OnDeleteActor.
		hookA.reset()
		hookB.reset()
		hookC.reset()
		hookB.mu.Lock()
		hookB.denyCreate = true
		hookB.mu.Unlock()
		hookC.mu.Lock()
		hookC.createDelay = 200 * time.Millisecond
		hookC.mu.Unlock()

		_, err = tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{
			Actor: &ateapi.Actor{
				Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "timeout-and-deny-actor"},
				ActorTemplate: tmplRef,
			},
		})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("CreateActor err = %v, want PermissionDenied", err)
		}
		for _, h := range []*mTLSHookServer{hookA, hookC} {
			calls := h.recorded()
			if len(calls) != 2 || calls[0].method != "OnCreateActor" || calls[1].method != "OnDeleteActor" {
				t.Errorf("%s calls = %+v, want OnCreateActor followed by compensating OnDeleteActor", h.name, calls)
			}
		}

		hookB.reset()
		hookC.reset()
		if _, err := tc.client.DeleteActor(ctx, &ateapi.DeleteActorRequest{
			Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "slow-hook-actor"},
		}); err != nil {
			t.Fatalf("DeleteActor(slow-hook-actor): %v", err)
		}
	})

	t.Run("GoldenActorAndResumeSuspendBypassHooks", func(t *testing.T) {
		createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
		workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
		goldenTmpl, err := tc.client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
			ActorTemplate: &ateapi.ActorTemplate{
				Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "golden-tmpl"},
				Containers: []*ateapi.Container{{
					Name:    "main",
					Image:   "main@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
					Command: []string{"/main"},
					WakeupProbe: &ateapi.WakeupProbe{
						HttpGet:        &ateapi.HttpGetAction{Port: 8080, Path: "/"},
						TimeoutSeconds: 30,
					},
				}},
				WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{poolLabelKey: ns}},
				Resources:      &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 512<<20)},
			},
		})
		if err != nil {
			t.Fatalf("CreateActorTemplate(golden-tmpl): %v", err)
		}
		goldenTmplRef := resources.ActorTemplateRefFromActorTemplate(goldenTmpl)

		hookA.reset()
		hookB.reset()
		hookC.reset()

		reconcilerCtx, cancelReconciler := context.WithCancel(ctx)
		defer cancelReconciler()
		controlapi.NewActorTemplateReconciler(tc.persistence, tc.service, 50*time.Millisecond).Start(reconcilerCtx)

		err = wait.PollUntilContextTimeout(ctx, 20*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
			cur, err := tc.client.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{ActorTemplate: goldenTmplRef.ToObjectRef()})
			return cur.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag() != nil, err
		})
		if err != nil {
			t.Fatalf("waiting for golden tag: %v", err)
		}
		cancelReconciler()

		for _, h := range []*mTLSHookServer{hookA, hookB, hookC} {
			if calls := h.recorded(); len(calls) != 0 {
				t.Errorf("%s received calls during golden actor reconciliation: %+v", h.name, calls)
			}
		}

		waitForWorkerAvailable(t, tc, workerName)
		hookedRef := resources.ActorRefFromActor(createdActor).ToObjectRef()
		if _, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: hookedRef}); err != nil {
			t.Fatalf("ResumeActor: %v", err)
		}
		if _, err := tc.client.SuspendActor(ctx, &ateapi.SuspendActorRequest{Actor: hookedRef}); err != nil {
			t.Fatalf("SuspendActor: %v", err)
		}
		for _, h := range []*mTLSHookServer{hookA, hookB, hookC} {
			if calls := h.recorded(); len(calls) != 0 {
				t.Errorf("%s received calls during ResumeActor/SuspendActor: %+v", h.name, calls)
			}
		}
	})

	t.Run("DeleteActorNotificationAndErrorResilience", func(t *testing.T) {
		hookA.reset()
		hookB.reset()
		hookC.reset()
		hookB.mu.Lock()
		hookB.deleteErr = status.Error(codes.Unavailable, "hook-b down")
		hookB.mu.Unlock()
		hookC.mu.Lock()
		hookC.deleteDelay = 200 * time.Millisecond
		hookC.mu.Unlock()
		defer func() {
			hookB.reset()
			hookC.reset()
		}()

		hookedRef := resources.ActorRefFromActor(createdActor).ToObjectRef()
		if _, err := tc.client.DeleteActor(ctx, &ateapi.DeleteActorRequest{Actor: hookedRef}); err != nil {
			t.Fatalf("DeleteActor with failing/slow hooks: %v", err)
		}
		if _, err := tc.client.GetActor(ctx, &ateapi.GetActorRequest{Actor: hookedRef}); status.Code(err) != codes.NotFound {
			t.Fatalf("GetActor after delete = %v, want NotFound", err)
		}
		for _, h := range []*mTLSHookServer{hookA, hookB, hookC} {
			calls := h.recorded()
			if len(calls) != 1 || calls[0].method != "OnDeleteActor" || calls[0].uid != createdActor.GetMetadata().GetUid() {
				t.Errorf("%s delete calls = %+v, want 1 OnDeleteActor for %s", h.name, calls, createdActor.GetMetadata().GetUid())
			}
		}
	})
}

func setupBenchClient(b *testing.B) (ateapi.ControlClient, *controlapi.RPCService, *ateapi.ObjectRef) {
	b.Helper()
	persistence, cleanupStore := storetest.SetupTestStore(b)
	b.Cleanup(cleanupStore)

	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		b.Fatal(err)
	}
	substrateClient, err := versioned.NewForConfig(cfg)
	if err != nil {
		b.Fatal(err)
	}
	ateletFactory, ateletInformer := controlapi.AteletInformer(k8sClient, installdefaults.SystemNamespace)
	scFactory := informers.NewSharedInformerFactory(k8sClient, 0)
	substrateInformerFactory := externalversions.NewSharedInformerFactory(substrateClient, 0)

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	wc := workercache.New(persistence, 5*time.Minute)
	if err := wc.Start(ctx); err != nil {
		b.Fatal(err)
	}
	dialer := controlapi.NewAteletDialer(ateletInformer.GetIndexer(), installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), "", "",
		controlapi.WithDialCredentials(func(_ string) (credentials.TransportCredentials, error) {
			return insecure.NewCredentials(), nil
		}))
	instruments, err := controlapi.NewInstruments(sdkmetric.NewMeterProvider().Meter("ateapi"))
	if err != nil {
		b.Fatal(err)
	}
	jwtAuth, err := localjwtauthority.GenerateAuthority("ES256", "1")
	if err != nil {
		b.Fatal(err)
	}
	jwtPool := &localjwtauthority.ConcretePool{
		Authorities:      []*localjwtauthority.Authority{jwtAuth},
		ActiveForSigning: "1",
	}
	objStore := objectstoretest.New()
	service := controlapi.NewRPCService(
		persistence,
		wc,
		substrateInformerFactory.Ate().V1().CSIDriverConfigs().Lister(),
		scFactory.Storage().V1().StorageClasses().Lister(),
		dialer,
		instruments,
		"",
		map[string]volume.VolumePluginControlPlane{},
		objectstoreplugintest.ControlClient(objStore),
		testActorJWTIssuer,
		jwtPool,
		testStorageLocation,
	)

	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		ateinterceptors.ServerUnaryInterceptor,
		ateinterceptors.RejectUnknownFieldsUnaryInterceptor,
		preview.UnaryServerInterceptor,
	))
	ateapi.RegisterControlServer(grpcServer, service)
	lis := bufconn.Listen(8192)
	go func() { _ = grpcServer.Serve(lis) }()
	b.Cleanup(grpcServer.Stop)

	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	}, preview.DialOptionsWithOptIn()...)
	conn, err := grpc.NewClient("passthrough://bufnet", dialOpts...)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = conn.Close() })
	client := ateapi.NewControlClient(conn)

	if _, err := client.CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{
		Atespace: &ateapi.Atespace{Metadata: &ateapi.ResourceMetadata{Name: testAtespace}},
	}); err != nil {
		b.Fatal(err)
	}
	tmpl, err := client.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
		ActorTemplate: &ateapi.ActorTemplate{
			Metadata: &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "bench-tmpl"},
			SnapshotConfig: &ateapi.SnapshotConfig{
				GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED},
			},
			Containers: []*ateapi.Container{{Name: "main", Image: "main@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Command: []string{"/main"}}},
			Resources:  &ateapi.ResourceRequirements{Limits: resources.CPUMemory(1000, 512<<20)},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	_ = ateletFactory
	return client, service, resources.ActorTemplateRefFromActorTemplate(tmpl).ToObjectRef()
}

func BenchmarkCreateActorLifecycleHooks(b *testing.B) {
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.DiscardHandler))
	b.Cleanup(func() { slog.SetDefault(prevLogger) })

	preview.InitForTest(b, string(preview.GateLifecycleHooks))

	pki := newHookPKI(b)
	hookA := pki.startHookServer(b, "hook-a", "a.example.com/")
	hookB := pki.startHookServer(b, "hook-b", "b.example.com/")
	hookC := pki.startHookServer(b, "hook-c", "c.example.com/")

	cases := []struct {
		name  string
		hooks []*mTLSHookServer
	}{
		{name: "0_NoHooks"},
		{name: "1_SingleMTLSHook", hooks: []*mTLSHookServer{hookA}},
		{name: "3_ParallelMTLSHooks", hooks: []*mTLSHookServer{hookA, hookB, hookC}},
	}

	for _, bc := range cases {
		b.Run(bc.name, func(b *testing.B) {
			client, service, tmplRef := setupBenchClient(b)
			if len(bc.hooks) > 0 {
				cfg := &lifecyclehook.Config{MaxWaitDuration: "100ms"}
				for _, h := range bc.hooks {
					cfg.Hooks = append(cfg.Hooks, lifecyclehook.HookTarget{
						Name:                      h.name,
						Address:                   h.addr,
						AllowedAnnotationPrefixes: []string{h.prefix},
						TLS: lifecyclehook.HookTLSConfig{
							CACertFile:     pki.caFile,
							ServerName:     h.name + ".ate-system.svc",
							ClientAuthMode: lifecyclehook.HookClientAuthModeMTLS,
						},
					})
				}
				d, err := lifecyclehook.NewDispatcher(cfg, lifecyclehook.WithTLSOptions(lifecyclehook.TLSOptions{
					DefaultClientCredBundlePath: pki.clientBundleFile,
				}))
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = d.Close() })
				service.SetLifecycleHooks(d)
			}

			ctx := context.Background()
			// Warm up the gRPC subchannels before timing.
			if _, err := client.CreateActor(ctx, &ateapi.CreateActorRequest{
				Actor: &ateapi.Actor{
					Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "warmup"},
					ActorTemplate: tmplRef,
				},
			}); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			var i int
			start := time.Now()
			for b.Loop() {
				i++
				if _, err := client.CreateActor(ctx, &ateapi.CreateActorRequest{
					Actor: &ateapi.Actor{
						Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: fmt.Sprintf("bench-actor-%d", i)},
						ActorTemplate: tmplRef,
					},
				}); err != nil && !errors.Is(err, context.Canceled) {
					b.Fatal(err)
				}
			}
			elapsed := time.Since(start)
			if i > 0 && elapsed > 0 {
				b.ReportMetric(float64(i)/elapsed.Seconds(), "actors/s")
			}
		})
	}
}
