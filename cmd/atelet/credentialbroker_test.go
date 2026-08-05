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

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/substratex509"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
)

type brokerControlClient struct {
	ateapipb.ControlClient
	worker *ateapipb.Worker
	actor  *ateapipb.Actor
	get    *ateapipb.GetWorkerRequest
}

func (c *brokerControlClient) GetWorker(_ context.Context, req *ateapipb.GetWorkerRequest, _ ...grpc.CallOption) (*ateapipb.Worker, error) {
	c.get = req
	return c.worker, nil
}

func (c *brokerControlClient) GetActor(context.Context, *ateapipb.GetActorRequest, ...grpc.CallOption) (*ateapipb.Actor, error) {
	return c.actor, nil
}

type brokerIdentityClient struct {
	ateapipb.ActorIdentityClient
	request *ateapipb.MintCertRequest
}

func (c *brokerIdentityClient) MintCert(_ context.Context, req *ateapipb.MintCertRequest, _ ...grpc.CallOption) (*ateapipb.MintCertResponse, error) {
	c.request = req
	return &ateapipb.MintCertResponse{ActorCertificates: [][]byte{{1, 2, 3}}}, nil
}

func TestCredentialBrokerDerivesActorFromWorkerCertificate(t *testing.T) {
	control := &brokerControlClient{
		worker: &ateapipb.Worker{
			WorkerNamespace: "workers",
			WorkerPod:       "worker",
			WorkerPodUid:    "worker-uid",
			Assignment:      &ateapipb.Assignment{Actor: &ateapipb.ObjectRef{Atespace: "team", Name: "actor"}},
		},
		actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "team", Name: "actor", Uid: "actor-uid"}},
	}
	identity := &brokerIdentityClient{}
	broker := &credentialBroker{control: control, actorIdentityClient: identity}
	csr := []byte{4, 5, 6}
	resp, err := broker.MintActorCertificate(workerContext(t, "worker-uid"), &ateletpb.MintActorCertificateRequest{CertificateSigningRequest: csr})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(resp, &ateletpb.MintActorCertificateResponse{ActorCertificates: [][]byte{{1, 2, 3}}}) {
		t.Fatalf("response = %+v", resp)
	}
	if want := (&ateapipb.GetWorkerRequest{WorkerNamespace: "workers", WorkerPod: "worker"}); !proto.Equal(control.get, want) {
		t.Fatalf("GetWorker request = %+v, want %+v", control.get, want)
	}
	want := &ateapipb.MintCertRequest{Atespace: "team", ActorName: "actor", ActorUid: "actor-uid", WorkerPodUid: "worker-uid", CertificateSigningRequest: csr, Purpose: ateapipb.ActorCertificatePurpose_ACTOR_CERTIFICATE_PURPOSE_ATUNNEL}
	if !proto.Equal(identity.request, want) {
		t.Fatalf("MintCert request = %+v, want %+v", identity.request, want)
	}
}

func TestCredentialBrokerRejectsUnknownWorker(t *testing.T) {
	broker := &credentialBroker{control: &brokerControlClient{}}
	if _, err := broker.MintActorCertificate(workerContext(t, "unknown-worker"), &ateletpb.MintActorCertificateRequest{}); err == nil {
		t.Fatal("unknown worker was accepted")
	}
}

func TestCredentialBrokerRejectsReplacementWorker(t *testing.T) {
	broker := &credentialBroker{control: &brokerControlClient{worker: &ateapipb.Worker{WorkerPodUid: "replacement-uid"}}}
	if _, err := broker.MintActorCertificate(workerContext(t, "old-uid"), &ateletpb.MintActorCertificateRequest{}); err == nil {
		t.Fatal("replacement worker was accepted")
	}
}

func workerContext(t *testing.T, podUID string) context.Context {
	t.Helper()
	cert := workerCertificate(t, podUID, "node")
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}}})
}

func TestVerifyClientOnSameNode(t *testing.T) {
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{workerCertificate(t, "worker-uid", "node-a")}}
	nodeA := &substratex509.PodIdentity{NodeName: "node-a", NodeUID: "node-uid"}
	if err := verifyClientOnSameNode(nodeA)(state); err != nil {
		t.Fatalf("same-node worker rejected: %v", err)
	}
	if err := verifyClientOnSameNode(&substratex509.PodIdentity{NodeName: "node-b", NodeUID: "node-uid"})(state); err == nil {
		t.Fatal("cross-node worker accepted")
	}
	if err := verifyClientOnSameNode(&substratex509.PodIdentity{NodeName: "node-a", NodeUID: "replacement-node"})(state); err == nil {
		t.Fatal("replacement node accepted")
	}
}

func workerCertificate(t *testing.T, podUID, nodeName string) *x509.Certificate {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour)}
	if err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
		Namespace: "workers", ServiceAccountName: "default", ServiceAccountUID: "sa-uid",
		PodName: "worker", PodUID: podUID, NodeName: nodeName, NodeUID: "node-uid",
	}, template); err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}
