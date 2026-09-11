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

// fakeate is a stand-in ate-apiserver for the Envoy Rust dynamic module demo.
//
// It serves the real ateapipb.Control gRPC service over TLS, exactly as the
// atenet router expects to find it, plus a small JSON endpoint the Rust
// dynamic module calls out to. Both entry points resolve an actor to a worker
// IP through the same code path and the same simulated control-plane cost, so
// the two demo arms are charged identically for a cache miss and the only
// difference measured is where the resolution happens.
//
// It deliberately implements no scheduling: every actor is already RUNNING and
// pinned to a worker by a stable hash. That models the hot-actor case, which is
// where the per-request ResumeActor RPC is pure overhead.
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var (
	grpcAddr      = flag.String("grpc-addr", ":9443", "TLS gRPC listen address for the ateapipb.Control service")
	httpAddr      = flag.String("http-addr", ":8081", "plaintext HTTP listen address for module callouts and /stats")
	caOut         = flag.String("ca-out", "/shared/ca.pem", "path to write the generated CA certificate to")
	clientOut     = flag.String("client-bundle-out", "/shared/client-bundle.pem", "path to write the router's client credential bundle (cert+key PEM) to")
	certDNS       = flag.String("cert-dns", "fakeate", "DNS SAN to put on the serving certificate")
	workersFlag   = flag.String("workers", "", "comma-separated worker pod IPs actors are pinned to")
	resumeLatency = flag.Duration("resume-latency", time.Millisecond, "simulated control-plane cost of one ResumeActor call")
)

// stats counts how much control-plane work each arm of the demo actually caused.
// grpcResumes is what the Go ext_proc path drives; httpResumes is what the Rust
// module drives on a cache miss. The gap between them is the headline number.
type stats struct {
	grpcResumes atomic.Int64
	httpResumes atomic.Int64
}

var counters stats

// resolver pins each actor to a worker by a stable hash of its name, so a given
// actor always resolves to the same worker for the life of the demo.
type resolver struct{ workers []string }

func (r *resolver) workerFor(atespace, actor string) string {
	h := fnv.New32a()
	fmt.Fprintf(h, "%s/%s", atespace, actor)
	return r.workers[int(h.Sum32())%len(r.workers)]
}

type controlServer struct {
	ateapipb.UnimplementedControlServer
	res *resolver
}

func (s *controlServer) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	counters.grpcResumes.Add(1)
	time.Sleep(*resumeLatency)

	ref := req.GetActor()
	if ref.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "actor name is required")
	}
	return &ateapipb.ResumeActorResponse{
		Actor:   s.actor(ref.GetAtespace(), ref.GetName()),
		Resumed: false, // the actor was already running: the hot path this demo measures
	}, nil
}

func (s *controlServer) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	ref := req.GetActor()
	return s.actor(ref.GetAtespace(), ref.GetName()), nil
}

// ListActors exists only because the router's health checker calls it once per
// health interval to decide whether ateapi is reachable.
func (s *controlServer) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	return &ateapipb.ListActorsResponse{}, nil
}

func (s *controlServer) actor(atespace, name string) *ateapipb.Actor {
	return &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: atespace, Name: name, Uid: atespace + "/" + name, Version: 1},
		ActorTemplateNamespace: "demo",
		ActorTemplateName:      "echo",
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_RUNNING,
			WorkerAssignment: &ateapipb.WorkerAssignment{
				Worker:          &ateapipb.ObjectRef{Name: "worker-1"},
				WorkerNamespace: "ate-system",
				WorkerPool:      "demo-pool",
				WorkerPodIp:     s.res.workerFor(atespace, name),
			},
		},
	}
}

func main() {
	flag.Parse()
	if *workersFlag == "" {
		log.Fatal("--workers is required")
	}
	res := &resolver{workers: strings.Split(*workersFlag, ",")}

	caPEM, srvCert, clientBundle, caPool, err := generateCerts(*certDNS)
	if err != nil {
		log.Fatalf("generating certificates: %v", err)
	}
	if err := os.WriteFile(*caOut, caPEM, 0o644); err != nil {
		log.Fatalf("writing CA to %s: %v", *caOut, err)
	}
	if err := os.WriteFile(*clientOut, clientBundle, 0o600); err != nil {
		log.Fatalf("writing client bundle to %s: %v", *clientOut, err)
	}
	log.Printf("wrote CA to %s and client bundle to %s; pinning actors across workers %v", *caOut, *clientOut, res.workers)

	go serveHTTP(res)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listening on %s: %v", *grpcAddr, err)
	}
	// Require a client certificate, as the real ate-apiserver does: the router
	// authenticates to it with the podidentity credential bundle. Keeping mTLS
	// in the demo matters because it is one of the things a dynamic module
	// cannot do for itself — see the README's "what stays in Go".
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{srvCert},
		MinVersion:   tls.VersionTLS13,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	})))
	ateapipb.RegisterControlServer(srv, &controlServer{res: res})
	log.Printf("fakeate gRPC (TLS) on %s, HTTP on %s, resume latency %s", *grpcAddr, *httpAddr, *resumeLatency)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serving gRPC: %v", err)
	}
}

// serveHTTP exposes the resolve endpoint the Rust dynamic module calls out to,
// plus the counters the benchmark reads. The endpoint is plaintext HTTP because
// the demo's point is the routing path, not the transport; see the README for
// what production would need instead.
func serveHTTP(res *resolver) {
	mux := http.NewServeMux()

	// GET /v1/resume?atespace=<a>&actor=<n> -> {"worker_ip":"..."}
	// This is the module's cache-miss path. It is charged the same simulated
	// control-plane latency as the gRPC ResumeActor above.
	mux.HandleFunc("/v1/resume", func(w http.ResponseWriter, r *http.Request) {
		counters.httpResumes.Add(1)
		time.Sleep(*resumeLatency)

		atespace := r.URL.Query().Get("atespace")
		actor := r.URL.Query().Get("actor")
		if atespace == "" || actor == "" {
			http.Error(w, `{"error":"atespace and actor are required"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"worker_ip": res.workerFor(atespace, actor)})
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		json.NewEncoder(w).Encode(map[string]int64{
			"grpc_resume_calls": counters.grpcResumes.Load(),
			"http_resume_calls": counters.httpResumes.Load(),
		})
	})

	mux.HandleFunc("/stats/reset", func(w http.ResponseWriter, r *http.Request) {
		counters.grpcResumes.Store(0)
		counters.httpResumes.Store(0)
		w.WriteHeader(http.StatusNoContent)
	})

	if err := http.ListenAndServe(*httpAddr, mux); err != nil {
		log.Fatalf("serving HTTP: %v", err)
	}
}

// generateCerts mints a throwaway CA and a serving certificate for it. The
// router verifies the server against the CA PEM this returns, which is the same
// trust model as the real deployment, just with a CA that lives for one demo run.
func generateCerts(dnsName string) (caPEM []byte, srvCert tls.Certificate, clientBundle []byte, caPool *x509.CertPool, err error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fakeate-demo-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{dnsName, "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}
	srvKeyDER, err := x509.MarshalPKCS8PrivateKey(srvKey)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}

	// The router's client identity, standing in for the podidentity bundle.
	cliKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}
	cliTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "atenet-router"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	cliDER, err := x509.CreateCertificate(rand.Reader, cliTmpl, caCert, &cliKey.PublicKey, caKey)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}
	cliKeyDER, err := x509.MarshalPKCS8PrivateKey(cliKey)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}

	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	srvCert, err = tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: srvKeyDER}),
	)
	if err != nil {
		return nil, tls.Certificate{}, nil, nil, err
	}

	// credbundle expects the certificate chain and the PKCS#8 key in one file.
	clientBundle = append(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cliDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: cliKeyDER})...,
	)

	caPool = x509.NewCertPool()
	caPool.AddCert(caCert)

	return caPEM, srvCert, clientBundle, caPool, nil
}
