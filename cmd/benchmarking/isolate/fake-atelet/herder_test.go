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
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const testSnapshotURI = "gs://bench-bucket/benchmark-workloads/sleep/atespaces/team-a/actors/0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f/snapshots/snap-1"

// recordingStorage records every object written.
type recordingStorage struct {
	mu      sync.Mutex
	written []string // bucket + "/" + object
	err     error
	delay   time.Duration
}

func (s *recordingStorage) PutObject(_ context.Context, bucket, object string, r io.Reader) error {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if _, err := io.ReadAll(r); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.written = append(s.written, bucket+"/"+object)
	return nil
}

func (s *recordingStorage) objects() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.written)
}

func uniformDelays(d time.Duration) delays {
	return delays{run: d, restore: d, checkpoint: d, uploadPausedCheckpoint: d, terminate: d}
}

// herderCalls invokes every AteomHerder method the herder implements. The
// checkpoint calls ask for an external snapshot, so each writes a placeholder.
func herderCalls(h *herder) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"Run": func(ctx context.Context) error {
			_, err := h.Run(ctx, &ateletpb.RunRequest{})
			return err
		},
		"Restore": func(ctx context.Context) error {
			_, err := h.Restore(ctx, &ateletpb.RestoreRequest{})
			return err
		},
		"Checkpoint": func(ctx context.Context) error {
			_, err := h.Checkpoint(ctx, externalCheckpoint(testSnapshotURI))
			return err
		},
		"UploadPausedCheckpoint": func(ctx context.Context) error {
			_, err := h.UploadPausedCheckpoint(ctx, &ateletpb.UploadPausedCheckpointRequest{DestinationSnapshotUri: testSnapshotURI})
			return err
		},
		"Terminate": func(ctx context.Context) error {
			_, err := h.Terminate(ctx, &ateletpb.TerminateRequest{})
			return err
		},
	}
}

func externalCheckpoint(uri string) *ateletpb.CheckpointRequest {
	return &ateletpb.CheckpointRequest{
		Type:           ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{SnapshotUri: uri},
	}
}

func TestHerderSucceedsAfterDelay(t *testing.T) {
	const d = 20 * time.Millisecond
	for name, call := range herderCalls(&herder{delays: uniformDelays(d), storage: &recordingStorage{}}) {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			if err := call(context.Background()); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got := time.Since(start); got < d {
				t.Errorf("%s returned after %v, want at least %v", name, got, d)
			}
		})
	}
}

func TestHerderZeroDelay(t *testing.T) {
	for name, call := range herderCalls(&herder{storage: &recordingStorage{}}) {
		if err := call(context.Background()); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestHerderStopsWhenCallerGivesUp(t *testing.T) {
	for name, call := range herderCalls(&herder{delays: uniformDelays(time.Hour), storage: &recordingStorage{}}) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if err := call(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("%s: got %v, want %v", name, err, context.DeadlineExceeded)
			}
		})
	}
}

func TestHerderUsesEachCallsOwnDelay(t *testing.T) {
	h := &herder{delays: delays{run: time.Hour}, storage: &recordingStorage{}}
	if _, err := h.Restore(context.Background(), &ateletpb.RestoreRequest{}); err != nil {
		t.Fatalf("Restore with no delay of its own: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := h.Run(ctx, &ateletpb.RunRequest{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Run with an hour's delay: got %v, want %v", err, context.DeadlineExceeded)
	}
}

// ate-api-server copies an actor's snapshot when it creates a tag and refuses
// to copy an empty one, so both calls that would upload a snapshot must leave
// an object under its URI.
func TestHerderWritesPlaceholderUnderSnapshotURI(t *testing.T) {
	const want = "bench-bucket/benchmark-workloads/sleep/atespaces/team-a/actors/0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f/snapshots/snap-1/" + placeholderObject
	for _, name := range []string{"Checkpoint", "UploadPausedCheckpoint"} {
		t.Run(name, func(t *testing.T) {
			storage := &recordingStorage{}
			if err := herderCalls(&herder{storage: storage})[name](context.Background()); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got := storage.objects(); len(got) != 1 || got[0] != want {
				t.Errorf("%s wrote %v, want [%s]", name, got, want)
			}
		})
	}
}

func TestHerderWritesNothingForLocalCheckpoint(t *testing.T) {
	storage := &recordingStorage{}
	h := &herder{storage: storage}
	if _, err := h.Checkpoint(context.Background(), &ateletpb.CheckpointRequest{Type: ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL}); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for name, call := range herderCalls(h) {
		if name == "Checkpoint" || name == "UploadPausedCheckpoint" {
			continue
		}
		if err := call(context.Background()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if got := storage.objects(); len(got) != 0 {
		t.Errorf("wrote %v, want nothing outside external checkpoints", got)
	}
}

func TestHerderFailsWhenPlaceholderCannotBeWritten(t *testing.T) {
	h := &herder{storage: &recordingStorage{err: errors.New("bucket unavailable")}}
	if _, err := h.Checkpoint(context.Background(), externalCheckpoint(testSnapshotURI)); err == nil {
		t.Error("Checkpoint succeeded with no placeholder written; a later tag of this snapshot would fail")
	}
	if _, err := h.Checkpoint(context.Background(), externalCheckpoint("not a snapshot uri")); err == nil {
		t.Error("Checkpoint succeeded with an unparseable snapshot URI")
	}
}

// The write runs inside the delay, so it does not add to it.
func TestHerderPlaceholderWriteRunsInsideDelay(t *testing.T) {
	const d = 100 * time.Millisecond
	h := &herder{delays: uniformDelays(d), storage: &recordingStorage{delay: 60 * time.Millisecond}}
	start := time.Now()
	if _, err := h.Checkpoint(context.Background(), externalCheckpoint(testSnapshotURI)); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := time.Since(start); got < d || got > d+50*time.Millisecond {
		t.Errorf("Checkpoint took %v, want about %v", got, d)
	}
}

// A write slower than a nonzero delay is worth a warning; with no delay every
// write is slower, and warning on each would flood the log.
func TestHerderWarnsOnlyWhenWriteOutlastsANonzeroDelay(t *testing.T) {
	for name, tc := range map[string]struct {
		delay    time.Duration
		wantWarn bool
	}{
		"no delay":          {delay: 0, wantWarn: false},
		"delay outlasted":   {delay: time.Millisecond, wantWarn: true},
		"delay not reached": {delay: time.Hour, wantWarn: false},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })

			h := &herder{storage: &recordingStorage{delay: 20 * time.Millisecond}}
			if err := h.writePlaceholder(context.Background(), time.Now(), tc.delay, testSnapshotURI); err != nil {
				t.Fatalf("writePlaceholder: %v", err)
			}
			if got := strings.Contains(logs.String(), "outlasted the configured delay"); got != tc.wantWarn {
				t.Errorf("warned = %v, want %v; log:\n%s", got, tc.wantWarn, logs.String())
			}
		})
	}
}

// recordingMinter records every mint, as ate-api-server's WorkerService.
type recordingMinter struct {
	mu    sync.Mutex
	reqs  []*ateapipb.MintAteomActorCertificateRequest
	err   error
	delay time.Duration
}

func (m *recordingMinter) MintAteomActorCertificate(_ context.Context, in *ateapipb.MintAteomActorCertificateRequest, _ ...grpc.CallOption) (*ateapipb.MintAteomActorCertificateResponse, error) {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reqs = append(m.reqs, proto.CloneOf(in))
	if m.err != nil {
		return nil, m.err
	}
	return &ateapipb.MintAteomActorCertificateResponse{ActorCertificates: [][]byte{[]byte("leaf")}}, nil
}

func (m *recordingMinter) mints() []*ateapipb.MintAteomActorCertificateRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.reqs)
}

const testActorUID = "0f9c1d2e-3b4a-4c5d-8e6f-7a8b9c0d1e2f"

var testGateway = &ateletpb.EgressGateway{Address: "atenet-egress.ate-system.svc:443"}

// activations invokes Run and Restore for one actor, naming gateway.
func activations(h *herder, gateway *ateletpb.EgressGateway) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"Run": func(ctx context.Context) error {
			_, err := h.Run(ctx, &ateletpb.RunRequest{Atespace: "team-a", ActorName: "sleep-1", ActorUid: testActorUID, EgressGateway: gateway})
			return err
		},
		"Restore": func(ctx context.Context) error {
			_, err := h.Restore(ctx, &ateletpb.RestoreRequest{Atespace: "team-a", ActorName: "sleep-1", ActorUid: testActorUID, EgressGateway: gateway})
			return err
		},
	}
}

func newMintingHerder(t *testing.T, m *recordingMinter, d delays) *herder {
	t.Helper()
	csr, err := newActorCSR()
	if err != nil {
		t.Fatal(err)
	}
	return &herder{delays: d, storage: &recordingStorage{}, minter: m, csr: csr}
}

// ateom mints the actor's certificate before starting any workload with
// tunneled egress, so each Run and Restore that names a gateway costs
// ate-api-server one mint, for that actor, with a CSR it accepts.
func TestHerderMintsActorCertificateOnActivation(t *testing.T) {
	for _, name := range []string{"Run", "Restore"} {
		t.Run(name, func(t *testing.T) {
			m := &recordingMinter{}
			if err := activations(newMintingHerder(t, m, delays{}), testGateway)[name](context.Background()); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			got := m.mints()
			if len(got) != 1 {
				t.Fatalf("%s minted %d times, want once", name, len(got))
			}
			want := &ateapipb.ObjectRef{Atespace: "team-a", Name: "sleep-1"}
			if !proto.Equal(got[0].GetActor(), want) || got[0].GetActorUid() != testActorUID {
				t.Errorf("%s minted for %v/%q, want %v/%q", name, got[0].GetActor(), got[0].GetActorUid(), want, testActorUID)
			}
			// ate-api-server parses the CSR and checks its signature.
			csr, err := x509.ParseCertificateRequest(got[0].GetCertificateSigningRequest())
			if err != nil {
				t.Fatalf("CSR does not parse: %v", err)
			}
			if err := csr.CheckSignature(); err != nil {
				t.Errorf("CSR signature: %v", err)
			}
		})
	}
}

// ateom mints only for tunneled egress, which ate-api-server asks for by
// naming a gateway; and --mint-actor-certificate=false leaves the mint out.
func TestHerderSkipsMint(t *testing.T) {
	m := &recordingMinter{}
	for name, call := range activations(newMintingHerder(t, m, delays{}), nil) {
		if err := call(context.Background()); err != nil {
			t.Fatalf("%s without a gateway: %v", name, err)
		}
	}
	if got := len(m.mints()); got != 0 {
		t.Errorf("minted %d times with no egress gateway, want none", got)
	}
	for name, call := range activations(&herder{storage: &recordingStorage{}}, testGateway) {
		if err := call(context.Background()); err != nil {
			t.Errorf("%s with minting off: %v", name, err)
		}
	}
}

// A failed mint fails the activation, as tunneled egress fails closed in ateom.
func TestHerderFailsWhenMintFails(t *testing.T) {
	m := &recordingMinter{err: errors.New("actor not found")}
	for name, call := range activations(newMintingHerder(t, m, delays{}), testGateway) {
		if err := call(context.Background()); err == nil {
			t.Errorf("%s succeeded with the mint refused", name)
		}
	}
}

// The mint is ate-api-server's time, so it adds to the data plane's delay: a
// slower mint makes a slower resume, as in production.
func TestHerderMintAddsToDelay(t *testing.T) {
	const d, mint = 100 * time.Millisecond, 60 * time.Millisecond
	for name, call := range activations(newMintingHerder(t, &recordingMinter{delay: mint}, uniformDelays(d)), testGateway) {
		start := time.Now()
		if err := call(context.Background()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := time.Since(start); got < d+mint {
			t.Errorf("%s took %v, want at least the mint plus the delay, %v", name, got, d+mint)
		}
	}
}
