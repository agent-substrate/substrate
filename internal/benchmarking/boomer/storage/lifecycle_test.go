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

package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/atenet"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestElapsedFromMD(t *testing.T) {
	fallback := 100 * time.Millisecond

	// Case 1: Empty metadata returns fallback and sourceClient
	mdEmpty := metadata.MD{}
	latency, source := elapsedFromMD(mdEmpty, ateinterceptors.ServerElapsedTrailer, fallback)
	if latency != fallback || source != sourceClient {
		t.Errorf("empty MD: got (%v, %v), want (%v, %v)", latency, source, fallback, sourceClient)
	}

	// Case 2: Server elapsed trailer in microseconds
	mdValid := metadata.Pairs(ateinterceptors.ServerElapsedTrailer, "1500")
	latency, source = elapsedFromMD(mdValid, ateinterceptors.ServerElapsedTrailer, fallback)
	wantLatency := 1500 * time.Microsecond
	if latency != wantLatency || source != sourceServer {
		t.Errorf("valid MD: got (%v, %v), want (%v, %v)", latency, source, wantLatency, sourceServer)
	}

	// Case 3: Malformed value falls back
	mdInvalid := metadata.Pairs(ateinterceptors.ServerElapsedTrailer, "not-a-number")
	latency, source = elapsedFromMD(mdInvalid, ateinterceptors.ServerElapsedTrailer, fallback)
	if latency != fallback || source != sourceClient {
		t.Errorf("invalid MD: got (%v, %v), want (%v, %v)", latency, source, fallback, sourceClient)
	}
}

func TestDynamicWait(t *testing.T) {
	holder := dynconfig.NewHolder(dynconfig.Config{
		MinWait: 50 * time.Millisecond,
		MaxWait: 100 * time.Millisecond,
	})
	rt := &taskRuntime{cfg: &userclass.Config{Dyn: holder}}

	for i := 0; i < 20; i++ {
		wait := rt.dynamicWait()
		if wait < 50*time.Millisecond || wait > 100*time.Millisecond {
			t.Errorf("dynamicWait() = %v, want between 50ms and 100ms", wait)
		}
	}
}

func TestMsFloat(t *testing.T) {
	d := 1500 * time.Microsecond
	got := msFloat(d)
	if got != 1.5 {
		t.Errorf("msFloat(1500us) = %v, want 1.5", got)
	}
}

func TestWriteDiskRoutesByTargetActorHeader(t *testing.T) {
	type seen struct{ path, target string }
	reqs := make(chan seen, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqs <- seen{path: r.URL.Path, target: r.Header.Get(atenet.TargetActorHeader)}
		body, err := proto.Marshal(&gluttonpb.WriteDiskResponse{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	u := &gluttonStorageUser{
		cfg: &userclass.Config{
			Atespace:   "benchmark",
			RouterURL:  srv.URL,
			HTTPClient: srv.Client(),
			Tracer:     noop.NewTracerProvider().Tracer("test"),
		},
		actorName: "sb-st-test",
	}
	u.writeDisk(context.Background())

	got := <-reqs
	if got.path != writeDiskPath {
		t.Errorf("path = %q, want %q", got.path, writeDiskPath)
	}
	if want := "benchmark/sb-st-test"; got.target != want {
		t.Errorf("%s = %q, want %q", atenet.TargetActorHeader, got.target, want)
	}
	if _, err := atenet.ParseTargetActor(got.target); err != nil {
		t.Errorf("router would reject the header: %v", err)
	}
}
