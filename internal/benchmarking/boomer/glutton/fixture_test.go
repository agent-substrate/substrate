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

package glutton

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type fakeControlClient struct {
	ateapipb.ControlClient
	mu          sync.Mutex
	calls       []string
	resumeBoots []bool
}

func (f *fakeControlClient) CreateAtespace(ctx context.Context, in *ateapipb.CreateAtespaceRequest, opts ...grpc.CallOption) (*ateapipb.Atespace, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "CreateAtespace")
	return &ateapipb.Atespace{}, nil
}

func (f *fakeControlClient) CreateActor(ctx context.Context, in *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "CreateActor")
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "ResumeActor")
	f.resumeBoots = append(f.resumeBoots, in.GetBoot())
	return &ateapipb.ResumeActorResponse{}, nil
}

func (f *fakeControlClient) SuspendActor(ctx context.Context, in *ateapipb.SuspendActorRequest, opts ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "SuspendActor")
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControlClient) DeleteActor(ctx context.Context, in *ateapipb.DeleteActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "DeleteActor")
	return &ateapipb.Actor{}, nil
}

func (f *fakeControlClient) recordedCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeControlClient) recordedBoots() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bool(nil), f.resumeBoots...)
}

// diskServer models an actor holding a file for HTTP tests.
// The data slice is the source of truth: both routes report len(data) and
// sha256(data), and /readdisk serves data as payload.
// Each override field makes the actor lie about exactly one property.
type diskServer struct {
	// data is the file the actor holds, driving size, digest, and payload.
	data []byte
	// digest overrides the sha256 returned by both routes, leaving size and payload honest.
	digest []byte
	// corruptPayload is served by /readdisk instead of data, keeping size and digest honest.
	corruptPayload []byte
	// emptyPayload causes /readdisk to omit the Data field entirely (digest-only wire format).
	// Silently takes precedence over corruptPayload if both are set.
	emptyPayload bool
	// status fails every route with this HTTP status code.
	status int
	// elapsedUs sets the x-server-elapsed-us timing header/trailer.
	elapsedUs string

	mu         sync.Mutex
	paths      []string
	writeSizes []int32
	readModes  []gluttonpb.ReadMode
}

func (s *diskServer) reportedDigest() []byte {
	if s.digest != nil {
		return s.digest
	}
	h := sha256.Sum256(s.data)
	return h[:]
}

func (s *diskServer) hexDigest() string {
	return hex.EncodeToString(s.reportedDigest())
}

func (s *diskServer) reportedPayload() []byte {
	if s.emptyPayload {
		return nil
	}
	if s.corruptPayload != nil {
		return s.corruptPayload
	}
	return s.data
}

func (s *diskServer) recordedPaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.paths...)
}

func (s *diskServer) recordedWriteSizes() []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int32(nil), s.writeSizes...)
}

func (s *diskServer) recordedReadModes() []gluttonpb.ReadMode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]gluttonpb.ReadMode(nil), s.readModes...)
}

func (s *diskServer) start(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(ts.Close)
	return ts
}

func (s *diskServer) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.paths = append(s.paths, r.URL.Path)
	s.mu.Unlock()

	if s.status != 0 {
		http.Error(w, http.StatusText(s.status), s.status)
		return
	}

	if s.elapsedUs != "" {
		w.Header().Set(ateinterceptors.ServerElapsedTrailer, s.elapsedUs)
	}
	w.Header().Set("Content-Type", "application/x-protobuf")

	switch r.URL.Path {
	case writeDiskRoute:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req gluttonpb.WriteDiskRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.writeSizes = append(s.writeSizes, req.GetSize())
		s.mu.Unlock()

		resp, _ := proto.Marshal(&gluttonpb.WriteDiskResponse{
			Size:   int64(len(s.data)),
			Sha256: s.reportedDigest(),
		})
		_, _ = w.Write(resp)

	case readDiskRoute:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req gluttonpb.ReadDiskRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.readModes = append(s.readModes, req.GetReadMode())
		s.mu.Unlock()

		resp, _ := proto.Marshal(&gluttonpb.ReadDiskResponse{
			Size:   int64(len(s.data)),
			Sha256: s.reportedDigest(),
			Data:   s.reportedPayload(),
		})
		_, _ = w.Write(resp)

	default:
		http.NotFound(w, r)
	}
}

// newTestConfig starts srv, sets HTTPClient and RouterURL, and ensures
// APIStub, Tracer, and Dyn are populated if nil.
func newTestConfig(t *testing.T, srv *diskServer, cfg *userclass.Config) *userclass.Config {
	t.Helper()
	ts := srv.start(t)
	if cfg == nil {
		cfg = &userclass.Config{}
	}
	if cfg.APIStub == nil {
		cfg.APIStub = &fakeControlClient{}
	}
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("test")
	}
	if cfg.Dyn == nil {
		cfg.Dyn = dynconfig.NewHolder(dynconfig.Config{})
	}
	cfg.HTTPClient = ts.Client()
	cfg.RouterURL = ts.URL
	return cfg
}

func newTestDurDirUser(t *testing.T, srv *diskServer, cfg *userclass.Config) *durDirUser {
	t.Helper()
	c := newTestConfig(t, srv, cfg)
	return &durDirUser{
		cfg:          c,
		actorName:    "duractor",
		hostHeader:   "duractor.benchmark." + actorDomain,
		templateName: defaultDurTemplate,
		userClass:    durDirUserClass,
		expectedSize: int64(len(srv.data)),
	}
}
