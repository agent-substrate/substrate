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

// Package fake provides an httptest-backed stand-in for a glutton actor.
package fake

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
	"google.golang.org/protobuf/proto"
)

// Routes the fake serves, mirroring glutton's real HTTP mux. Declared here so
// the fake depends on nothing; collapses onto one source when glutton's core moves.
const (
	WriteDiskRoute = "/writedisk"
	ReadDiskRoute  = "/readdisk"
	WriteRAMRoute  = "/writeram"
)

// Server is an httptest-backed stand-in for a glutton actor holding one file.
// The Data slice is the source of truth: both routes report len(Data) and
// sha256(Data), and /readdisk serves Data as payload.
// Each override field makes the actor lie about exactly one property.
type Server struct {
	// Data is the file the actor holds, driving size, digest, and payload.
	Data []byte
	// Digest overrides the sha256 returned by both routes, leaving size and payload honest.
	Digest []byte
	// CorruptPayload is served by /readdisk instead of Data, keeping size and digest honest.
	CorruptPayload []byte
	// EmptyPayload causes /readdisk to omit the Data field entirely (digest-only wire format).
	// Silently takes precedence over CorruptPayload if both are set.
	EmptyPayload bool
	// Status fails every route with this HTTP status code.
	Status int
	// ElapsedUs sets the x-server-elapsed-us timing header/trailer.
	ElapsedUs string

	mu            sync.Mutex
	paths         []string
	writeSizes    []int32
	readModes     []gluttonpb.ReadMode
	ramWriteSizes []string
}

func (s *Server) reportedDigest() []byte {
	if s.Digest != nil {
		return s.Digest
	}
	h := sha256.Sum256(s.Data)
	return h[:]
}

func (s *Server) HexDigest() string {
	return hex.EncodeToString(s.reportedDigest())
}

func (s *Server) reportedPayload() []byte {
	if s.EmptyPayload {
		return nil
	}
	if s.CorruptPayload != nil {
		return s.CorruptPayload
	}
	return s.Data
}

func (s *Server) RecordedPaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.paths...)
}

func (s *Server) RecordedWriteSizes() []int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int32(nil), s.writeSizes...)
}

func (s *Server) RecordedReadModes() []gluttonpb.ReadMode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]gluttonpb.ReadMode(nil), s.readModes...)
}

// RecordedRAMWriteSizes returns each /writeram request's size string.
func (s *Server) RecordedRAMWriteSizes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ramWriteSizes...)
}

func (s *Server) Start(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(ts.Close)
	return ts
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.paths = append(s.paths, r.URL.Path)
	s.mu.Unlock()

	if s.Status != 0 {
		http.Error(w, http.StatusText(s.Status), s.Status)
		return
	}

	if s.ElapsedUs != "" {
		w.Header().Set(ateinterceptors.ServerElapsedTrailer, s.ElapsedUs)
	}
	w.Header().Set("Content-Type", "application/x-protobuf")

	switch r.URL.Path {
	case WriteDiskRoute:
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
			Size:   int64(len(s.Data)),
			Sha256: s.reportedDigest(),
		})
		_, _ = w.Write(resp)

	case ReadDiskRoute:
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
			Size:   int64(len(s.Data)),
			Sha256: s.reportedDigest(),
			Data:   s.reportedPayload(),
		})
		_, _ = w.Write(resp)

	case WriteRAMRoute:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req gluttonpb.WriteRAMRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.ramWriteSizes = append(s.ramWriteSizes, req.GetSize())
		s.mu.Unlock()

		resp, _ := proto.Marshal(&gluttonpb.WriteRAMResponse{})
		_, _ = w.Write(resp)

	default:
		http.NotFound(w, r)
	}
}
