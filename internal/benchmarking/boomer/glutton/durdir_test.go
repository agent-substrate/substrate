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
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

func TestDurDirLoopSequence(t *testing.T) {
	tests := []struct {
		name         string
		resumeMode   string
		wantGRPCCall []string
		wantHTTPCall []string
	}{
		{
			name:         "explicit resume mode",
			resumeMode:   dynconfig.ResumeModeExplicit,
			wantGRPCCall: []string{"SuspendActor", "ResumeActor"},
			wantHTTPCall: []string{fake.ReadDiskRoute, fake.ReadDiskRoute, fake.WriteDiskRoute},
		},
		{
			name:         "implicit resume mode",
			resumeMode:   dynconfig.ResumeModeImplicit,
			wantGRPCCall: []string{"SuspendActor"}, // No ResumeActor RPC!
			wantHTTPCall: []string{fake.ReadDiskRoute, fake.ReadDiskRoute, fake.WriteDiskRoute},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fake.Server{Data: []byte("seq content")}
			fakeCtrl := &fakeControlClient{}
			cfg := &userclass.Config{
				APIStub: fakeCtrl,
				Dyn: dynconfig.NewHolder(dynconfig.Config{
					ResumeMode: tc.resumeMode,
				}),
			}
			du := newTestDurDirUser(t, srv, cfg)
			du.expectedDigest = srv.HexDigest()

			dynCfg := cfg.Dyn.Load()
			du.step(context.Background(), dynCfg)

			if got := fakeCtrl.recordedCalls(); !reflect.DeepEqual(got, tc.wantGRPCCall) {
				t.Errorf("gRPC calls: got %v, want %v", got, tc.wantGRPCCall)
			}
			if got := srv.RecordedPaths(); !reflect.DeepEqual(got, tc.wantHTTPCall) {
				t.Errorf("HTTP calls: got %v, want %v", got, tc.wantHTTPCall)
			}
		})
	}
}

func TestDurDirUsesConfiguredFileSize(t *testing.T) {
	configuredSize := int64(1048576) // 1 MiB
	srv := &fake.Server{Data: make([]byte, configuredSize)}
	du := newTestDurDirUser(t, srv, nil)

	if err := du.writeDisk(context.Background(), "TestConfiguredSize", configuredSize, gluttonpb.WriteMode_WRITE_MODE_TRUNCATE); err != nil {
		t.Fatalf("writeDisk failed: %v", err)
	}

	recorded := srv.RecordedWriteSizes()
	if len(recorded) != 1 {
		t.Fatalf("recorded write sizes: got %d calls, want 1", len(recorded))
	}
	if int64(recorded[0]) != configuredSize {
		t.Errorf("WriteDisk received size %d, want %d", recorded[0], configuredSize)
	}
}

func TestDurDirTestFileIsAValidGluttonKey(t *testing.T) {
	if !regexp.MustCompile(`^[a-zA-Z0-9_-]+$`).MatchString(durDirTestFile) {
		t.Fatalf("durDirTestFile %q would be rejected by glutton", durDirTestFile)
	}
}

func TestDurDirDigestOnlyAcceptsEmptyPayload(t *testing.T) {
	srv := &fake.Server{
		Data:         make([]byte, 1024),
		EmptyPayload: true,
	}
	du := newTestDurDirUser(t, srv, nil)
	du.expectedDigest = srv.HexDigest()

	if err := du.readDisk(context.Background(), t.Name(), gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY); err != nil {
		t.Fatalf("expected readDisk to succeed in digest-only mode with empty payload, got: %v", err)
	}
}

func TestDurDirDataModeRejectsEmptyPayload(t *testing.T) {
	srv := &fake.Server{
		Data:         make([]byte, 1024),
		EmptyPayload: true,
	}
	du := newTestDurDirUser(t, srv, nil)
	du.expectedDigest = srv.HexDigest()

	if err := du.readDisk(context.Background(), t.Name(), gluttonpb.ReadMode_READ_MODE_DATA); err == nil {
		t.Fatalf("expected readDisk to fail in data mode with empty payload, got nil")
	}
}

func TestDurDirDigestOnlyStillRejectsWrongDigest(t *testing.T) {
	wrongHash := sha256.Sum256([]byte("wrong data"))
	srv := &fake.Server{
		Data:         make([]byte, 1024),
		Digest:       wrongHash[:],
		EmptyPayload: true,
	}
	du := newTestDurDirUser(t, srv, nil)
	h := sha256.Sum256(srv.Data)
	du.expectedDigest = hex.EncodeToString(h[:])

	if err := du.readDisk(context.Background(), t.Name(), gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY); err == nil {
		t.Fatalf("expected readDisk to fail in digest-only mode on wrong digest, got nil")
	}
}

func TestDurDirReadModeSentOnWire(t *testing.T) {
	srv := &fake.Server{}
	du := newTestDurDirUser(t, srv, nil)
	du.expectedDigest = srv.HexDigest()

	if err := du.readDisk(context.Background(), t.Name(), gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY); err != nil {
		t.Fatalf("readDisk failed: %v", err)
	}

	recorded := srv.RecordedReadModes()
	if len(recorded) != 1 {
		t.Fatalf("recorded read modes: got %d calls, want 1", len(recorded))
	}
	if recorded[0] != gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY {
		t.Errorf("wire ReadMode: got %v, want %v", recorded[0], gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY)
	}
}

func TestDurDirBootstrapDoesNotBoot(t *testing.T) {
	srv := &fake.Server{Data: []byte("data")}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub: fakeCtrl,
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			DurDirFileSize: int64(len(srv.Data)),
		}),
	})

	rt := &durDirRuntime{cfg: cfg}
	_, err := rt.startUser(context.Background(), cfg.Dyn.Load())
	if err != nil {
		t.Fatalf("startUser failed: %v", err)
	}

	boots := fakeCtrl.recordedBoots()
	if len(boots) == 0 {
		t.Fatalf("expected ResumeActor to be called during bootstrap, got 0 calls")
	}
	if boots[0] {
		t.Errorf("bootstrap ResumeActor Boot: got %v, want false", boots[0])
	}
}

func TestDurDirBootstrapUsesConfiguredResumeMode(t *testing.T) {
	tests := []struct {
		name            string
		resumeMode      string
		wantResumeActor bool
	}{
		{
			name:            "explicit resume mode",
			resumeMode:      dynconfig.ResumeModeExplicit,
			wantResumeActor: true,
		},
		{
			name:            "implicit resume mode",
			resumeMode:      dynconfig.ResumeModeImplicit,
			wantResumeActor: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &fake.Server{Data: []byte("data")}
			fakeCtrl := &fakeControlClient{}
			cfg := newTestConfig(t, srv, &userclass.Config{
				APIStub: fakeCtrl,
				Dyn: dynconfig.NewHolder(dynconfig.Config{
					DurDirFileSize: int64(len(srv.Data)),
					ResumeMode:     tc.resumeMode,
				}),
			})

			rt := &durDirRuntime{cfg: cfg}
			_, err := rt.startUser(context.Background(), cfg.Dyn.Load())
			if err != nil {
				t.Fatalf("startUser failed: %v", err)
			}

			calls := fakeCtrl.recordedCalls()
			gotResumeActor := slices.Contains(calls, "ResumeActor")
			if gotResumeActor != tc.wantResumeActor {
				t.Errorf("ResumeActor in recordedCalls: got %v, want %v (calls = %v)", gotResumeActor, tc.wantResumeActor, calls)
			}
		})
	}
}

func TestDurDirBootstrapFailureSuspendsBeforeDelete(t *testing.T) {
	srv := &fake.Server{Status: http.StatusInternalServerError}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub: fakeCtrl,
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			DurDirFileSize: 1024,
			ResumeMode:     dynconfig.ResumeModeExplicit,
		}),
	})

	rt := &durDirRuntime{cfg: cfg}
	_, err := rt.startUser(context.Background(), cfg.Dyn.Load())
	if err == nil {
		t.Fatalf("startUser expected error on failing server, got nil")
	}

	calls := fakeCtrl.recordedCalls()
	if len(calls) < 2 || calls[len(calls)-2] != "SuspendActor" || calls[len(calls)-1] != "DeleteActor" {
		t.Errorf("recordedCalls must end with [SuspendActor, DeleteActor], got %v", calls)
	}
}

func TestDurDirShutdownSuspendsBeforeDelete(t *testing.T) {
	fakeCtrl := &fakeControlClient{}
	cfg := &userclass.Config{
		APIStub: fakeCtrl,
	}
	du := newTestDurDirUser(t, &fake.Server{}, cfg)

	rt := &durDirRuntime{cfg: du.cfg}
	rt.users.Store(goroutineID(), du)
	rt.shutdown(context.Background())

	calls := fakeCtrl.recordedCalls()
	if len(calls) < 2 || calls[len(calls)-2] != "SuspendActor" || calls[len(calls)-1] != "DeleteActor" {
		t.Errorf("recordedCalls must end with [SuspendActor, DeleteActor], got %v", calls)
	}
}
