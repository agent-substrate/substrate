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
			wantHTTPCall: []string{readDiskRoute, readDiskRoute, writeDiskRoute},
		},
		{
			name:         "implicit resume mode",
			resumeMode:   dynconfig.ResumeModeImplicit,
			wantGRPCCall: []string{"SuspendActor"}, // No ResumeActor RPC!
			wantHTTPCall: []string{readDiskRoute, readDiskRoute, writeDiskRoute},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := &diskServer{data: []byte("seq content")}
			fakeCtrl := &fakeControlClient{}
			cfg := &Config{
				APIStub: fakeCtrl,
				Dyn: dynconfig.NewHolder(dynconfig.Config{
					ResumeMode: tc.resumeMode,
				}),
			}
			du := newTestDurDirUser(t, srv, cfg)
			du.expectedDigest = srv.hexDigest()

			dynCfg := cfg.Dyn.Load()
			du.step(context.Background(), dynCfg)

			if got := fakeCtrl.recordedCalls(); !reflect.DeepEqual(got, tc.wantGRPCCall) {
				t.Errorf("gRPC calls: got %v, want %v", got, tc.wantGRPCCall)
			}
			if got := srv.recordedPaths(); !reflect.DeepEqual(got, tc.wantHTTPCall) {
				t.Errorf("HTTP calls: got %v, want %v", got, tc.wantHTTPCall)
			}
			if !du.actorRunning {
				t.Errorf("actorRunning: got false, want true after successful step")
			}
		})
	}
}

func TestDurDirUsesConfiguredFileSize(t *testing.T) {
	configuredSize := int64(1048576) // 1 MiB
	srv := &diskServer{data: make([]byte, configuredSize)}
	du := newTestDurDirUser(t, srv, nil)

	if err := du.writeDisk(context.Background(), "TestConfiguredSize", configuredSize, gluttonpb.WriteMode_WRITE_MODE_TRUNCATE); err != nil {
		t.Fatalf("writeDisk failed: %v", err)
	}

	recorded := srv.recordedWriteSizes()
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
	srv := &diskServer{
		data:         make([]byte, 1024),
		emptyPayload: true,
	}
	du := newTestDurDirUser(t, srv, nil)
	du.expectedDigest = srv.hexDigest()

	if err := du.readDisk(context.Background(), t.Name(), gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY); err != nil {
		t.Fatalf("expected readDisk to succeed in digest-only mode with empty payload, got: %v", err)
	}
}

func TestDurDirDataModeRejectsEmptyPayload(t *testing.T) {
	srv := &diskServer{
		data:         make([]byte, 1024),
		emptyPayload: true,
	}
	du := newTestDurDirUser(t, srv, nil)
	du.expectedDigest = srv.hexDigest()

	if err := du.readDisk(context.Background(), t.Name(), gluttonpb.ReadMode_READ_MODE_DATA); err == nil {
		t.Fatalf("expected readDisk to fail in data mode with empty payload, got nil")
	}
}

func TestDurDirDigestOnlyStillRejectsWrongDigest(t *testing.T) {
	wrongHash := sha256.Sum256([]byte("wrong data"))
	srv := &diskServer{
		data:         make([]byte, 1024),
		digest:       wrongHash[:],
		emptyPayload: true,
	}
	du := newTestDurDirUser(t, srv, nil)
	h := sha256.Sum256(srv.data)
	du.expectedDigest = hex.EncodeToString(h[:])

	if err := du.readDisk(context.Background(), t.Name(), gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY); err == nil {
		t.Fatalf("expected readDisk to fail in digest-only mode on wrong digest, got nil")
	}
}

func TestDurDirReadModeSentOnWire(t *testing.T) {
	srv := &diskServer{}
	du := newTestDurDirUser(t, srv, nil)
	du.expectedDigest = srv.hexDigest()

	if err := du.readDisk(context.Background(), t.Name(), gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY); err != nil {
		t.Fatalf("readDisk failed: %v", err)
	}

	recorded := srv.recordedReadModes()
	if len(recorded) != 1 {
		t.Fatalf("recorded read modes: got %d calls, want 1", len(recorded))
	}
	if recorded[0] != gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY {
		t.Errorf("wire ReadMode: got %v, want %v", recorded[0], gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY)
	}
}

func TestDurDirBootstrapDoesNotBoot(t *testing.T) {
	srv := &diskServer{data: []byte("data")}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &Config{
		APIStub: fakeCtrl,
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			DurDirFileSize: int64(len(srv.data)),
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
			srv := &diskServer{data: []byte("data")}
			fakeCtrl := &fakeControlClient{}
			cfg := newTestConfig(t, srv, &Config{
				APIStub: fakeCtrl,
				Dyn: dynconfig.NewHolder(dynconfig.Config{
					DurDirFileSize: int64(len(srv.data)),
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

func TestImplicitResumeDoesNotClaimRunningOnFailedServe(t *testing.T) {
	srv := &diskServer{status: http.StatusInternalServerError}
	cfg := &Config{
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			ResumeMode: dynconfig.ResumeModeImplicit,
		}),
	}
	du := newTestDurDirUser(t, srv, cfg)
	du.expectedDigest = "abcd"

	dynCfg := cfg.Dyn.Load()
	du.step(context.Background(), dynCfg)

	if du.actorRunning {
		t.Errorf("actorRunning: got true, want false after failed serve in implicit mode")
	}
}
