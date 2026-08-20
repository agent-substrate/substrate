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
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/registry"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// The application layer cannot finish downloading until PrepareSandbox runs.
// This fails if startup is accidentally made serial, and checks cleanup when
// that image leg fails after the worker has prepared the sandbox.
func TestRunPreparesSandboxDuringImagePull(t *testing.T) {
	for _, failImage := range []bool{false, true} {
		t.Run(fmt.Sprintf("image_failure=%v", failImage), func(t *testing.T) {
			useTempNodeDirs(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			imageStarted, allowImage := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var pulling atomic.Bool
			layer := singleFileLayer(t, "bin/app", "application")
			digest, err := layer.Digest()
			if err != nil {
				t.Fatal(err)
			}
			reg := registry.New(registry.Logger(log.New(io.Discard, "", 0)))
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if pulling.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/blobs/"+digest.String()) {
					once.Do(func() { close(imageStarted) })
					select {
					case <-allowImage:
					case <-r.Context().Done():
						return
					}
					if failImage {
						http.Error(w, "image unavailable", http.StatusNotFound)
						return
					}
				}
				reg.ServeHTTP(w, r)
			}))
			defer server.Close()
			host := strings.TrimPrefix(server.URL, "http://")
			app := pushTestImage(t, host+"/app:v1", layer)
			pause := pushTestImage(t, host+"/pause:v1", singleFileLayer(t, "pause", "pause"))
			pulling.Store(true)
			discarded := make(chan error, 1)
			worker := &fakeWorker{
				prepareHook: func(rpcCtx context.Context) error {
					select {
					case <-imageStarted:
						close(allowImage)
						return nil
					case <-rpcCtx.Done():
						return rpcCtx.Err()
					}
				},
				discardHook: func(cleanupCtx context.Context) error {
					if cleanupCtx.Err() != nil {
						discarded <- cleanupCtx.Err()
					} else if _, ok := cleanupCtx.Deadline(); !ok {
						discarded <- fmt.Errorf("cleanup has no deadline")
					} else {
						discarded <- nil
					}
					return nil
				},
			}
			serveFakeWorker(t, worker)
			asset := []byte("runsc")
			s := &Atelet{workerDialer: newWorkerDialer(1), imageCache: newImageVolumeStore(t), anonGCSClient: fakeObjectStorage{data: asset}, systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil)}
			_, err = s.Run(ctx, &ateletpb.RunRequest{
				Atespace: "demo", ActorName: "actor", ActorUid: "01234567-89ab-cdef-0123-456789abcdef", WorkerPodUid: "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e5f",
				SandboxAssets: &ateletpb.SandboxAssets{SandboxClass: "gvisor", PauseImage: pause, Assets: map[string]*ateletpb.ArchAssets{
					runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{runscAssetName: {Url: "gs://test/runsc", Sha256: fmt.Sprintf("%x", sha256.Sum256(asset))}}},
				}},
				Spec: &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "app", Image: app, Command: []string{"/bin/app"}}}},
			})
			if failImage {
				if err == nil {
					t.Fatal("Run succeeded after image failure")
				}
				select {
				case cleanupErr := <-discarded:
					if cleanupErr != nil {
						t.Fatal(cleanupErr)
					}
				case <-ctx.Done():
					t.Fatal("prepared sandbox was not discarded")
				}
				if worker.actorDirs["RunWorkload"] != nil {
					t.Fatal("application started after image failure")
				}
			} else if err != nil {
				t.Fatalf("parallel Run: %v", err)
			}
		})
	}
}
