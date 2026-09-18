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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/internal/imagestreaming"
	_ "github.com/agent-substrate/substrate/internal/imagestreaming/drivers/remotesnapshotter"
	_ "github.com/agent-substrate/substrate/internal/imagestreaming/drivers/riptide"
	_ "github.com/agent-substrate/substrate/internal/imagestreaming/drivers/soci"
)

func main() {
	provider := flag.String("provider", "riptide", "Streaming provider: riptide, soci, remotesnapshotter")
	socket := flag.String("socket", "", "Path to daemon gRPC socket (default depends on provider)")
	imageRef := flag.String("image", "", "Image reference to stream")
	workDir := flag.String("work-dir", "/tmp/streaming-e2e", "Work directory for streamed layers")
	bench := flag.Bool("bench", false, "Run comparative performance benchmark against traditional pull/unpack baseline")
	flag.Parse()

	if *socket == "" {
		if *provider == "soci" {
			*socket = "/run/soci-snapshotter-grpc/soci-snapshotter-grpc.sock"
		} else {
			*socket = "/run/containerd-gcfs-grpc"
		}
	}
	if *imageRef == "" {
		if *provider == "soci" {
			*imageRef = "public.ecr.aws/soci-workshop-examples/ffmpeg:latest"
		} else {
			*imageRef = "us-central1-docker.pkg.dev/kuiyue-gke-dev/kuiyue-gke-dev-repo/image-b:latest"
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	fmt.Println("=== Agent Substrate Image Streaming Live E2E Probe ===")
	fmt.Printf("Provider: %s\n", *provider)
	fmt.Printf("Socket:   %s\n", *socket)
	fmt.Printf("Image:    %s\n", *imageRef)
	fmt.Printf("WorkDir:  %s\n", *workDir)
	fmt.Printf("Bench:    %v\n", *bench)

	streamer, err := imagestreaming.Get(ctx, *provider, imagestreaming.Config{
		imagestreaming.SocketPathKey: *socket,
		"work_dir":                   *workDir,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: failed to get riptide streamer: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Initialized streamer provider: %s\n", streamer.Name())

	if *bench {
		if err := runBenchmark(ctx, streamer, *imageRef, *provider); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: benchmark execution failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	req := &imagestreaming.StreamRequest{
		ImageRef: *imageRef,
	}

	canStream, err := streamer.CanStream(ctx, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: CanStream returned error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("CanStream check result: %v\n", canStream)
	if !canStream {
		fmt.Fprintf(os.Stderr, "FAIL: CanStream returned false\n")
		os.Exit(1)
	}

	fmt.Println("\nInvoking PrepareLayers on live GCFS daemon...")
	t0 := time.Now()
	res, err := streamer.PrepareLayers(ctx, req)
	dur := time.Since(t0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: PrepareLayers failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PrepareLayers SUCCEEDED in %v\n", dur)
	fmt.Printf("Image Digest: %s\n", res.ImageDigest)
	fmt.Printf("Prepared Layers Count: %d\n", len(res.LayerDirs))

	for i, layerDir := range res.LayerDirs {
		fmt.Printf("\n--- Layer %d: %s ---\n", i, layerDir)
		finalizedMarker := filepath.Join(layerDir, "finalized")
		if _, err := os.Stat(finalizedMarker); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: missing finalized marker: %v\n", err)
		} else {
			fmt.Printf("  [OK] Finalized marker exists: %s\n", finalizedMarker)
		}

		fsLink := filepath.Join(layerDir, "fs")
		target, err := os.Readlink(fsLink)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: reading fs symlink %s: %v\n", fsLink, err)
			os.Exit(1)
		}
		fmt.Printf("  [OK] fs symlink: %s -> %s\n", fsLink, target)

		// List entries in fs directory
		entries, err := os.ReadDir(fsLink)
		if err != nil {
			fmt.Printf("  [INFO] os.ReadDir(%s): %v\n", fsLink, err)
		} else {
			fmt.Printf("  [OK] Found %d rootfs entries in layer:\n", len(entries))
			limit := 8
			for j, e := range entries {
				if j >= limit {
					fmt.Printf("    ... and %d more entries\n", len(entries)-limit)
					break
				}
				fmt.Printf("    - %s (dir=%v)\n", e.Name(), e.IsDir())
			}
		}
	}

	fmt.Println("\nInvoking ReleaseLayers...")
	if err := streamer.ReleaseLayers(ctx, req); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: ReleaseLayers failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("[OK] ReleaseLayers completed successfully.")

	fmt.Println("\n=== E2E LIVE TEST SUCCESSFUL ===")
}
