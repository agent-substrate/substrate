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

package combinedvolumes

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
)

type latencySample struct {
	Mode         string          `json:"mode"`
	Index        int             `json:"index"`
	Warmup       bool            `json:"warmup"`
	Milliseconds float64         `json:"milliseconds"`
	Success      bool            `json:"success"`
	Actor        string          `json:"actor"`
	BootID       string          `json:"boot_id,omitempty"`
	ProcessNonce string          `json:"process_nonce,omitempty"`
	Snapshots    json.RawMessage `json:"source_snapshots"`
}

func TestRuntimeGoldenLatency(t *testing.T) {
	if os.Getenv("E2E_RUNTIME_GOLDEN_BENCHMARK") != "true" {
		t.Skip("optional latency experiment")
	}
	requireRuntimeGolden(t)
	var err error
	samples := 100
	if v := os.Getenv("E2E_LATENCY_SAMPLES"); v != "" {
		samples, err = strconv.Atoi(v)
		if err != nil || samples <= 0 {
			t.Fatal("invalid sample count")
		}
	}
	output := os.Getenv("E2E_LATENCY_OUTPUT")
	if output == "" {
		t.Fatal("E2E_LATENCY_OUTPUT required")
	}
	outputFile, err := os.Create(output)
	if err != nil {
		t.Fatal(err)
	}
	defer outputFile.Close()
	enc := json.NewEncoder(outputFile)
	f := newRuntimeGoldenFixture(t)
	ctx := context.Background()
	// Identify the template kernel once, outside measured activations.
	seed := f.create(t, f.golden, "identity-"+f.name, f.volumes[0])
	f.resume(t, seed)
	goldenBoot := probeJSON(ctx, t, f.router, resources.ActorRefFromActor(seed), runtimeStatePath())["boot_id"]
	f.remove(t, seed)
	rng := rand.New(rand.NewPCG(42, 91))
	for i := -5; i < samples; i++ {
		modes := []string{"cold", "runtime-golden", "own"}
		rng.Shuffle(len(modes), func(a, b int) { modes[a], modes[b] = modes[b], modes[a] })
		for _, mode := range modes {
			t.Run(fmt.Sprintf("%s/%d", mode, i), func(t *testing.T) {
				tmpl := f.golden
				if mode == "cold" {
					tmpl = f.cold
				}
				a := f.create(t, tmpl, fmt.Sprintf("sample-%s-%d-%s", mode, i+5, f.name), f.volumes[0])
				defer f.remove(t, a)
				var ownNonce string
				if mode == "own" {
					f.resume(t, a)
					ownNonce = probeJSON(ctx, t, f.router, resources.ActorRefFromActor(a), runtimeStatePath())["process_nonce"]
					a = f.suspend(t, a)
				}
				snap, _ := json.Marshal(a.GetStatus().GetSnapshots())
				sample := latencySample{Mode: mode, Index: i, Warmup: i < 0, Actor: a.GetMetadata().GetName(), Snapshots: snap}
				start := time.Now()
				defer func() {
					sample.Milliseconds = float64(time.Since(start).Microseconds()) / 1000
					sample.Success = !t.Failed()
					if err := enc.Encode(sample); err != nil {
						t.Error(err)
					}
					if err := outputFile.Sync(); err != nil {
						t.Error(err)
					}
				}()
				if mode == "cold" && len(a.GetStatus().GetSnapshots()) != 0 {
					t.Fatal("cold sample has snapshot")
				}
				if mode != "cold" && len(a.GetStatus().GetSnapshots()) == 0 {
					t.Fatal("restore sample has no snapshot")
				}
				f.resume(t, a)
				ref := resources.ActorRefFromActor(a)
				state := probeJSON(ctx, t, f.router, ref, runtimeStatePath())
				sample.BootID = state["boot_id"]
				sample.ProcessNonce = state["process_nonce"]
				if usesGuestBootMarker() && mode == "cold" && sample.BootID == goldenBoot {
					t.Fatal("cold sample restored golden")
				}
				if usesGuestBootMarker() && mode != "cold" && sample.BootID != goldenBoot {
					t.Fatal("restore sample cold-booted")
				}
				if mode == "own" && sample.ProcessNonce != ownNonce {
					t.Fatal("own restore restarted process")
				}
				path := sharedPath + "/" + sample.Actor
				requireWrite(ctx, t, f.router, ref, path)
				requireContent(ctx, t, f.router, ref, path, probeWrittenContent)
			})
		}
	}
}
