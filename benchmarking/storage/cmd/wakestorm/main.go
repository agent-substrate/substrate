// Copyright 2026 The Agent Substrate Authors
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

// wakestorm times batches of actor lifecycle calls against a live Agent
// Substrate cluster through ateapi.
//
// Each phase sends one call per actor with a set concurrency, times every call
// and the whole batch, and writes calls.csv and summary.json to -out:
//
//   - create: creates -n actors and resumes each once (first resume).
//   - lifecycle: runs -repeat cycles of create, first resume, suspend, resume
//     and delete on -n new actors each.
//   - delete: deletes every actor with -prefix and waits until they are gone.
//
// Usage:
//
//	go run ./benchmarking/storage/cmd/wakestorm -phase lifecycle -n 1 -repeat 30 -out /tmp/latency
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/preview"
)

var (
	flagPhase           = flag.String("phase", "", "create | lifecycle | delete")
	flagN               = flag.Int("n", 1, "number of actors")
	flagStart           = flag.Int("start", 0, "index of the first actor name (prefix-NNNN)")
	flagPrefix          = flag.String("prefix", "ws", "actor name prefix")
	flagAtespace        = flag.String("atespace", "benchmark", "atespace to create actors in")
	flagTemplate        = flag.String("template", "glutton-storage", "actor template name")
	flagTemplateSpace   = flag.String("template-atespace", "benchmark-workloads", "atespace of the actor template")
	flagRepeat          = flag.Int("repeat", 30, "with -phase lifecycle, the number of cycles")
	flagCreateConc      = flag.Int("create-concurrency", 20, "concurrent calls in create and lifecycle phases; 0 means all at once")
	flagDeleteConc      = flag.Int("delete-concurrency", 0, "concurrent DeleteActor calls; 0 means all at once")
	flagCreateRate      = flag.Float64("create-rate", 0, "most first resumes started per second; 0 means no limit")
	flagRepairRounds    = flag.Int("repair-rounds", 5, "after the first resume, rounds of slow retries for actors that are not RUNNING")
	flagSettle          = flag.Duration("settle", 15*time.Second, "pause between steps, excluded from call times")
	flagCallTimeout     = flag.Duration("call-timeout", 15*time.Minute, "timeout for one API call, including its retries")
	flagMaxRetries      = flag.Int("max-conflict-retries", 20, "retries when a call fails with Aborted (concurrent update)")
	flagRetryContention = flag.Bool("retry-contention", false, "retry ResumeActor when the volume claim fails with \"please retry\"")
	flagRetryNoRoom     = flag.Bool("retry-no-room", false, "retry ResumeActor every second while it fails with \"no worker has room\"")
	flagOut             = flag.String("out", "", "directory for calls.csv and summary.json")
	flagKubeconfig      = flag.String("kubeconfig", "", "kubeconfig path")
	flagContext         = flag.String("context", "", "kubeconfig context")
	flagEndpoint        = flag.String("endpoint", "", "ateapi endpoint; empty port-forwards")
	flagTokenFile       = flag.String("token-file", "", "bearer token file")
)

type runner struct {
	cli     *ateclient.Client
	retry   retryPolicy
	outDir  string
	started time.Time

	mu     sync.Mutex
	calls  []callRecord
	stats  []phaseStats
	actors []string
	nodeOf map[string]string
}

func main() {
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *flagN <= 0 {
		fatal(fmt.Errorf("-n must be positive, got %d", *flagN))
	}
	if *flagOut == "" {
		*flagOut = filepath.Join(os.TempDir(), "wakestorm-"+time.Now().UTC().Format("20060102-150405"))
	}
	if err := os.MkdirAll(*flagOut, 0o755); err != nil {
		fatal(err)
	}

	// External volumes are a preview feature, so every call opts in to preview
	// features.
	cli, err := ateclient.NewClient(ctx, *flagKubeconfig, *flagContext, *flagEndpoint, *flagTokenFile, false, preview.DialOptionsWithOptIn()...)
	if err != nil {
		fatal(fmt.Errorf("connect to ateapi: %w", err))
	}
	defer cli.Close()

	r := &runner{
		cli: cli,
		retry: retryPolicy{
			maxConflict: *flagMaxRetries,
			contention:  *flagRetryContention,
			noRoom:      *flagRetryNoRoom,
		},
		outDir:  *flagOut,
		started: time.Now(),
		actors:  actorNames(*flagPrefix, *flagStart, *flagN),
	}
	logf("out=%s phase=%s n=%d atespace=%s template=%s/%s", r.outDir, *flagPhase, *flagN, *flagAtespace, *flagTemplateSpace, *flagTemplate)

	switch *flagPhase {
	case "create":
		r.create(ctx)
		r.firstResume(ctx)
		r.printSummary()
	case "lifecycle":
		r.lifecycle(ctx)
		r.printLifecycle()
	case "delete":
		r.loadExisting(ctx)
		if len(r.actors) == 0 {
			return
		}
		r.delete(ctx)
		r.printSummary()
	default:
		fatal(fmt.Errorf("-phase must be create, lifecycle or delete, got %q", *flagPhase))
	}
	if err := r.save(); err != nil {
		fatal(err)
	}
	fmt.Printf("results: %s\n", r.outDir)
	if ctx.Err() != nil {
		fmt.Fprintln(os.Stderr, "interrupted; results are partial")
		os.Exit(130)
	}
	if n := failedCalls(r.calls); n > 0 {
		fmt.Fprintf(os.Stderr, "%d calls failed; see %s\n", n, filepath.Join(r.outDir, "calls.csv"))
		os.Exit(1)
	}
}

// failedCalls counts the calls that did not succeed.
func failedCalls(calls []callRecord) int {
	n := 0
	for _, c := range calls {
		if !c.OK {
			n++
		}
	}
	return n
}

// actorNames returns n names prefix-NNNN, starting at start.
func actorNames(prefix string, start, n int) []string {
	names := make([]string, 0, n)
	for i := start; i < start+n; i++ {
		names = append(names, fmt.Sprintf("%s-%04d", prefix, i))
	}
	return names
}

func (r *runner) ref(name string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Atespace: *flagAtespace, Name: name}
}
