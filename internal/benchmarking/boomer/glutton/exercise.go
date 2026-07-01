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
	"fmt"
	"net/http"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel"
)

// ExerciseBenchmark runs one full boomer VU lifecycle against a live
// cluster. It returns an error if there are any problems running the
// benchmark.
//
// interCallDelay is the pause inserted between successive RPC calls in
// each iteration (Resume→Ping and Ping→Suspend). Pass zero to disable;
// pass a small delay (e.g. 200ms) on slow CI runners where the atenet
// router needs time to pick up a Resume before the Ping is routable.
func ExerciseBenchmark(ctx context.Context, apiStub ateapipb.ControlClient, httpClient *http.Client, routerURL string, interCallDelay time.Duration) error {
	cfg := &Config{
		APIStub:        apiStub,
		HTTPClient:     httpClient,
		RouterURL:      routerURL,
		Atespace:       "benchmark",
		Dyn:            dynconfig.NewHolder(dynconfig.Config{}),
		Tracer:         otel.Tracer("substrate-boomer/glutton"),
		InterCallDelay: interCallDelay,
	}
	u := &gluttonUser{
		cfg:         cfg,
		actorID:     "contract",
		firstResume: true,
	}
	u.hostHeader = u.actorID + "." + cfg.Atespace + "." + actorDomain

	if err := u.ensureAtespace(ctx); err != nil {
		return fmt.Errorf("EnsureAtespace: %w", err)
	}
	if err := u.create(ctx); err != nil {
		return fmt.Errorf("CreateActor: %w", err)
	}
	for _, phase := range []string{"cold", "warm"} {
		if err := u.runIteration(ctx); err != nil {
			return fmt.Errorf("%s iteration: %w", phase, err)
		}
	}
	if err := u.delete(ctx); err != nil {
		return fmt.Errorf("DeleteActor: %w", err)
	}
	return nil
}
