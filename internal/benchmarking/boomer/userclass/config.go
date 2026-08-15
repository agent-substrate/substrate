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

package userclass

import (
	"net/http"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/otel/trace"
)

// Config holds the dependencies a user class needs. Built once at startup
// and passed to the entry's Init func.
type Config struct {
	// APIStub is the shared gRPC client to ateapi (one connection, all goroutines).
	APIStub ateapipb.ControlClient
	// HTTPClient is the shared HTTP client for atenet pings.
	HTTPClient *http.Client
	// RouterURL is the base URL of the atenet router (no trailing slash).
	RouterURL string
	// Atespace every actor this worker creates lives in. Required; caller
	// is responsible for having ensured it exists (see EnsureAtespace).
	Atespace string
	// Dyn is the runtime-mutable config (wait-time bounds, trace
	// probability). Required — every per-iteration read goes through it,
	// so tests can mutate it without touching glutton internals.
	Dyn *dynconfig.Holder
	// Tracer anchors sampled spans; falls back to the otel global if nil.
	Tracer trace.Tracer
}
