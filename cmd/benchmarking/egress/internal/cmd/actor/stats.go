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

package actor

import "time"

// Stats is what one actor's loop measured. The counters are cumulative since
// /start. GET /stats serves them alone; POST /collect and POST /stop add the
// distributions recorded since the previous cut.
type Stats struct {
	// Scheme is the scheme the loop requested endpoints over.
	Scheme  string        `json:"scheme,omitempty"`
	Elapsed time.Duration `json:"elapsed"`
	// Requests counts every request the loop finished, failed ones included.
	Requests  int64 `json:"requests"`
	Successes int64 `json:"successes"`
	// NewConns counts requests that went out on a connection opened for them
	// rather than on a kept-alive one. A tunnel the gateway reset shows up
	// here rather than in Errors: net/http redials, and retries a GET that
	// failed before any of the response arrived.
	NewConns int64 `json:"newConns"`
	// Opened counts endpoints whose first request has completed, so a reader
	// can tell the opening from the steady state.
	Opened int64 `json:"opened"`
	// OpeningDone is Elapsed at the moment the last endpoint's first request
	// completed; zero until every endpoint has opened.
	OpeningDone time.Duration `json:"openingDone"`
	// RequestsAtOpeningDone is Requests at that same moment, so a reader can
	// price this actor's opening alone; zero until every endpoint has opened.
	RequestsAtOpeningDone int64 `json:"requestsAtOpeningDone"`
	// Rounds is the number of requests the least-visited endpoint has
	// completed: whether this actor is still making progress.
	Rounds int64 `json:"rounds"`
	// LongestGap is the longest time between the starts of two consecutive
	// requests on one endpoint: how quiet the loop has left a tunnel. Each
	// endpoint is requested once per period, C times the interval, so with
	// prompt endpoints every gap is one period; a request that outlasts the
	// period delays only that endpoint's next request, so a longer gap means a
	// request on that tunnel took about that long. Keep the period under the
	// gateway's stream idle timeout.
	LongestGap time.Duration `json:"longestGap"`
	// Errors counts failed requests by class, as the actor names them: one
	// low-cardinality class per failure mode.
	Errors map[string]int64 `json:"errors,omitempty"`
	// Endpoints holds per-endpoint counts, indexed by endpoint number. It is
	// served only when asked for with ?endpoints=1.
	Endpoints []Endpoint `json:"endpoints,omitempty"`
	// Distributions are the samples recorded since the previous cut, keyed
	// by DistributionNames. A distribution with no sample in the window is
	// absent. GET /stats leaves them out.
	Distributions map[Distribution]Point `json:"distributions,omitempty"`
}

// Endpoint holds the counts for one endpoint.
type Endpoint struct {
	Requests int64 `json:"requests"`
	Errors   int64 `json:"errors"`
	// LongestGap is Stats.LongestGap for this endpoint alone, so a quiet
	// tunnel can be named.
	LongestGap time.Duration `json:"longestGap"`
}
