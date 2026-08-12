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

package extproc

import (
	"strings"
	"sync"
	"time"
)

type RecordedQuery struct {
	Timestamp time.Time     `json:"timestamp"`
	Client    string        `json:"client"`
	Host      string        `json:"host"`
	Path      string        `json:"path"`
	Method    string        `json:"method"`
	Action    string        `json:"action"`
	Target    string        `json:"target"`
	Duration  time.Duration `json:"duration"`
}

// QueryRecorder is the fixed-size ring of recently processed requests behind
// the router's /statusz page.
type QueryRecorder struct {
	mu      sync.RWMutex
	queries []RecordedQuery
	size    int
	index   int
}

func NewQueryRecorder(size int) *QueryRecorder {
	return &QueryRecorder{
		queries: make([]RecordedQuery, 0, size),
		size:    size,
	}
}

func (qr *QueryRecorder) Add(q RecordedQuery) {
	if qr == nil {
		return
	}

	qr.mu.Lock()
	defer qr.mu.Unlock()

	if len(qr.queries) < qr.size {
		qr.queries = append(qr.queries, q)
	} else {
		qr.queries[qr.index] = q
		qr.index = (qr.index + 1) % qr.size
	}
}

func (qr *QueryRecorder) Get() []RecordedQuery {
	if qr == nil {
		return nil
	}

	qr.mu.RLock()
	defer qr.mu.RUnlock()

	n := len(qr.queries)
	if n == 0 {
		return nil
	}

	res := make([]RecordedQuery, n)
	if n < qr.size {
		for i := 0; i < n; i++ {
			res[i] = qr.queries[n-1-i]
		}
	} else {
		for i := 0; i < n; i++ {
			pos := (qr.index - 1 - i + n) % n
			res[i] = qr.queries[pos]
		}
	}

	return res
}

// redactPath drops the query string, which may carry credentials (CWE-598).
func redactPath(path string) string {
	p, _, _ := strings.Cut(path, "?")
	return p
}

func (qr *QueryRecorder) AddRouterRequest(
	start time.Time,
	duration time.Duration,
	action,
	target string,
	m *RequestMetadata,
) {
	qr.Add(RecordedQuery{
		Timestamp: start,
		Client:    m.Headers[AuthorityHeader],
		Host:      m.Host,
		Path:      redactPath(m.Path),
		Method:    m.Headers[":method"],
		Action:    action,
		Target:    target,
		Duration:  duration,
	})
}
