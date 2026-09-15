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
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/statusz"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/principal"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	maxRecordedRPCFailures = 100
	maxRetainedTextBytes   = 256
)

var controlMethodPrefix = "/" + ateapipb.Control_ServiceDesc.ServiceName + "/"

type rpcFailureRecorder struct {
	mu       sync.RWMutex
	failures []recordedRPCFailure
	now      func() time.Time
}

type recordedRPCFailure struct {
	completedAt time.Time
	failure     statusz.RPCFailure
}

func newRPCFailureRecorder(now func() time.Time) *rpcFailureRecorder {
	return &rpcFailureRecorder{
		failures: make([]recordedRPCFailure, 0, maxRecordedRPCFailures),
		now:      now,
	}
}

func (r *rpcFailureRecorder) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		startedAt := r.now()
		resp, err := handler(ctx, req)
		completedAt := r.now()
		if err == nil || !strings.HasPrefix(info.FullMethod, controlMethodPrefix) {
			return resp, err
		}
		p, ok := principal.FromContext(ctx)
		if !ok {
			return resp, err
		}
		elapsed := completedAt.Sub(startedAt)
		if elapsed < 0 {
			elapsed = 0
		}
		r.add(completedAt, statusz.RPCFailure{
			CompletedAt:   completedAt.UTC().Format(time.RFC3339Nano),
			Method:        boundedUTF8(info.FullMethod),
			PrincipalKind: boundedUTF8(p.Kind),
			PrincipalID:   boundedUTF8(p.ID),
			Code:          status.Code(err).String(),
			Elapsed:       elapsed.String(),
		})
		return resp, err
	}
}

func (r *rpcFailureRecorder) add(completedAt time.Time, failure statusz.RPCFailure) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := recordedRPCFailure{completedAt: completedAt, failure: failure}
	position := sort.Search(len(r.failures), func(i int) bool {
		return !r.failures[i].completedAt.After(completedAt)
	})
	if len(r.failures) < maxRecordedRPCFailures {
		r.failures = append(r.failures, recordedRPCFailure{})
		copy(r.failures[position+1:], r.failures[position:])
		r.failures[position] = entry
		return
	}
	if position == len(r.failures) {
		return
	}
	copy(r.failures[position+1:], r.failures[position:len(r.failures)-1])
	r.failures[position] = entry
}

// Failures returns a detached newest-first snapshot, so rendering does not
// retain the recorder lock.
func (r *rpcFailureRecorder) Failures() []statusz.RPCFailure {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := len(r.failures)
	result := make([]statusz.RPCFailure, n)
	for i := range n {
		result[i] = r.failures[i].failure
	}
	return result
}

func boundedUTF8(value string) string {
	if len(value) > maxRetainedTextBytes {
		value = value[:maxRetainedTextBytes]
	}
	value = strings.ToValidUTF8(value, "\uFFFD")
	if len(value) > maxRetainedTextBytes {
		value = value[:maxRetainedTextBytes]
		for !utf8.ValidString(value) {
			value = value[:len(value)-1]
		}
	}
	return strings.Clone(value)
}

func unaryServerInterceptors(auth grpc.UnaryServerInterceptor, failures *rpcFailureRecorder) []grpc.UnaryServerInterceptor {
	return []grpc.UnaryServerInterceptor{
		auth,
		failures.UnaryServerInterceptor(),
		ateinterceptors.MaxDeadlineUnaryInterceptor(maxRPCDeadline),
		ateinterceptors.ServerUnaryInterceptor,
		ateinterceptors.RejectUnknownFieldsUnaryInterceptor,
	}
}
