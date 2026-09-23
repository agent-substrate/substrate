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

package logredact

import (
	"io"
	"log/slog"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func discardHandler() slog.Handler {
	return slog.NewJSONHandler(io.Discard, nil)
}

func runRequest(withEnv bool) *ateletpb.RunRequest {
	container := &ateletpb.Container{Name: "main"}
	if withEnv {
		container.Env = []*ateletpb.EnvEntry{{Name: "API_KEY", Value: "sk-secret"}}
	}
	return &ateletpb.RunRequest{Spec: &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{container}}}
}

// BenchmarkJSONHandler is the floor: the JSON handler on its own.
func BenchmarkJSONHandler(b *testing.B) {
	logger := slog.New(discardHandler())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Info("Handle RPC", slog.String("method", "/ateapi.Control/GetActor"), slog.Int("code", 0))
	}
}

// BenchmarkHandlerPrimitiveAttrs is the cost of the redaction layer on a record
// that carries no protobuf at all.
func BenchmarkHandlerPrimitiveAttrs(b *testing.B) {
	logger := slog.New(NewHandler(discardHandler()))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Info("Handle RPC", slog.String("method", "/ateapi.Control/GetActor"), slog.Int("code", 0))
	}
}

// BenchmarkHandlerProtoWithoutRedactedFields covers the per-type cache: the
// message graph has no debug_redact field, so the value is forwarded as is.
func BenchmarkHandlerProtoWithoutRedactedFields(b *testing.B) {
	logger := slog.New(NewHandler(discardHandler()))
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "a", Name: "actor-1"}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Info("Handle RPC", slog.Any("resp", actor))
	}
}

// BenchmarkHandlerProtoRedactedFieldUnset logs a message whose type can reach a
// redacted field, but carries no populated one: the walk finds nothing and the
// original value is forwarded.
func BenchmarkHandlerProtoRedactedFieldUnset(b *testing.B) {
	logger := slog.New(NewHandler(discardHandler()))
	req := runRequest(false)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Info("Handle RPC", slog.Any("req", req))
	}
}

// BenchmarkJSONHandlerProto is the floor for a record that carries a protobuf
// message, with no redaction at all.
func BenchmarkJSONHandlerProto(b *testing.B) {
	logger := slog.New(discardHandler())
	req := runRequest(true)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Info("Handle RPC", slog.Any("req", req))
	}
}

// BenchmarkHandlerProtoRedactedFieldSet is the worst case: a populated redacted
// field means a clone plus a clearing walk.
func BenchmarkHandlerProtoRedactedFieldSet(b *testing.B) {
	logger := slog.New(NewHandler(discardHandler()))
	req := runRequest(true)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Info("Handle RPC", slog.Any("req", req))
	}
}

// BenchmarkInterceptorStyleRedaction is the pre-handler shape: the call site
// sanitizes, and the handler does nothing.
func BenchmarkInterceptorStyleRedaction(b *testing.B) {
	logger := slog.New(discardHandler())
	req := runRequest(true)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		logger.Info("Handle RPC", slog.Any("req", Sanitize(req)))
	}
}
