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

// Package trace wires the OTLP tracer + W3C propagator used by the boomer
// load-test workers. Same shape as internal/serverboot.InitTracing but
// without the metrics/HTTP surface that workers don't need.
package trace

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// UpdatableSampler wraps a TraceIDRatioBased sampler whose probability can
// be swapped at runtime (mirrors common/trace.py:UpdatableSampler on the
// Python side). The OTel TracerProvider only accepts one Sampler at
// construction, so to change probability later you have to hand it a
// stable wrapper that internally swaps.
type UpdatableSampler struct {
	inner atomic.Pointer[sdktrace.Sampler]
}

func NewUpdatableSampler(initialProbability float64) *UpdatableSampler {
	s := &UpdatableSampler{}
	s.UpdateProbability(initialProbability)
	return s
}

func (u *UpdatableSampler) UpdateProbability(p float64) {
	s := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(p))
	u.inner.Store(&s)
}

func (u *UpdatableSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	return (*u.inner.Load()).ShouldSample(p)
}

func (u *UpdatableSampler) Description() string {
	return "UpdatableSampler"
}

// Init registers a global TracerProvider with the given sampler and the
// W3C TraceContext propagator. The OTLP gRPC exporter honors
// OTEL_EXPORTER_OTLP_ENDPOINT.
func Init(ctx context.Context, serviceName string, sampler sdktrace.Sampler) (*sdktrace.TracerProvider, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceInstanceID(uuid.NewString()),
		),
		resource.WithFromEnv(),
	)
	// Partial-resource warnings come back as errors; tolerate them the way
	// serverboot.InitTracing does so a missing OTEL_* env var doesn't fail
	// the whole worker.
	if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
		slog.WarnContext(ctx, "partial telemetry resource", slog.Any("err", err))
	} else if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("create OTLP exporter: %w", err)
	}

	// Route OTel SDK errors (export failures, queue drops) into slog so they
	// land in boomer-glutton's stdout — runner.py pumps that into logs.txt.
	// Without this they go to the SDK's default handler (stderr via log.Println)
	// and can be lost depending on stream wiring.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Error("otel error", slog.String("err", err.Error()))
	}))

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp, nil
}
