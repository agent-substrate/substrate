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

package serverboot

import (
	"context"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const testDefaultRatio = 0.25

func TestResolveTraceSampling(t *testing.T) {
	def := ParentRatioSampling(testDefaultRatio)

	tests := []struct {
		name        string
		sampler     string
		samplerSet  bool
		arg         string
		argSet      bool
		def         TraceSampling
		want        sdktrace.Sampler
		wantPercent float64
		wantErr     bool
	}{
		{
			name:        "unset keeps ratio default",
			def:         def,
			want:        def.Sampler(),
			wantPercent: testDefaultRatio * 100,
		},
		{
			name:        "unset keeps never default",
			def:         ParentNeverSampling(),
			want:        ParentNeverSampling().Sampler(),
			wantPercent: 0,
		},
		{
			name:        "set but empty keeps default",
			samplerSet:  true,
			def:         def,
			want:        def.Sampler(),
			wantPercent: testDefaultRatio * 100,
		},
		{
			name:        "always_on",
			sampler:     "always_on",
			samplerSet:  true,
			def:         def,
			want:        sdktrace.AlwaysSample(),
			wantPercent: 100,
		},
		{
			name:        "always_off",
			sampler:     "always_off",
			samplerSet:  true,
			def:         def,
			want:        sdktrace.NeverSample(),
			wantPercent: 0,
		},
		{
			name:        "traceidratio with arg",
			sampler:     "traceidratio",
			samplerSet:  true,
			arg:         "0.5",
			argSet:      true,
			def:         def,
			want:        sdktrace.TraceIDRatioBased(0.5),
			wantPercent: 50,
		},
		{
			name:        "parentbased_always_on",
			sampler:     "parentbased_always_on",
			samplerSet:  true,
			def:         def,
			want:        sdktrace.ParentBased(sdktrace.AlwaysSample()),
			wantPercent: 100,
		},
		{
			name:        "parentbased_always_off",
			sampler:     "parentbased_always_off",
			samplerSet:  true,
			def:         def,
			want:        sdktrace.ParentBased(sdktrace.NeverSample()),
			wantPercent: 0,
		},
		{
			name:        "parentbased_traceidratio with arg",
			sampler:     "parentbased_traceidratio",
			samplerSet:  true,
			arg:         "0.5",
			argSet:      true,
			def:         def,
			want:        sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.5)),
			wantPercent: 50,
		},
		{
			name:        "mixed case and whitespace normalize",
			sampler:     "  ParentBased_TraceIDRatio  ",
			samplerSet:  true,
			arg:         " 0.5 ",
			argSet:      true,
			def:         def,
			want:        sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.5)),
			wantPercent: 50,
		},
		{
			name:        "ratio without arg keeps default",
			sampler:     "parentbased_traceidratio",
			samplerSet:  true,
			def:         def,
			want:        def.Sampler(),
			wantPercent: testDefaultRatio * 100,
			wantErr:     true,
		},
		{
			name:        "ratio with empty arg keeps default",
			sampler:     "traceidratio",
			samplerSet:  true,
			arg:         "  ",
			argSet:      true,
			def:         def,
			want:        def.Sampler(),
			wantPercent: testDefaultRatio * 100,
			wantErr:     true,
		},
		{
			name:        "unknown sampler keeps default",
			sampler:     "jaeger_remote",
			samplerSet:  true,
			def:         def,
			want:        def.Sampler(),
			wantPercent: testDefaultRatio * 100,
			wantErr:     true,
		},
		{
			name:        "unparsable arg keeps default",
			sampler:     "parentbased_traceidratio",
			samplerSet:  true,
			arg:         "abc",
			argSet:      true,
			def:         def,
			want:        def.Sampler(),
			wantPercent: testDefaultRatio * 100,
			wantErr:     true,
		},
		{
			name:        "arg above 1 keeps default",
			sampler:     "parentbased_traceidratio",
			samplerSet:  true,
			arg:         "1.5",
			argSet:      true,
			def:         def,
			want:        def.Sampler(),
			wantPercent: testDefaultRatio * 100,
			wantErr:     true,
		},
		{
			name:        "negative arg keeps default",
			sampler:     "traceidratio",
			samplerSet:  true,
			arg:         "-0.1",
			argSet:      true,
			def:         def,
			want:        def.Sampler(),
			wantPercent: testDefaultRatio * 100,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveTraceSampling(tt.sampler, tt.samplerSet, tt.arg, tt.argSet, tt.def)
			if (err != nil) != tt.wantErr {
				t.Fatalf("resolveTraceSampling() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got.Sampler().Description() != tt.want.Description() {
				t.Errorf("sampler = %s, want %s", got.Sampler().Description(), tt.want.Description())
			}
			if got.RootSamplingPercent() != tt.wantPercent {
				t.Errorf("RootSamplingPercent() = %v, want %v", got.RootSamplingPercent(), tt.wantPercent)
			}
		})
	}
}

func TestResolveTraceSamplingReadsEnv(t *testing.T) {
	def := ParentNeverSampling()

	t.Setenv("OTEL_TRACES_SAMPLER", "parentbased_traceidratio")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.5")
	got := ResolveTraceSampling(context.Background(), def)
	want := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.5))
	if got.Sampler().Description() != want.Description() {
		t.Errorf("sampler = %s, want %s", got.Sampler().Description(), want.Description())
	}

	t.Setenv("OTEL_TRACES_SAMPLER", "not_a_sampler")
	got = ResolveTraceSampling(context.Background(), def)
	if got.Sampler().Description() != def.Sampler().Description() {
		t.Errorf("sampler = %s, want the default %s", got.Sampler().Description(), def.Sampler().Description())
	}
}

func TestResolveTraceSamplingZeroValueDefault(t *testing.T) {
	t.Setenv("OTEL_TRACES_SAMPLER", "not_a_sampler")
	got := ResolveTraceSampling(context.Background(), TraceSampling{})
	if got.Sampler() != nil {
		t.Errorf("Sampler() = %v, want nil from a zero-value default", got.Sampler())
	}
	if got.RootSamplingPercent() != 0 {
		t.Errorf("RootSamplingPercent() = %v, want 0", got.RootSamplingPercent())
	}
}

func TestParentSamplingConstructors(t *testing.T) {
	if got := ParentRatioSampling(0.01).RootSamplingPercent(); got != 1 {
		t.Errorf("ParentRatioSampling(0.01).RootSamplingPercent() = %v, want 1", got)
	}
	if got := ParentNeverSampling().RootSamplingPercent(); got != 0 {
		t.Errorf("ParentNeverSampling().RootSamplingPercent() = %v, want 0", got)
	}
	if got := ParentRatioSampling(1.5).RootSamplingPercent(); got != 100 {
		t.Errorf("ParentRatioSampling(1.5).RootSamplingPercent() = %v, want 100 (clamped)", got)
	}
	if got := ParentRatioSampling(-1).RootSamplingPercent(); got != 0 {
		t.Errorf("ParentRatioSampling(-1).RootSamplingPercent() = %v, want 0 (clamped)", got)
	}
}

func TestInitTracingRequiresSampling(t *testing.T) {
	if _, err := InitTracing(context.Background(), TracingOptions{ServiceName: "x"}); err == nil {
		t.Error("InitTracing() with zero Sampling: expected error, got nil")
	}
}
