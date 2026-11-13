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

package router

import (
	"testing"
	"time"

	"github.com/spf13/pflag"
	"go.ate.dev/substrate/cmd/atenet/internal/router/ingress"
)

// Envoy must hold an ext_proc request at least as long as the resumer retries
// it, or Envoy fails the stream before the router can send its own 503.
func TestEnvoyExtProcTimeoutCoversResumeBudget(t *testing.T) {
	cases := []struct {
		name    string
		parkCfg ingress.ParkedRequestConfig
		want    time.Duration
	}{
		{
			name:    "parking disabled",
			parkCfg: ingress.ParkedRequestConfig{Max: 0}.Normalized(),
			want:    20 * time.Second,
		},
		{
			name:    "parking enabled",
			parkCfg: ingress.ParkedRequestConfig{Budget: 5 * time.Second, Max: 1024}.Normalized(),
			want:    10 * time.Second,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg routerConfig
			fs := pflag.NewFlagSet("router", pflag.ContinueOnError)
			bindRouterFlags(fs, &cfg)
			if err := fs.Parse([]string{"--mode=ingress", "--otlp-collector-address="}); err != nil {
				t.Fatalf("parsing flags: %v", err)
			}
			s := &RouterServer{cfg: cfg}
			x, err := s.newEnvoyXdsServer(t.Context(), tc.parkCfg, 0)
			if err != nil {
				t.Fatalf("newEnvoyXdsServer: %v", err)
			}
			if x.extProcMessageTimeout != tc.want {
				t.Errorf("extProcMessageTimeout = %v, want %v", x.extProcMessageTimeout, tc.want)
			}
		})
	}
}
