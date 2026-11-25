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

package pprofserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/spf13/pflag"
)

func TestRegisterFlags(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		args    []string
		want    Config
		wantErr string
	}{
		{
			name: "unset env leaves zero config",
			want: Config{},
		},
		{
			name: "defaults from ATE_PPROF env vars",
			env: map[string]string{
				"ATE_PPROF_LISTEN_ADDR":    ":6060",
				"ATE_PPROF_MUTEX_FRACTION": "10",
				"ATE_PPROF_BLOCK_RATE":     "1000000",
			},
			want: Config{
				ListenAddr:    ":6060",
				MutexFraction: 10,
				BlockRate:     1000000,
			},
		},
		{
			name: "explicit flags override invalid env vars",
			env: map[string]string{
				"ATE_PPROF_LISTEN_ADDR":    ":6060",
				"ATE_PPROF_MUTEX_FRACTION": "abc",
				"ATE_PPROF_BLOCK_RATE":     "0",
			},
			args: []string{
				"--pprof-listen-addr=:7070",
				"--pprof-mutex-fraction=20",
				"--pprof-block-rate=500",
			},
			want: Config{
				ListenAddr:    ":7070",
				MutexFraction: 20,
				BlockRate:     500,
			},
		},
		{
			name: "explicit zero mutex fraction disables env var",
			env: map[string]string{
				"ATE_PPROF_MUTEX_FRACTION": "10",
			},
			args: []string{
				"--pprof-mutex-fraction=0",
			},
			want: Config{
				MutexFraction: 0,
			},
		},
		{
			name: "zero mutex fraction env var fails validation",
			env: map[string]string{
				"ATE_PPROF_MUTEX_FRACTION": "0",
			},
			wantErr: "ATE_PPROF_MUTEX_FRACTION must be a positive integer",
		},
		{
			name: "negative block rate env var fails validation",
			env: map[string]string{
				"ATE_PPROF_BLOCK_RATE": "-5",
			},
			wantErr: "ATE_PPROF_BLOCK_RATE must be a positive integer",
		},
		{
			name: "non-integer mutex fraction env var fails validation",
			env: map[string]string{
				"ATE_PPROF_MUTEX_FRACTION": "abc",
			},
			wantErr: "ATE_PPROF_MUTEX_FRACTION must be a positive integer",
		},
		{
			name: "negative mutex fraction flag fails validation",
			args: []string{
				"--pprof-mutex-fraction=-1",
			},
			wantErr: "--pprof-mutex-fraction must not be negative",
		},
		{
			name: "negative block rate flag fails validation",
			args: []string{
				"--pprof-block-rate=-1",
			},
			wantErr: "--pprof-block-rate must not be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, k := range []string{"ATE_PPROF_LISTEN_ADDR", "ATE_PPROF_MUTEX_FRACTION", "ATE_PPROF_BLOCK_RATE"} {
				t.Setenv(k, tt.env[k])
			}

			pf := pflag.NewFlagSet("pflag", pflag.ContinueOnError)
			cfg := RegisterFlags(pf)
			if err := pf.Parse(tt.args); err != nil {
				t.Fatalf("Parse(%v): %v", tt.args, err)
			}
			err := cfg.Validate()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Validate() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() unexpected error: %v", err)
			}
			if diff := cmp.Diff(tt.want, *cfg, cmpopts.IgnoreUnexported(Config{})); diff != "" {
				t.Errorf("Config mismatch (-want +got):\n%s", diff)
			}
		})
	}

	t.Run("nil config validate is nil", func(t *testing.T) {
		var cfg *Config
		if err := cfg.Validate(); err != nil {
			t.Fatalf("(*Config)(nil).Validate() = %v, want nil", err)
		}
	})
}

func TestIntFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		val     string
		want    int
		wantErr bool
	}{
		{name: "empty returns zero", val: "", want: 0},
		{name: "positive integer", val: "10", want: 10},
		{name: "zero rejected", val: "0", wantErr: true},
		{name: "negative rejected", val: "-1", wantErr: true},
		{name: "non-integer rejected", val: "abc", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const key = "ATE_PPROF_TEST_INT"
			t.Setenv(key, tt.val)
			got, err := IntFromEnv(key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("IntFromEnv() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if !strings.Contains(err.Error(), key+" must be a positive integer") {
					t.Errorf("IntFromEnv() error = %q, want substring %q", err.Error(), key+" must be a positive integer")
				}
				return
			}
			if got != tt.want {
				t.Errorf("IntFromEnv() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestStartNoOp(t *testing.T) {
	prev := runtime.SetMutexProfileFraction(-1)
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(prev)
		runtime.SetBlockProfileRate(0)
	})

	Start(context.Background(), nil)
	Start(context.Background(), &Config{})
	Start(context.Background(), &Config{MutexFraction: 10, BlockRate: 1000})

	if got := runtime.SetMutexProfileFraction(-1); got != prev {
		t.Errorf("mutex profile fraction = %d, want unchanged %d", got, prev)
	}
}

func TestPprofMuxAndServer(t *testing.T) {
	srv := newHTTPServer(":0")
	if srv.ReadHeaderTimeout != httpServerReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", srv.ReadHeaderTimeout, httpServerReadHeaderTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0", srv.WriteTimeout)
	}

	if srv.Handler == nil || srv.Handler == http.DefaultServeMux {
		t.Fatalf("srv.Handler = %v, want dedicated non-nil ServeMux", srv.Handler)
	}

	for _, tc := range []struct {
		path string
		want int
	}{
		{path: "/debug/pprof/", want: http.StatusOK},
		{path: "/debug/pprof/cmdline", want: http.StatusOK},
		{path: "/debug/pprof/symbol", want: http.StatusOK},
		{path: "/metrics", want: http.StatusNotFound},
		{path: "/readyz", want: http.StatusNotFound},
	} {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rec := httptest.NewRecorder()
			srv.Handler.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("GET %s = %d, want %d", tc.path, rec.Code, tc.want)
			}
		})
	}
}
