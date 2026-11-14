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

package lifecyclehook

import (
	"strings"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "valid manifest with mutating and admission-only hooks",
			yaml: `
apiVersion: ate.dev/v1
kind: LifecycleHookConfig
metadata:
  name: default
spec:
  maxWaitDuration: 120ms
  hooks:
    - name: id-hook
      address: dns:///id-hook.ate-system.svc:8443
      allowedAnnotationPrefixes: ["id.example.com/"]
      tls:
        caCertFile: /etc/certs/id-ca.crt
        serverName: id-hook.ate-system.svc
    - name: quota-hook
      address: tcp://quota-hook.ate-system.svc:8443
      tls:
        caCertFile: /etc/certs/ca.crt
        serverName: quota-hook.ate-system.svc
        clientAuthMode: TLSOnly
`,
		},
		{
			name: "maxWaitDuration exceeds limit",
			yaml: `
maxWaitDuration: 300ms
hooks:
  - name: h1
    address: dns:///h1:8443
    tls:
      caCertFile: /etc/ca.crt
      serverName: h1
`,
			wantErr: "must be in (0s, 250ms]",
		},
		{
			name: "overlapping annotation prefixes rejected",
			yaml: `
hooks:
  - name: h1
    address: dns:///h1:8443
    allowedAnnotationPrefixes: ["platform.example.com/"]
    tls:
      caCertFile: /etc/ca.crt
      serverName: h1
  - name: h2
    address: dns:///h2:8443
    allowedAnnotationPrefixes: ["platform.example.com/sub/"]
    tls:
      caCertFile: /etc/ca.crt
      serverName: h2
`,
			wantErr: "overlaps with prefix",
		},
		{
			name: "multiple trust sources rejected",
			yaml: `
hooks:
  - name: h1
    address: dns:///h1:8443
    tls:
      caBundle: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCg==
      caCertFile: /etc/ca.crt
      serverName: h1
`,
			wantErr: "exactly one of caBundle or caCertFile",
		},
		{
			name: "missing serverName rejected",
			yaml: `
hooks:
  - name: h1
    address: dns:///h1:8443
    tls:
      caCertFile: /etc/ca.crt
`,
			wantErr: "serverName is required",
		},
		{
			name: "reserved ate.dev annotation prefix rejected",
			yaml: `
hooks:
  - name: h1
    address: dns:///h1:8443
    allowedAnnotationPrefixes: ["ate.dev/"]
    tls:
      caCertFile: /etc/ca.crt
      serverName: h1
`,
			wantErr: "reserved ate.dev domain",
		},
		{
			name: "reserved subdomain .ate.dev annotation prefix rejected",
			yaml: `
hooks:
  - name: h1
    address: dns:///h1:8443
    allowedAnnotationPrefixes: ["sub.ate.dev/"]
    tls:
      caCertFile: /etc/ca.crt
      serverName: h1
`,
			wantErr: "reserved ate.dev domain",
		},
		{
			name: "mixing spec and top-level fields rejected",
			yaml: `
maxWaitDuration: 100ms
spec:
  hooks:
    - name: h1
      address: dns:///h1:8443
      tls:
        caCertFile: /etc/ca.crt
        serverName: h1
`,
			wantErr: "spec cannot be combined with top-level",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseConfig([]byte(tc.yaml))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ParseConfig() err = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseConfig() unexpected error: %v", err)
			}
			if err := cfg.NormalizeAndValidate(); err != nil {
				t.Fatalf("second NormalizeAndValidate() unexpected error: %v", err)
			}
			if cfg.WaitDuration() != 120*time.Millisecond {
				t.Errorf("WaitDuration() = %v, want 120ms", cfg.WaitDuration())
			}
		})
	}
}
