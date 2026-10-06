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

package steps

import (
	"os"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

func TestParseDSNTarget(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		want    dsnTarget
		wantErr string
	}{
		{
			name: "uri",
			dsn:  "postgres://ate_api@db.example.com:5433/ate?sslmode=verify-full",
			want: dsnTarget{Host: "db.example.com", Port: "5433", User: "ate_api"},
		},
		{
			name: "keyword defaults port and database",
			dsn:  "user=ate_api host=db.example.com sslmode=require",
			want: dsnTarget{Host: "db.example.com", Port: "5432", User: "ate_api"},
		},
		{
			name:    "uri password",
			dsn:     "postgres://u:secret@h/d?sslmode=require",
			wantErr: "password",
		},
		{
			name:    "keyword password",
			dsn:     "user=u host=h password=x sslmode=require",
			wantErr: "password",
		},
		{
			name:    "no TLS",
			dsn:     "user=u host=h sslmode=disable",
			wantErr: "TLS",
		},
		{
			name:    "sslmode unset defaults to prefer",
			dsn:     "user=u host=h",
			wantErr: "TLS",
		},
		{
			name:    "multiple hosts",
			dsn:     "user=u host=a,b sslmode=require",
			wantErr: "one host",
		},
		{
			name:    "no user",
			dsn:     "host=h sslmode=require",
			wantErr: "user",
		},
		{
			name:    "quoted keyword value",
			dsn:     "user='u' host=h sslmode=require",
			wantErr: "quoted",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDSNTarget(tt.dsn)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseDSNTarget() error = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseDSNTarget() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("parseDSNTarget() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestWithPassfile(t *testing.T) {
	tests := map[string]string{
		"postgres://u@h/d?sslmode=require": "postgres://u@h/d?sslmode=require&passfile=" + rdsIAMPassfile,
		"postgres://u@h/d":                 "postgres://u@h/d?passfile=" + rdsIAMPassfile,
		"user=u host=h":                    "user=u host=h passfile=" + rdsIAMPassfile,
		"user=u host=h passfile=/x":        "user=u host=h passfile=/x",
	}
	for in, want := range tests {
		if got := withPassfile(in); got != want {
			t.Errorf("withPassfile(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestWithAWSIAM(t *testing.T) {
	rw := "postgres://ate_api@mydb.example.com/atepg?sslmode=verify-full"

	gotRW, gotOwner, vars, err := withAWSIAM(rw, rw)
	if err != nil {
		t.Fatalf("withAWSIAM() error = %v", err)
	}
	for _, dsn := range []string{gotRW, gotOwner} {
		if !strings.HasSuffix(dsn, "&passfile="+rdsIAMPassfile) {
			t.Errorf("DSN %q does not point at the passfile", dsn)
		}
	}
	want := map[string]string{
		envAWSIAMAuth: "true",
		envRDSIAMHost: "mydb.example.com",
		envRDSIAMPort: "5432",
		envRDSIAMUser: "ate_api",
	}
	if len(vars) != len(want) {
		t.Fatalf("withAWSIAM() vars = %v, want %v", vars, want)
	}
	for k, v := range want {
		if vars[k] != v {
			t.Errorf("withAWSIAM() vars[%s] = %q, want %q", k, vars[k], v)
		}
	}

	otherUser := "postgres://ate_owner@mydb.example.com/atepg?sslmode=verify-full"
	if _, _, _, err := withAWSIAM(rw, otherUser); err == nil {
		t.Error("withAWSIAM() with different users error = nil, want an error: one token serves both pools")
	}
	if _, _, _, err := withAWSIAM("user=u host=h sslmode=disable", "user=u host=h sslmode=disable"); err == nil {
		t.Error("withAWSIAM() without TLS error = nil, want an error")
	}
}

func TestAWSIAMEnabled(t *testing.T) {
	recorded := apiServerEnvVarsConfigMap(map[string]string{envAWSIAMAuth: "true"})
	tests := []struct {
		name string
		auth string
		kube bool
		want bool
	}{
		{name: "explicit true", auth: "true", want: true},
		{name: "explicit false beats the record", auth: "false", kube: true, want: false},
		{name: "unset adopts the record", kube: true, want: true},
		{name: "unset and nothing recorded", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Env{Cfg: &config.Config{AWSIAM: config.AWSIAMConfig{Auth: tt.auth}}}
			if tt.kube {
				e.Kube = fakeKube(t, recorded)
			} else {
				e.Kube = fakeKube(t)
			}
			got, err := e.awsIAMEnabled(t.Context())
			if err != nil {
				t.Fatalf("awsIAMEnabled() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("awsIAMEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRDSIAMPatchNamesTheContainer(t *testing.T) {
	cfg := &config.Config{Root: repoRoot(t)}
	patch, err := os.ReadFile(cfg.Manifest("rds", "token-refresher-patch.yaml"))
	if err != nil {
		t.Fatalf("reading the token-refresher patch: %v", err)
	}
	var dep appsv1.Deployment
	if err := yaml.Unmarshal(patch, &dep); err != nil {
		t.Fatalf("parsing the token-refresher patch: %v", err)
	}
	found := false
	for _, c := range dep.Spec.Template.Spec.InitContainers {
		if c.Name == rdsIAMContainer {
			found = true
			if c.RestartPolicy == nil || *c.RestartPolicy != corev1.ContainerRestartPolicyAlways {
				t.Errorf("%s restartPolicy = %v, want Always so it runs as a native sidecar", c.Name, c.RestartPolicy)
			}
		}
	}
	if !found {
		t.Errorf("the patch declares no %q initContainer; reconcileRDSIAMSidecar keys its removal branch on that name", rdsIAMContainer)
	}
}
