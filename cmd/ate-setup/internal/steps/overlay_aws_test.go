// Copyright 2026 Ant Weiss
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
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

// SystemOverlay now has to pick one of six overlays, keyed on two orthogonal
// switches (kind/AWS/neither × envoy/agentgateway) that config validation keeps
// mutually exclusive. The table pins each cell, so later additions can't
// silently collapse one case into another: if --aws stops picking the aws
// overlay the install points atelet at GCS without saying so.
func TestSystemOverlayAWSSelection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cfg    config.Config
		want   string
	}{
		{
			name: "aws + envoy",
			cfg:  config.Config{AWS: true, Router: config.RouterEnvoy},
			want: installDir + "/aws",
		},
		{
			name: "aws + agentgateway",
			cfg:  config.Config{AWS: true, Router: config.RouterAgentgateway},
			want: installDir + "/aws-agentgateway",
		},
		{
			name: "kind + envoy (unaffected by aws plumbing)",
			cfg:  config.Config{Kind: true, Router: config.RouterEnvoy},
			want: installDir + "/kind",
		},
		{
			name: "kind + agentgateway (unaffected by aws plumbing)",
			cfg:  config.Config{Kind: true, Router: config.RouterAgentgateway},
			want: installDir + "/kind-agentgateway",
		},
		{
			name: "base (neither flag) + envoy",
			cfg:  config.Config{Router: config.RouterEnvoy},
			want: installDir + "/base",
		},
		{
			name: "base (neither flag) + agentgateway",
			cfg:  config.Config{Router: config.RouterAgentgateway},
			want: installDir + "/agentgateway",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SystemOverlay(&tc.cfg); got != tc.want {
				t.Errorf("SystemOverlay(%+v) = %q, want %q", tc.cfg, got, tc.want)
			}
		})
	}
}

// --aws and --kind pick two overlays that cannot coexist (kind points atelet
// at in-cluster rustfs with static creds, aws points it at S3 with IRSA), so
// config.Load must reject the combination. Both profiles reaching
// SystemOverlay at once would mean one silently wins and the install drifts
// from what the operator asked for.
func TestConfigRejectsAWSAndKindTogether(t *testing.T) {
	t.Setenv("ATE_INSTALL_KIND", "true")
	t.Setenv("ATE_INSTALL_AWS", "true")
	// Isolate from any ambient dev-env the test host may have exported.
	t.Setenv("NO_DEV_ENV", "1")

	_, err := config.Load(config.Options{})
	if err == nil {
		t.Fatal("config.Load() accepted --kind and --aws together")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error is missing the mutual-exclusion phrasing: %v", err)
	}
}

// SubstituteVersion is the one place the AWS overlay's ${AWS_REGION} +
// ${ATE_*_ROLE_ARN} placeholders are expanded. On an AWS install they must be
// gone from the rendered bytes; on a non-AWS install the same call must leave
// a manifest without them untouched.
func TestSubstituteVersionExpandsAWSPlaceholders(t *testing.T) {
	in := []byte("metadata:\n" +
		"  annotations:\n" +
		"    eks.amazonaws.com/role-arn: ${ATE_API_SERVER_ROLE_ARN}\n" +
		"env:\n" +
		"- name: AWS_REGION\n" +
		"  value: ${AWS_REGION}\n" +
		"- name: ATELET_ROLE_ARN\n" +
		"  value: ${ATELET_ROLE_ARN}\n")

	e := &Env{
		Cfg: &config.Config{
			AWS:                 true,
			AWSRegion:           "us-west-2",
			AteAPIServerRoleARN: "arn:aws:iam::111:role/api",
			AteletRoleARN:       "arn:aws:iam::111:role/atelet",
		},
		substrateVersion:       "v1.2.3",
		substrateVersionSuffix: "v1-2-3",
	}
	got, err := e.SubstituteVersion(in)
	if err != nil {
		t.Fatalf("SubstituteVersion: %v", err)
	}
	for _, want := range []string{
		"us-west-2",
		"arn:aws:iam::111:role/api",
		"arn:aws:iam::111:role/atelet",
	} {
		if !strings.Contains(string(got), want) {
			t.Errorf("rendered manifest is missing %q:\n%s", want, got)
		}
	}
	for _, leftover := range []string{
		"${AWS_REGION}",
		"${ATE_API_SERVER_ROLE_ARN}",
		"${ATELET_ROLE_ARN}",
	} {
		if strings.Contains(string(got), leftover) {
			t.Errorf("rendered manifest still carries placeholder %q:\n%s", leftover, got)
		}
	}
}
