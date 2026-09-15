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

package printer

import (
	"bytes"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPrintEgressPolicyTo(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	policy := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   "team-a",
			Name:       "default",
			Uid:        "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
			Version:    2,
			CreateTime: timestamppb.New(now.Add(-5 * time.Minute)),
		},
		Rules: []*ateapipb.EgressRule{
			{Hostnames: &ateapipb.HostnameRule{Patterns: []string{"api.example.com"}}},
			{Cidrs: &ateapipb.CIDRRule{Cidrs: []string{"10.64.0.0/16"}}},
		},
	}
	empty := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "default", Version: 1, CreateTime: timestamppb.New(now.Add(-30 * time.Second))},
	}

	tests := []struct {
		name    string
		format  string
		actor   string
		policy  *ateapipb.EgressPolicy
		want    string
		wantErr bool
	}{
		{
			name:   "yaml bare document",
			format: "yaml",
			actor:  "c1",
			policy: policy,
			want: `metadata:
  atespace: team-a
  createTime: "2026-01-01T11:55:00Z"
  name: default
  uid: 3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d
  version: "2"
rules:
- hostnames:
    patterns:
    - api.example.com
- cidrs:
    cidrs:
    - 10.64.0.0/16
`,
		},
		{
			name:   "json bare document",
			format: "json",
			actor:  "c1",
			policy: policy,
			want: `{
  "metadata": {
    "atespace": "team-a",
    "createTime": "2026-01-01T11:55:00Z",
    "name": "default",
    "uid": "3f2b1c0e-8d5a-4b6e-9c1d-2a7e4f6b8c0d",
    "version": "2"
  },
  "rules": [
    {
      "hostnames": {
        "patterns": [
          "api.example.com"
        ]
      }
    },
    {
      "cidrs": {
        "cidrs": [
          "10.64.0.0/16"
        ]
      }
    }
  ]
}
`,
		},
		{
			name:   "table one row",
			format: "table",
			actor:  "c1",
			policy: policy,
			want: `ATESPACE   ACTOR   RULES   VERSION   AGE
team-a     c1      2       2         5m
`,
		},
		{
			name:   "table no rules",
			format: "table",
			actor:  "c2",
			policy: empty,
			want: `ATESPACE   ACTOR   RULES   VERSION   AGE
team-a     c2      0       1         30s
`,
		},
		{name: "xml rejected", format: "xml", actor: "c1", policy: policy, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := PrintEgressPolicyTo(&buf, test.actor, test.policy, test.format)
			if (err != nil) != test.wantErr {
				t.Fatalf("PrintEgressPolicyTo(%q) error = %v, wantErr %t", test.format, err, test.wantErr)
			}
			if diff := cmp.Diff(test.want, buf.String()); diff != "" {
				t.Errorf("output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
