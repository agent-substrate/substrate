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

package cmd

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestParseAtespacedName(t *testing.T) {
	tests := []struct {
		name            string
		value           string
		defaultAtespace string
		want            *ateapipb.ObjectRef
		wantErr         bool
	}{
		{
			name:            "qualified reference",
			value:           "team-a/before-upgrade",
			defaultAtespace: "default-atespace",
			want:            &ateapipb.ObjectRef{Atespace: "team-a", Name: "before-upgrade"},
		},
		{
			name:            "bare name uses the default atespace",
			value:           "before-upgrade",
			defaultAtespace: "default-atespace",
			want:            &ateapipb.ObjectRef{Atespace: "default-atespace", Name: "before-upgrade"},
		},
		{
			name:            "bare name with no default atespace",
			value:           "before-upgrade",
			defaultAtespace: "",
			wantErr:         true,
		},
		{
			name:            "name contains an extra slash",
			value:           "team-a/before/upgrade",
			defaultAtespace: "default-atespace",
			wantErr:         true,
		},
		{
			name:            "empty atespace before the slash",
			value:           "/before-upgrade",
			defaultAtespace: "default-atespace",
			wantErr:         true,
		},
		{
			name:            "empty name after the slash",
			value:           "team-a/",
			defaultAtespace: "default-atespace",
			wantErr:         true,
		},
		{
			name:            "invalid atespace characters",
			value:           "Team_A/before-upgrade",
			defaultAtespace: "default-atespace",
			wantErr:         true,
		},
		{
			name:            "invalid name characters",
			value:           "team-a/Before_Upgrade",
			defaultAtespace: "default-atespace",
			wantErr:         true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseAtespacedName(test.value, test.defaultAtespace)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseAtespacedName(%q, %q) error = %v, wantErr %t", test.value, test.defaultAtespace, err, test.wantErr)
			}
			if test.wantErr {
				return
			}
			if diff := cmp.Diff(test.want, got, protocmp.Transform()); diff != "" {
				t.Errorf("parseAtespacedName(%q, %q) mismatch (-want +got):\n%s", test.value, test.defaultAtespace, diff)
			}
		})
	}
}
