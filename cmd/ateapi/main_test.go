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

package main

import (
	"context"
	"strings"
	"testing"
)

func TestConnectStoreRequiresPostgresConnectionString(t *testing.T) {
	oldDSN := *postgresConnectionString
	t.Cleanup(func() {
		*postgresConnectionString = oldDSN
	})
	*postgresConnectionString = ""

	_, err := connectStore(context.Background())
	if err == nil || !strings.Contains(err.Error(), "--postgres-connection-string is required") {
		t.Fatalf("connectStore() error = %v, want missing-connection-string error", err)
	}
}

func TestResolveActorJWTIssuer(t *testing.T) {
	tests := []struct {
		name      string
		flagValue string
		namespace string
		want      string
		wantErr   bool
	}{
		{name: "unset uses the namespace's idp Service", namespace: "ate-system", want: "https://idp.ate-system.svc"},
		{name: "unset in a relocated install", namespace: "team-a", want: "https://idp.team-a.svc"},
		{name: "set is used as given", flagValue: "https://idp.example.com/prod/", namespace: "ate-system", want: "https://idp.example.com/prod/"},
		{name: "set but invalid", flagValue: "http://idp.example.com", namespace: "ate-system", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveActorJWTIssuer(tt.flagValue, tt.namespace)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveActorJWTIssuer(%q, %q) = %q, want error", tt.flagValue, tt.namespace, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveActorJWTIssuer(%q, %q) returned error: %v", tt.flagValue, tt.namespace, err)
			}
			if got != tt.want {
				t.Errorf("resolveActorJWTIssuer(%q, %q) = %q, want %q", tt.flagValue, tt.namespace, got, tt.want)
			}
		})
	}
}
