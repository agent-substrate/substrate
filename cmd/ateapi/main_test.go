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
	"os"
	"strings"
	"testing"
)

func TestLoadFlagsFromEnv(t *testing.T) {
	oldDSN, oldSchema := *postgresConnectionString, *postgresSchema
	t.Cleanup(func() {
		*postgresConnectionString, *postgresSchema = oldDSN, oldSchema
	})

	for _, tc := range []struct {
		name, flag, value, want string
		unset                   bool
	}{
		{name: "sentinel", flag: "@env", value: "from-env", want: "from-env"},
		{name: "explicit flag", flag: "explicit", value: "from-env", want: "explicit"},
		{name: "empty flag", value: "from-env"},
		{name: "empty environment", flag: "@env"},
		{name: "unset environment", flag: "@env", unset: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, name := range []string{"ATE_API_POSTGRES_CONNECTION_STRING", "ATE_API_POSTGRES_SCHEMA"} {
				t.Setenv(name, tc.value)
				if tc.unset {
					if err := os.Unsetenv(name); err != nil {
						t.Fatal(err)
					}
				}
			}
			*postgresConnectionString, *postgresSchema = tc.flag, tc.flag
			loadFlagsFromEnv()
			if *postgresConnectionString != tc.want || *postgresSchema != tc.want {
				t.Fatalf("resolved flags = (%q, %q), want (%q, %q)", *postgresConnectionString, *postgresSchema, tc.want, tc.want)
			}
		})
	}
}

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
