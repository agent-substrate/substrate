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

package dsnredact

import "testing"

func TestRedact(t *testing.T) {
	for _, tc := range []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "URI userinfo password",
			dsn:  "postgresql://ate:hunter2@db.example.com:5432/atepg?sslmode=require",
			want: "postgresql://ate:***@db.example.com:5432/atepg?sslmode=require",
		},
		{
			name: "keyword/value password",
			dsn:  "user=ate password=hunter2 host=db.example.com",
			want: "user=ate password=*** host=db.example.com",
		},
		{
			name: "query parameter password",
			dsn:  "postgresql://db.example.com/atepg?password=hunter2&sslmode=require",
			want: "postgresql://db.example.com/atepg?password=***&sslmode=require",
		},
		{
			name: "passwordless DSN is unchanged",
			dsn:  "user=ate@p.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable",
			want: "user=ate@p.iam host=127.0.0.1 port=5432 dbname=atepg sslmode=disable",
		},
		{
			name: "a URI without a password keeps its userinfo intact",
			dsn:  "postgresql://ate@db.example.com:5432/atepg",
			want: "postgresql://ate@db.example.com:5432/atepg",
		},
		{
			name: "an empty DSN is unchanged",
			dsn:  "",
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Redact(tc.dsn); got != tc.want {
				t.Errorf("Redact() = %q, want %q", got, tc.want)
			}
		})
	}
}
