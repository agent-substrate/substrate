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

package pgdsn

import (
	"strings"
	"testing"
)

func TestRejectInlineSecrets(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dsn     string
		wantErr string
	}{
		{name: "empty", dsn: ""},
		{name: "uri without password", dsn: "postgresql://someone@db.example:5432/atepg?sslmode=verify-full"},
		{name: "uri with passfile", dsn: "postgres://someone@db.example/atepg?passfile=/run/pg/pgpass"},
		{name: "uri with client key file", dsn: "postgresql://someone@db/atepg?sslcert=/run/c.pem&sslkey=/run/c.pem"},
		{name: "keyword/value without password", dsn: "host=127.0.0.1 user=svc@p.iam dbname=atepg"},
		{name: "keyword/value with passfile", dsn: "host=db user=svc passfile='/run/pg pass/pgpass'"},
		{name: "quoted value mentioning password", dsn: `host=db application_name='password=x'`},
		{name: "escaped value mentioning password", dsn: `host=db application_name=a\ password=x`},
		{name: "uri userinfo password", dsn: "postgresql://someone:s3cret@db/atepg", wantErr: `"password"`},
		{name: "uri empty userinfo password", dsn: "postgresql://someone:@db/atepg", wantErr: `"password"`},
		{name: "uri password parameter", dsn: "postgresql://someone@db/atepg?password=s3cret", wantErr: `"password"`},
		{name: "uri sslpassword parameter", dsn: "postgresql://someone@db/atepg?sslkey=/k.pem&sslpassword=s3cret", wantErr: `"sslpassword"`},
		{name: "keyword/value password", dsn: "host=db user=svc password=s3cret", wantErr: `"password"`},
		{name: "keyword/value quoted password", dsn: "host=db password = 's3 cret' user=svc", wantErr: `"password"`},
		{name: "keyword/value empty password", dsn: "host=db password=", wantErr: `"password"`},
		{name: "keyword/value sslpassword", dsn: "sslkey=/k.pem sslpassword=s3cret", wantErr: `"sslpassword"`},
		{name: "unparseable keyword/value", dsn: "host=db s3cret", wantErr: "does not parse"},
		{name: "unterminated quote", dsn: "host=db password='s3cret", wantErr: "does not parse"},
		{name: "unparseable uri", dsn: "postgresql://someone:s3cret@db:port/atepg", wantErr: "does not parse"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := RejectInlineSecrets(tc.dsn)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("RejectInlineSecrets() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("RejectInlineSecrets() = %v, want an error containing %s", err, tc.wantErr)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Errorf("RejectInlineSecrets() error %q echoes the secret", err)
			}
		})
	}
}
