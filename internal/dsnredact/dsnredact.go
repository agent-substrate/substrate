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

// Package dsnredact masks the password in a PostgreSQL connection string so
// the DSN can be logged. ate-setup stores the DSN in a Secret for this
// reason; every component that also prints it needs the same treatment.
package dsnredact

import "regexp"

var (
	// dsnURIPassword matches the password in a URI userinfo section.
	dsnURIPassword = regexp.MustCompile(`(://[^:/@]*):[^@]*@`)
	// dsnKeywordPassword matches a keyword/value or query parameter password.
	dsnKeywordPassword = regexp.MustCompile(`(password=)[^ &]*`)
)

// Redact masks any password in dsn. A DSN with no password is returned
// unchanged, so the in-cluster and Cloud SQL IAM forms still log as
// themselves.
func Redact(dsn string) string {
	redacted := dsnURIPassword.ReplaceAllString(dsn, "$1:***@")
	return dsnKeywordPassword.ReplaceAllString(redacted, "$1***")
}
