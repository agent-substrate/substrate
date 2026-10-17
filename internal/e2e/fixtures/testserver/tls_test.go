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
	"net/http"
	"testing"
)

func TestTLSConfigurationRejectsIncompleteOrUnreadableCredentials(t *testing.T) {
	for name, paths := range map[string][2]string{
		"missing key":     {"cert.pem", ""},
		"missing cert":    {"", "key.pem"},
		"unreadable pair": {"missing-cert.pem", "missing-key.pem"},
	} {
		t.Run(name, func(t *testing.T) {
			server := &http.Server{}
			if err := configureTLSServer(server, paths[0], paths[1]); err == nil {
				t.Fatal("configureTLSServer succeeded, want error")
			}
		})
	}
}
