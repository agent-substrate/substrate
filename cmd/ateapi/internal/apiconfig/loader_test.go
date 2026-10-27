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

package apiconfig

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigValidatesLogLevel(t *testing.T) {
	testCases := []struct {
		name    string
		content string
		wantErr bool
	}{
		{name: "debug", content: `logging { level: "debug" }`},
		{name: "info", content: `logging { level: "info" }`},
		{name: "warn", content: `logging { level: "warn" }`},
		{name: "error", content: `logging { level: "error" }`},
		{name: "invalid level", content: `logging { level: "verbose" }`, wantErr: true},
		{name: "missing level", content: ``, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			configFile := filepath.Join(t.TempDir(), "config.textproto")
			if err := os.WriteFile(configFile, []byte(tc.content), 0o600); err != nil {
				t.Fatalf("Failed to write config file: %v", err)
			}

			_, err := LoadConfig(configFile)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("LoadConfig() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
