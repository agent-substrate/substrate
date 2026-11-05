//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWarnLowMaxMapCount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content *string
		want    string
	}{
		{name: "kernel default", content: ptr("65530\n"), want: "level=ERROR"},
		{name: "recommended", content: ptr("4194304\n"), want: ""},
		{name: "above recommended", content: ptr("8388608\n"), want: ""},
		{name: "not a number", content: ptr("many\n"), want: "level=WARN"},
		{name: "missing file", content: nil, want: "level=WARN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "max_map_count")
			if tc.content != nil {
				if err := os.WriteFile(path, []byte(*tc.content), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			var buf bytes.Buffer
			prev := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
			t.Cleanup(func() { slog.SetDefault(prev) })

			warnLowMaxMapCount(t.Context(), path)

			if tc.want == "" && buf.Len() > 0 {
				t.Errorf("logged %q, want nothing", buf.String())
			}
			if !strings.Contains(buf.String(), tc.want) {
				t.Errorf("log = %q, want it to contain %q", buf.String(), tc.want)
			}
		})
	}
}

func ptr(s string) *string { return &s }
