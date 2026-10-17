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
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeProcSys(t *testing.T, instances, watches string) string {
	t.Helper()
	dir := t.TempDir()
	inotify := filepath.Join(dir, "fs", "inotify")
	if err := os.MkdirAll(inotify, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"max_user_instances": instances, "max_user_watches": watches} {
		if err := os.WriteFile(filepath.Join(inotify, name), []byte(value+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func TestLogNodeLimits(t *testing.T) {
	for _, tc := range []struct {
		name, instances, wantLevel string
	}{
		{"low", "128", "level=WARN"},
		{"at floor", "8192", "level=INFO"},
		{"unreadable", "", "level=INFO"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			procSys := writeProcSys(t, tc.instances, "1048576")
			if tc.instances == "" {
				procSys = t.TempDir()
			}
			out := captureLog(t)
			logNodeLimits(context.Background(), procSys)
			got := out.String()
			if !strings.Contains(got, tc.wantLevel) {
				t.Errorf("log %q, want %s", got, tc.wantLevel)
			}
			want := "inotify_max_user_instances=" + tc.instances
			if tc.instances == "" {
				want = "inotify_max_user_instances=0"
			}
			if !strings.Contains(got, want) || !strings.Contains(got, "page_size=") {
				t.Errorf("log %q lacks %s or page_size", got, want)
			}
		})
	}
}
