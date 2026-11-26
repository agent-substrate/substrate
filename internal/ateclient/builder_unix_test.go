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

//go:build unix

package ateclient

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestFileBearerTokenCredentials_FIFO(t *testing.T) {
	t.Run("read once and reused on RPCs", func(t *testing.T) {
		fifoPath := filepath.Join(t.TempDir(), "token.fifo")
		if err := unix.Mkfifo(fifoPath, 0o600); err != nil {
			t.Fatalf("Mkfifo: %v", err)
		}
		writeErr := make(chan error, 1)
		go func() {
			f, err := os.OpenFile(fifoPath, os.O_WRONLY, 0)
			if err != nil {
				writeErr <- err
				return
			}
			_, werr := f.WriteString("pipe-token\n")
			cerr := f.Close()
			if werr != nil {
				writeErr <- werr
				return
			}
			writeErr <- cerr
		}()

		creds, err := fileBearerTokenCredentials(fifoPath)
		if err != nil {
			t.Fatalf("fileBearerTokenCredentials(fifo) error = %v", err)
		}
		if err := <-writeErr; err != nil {
			t.Fatalf("writing fifo: %v", err)
		}
		md, err := creds.GetRequestMetadata(context.Background())
		if err != nil {
			t.Fatalf("GetRequestMetadata() after fifo EOF error = %v", err)
		}
		if got := md["authorization"]; got != "Bearer pipe-token" {
			t.Errorf("authorization = %q, want %q", got, "Bearer pipe-token")
		}
	})

	t.Run("empty kubeconfig fails without blocking on fifo", func(t *testing.T) {
		t.Setenv("KUBERNETES_SERVICE_HOST", "")
		t.Setenv("KUBERNETES_SERVICE_PORT", "")

		dir := t.TempDir()
		emptyKubeconfig := filepath.Join(dir, "empty-kubeconfig")
		if err := os.WriteFile(emptyKubeconfig, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		fifoPath := filepath.Join(dir, "unopened.fifo")
		if err := unix.Mkfifo(fifoPath, 0o600); err != nil {
			t.Fatalf("Mkfifo: %v", err)
		}

		_, err := NewClient(context.Background(), emptyKubeconfig, "", "", fifoPath, false)
		if err == nil || !strings.Contains(err.Error(), "no Kubernetes cluster context configured") {
			t.Fatalf("NewClient() error = %v, want empty kubeconfig error", err)
		}
	})
}
