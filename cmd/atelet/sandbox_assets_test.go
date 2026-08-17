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
	"archive/tar"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExtractTarArchive(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		compress  func(io.Writer) io.WriteCloser
		wantError bool
		errMsg    string
	}{
		{
			name: ".tar.gz format",
			url:  "https://example.com/gvisor.tar.gz",
			compress: func(w io.Writer) io.WriteCloser {
				return gzip.NewWriter(w)
			},
			wantError: false,
		},
		{
			name: ".tgz format",
			url:  "https://example.com/gvisor.tgz",
			compress: func(w io.Writer) io.WriteCloser {
				return gzip.NewWriter(w)
			},
			wantError: false,
		},
		{
			name:      ".tar.bz2 format",
			url:       "https://example.com/gvisor.tar.bz2",
			compress:  nil,
			wantError: false,
		},
		{
			name: ".tar format (uncompressed)",
			url:  "https://example.com/gvisor.tar",
			compress: func(w io.Writer) io.WriteCloser {
				return nopWriteCloser{w}
			},
			wantError: false,
		},
		{
			name:      "unsupported format",
			url:       "https://example.com/gvisor.zip",
			compress:  nil,
			wantError: true,
			errMsg:    "unsupported archive format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			archivePath := filepath.Join(tempDir, "archive")
			destDir := filepath.Join(tempDir, "dest")

			if tt.compress != nil {
				f, err := os.Create(archivePath)
				if err != nil {
					t.Fatalf("failed to create archive: %v", err)
				}

				cw := tt.compress(f)
				tw := tar.NewWriter(cw)

				hdr := &tar.Header{
					Name: "test.txt",
					Mode: 0644,
					Size: int64(len("hello world")),
				}
				if err := tw.WriteHeader(hdr); err != nil {
					t.Fatalf("failed to write tar header: %v", err)
				}
				if _, err := tw.Write([]byte("hello world")); err != nil {
					t.Fatalf("failed to write tar content: %v", err)
				}

				tw.Close()
				cw.Close()
				f.Close()
			} else if !tt.wantError && strings.Contains(tt.url, ".tar.bz2") {
				f, err := os.Create(archivePath)
				if err != nil {
					t.Fatalf("failed to create archive: %v", err)
				}
				f.Write([]byte("invalid bzip2 data"))
				f.Close()

				err = extractTarArchive(context.Background(), archivePath, tt.url, destDir)
				if err == nil {
					t.Fatal("expected error for invalid bzip2 data, got nil")
				}
				if strings.Contains(err.Error(), "unsupported archive format") {
					t.Errorf("expected bzip2 read error, got: %v", err)
				}
				return
			}

			err := extractTarArchive(context.Background(), archivePath, tt.url, destDir)
			if (err != nil) != tt.wantError {
				t.Fatalf("extractTarArchive() error = %v, wantError = %v", err, tt.wantError)
			}

			if tt.wantError {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errMsg)
				}
				return
			}

			content, err := os.ReadFile(filepath.Join(destDir, "test.txt"))
			if err != nil {
				t.Fatalf("failed to read extracted file: %v", err)
			}
			if string(content) != "hello world" {
				t.Errorf("extracted content = %q, want %q", content, "hello world")
			}
		})
	}
}

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error {
	return nil
}
