//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package objectstorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// ---------------------------------------------------------------------------
// ParseObjectURL tests (pure function, no dependencies)
// ---------------------------------------------------------------------------

func TestParseObjectURL(t *testing.T) {
	tests := []struct {
		name       string
		rawURL     string
		wantBucket string
		wantObject string
		wantErr    bool
		errContain string
	}{
		{
			name:       "gs URL",
			rawURL:     "gs://my-bucket/path/to/object",
			wantBucket: "my-bucket",
			wantObject: "path/to/object",
		},
		{
			name:       "s3 URL",
			rawURL:     "s3://my-bucket/path/to/object",
			wantBucket: "my-bucket",
			wantObject: "path/to/object",
		},
		{
			name:       "oss URL",
			rawURL:     "oss://my-bucket/path/to/object",
			wantBucket: "my-bucket",
			wantObject: "path/to/object",
		},
		{
			name:       "oss URL with nested path",
			rawURL:     "oss://test-bucket/dir1/dir2/file.txt",
			wantBucket: "test-bucket",
			wantObject: "dir1/dir2/file.txt",
		},
		{
			name:       "oss URL with single-slash object",
			rawURL:     "oss://bucket/object",
			wantBucket: "bucket",
			wantObject: "object",
		},
		{
			name:       "unsupported http scheme",
			rawURL:     "http://my-bucket/path/to/object",
			wantErr:    true,
			errContain: "unsupported",
		},
		{
			name:       "unsupported file scheme",
			rawURL:     "file:///local/path",
			wantErr:    true,
			errContain: "unsupported",
		},
		{
			name:       "empty string - no scheme",
			rawURL:     "just-a-string",
			wantErr:    true,
			errContain: "unsupported",
		},
		{
			name:       "empty scheme with slashes triggers parse error",
			rawURL:     "://bucket/object",
			wantErr:    true,
			errContain: "while parsing",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, object, err := ParseObjectURL(tt.rawURL)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseObjectURL(%q) err=%v, wantErr=%v", tt.rawURL, err, tt.wantErr)
				return
			}
			if err != nil && tt.errContain != "" {
				if !strings.Contains(err.Error(), tt.errContain) {
					t.Errorf("ParseObjectURL(%q) error=%q, want to contain %q", tt.rawURL, err.Error(), tt.errContain)
				}
				return
			}
			if bucket != tt.wantBucket {
				t.Errorf("ParseObjectURL(%q) bucket=%q, want=%q", tt.rawURL, bucket, tt.wantBucket)
			}
			if object != tt.wantObject {
				t.Errorf("ParseObjectURL(%q) object=%q, want=%q", tt.rawURL, object, tt.wantObject)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ParseGCSURL tests (pure function, no dependencies)
// ---------------------------------------------------------------------------

func TestParseGCSURL(t *testing.T) {
	tests := []struct {
		name       string
		gsURL      string
		wantBucket string
		wantObject string
		wantErr    bool
	}{
		{
			name:       "standard GCS URL",
			gsURL:      "gs://my-bucket/path/to/object",
			wantBucket: "my-bucket",
			wantObject: "path/to/object",
		},
		{
			name:       "GCS URL with no subpath",
			gsURL:      "gs://my-bucket/object.txt",
			wantBucket: "my-bucket",
			wantObject: "object.txt",
		},
		{
			name:       "GCS URL with deeply nested path",
			gsURL:      "gs://bucket/a/b/c/d/e/file",
			wantBucket: "bucket",
			wantObject: "a/b/c/d/e/file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, object, err := ParseGCSURL(tt.gsURL)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseGCSURL(%q) err=%v, wantErr=%v", tt.gsURL, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if bucket != tt.wantBucket {
					t.Errorf("ParseGCSURL(%q) bucket=%q, want=%q", tt.gsURL, bucket, tt.wantBucket)
				}
				if object != tt.wantObject {
					t.Errorf("ParseGCSURL(%q) object=%q, want=%q", tt.gsURL, object, tt.wantObject)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Error-inducing helpers
// ---------------------------------------------------------------------------

// errorReader is an io.ReadCloser that always returns an error on Read.
type errorReader struct {
	err error
}

func (e *errorReader) Read(_ []byte) (int, error) { return 0, e.err }
func (e *errorReader) Close() error               { return nil }

// errorReadCloser is an io.ReadCloser that returns data on first Read then error.
type partialErrorReader struct {
	data   []byte
	err    error
	called int
}

func (p *partialErrorReader) Read(buf []byte) (int, error) {
	if p.called == 0 {
		n := copy(buf, p.data)
		p.called++
		return n, nil
	}
	return 0, p.err
}
func (p *partialErrorReader) Close() error { return nil }

// ---------------------------------------------------------------------------
// mockObjectStorage - mock implementation of ObjectStorage interface
// ---------------------------------------------------------------------------

type mockObjectStorage struct {
	getFunc func(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	putFunc func(ctx context.Context, bucket, object string, reader io.Reader) error
}

func (m *mockObjectStorage) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	return m.getFunc(ctx, bucket, object)
}

func (m *mockObjectStorage) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	return m.putFunc(ctx, bucket, object, reader)
}

// ---------------------------------------------------------------------------
// FetchFromGCS tests (using mock ObjectStorage)
// ---------------------------------------------------------------------------

func TestFetchFromGCS(t *testing.T) {
	tests := []struct {
		name    string
		gsURL   string
		getFunc func(ctx context.Context, bucket, object string) (io.ReadCloser, error)
		wantErr bool
		want    string
	}{
		{
			name:  "success",
			gsURL: "gs://bucket/object",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("hello"))), nil
			},
			want:    "hello",
			wantErr: false,
		},
		{
			name:  "URL parse failure",
			gsURL: "://invalid",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				// Should not be called since ParseGCSURL fails first
				return nil, nil
			},
			wantErr: true,
		},
		{
			name:  "GetObject returns error",
			gsURL: "gs://bucket/object",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return nil, errors.New("not found")
			},
			wantErr: true,
		},
		{
			name:  "ReadAll fails",
			gsURL: "gs://bucket/object",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return &errorReader{err: errors.New("read error")}, nil
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockObjectStorage{getFunc: tt.getFunc}
			content, err := FetchFromGCS(context.Background(), client, tt.gsURL)
			if (err != nil) != tt.wantErr {
				t.Errorf("FetchFromGCS() err=%v, wantErr=%v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && string(content) != tt.want {
				t.Errorf("FetchFromGCS() content=%q, want=%q", string(content), tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// FetchLocalFileFromGCS tests (using mock ObjectStorage)
// ---------------------------------------------------------------------------

func TestFetchLocalFileFromGCS(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		gsURL   string
		getFunc func(ctx context.Context, bucket, object string) (io.ReadCloser, error)
		mode    os.FileMode
		wantErr bool
		want    string
	}{
		{
			name:  "success with mode 0600",
			gsURL: "gs://bucket/object",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("file-content"))), nil
			},
			mode:    0o600,
			want:    "file-content",
			wantErr: false,
		},
		{
			name:  "success with mode 0755",
			gsURL: "gs://bucket/object",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("executable"))), nil
			},
			mode:    0o755,
			want:    "executable",
			wantErr: false,
		},
		{
			name:  "GetObject returns error",
			gsURL: "gs://bucket/object",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return nil, errors.New("object not found")
			},
			mode:    0o600,
			wantErr: true,
		},
		{
			name:  "io.Copy fails during file write",
			gsURL: "gs://bucket/object",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return &partialErrorReader{data: []byte("partial"), err: errors.New("copy error")}, nil
			},
			mode:    0o600,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockObjectStorage{getFunc: tt.getFunc}
			localPath := filepath.Join(tmpDir, tt.name)
			err := FetchLocalFileFromGCS(context.Background(), client, tt.gsURL, localPath, tt.mode)
			if (err != nil) != tt.wantErr {
				t.Errorf("FetchLocalFileFromGCS() err=%v, wantErr=%v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				content, err := os.ReadFile(localPath)
				if err != nil {
					t.Fatalf("Failed to read local file: %v", err)
				}
				if string(content) != tt.want {
					t.Errorf("FetchLocalFileFromGCS() content=%q, want=%q", string(content), tt.want)
				}
				info, err := os.Stat(localPath)
				if err != nil {
					t.Fatalf("Failed to stat local file: %v", err)
				}
				if info.Mode().Perm() != tt.mode {
					t.Errorf("FetchLocalFileFromGCS() mode=%v, want=%v", info.Mode().Perm(), tt.mode)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// NewGCSClient / NewS3Client tests (type assertion)
// ---------------------------------------------------------------------------

func TestNewGCSClient(t *testing.T) {
	// Verify the type assertion by passing nil. The interface is satisfied
	// even though the underlying client would panic on actual use.
	storage := NewGCSClient(nil)
	if storage == nil {
		t.Fatal("NewGCSClient returned nil")
	}
	var _ ObjectStorage = storage
}

func TestNewS3Client(t *testing.T) {
	// Similar to NewGCSClient - verify type assertion.
	storage := NewS3Client(nil)
	if storage == nil {
		t.Fatal("NewS3Client returned nil")
	}
	var _ ObjectStorage = storage
}

// ---------------------------------------------------------------------------
// SendToGCSWithZstd tests (using mock ObjectStorage + zstd)
// ---------------------------------------------------------------------------

func TestSendToGCSWithZstd(t *testing.T) {
	tests := []struct {
		name    string
		gsURL   string
		putFunc func(ctx context.Context, bucket, object string, reader io.Reader) error
		content string
		wantErr bool
	}{
		{
			name:    "success",
			gsURL:   "gs://bucket/compressed-object",
			content: "data to compress",
			putFunc: func(ctx context.Context, bucket, object string, reader io.Reader) error {
				// Read the compressed data to verify it's valid zstd
				compressed, err := io.ReadAll(reader)
				if err != nil {
					return fmt.Errorf("reading compressed data: %w", err)
				}
				// Decompress to verify the content
				zr, err := zstd.NewReader(bytes.NewReader(compressed), zstd.WithDecoderConcurrency(1))
				if err != nil {
					return fmt.Errorf("creating zstd reader: %w", err)
				}
				defer zr.Close()
				decompressed, err := io.ReadAll(zr)
				if err != nil {
					return fmt.Errorf("decompressing: %w", err)
				}
				if string(decompressed) != "data to compress" {
					return fmt.Errorf("decompressed content mismatch: got %q", string(decompressed))
				}
				return nil
			},
			wantErr: false,
		},
		{
			name:    "invalid URL",
			gsURL:   "://invalid",
			content: "data",
			putFunc: func(ctx context.Context, bucket, object string, reader io.Reader) error {
				return nil
			},
			wantErr: true,
		},
		{
			name:    "PutObject fails",
			gsURL:   "gs://bucket/object",
			content: "data",
			putFunc: func(ctx context.Context, bucket, object string, reader io.Reader) error {
				return errors.New("upload failed")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockObjectStorage{putFunc: tt.putFunc}
			err := SendToGCSWithZstd(context.Background(), client, tt.gsURL, bytes.NewReader([]byte(tt.content)))
			if (err != nil) != tt.wantErr {
				t.Errorf("SendToGCSWithZstd() err=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SendLocalFileToGCSWithZstd tests (using mock ObjectStorage + file I/O + zstd)
// ---------------------------------------------------------------------------

func TestSendLocalFileToGCSWithZstd(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		gsURL   string
		putFunc func(ctx context.Context, bucket, object string, reader io.Reader) error
		content string
		wantErr bool
	}{
		{
			name:    "success",
			gsURL:   "gs://bucket/object.zst",
			content: "local file data",
			putFunc: func(ctx context.Context, bucket, object string, reader io.Reader) error {
				return nil
			},
			wantErr: false,
		},
		{
			name:    "invalid URL",
			gsURL:   "://invalid",
			content: "data",
			putFunc: func(ctx context.Context, bucket, object string, reader io.Reader) error {
				return nil
			},
			wantErr: true,
		},
		{
			name:    "PutObject fails",
			gsURL:   "gs://bucket/object",
			content: "data",
			putFunc: func(ctx context.Context, bucket, object string, reader io.Reader) error {
				return errors.New("upload failed")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockObjectStorage{putFunc: tt.putFunc}
			localPath := filepath.Join(tmpDir, tt.name+".txt")
			if err := os.WriteFile(localPath, []byte(tt.content), 0o600); err != nil {
				t.Fatalf("Failed to write local file: %v", err)
			}
			err := SendLocalFileToGCSWithZstd(context.Background(), client, tt.gsURL, localPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("SendLocalFileToGCSWithZstd() err=%v, wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// fetchFromGCSWithZstd tests (using mock ObjectStorage + zstd decompression)
// ---------------------------------------------------------------------------

func compressZstd(data []byte) []byte {
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		panic(err)
	}
	w.Write(data)
	w.Close()
	return buf.Bytes()
}

func TestFetchFromGCSWithZstd(t *testing.T) {
	tests := []struct {
		name    string
		gsURL   string
		getFunc func(ctx context.Context, bucket, object string) (io.ReadCloser, error)
		want    string
		wantErr bool
	}{
		{
			name:  "success",
			gsURL: "gs://bucket/object.zst",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(compressZstd([]byte("decompressed content")))), nil
			},
			want:    "decompressed content",
			wantErr: false,
		},
		{
			name:  "invalid URL",
			gsURL: "://invalid",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return nil, nil
			},
			wantErr: true,
		},
		{
			name:  "GetObject fails",
			gsURL: "gs://bucket/object.zst",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return nil, errors.New("not found")
			},
			wantErr: true,
		},
		{
			name:  "corrupt zstd data",
			gsURL: "gs://bucket/object.zst",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader([]byte("not-valid-zstd"))), nil
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockObjectStorage{getFunc: tt.getFunc}
			var out bytes.Buffer
			err := fetchFromGCSWithZstd(context.Background(), client, tt.gsURL, &out)
			if (err != nil) != tt.wantErr {
				t.Errorf("fetchFromGCSWithZstd() err=%v, wantErr=%v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && out.String() != tt.want {
				t.Errorf("fetchFromGCSWithZstd() content=%q, want=%q", out.String(), tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// FetchLocalFileFromGCSWithZstd tests (using mock ObjectStorage + zstd + file I/O)
// ---------------------------------------------------------------------------

func TestFetchLocalFileFromGCSWithZstd(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name    string
		gsURL   string
		getFunc func(ctx context.Context, bucket, object string) (io.ReadCloser, error)
		want    string
		wantErr bool
	}{
		{
			name:  "success",
			gsURL: "gs://bucket/compressed-file.zst",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(compressZstd([]byte("decompressed file content")))), nil
			},
			want:    "decompressed file content",
			wantErr: false,
		},
		{
			name:  "GetObject fails",
			gsURL: "gs://bucket/object.zst",
			getFunc: func(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
				return nil, errors.New("not found")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockObjectStorage{getFunc: tt.getFunc}
			localPath := filepath.Join(tmpDir, tt.name)
			err := FetchLocalFileFromGCSWithZstd(context.Background(), client, tt.gsURL, localPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("FetchLocalFileFromGCSWithZstd() err=%v, wantErr=%v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				content, err := os.ReadFile(localPath)
				if err != nil {
					t.Fatalf("Failed to read local file: %v", err)
				}
				if string(content) != tt.want {
					t.Errorf("FetchLocalFileFromGCSWithZstd() content=%q, want=%q", string(content), tt.want)
				}
				info, err := os.Stat(localPath)
				if err != nil {
					t.Fatalf("Failed to stat local file: %v", err)
				}
				if info.Mode().Perm() != 0o600 {
					t.Errorf("FetchLocalFileFromGCSWithZstd() mode=%v, want 0600", info.Mode().Perm())
				}
			}
		})
	}
}
