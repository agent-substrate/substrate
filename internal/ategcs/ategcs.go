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

package ategcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel"
)

type ObjectStorage interface {
	GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader) error
}

type gcsClient struct {
	client *storage.Client
}

func NewGCSClient(client *storage.Client) ObjectStorage {
	return &gcsClient{client: client}
}

func (g *gcsClient) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	return g.client.Bucket(bucket).Object(object).NewReader(ctx)
}

func (g *gcsClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	wc := g.client.Bucket(bucket).Object(object).NewWriter(ctx)
	// io.Copy reports local read errors; wc.Close() reports the actual
	// GCS upload (auth, permissions, transient). Join both so the caller
	// doesn't lose either.
	_, copyErr := io.Copy(wc, reader)
	closeErr := wc.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("while putting GCS object: %w", err)
	}
	return nil
}

type s3Client struct {
	client *s3.Client
}

func NewS3Client(client *s3.Client) ObjectStorage {
	return &s3Client{client: client}
}

func (s *s3Client) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (s *s3Client) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
		Body:   reader,
	})
	return err
}

func ParseGCSURL(gsURL string) (string, string, error) {
	parsed, err := url.Parse(gsURL)
	if err != nil {
		return "", "", fmt.Errorf("while parsing %q: %w", gsURL, err)
	}

	return parsed.Host, strings.TrimPrefix(parsed.Path, "/"), nil
}

func FetchFromGCS(ctx context.Context, client ObjectStorage, gsURL string) ([]byte, error) {
	bucket, object, err := ParseGCSURL(gsURL)
	if err != nil {
		return nil, fmt.Errorf("while parsing url: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return nil, fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("while reading all content: %w", err)
	}

	return content, nil
}

func FetchLocalFileFromGCS(ctx context.Context, client ObjectStorage, gsURL, localPath string, mode os.FileMode) error {
	bucket, object, err := ParseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing url: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return fmt.Errorf("while getting object bucket=%q object=%q: %w", bucket, object, err)
	}
	defer rc.Close()

	localDir := filepath.Dir(localPath)
	tmpFile, err := os.CreateTemp(localDir, filepath.Base(localPath)+"-download-")
	if err != nil {
		return fmt.Errorf("while temp file: %w", err)
	}
	defer tmpFile.Close()

	_, err = io.Copy(tmpFile, rc)
	if err != nil {
		return fmt.Errorf("while copying data: %w", err)
	}

	if err := tmpFile.Chmod(mode); err != nil {
		return fmt.Errorf("while setting file mode: %w", err)
	}

	if err := os.Rename(tmpFile.Name(), localPath); err != nil {
		return fmt.Errorf("while renaming temp file to target: %w", err)
	}

	return nil
}

func SendToGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, content io.Reader) (err error) {
	bucket, object, err := ParseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}

	// Create a temporary file to store compressed data
	tmpFile, err := os.CreateTemp("", "substrate-upload-compress-")
	if err != nil {
		return fmt.Errorf("while creating temp compress file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	zwc, err := zstd.NewWriter(tmpFile)
	if err != nil {
		return fmt.Errorf("while creating zstd writer: %w", err)
	}

	_, err = io.Copy(zwc, content)
	if err != nil {
		zwc.Close()
		return fmt.Errorf("while compressing data to temp file: %w", err)
	}
	if err := zwc.Close(); err != nil {
		return fmt.Errorf("while closing zstd writer: %w", err)
	}

	// Seek back to the beginning of the temp file
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return fmt.Errorf("while seeking temp file: %w", err)
	}

	// Upload the seekable temp file
	if err := client.PutObject(ctx, bucket, object, tmpFile); err != nil {
		return fmt.Errorf("while putting object: %w", err)
	}
	return nil
}

func SendLocalFileToGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := SendToGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("in sendToGCSWithZstd: %w", err)
	}

	return nil
}

func fetchFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, out io.Writer) (err error) {
	bucket, object, err := ParseGCSURL(gsURL)
	if err != nil {
		return fmt.Errorf("while parsing URL: %w", err)
	}

	rc, err := client.GetObject(ctx, bucket, object)
	if err != nil {
		return fmt.Errorf("while getting object: %w", err)
	}
	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			if err != nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from rc.Close", slog.Any("err", closeErr))
			}
		}
	}()

	zrc, err := zstd.NewReader(rc, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("in zstd.NewReader: %w", err)
	}
	defer zrc.Close()

	_, err = io.Copy(out, zrc)
	if err != nil {
		return fmt.Errorf("in io.Copy: %w", err)
	}

	return nil
}

func FetchLocalFileFromGCSWithZstd(ctx context.Context, client ObjectStorage, gsURL string, localFilePath string) (err error) {
	tracer := otel.Tracer("ateom-gvisor")
	ctx, span := tracer.Start(ctx, "fetchLocalFileFromGCSWithZstd")
	defer span.End()

	localFile, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", localFilePath, err)
	}
	defer func() {
		if closeErr := localFile.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				slog.InfoContext(ctx, "Dropped error from closing localFile", slog.String("localFile", localFilePath), slog.Any("err", err))
			}
		}
	}()

	if err := localFile.Chmod(0o600); err != nil {
		return fmt.Errorf("in localFile.Chmod(0o600): %w", err)
	}

	if err := fetchFromGCSWithZstd(ctx, client, gsURL, localFile); err != nil {
		return fmt.Errorf("while fetching %q from GCS: %w", gsURL, err)
	}

	return nil
}
