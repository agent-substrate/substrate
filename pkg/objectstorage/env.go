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

package objectstorage

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// UsesS3 reports whether ATE_STORAGE_BACKEND selects S3. Any other value
// selects GCS, the default.
func UsesS3() bool {
	return os.Getenv("ATE_STORAGE_BACKEND") == "s3"
}

// NewS3ClientFromEnv builds an S3 client from the standard AWS environment
// variables. AWS_S3_USE_PATH_STYLE=true selects path-style addressing.
func NewS3ClientFromEnv(ctx context.Context) (*s3.Client, error) {
	slog.InfoContext(ctx, "Using S3 storage backend")
	// Depends on the standard AWS environment variables, which have to be set
	// on every pod that uses this client.
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading S3 config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if usePathStyle := os.Getenv("AWS_S3_USE_PATH_STYLE"); usePathStyle == "true" {
			o.UsePathStyle = true
		}
	}), nil
}

// NewFromEnv builds the ObjectStorage for the backend UsesS3 selects.
func NewFromEnv(ctx context.Context) (ObjectStorage, error) {
	if UsesS3() {
		client, err := NewS3ClientFromEnv(ctx)
		if err != nil {
			return nil, err
		}
		return NewS3Client(client), nil
	}
	// GCS is currently the default, TODO: we assume workload identity / ADC
	client, err := NewGCSClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}
	return client, nil
}
