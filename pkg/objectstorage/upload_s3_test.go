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
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// The real SDK uploader owns its normal part buffers; only its network calls
// are replaced, so a slow multipart upload exercises the production pipeline.
type slowMultipartAPI struct {
	created chan string
	parts   chan string
	release <-chan struct{}
	fail    string
	aborted atomic.Int64
}

func (s *slowMultipartAPI) CreateMultipartUpload(_ context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	s.created <- *in.Key
	return &s3.CreateMultipartUploadOutput{UploadId: in.Key}, nil
}
func (s *slowMultipartAPI) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if _, err := io.Copy(io.Discard, in.Body); err != nil {
		return nil, err
	}
	s.parts <- *in.Key
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if *in.Key == s.fail {
		return nil, errors.New("part upload failed")
	}
	return &s3.UploadPartOutput{ETag: aws.String("etag")}, nil
}
func (s *slowMultipartAPI) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return &s3.CompleteMultipartUploadOutput{}, nil
}
func (s *slowMultipartAPI) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	s.aborted.Add(1)
	return &s3.AbortMultipartUploadOutput{}, nil
}
func (*slowMultipartAPI) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, errors.New("expected a multipart upload")
}

func TestUploaderSlowS3Multipart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sparse-memory")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	// Incompressible data forces multiple SDK parts; the remaining 2 GiB is a
	// hole, exercising sparse compression without allocating the logical size.
	if _, err := io.CopyN(f, rand.New(rand.NewSource(2553)), 40<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(2 << 30); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failure], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gate := make(chan struct{})
				api := &slowMultipartAPI{created: make(chan string, 4), parts: make(chan string, 8), release: gate}
				if failure {
					api.fail = "first"
				}
				//nolint:staticcheck // SA1019: use the same SDK uploader as s3Client.
				client := &s3Client{uploader: manager.NewUploader(api, func(u *manager.Uploader) {
					u.PartSize = uploadPartSize
					u.Concurrency = uploadConcurrency
				})}
				u, err := NewUploader(client, uploadBufferMemory(client)+parallelWorkerMemory)
				if err != nil {
					t.Fatal(err)
				}
				send := func(name string) <-chan error {
					done := make(chan error, 1)
					go func() {
						file, err := os.Open(path)
						if err == nil {
							defer file.Close()
							err = u.SendFile(t.Context(), "gs://bucket/"+name, file)
						}
						done <- err
					}()
					return done
				}
				first := send("first")
				synctest.Wait()
				if got := len(api.parts); got != 2 {
					t.Errorf("active SDK parts = %d, want 2", got)
				}
				if got := <-api.created; got != "first" {
					t.Error(got)
				}
				next := send("next")
				synctest.Wait()
				select {
				case got := <-api.created:
					t.Errorf("%s started while first held SDK buffers", got)
				default:
				}
				close(gate)
				synctest.Wait()
				if err := <-first; (err != nil) != failure {
					t.Errorf("first: %v", err)
				}
				if err := <-next; err != nil {
					t.Errorf("next: %v", err)
				}
				if got := <-api.created; got != "next" {
					t.Error(got)
				}
				if failure && api.aborted.Load() != 1 {
					t.Error("failed multipart upload was not aborted")
				}
			})
		})
	}
}
