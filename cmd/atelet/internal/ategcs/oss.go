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
	"net/http"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

type ossClient struct {
	client *oss.Client
}

func NewOSSClient(client *oss.Client) ObjectStorage {
	return &ossClient{client: client}
}

// supportsStreamingPut marks OSS as a backend whose PutObject accepts a
// non-seekable body: the SDK streams unknown-length bodies with chunked
// transfer encoding (its signatures cover headers, not the payload). See
// streamingPutter.
func (*ossClient) supportsStreamingPut() {}

func (o *ossClient) GetObject(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	out, err := o.client.GetObject(ctx, &oss.GetObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(object),
	})
	if err != nil {
		var svcErr *oss.ServiceError
		if errors.As(err, &svcErr) && svcErr.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%w: Failed to get OSS Bucket:%q, Object:%q", ateerrors.ReasonFailedGetExternalObject, bucket, object)
		}
		return nil, err
	}
	return out.Body, nil
}

func (o *ossClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader) error {
	_, err := o.client.PutObject(ctx, &oss.PutObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(object),
		Body:   reader,
	})
	return err
}
