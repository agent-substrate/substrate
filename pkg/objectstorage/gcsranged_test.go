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
	"net/http"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

func TestGCSGetObjectCloseCancelsInitialRequest(t *testing.T) {
	initialContext := make(chan context.Context, 1)
	client, err := storage.NewClient(t.Context(), option.WithHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		select {
		case initialContext <- req.Context():
		default:
		}
		return &http.Response{
			StatusCode: http.StatusPartialContent,
			Header: http.Header{
				"Content-Range": {fmt.Sprintf("bytes 0-%d/%d", downloadChunkSize-1, downloadChunkSize+1)},
			},
			ContentLength: downloadChunkSize,
			Body:          http.NoBody,
		}, nil
	})}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	g := &gcsClient{client: client}
	g.poolOnce.Do(func() {}) // Use the same test client for every range.
	r, err := g.GetObject(t.Context(), "snapshots", "object")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	requestContext := <-initialContext
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if requestContext.Err() != context.Canceled {
		t.Fatal("Close did not cancel the initial GCS request")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
