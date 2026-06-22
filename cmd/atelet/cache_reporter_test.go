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
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

type fakeDigestSource struct{ digests []string }

func (f fakeDigestSource) Digests() []string { return f.digests }

type fakeNodeImageCacheReportClient struct {
	req *ateapipb.ReportNodeImageCacheRequest
}

func (f *fakeNodeImageCacheReportClient) ReportNodeImageCache(_ context.Context, req *ateapipb.ReportNodeImageCacheRequest, _ ...grpc.CallOption) (*ateapipb.ReportNodeImageCacheResponse, error) {
	f.req = req
	return &ateapipb.ReportNodeImageCacheResponse{}, nil
}

func TestNodeImageCacheReporter(t *testing.T) {
	client := &fakeNodeImageCacheReportClient{}
	reporter := &nodeImageCacheReporter{
		client:       client,
		cache:        fakeDigestSource{digests: []string{"sha256:a", "sha256:b"}},
		nodeName:     "node-1",
		ateletPodUID: "atelet-uid",
		interval:     time.Minute,
	}
	reporter.report(context.Background())

	cache := client.req.GetCache()
	if cache.GetNodeName() != "node-1" || cache.GetAteletPodUid() != "atelet-uid" {
		t.Fatalf("unexpected cache identity: %v", cache)
	}
	if got := cache.GetImageDigests(); len(got) != 2 || got[0] != "sha256:a" || got[1] != "sha256:b" {
		t.Fatalf("reported digests = %v, want [sha256:a sha256:b]", got)
	}
}
