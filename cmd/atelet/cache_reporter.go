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
	"log/slog"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

const imageCacheReportTimeout = 10 * time.Second

type nodeImageCacheReportClient interface {
	ReportNodeImageCache(context.Context, *ateapipb.ReportNodeImageCacheRequest, ...grpc.CallOption) (*ateapipb.ReportNodeImageCacheResponse, error)
}

type imageDigestSource interface {
	Digests() []string
}

type nodeImageCacheReporter struct {
	client       nodeImageCacheReportClient
	cache        imageDigestSource
	nodeName     string
	ateletPodUID string
	interval     time.Duration
}

func (r *nodeImageCacheReporter) run(ctx context.Context) {
	r.report(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.report(ctx)
		}
	}
}

func (r *nodeImageCacheReporter) report(ctx context.Context) {
	reportCtx, cancel := context.WithTimeout(ctx, imageCacheReportTimeout)
	defer cancel()
	_, err := r.client.ReportNodeImageCache(reportCtx, &ateapipb.ReportNodeImageCacheRequest{
		Cache: &ateapipb.NodeImageCache{
			NodeName:     r.nodeName,
			AteletPodUid: r.ateletPodUID,
			ImageDigests: r.cache.Digests(),
		},
	})
	if err != nil {
		slog.WarnContext(ctx, "Failed to report node image cache", "node", r.nodeName, "err", err)
	}
}
