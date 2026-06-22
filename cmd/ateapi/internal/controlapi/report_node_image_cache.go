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

package controlapi

import (
	"context"
	"sort"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// nodeImageCacheTTL allows several reports to be missed without immediately
// discarding useful affinity information. Stale data affects performance only:
// atelet still pulls an image normally if the scheduler predicted a cache hit.
const nodeImageCacheTTL = 2 * time.Minute

func (s *Service) ReportNodeImageCache(ctx context.Context, req *ateapipb.ReportNodeImageCacheRequest) (*ateapipb.ReportNodeImageCacheResponse, error) {
	cache := req.GetCache()
	if cache.GetNodeName() == "" {
		return nil, status.Error(codes.InvalidArgument, "cache.node_name is required")
	}
	if cache.GetAteletPodUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "cache.atelet_pod_uid is required")
	}

	cache = proto.Clone(cache).(*ateapipb.NodeImageCache)
	// Reports are authoritative snapshots. Normalize them to keep storage and
	// scheduler comparisons deterministic and avoid duplicate digest entries.
	dedup := make(map[string]struct{}, len(cache.GetImageDigests()))
	for _, digest := range cache.GetImageDigests() {
		if digest == "" {
			return nil, status.Error(codes.InvalidArgument, "cache.image_digests must not contain an empty digest")
		}
		dedup[digest] = struct{}{}
	}
	cache.ImageDigests = cache.ImageDigests[:0]
	for digest := range dedup {
		cache.ImageDigests = append(cache.ImageDigests, digest)
	}
	sort.Strings(cache.ImageDigests)

	if err := s.persistence.SetNodeImageCache(ctx, cache, nodeImageCacheTTL); err != nil {
		return nil, err
	}
	return &ateapipb.ReportNodeImageCacheResponse{}, nil
}
