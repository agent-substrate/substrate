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
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReportNodeImageCache(t *testing.T) {
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	service := &Service{persistence: persistence}

	_, err := service.ReportNodeImageCache(context.Background(), &ateapipb.ReportNodeImageCacheRequest{
		Cache: &ateapipb.NodeImageCache{
			NodeName:     "node-1",
			AteletPodUid: "atelet-uid",
			ImageDigests: []string{"sha256:b", "sha256:a", "sha256:a"},
		},
	})
	if err != nil {
		t.Fatalf("ReportNodeImageCache failed: %v", err)
	}

	got, err := persistence.GetNodeImageCache(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("GetNodeImageCache failed: %v", err)
	}
	if len(got.GetImageDigests()) != 2 || got.GetImageDigests()[0] != "sha256:a" || got.GetImageDigests()[1] != "sha256:b" {
		t.Fatalf("stored digests = %v, want [sha256:a sha256:b]", got.GetImageDigests())
	}
}

func TestReportNodeImageCacheValidatesIdentity(t *testing.T) {
	service := &Service{}
	_, err := service.ReportNodeImageCache(context.Background(), &ateapipb.ReportNodeImageCacheRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ReportNodeImageCache status = %v, want InvalidArgument", status.Code(err))
	}
}
