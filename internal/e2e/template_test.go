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

package e2e

import (
	"context"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateclient"
	"google.golang.org/grpc"
)

// disabledGoldenTemplateClient serves one ActorTemplate that opted out of the
// golden snapshot and has no golden status.
type disabledGoldenTemplateClient struct {
	ateapi.ControlClient
}

func (disabledGoldenTemplateClient) GetActorTemplate(_ context.Context, in *ateapi.GetActorTemplateRequest, _ ...grpc.CallOption) (*ateapi.ActorTemplate, error) {
	return &ateapi.ActorTemplate{
		Metadata:       &ateapi.ResourceMetadata{Atespace: in.GetActorTemplate().GetAtespace(), Name: in.GetActorTemplate().GetName()},
		SnapshotConfig: &ateapi.SnapshotConfig{GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED}},
	}, nil
}

// TestWaitForSubstrateTemplateReadyDisabledTemplate: a template that opted out
// never gets a golden tag or an error, so the helper must return instead of
// riding its timeout. The short E2E_TEMPLATE_READY_TIMEOUT keeps a regression
// from stalling the test for minutes.
func TestWaitForSubstrateTemplateReadyDisabledTemplate(t *testing.T) {
	t.Setenv("E2E_TEMPLATE_READY_TIMEOUT", "2s")
	clients := &Clients{SubstrateAPI: &ateclient.Client{ControlClient: disabledGoldenTemplateClient{}}}

	WaitForSubstrateTemplateReady(context.Background(), t, clients, "ate-demo", "counter")
}
