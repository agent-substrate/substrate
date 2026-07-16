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

package cmd

import (
	"context"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

type fakeCompletionClient struct {
	actorRequest *ateapipb.ListActorsRequest
}

func (*fakeCompletionClient) ListAtespaces(context.Context, *ateapipb.ListAtespacesRequest, ...grpc.CallOption) (*ateapipb.ListAtespacesResponse, error) {
	return &ateapipb.ListAtespacesResponse{Atespaces: []*ateapipb.Atespace{
		{Metadata: &ateapipb.ResourceMetadata{Name: "team-z"}},
		{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}},
	}}, nil
}

func (f *fakeCompletionClient) ListActors(_ context.Context, req *ateapipb.ListActorsRequest, _ ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	f.actorRequest = req
	return &ateapipb.ListActorsResponse{Actors: []*ateapipb.Actor{
		{Metadata: &ateapipb.ResourceMetadata{Name: "alpha"}},
		{Metadata: &ateapipb.ResourceMetadata{Name: "beta"}},
	}}, nil
}

func (*fakeCompletionClient) Close() {}

func TestLogsActorCompletions(t *testing.T) {
	fake := &fakeCompletionClient{}
	original := newResourceCompletionClient
	newResourceCompletionClient = func(context.Context) (resourceCompletionClient, error) { return fake, nil }
	t.Cleanup(func() {
		newResourceCompletionClient = original
		_ = logsActorsCmd.Flags().Set("atespace", "")
	})

	completeAtespace, ok := logsActorsCmd.GetFlagCompletionFunc("atespace")
	if !ok {
		t.Fatal("--atespace has no completion function")
	}
	atespaces, _ := completeAtespace(logsActorsCmd, nil, "team-")
	if got := strings.Join(atespaces, ","); got != "team-a,team-z" {
		t.Errorf("atespace completions = %q, want %q", got, "team-a,team-z")
	}

	_ = logsActorsCmd.Flags().Set("atespace", "team-a")
	actors, _ := logsActorsCmd.ValidArgsFunction(logsActorsCmd, nil, "a")
	if got := strings.Join(actors, ","); got != "alpha" {
		t.Errorf("actor completions = %q, want %q", got, "alpha")
	}
	if got := fake.actorRequest.GetAtespace(); got != "team-a" {
		t.Errorf("ListActors atespace = %q, want %q", got, "team-a")
	}
}
