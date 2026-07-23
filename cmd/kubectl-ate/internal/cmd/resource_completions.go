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
	"sort"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const resourceCompletionTimeout = 5 * time.Second

type resourceCompletionClient interface {
	ListAtespaces(context.Context, *ateapipb.ListAtespacesRequest, ...grpc.CallOption) (*ateapipb.ListAtespacesResponse, error)
	ListActors(context.Context, *ateapipb.ListActorsRequest, ...grpc.CallOption) (*ateapipb.ListActorsResponse, error)
	Close()
}

var newResourceCompletionClient = func(ctx context.Context) (resourceCompletionClient, error) {
	return ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, traceEnabled)
}

func registerAtespaceFlagCompletion(cmd *cobra.Command) {
	_ = cmd.RegisterFlagCompletionFunc("atespace", completeAtespaces)
}

func registerActorCompletions(cmd *cobra.Command) {
	registerAtespaceFlagCompletion(cmd)
	cmd.ValidArgsFunction = completeActors
}

func registerAtespaceArgCompletion(cmd *cobra.Command) {
	cmd.ValidArgsFunction = completeAtespaceArg
}

func completeAtespaces(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	ctx, cancel := resourceCompletionContext(cmd)
	defer cancel()

	client, err := newResourceCompletionClient(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	defer client.Close()

	resp, err := client.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	names := make([]string, 0, len(resp.GetAtespaces()))
	for _, atespace := range resp.GetAtespaces() {
		names = append(names, atespace.GetMetadata().GetName())
	}
	return matchingResourceNames(names, toComplete), cobra.ShellCompDirectiveNoFileComp
}

func completeAtespaceArg(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return completeAtespaces(cmd, args, toComplete)
}

func completeActors(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) != 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	atespace, err := cmd.Flags().GetString("atespace")
	if err != nil || atespace == "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	ctx, cancel := resourceCompletionContext(cmd)
	defer cancel()

	client, err := newResourceCompletionClient(ctx)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	defer client.Close()

	var names []string
	pageToken := ""
	for {
		resp, err := client.ListActors(ctx, &ateapipb.ListActorsRequest{
			Atespace:  atespace,
			PageSize:  1000,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		for _, actor := range resp.GetActors() {
			names = append(names, actor.GetMetadata().GetName())
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	return matchingResourceNames(names, toComplete), cobra.ShellCompDirectiveNoFileComp
}

func resourceCompletionContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	parent := cmd.Context()
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, resourceCompletionTimeout)
}

func matchingResourceNames(names []string, prefix string) []string {
	matches := names[:0]
	for _, name := range names {
		if name != "" && strings.HasPrefix(name, prefix) {
			matches = append(matches, name)
		}
	}
	sort.Strings(matches)
	return matches
}
