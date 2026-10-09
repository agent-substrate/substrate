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

package steps

import (
	"context"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// DeploymentRef identifies a pool Deployment a demo waits on.
type DeploymentRef struct {
	Namespace string
	Name      string
}

// listAllActors pages through every actor in every atespace.
func listAllActors(ctx context.Context, client *ateclient.Client) ([]*ateapipb.Actor, error) {
	var all []*ateapipb.Actor
	pageToken := ""
	for {
		// An empty Atespace lists across all atespaces.
		resp, err := client.ListActors(ctx, &ateapipb.ListActorsRequest{
			PageSize:  1000,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, resp.GetActors()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return all, nil
		}
	}
}
