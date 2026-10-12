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
	"os"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// GetServerInfo reports this instance's build. Its hostname is the pod name in
// Kubernetes, which tells replicas apart while an upgrade rolls through them.
func (s *RPCService) GetServerInfo(ctx context.Context, req *ateapipb.GetServerInfoRequest) (*ateapipb.ServerInfo, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, apierror.Internal("while reading hostname: %v", err)
	}
	return &ateapipb.ServerInfo{
		Instance:  hostname,
		Version:   version.Version,
		Commit:    version.Commit,
		BuildDate: version.BuildDate,
		Platform:  version.Platform(),
	}, nil
}
