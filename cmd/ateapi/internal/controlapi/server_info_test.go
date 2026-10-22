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
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/internal/version"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

func TestGetServerInfo(t *testing.T) {
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("os.Hostname() error = %v", err)
	}
	want := &ateapipb.ServerInfo{
		Instance:  hostname,
		Version:   version.Version,
		Commit:    version.Commit,
		BuildDate: version.BuildDate,
		Platform:  version.Platform(),
	}

	got, err := (&RPCService{}).GetServerInfo(context.Background(), &ateapipb.GetServerInfoRequest{})
	if err != nil {
		t.Fatalf("GetServerInfo() error = %v", err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetServerInfo() mismatch (-want +got):\n%s", diff)
	}
}
