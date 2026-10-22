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
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.ate.dev/substrate/internal/version"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// setClientVersion pins the client build identity for the duration of a test.
func setClientVersion(t *testing.T, v, commit, buildDate string) {
	t.Helper()
	oldV, oldCommit, oldBuildDate := version.Version, version.Commit, version.BuildDate
	version.Version, version.Commit, version.BuildDate = v, commit, buildDate
	t.Cleanup(func() {
		version.Version, version.Commit, version.BuildDate = oldV, oldCommit, oldBuildDate
	})
}

func serverInfo(resp *ateapipb.ServerInfo, err error) func(context.Context) (*ateapipb.ServerInfo, error) {
	return func(context.Context) (*ateapipb.ServerInfo, error) { return resp, err }
}

func TestVersionRunner(t *testing.T) {
	setClientVersion(t, "v0.2.0", "c1", "2026-01-01")
	platform := version.Platform()
	clientLine := "Client Version: v0.2.0 commit=c1 built=2026-01-01 " + platform + "\n"
	sameVersion := &ateapipb.ServerInfo{
		Instance: "ate-api-0", Version: "v0.2.0", Commit: "c1", BuildDate: "2026-01-01", Platform: "linux/amd64",
	}
	newerVersion := &ateapipb.ServerInfo{
		Instance: "ate-api-1", Version: "v0.3.0", Commit: "c2", BuildDate: "2026-02-01", Platform: "linux/arm64",
	}

	tests := []struct {
		name          string
		getServerInfo func(context.Context) (*ateapipb.ServerInfo, error)
		outputFmt     string
		wantOut       string
		wantErrOut    string
		wantErr       string
	}{
		{
			name:      "client only",
			outputFmt: "table",
			wantOut:   clientLine,
		},
		{
			name:          "matching server",
			getServerInfo: serverInfo(sameVersion, nil),
			outputFmt:     "table",
			wantOut:       clientLine + "Server Version: v0.2.0 commit=c1 built=2026-01-01 linux/amd64 instance=ate-api-0\n",
		},
		{
			name:          "skewed server warns",
			getServerInfo: serverInfo(newerVersion, nil),
			outputFmt:     "table",
			wantOut:       clientLine + "Server Version: v0.3.0 commit=c2 built=2026-02-01 linux/arm64 instance=ate-api-1\n",
			wantErrOut:    "WARNING: client version v0.2.0 (commit c1) differs from server version v0.3.0 (commit c2)\n",
		},
		{
			name:          "unreachable server still prints client",
			getServerInfo: serverInfo(nil, errors.New("connection refused")),
			outputFmt:     "table",
			wantOut:       clientLine,
			wantErr:       "failed to get server version: connection refused",
		},
		{
			name:          "json",
			getServerInfo: serverInfo(sameVersion, nil),
			outputFmt:     "json",
			wantOut: `{
  "clientVersion": {
    "version": "v0.2.0",
    "commit": "c1",
    "buildDate": "2026-01-01",
    "platform": "` + platform + `"
  },
  "serverVersion": {
    "instance": "ate-api-0",
    "version": "v0.2.0",
    "commit": "c1",
    "buildDate": "2026-01-01",
    "platform": "linux/amd64"
  }
}
`,
		},
		{
			name:      "yaml client only",
			outputFmt: "yaml",
			wantOut: `clientVersion:
  buildDate: "2026-01-01"
  commit: c1
  platform: ` + platform + `
  version: v0.2.0
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			runner := &VersionRunner{
				getServerInfo: test.getServerInfo,
				outputFmt:     test.outputFmt,
				out:           &out,
				errOut:        &errOut,
			}
			err := runner.Run(context.Background())
			if test.wantErr == "" && err != nil {
				t.Fatalf("Run() unexpected error: %v", err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("Run() error = %v, want it to contain %q", err, test.wantErr)
			}
			if diff := cmp.Diff(test.wantOut, out.String()); diff != "" {
				t.Errorf("stdout mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(test.wantErrOut, errOut.String()); diff != "" {
				t.Errorf("stderr mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSkewed(t *testing.T) {
	tests := []struct {
		name           string
		client, server buildInfo
		want           bool
	}{
		{
			name:   "same commit, versions built differently",
			client: buildInfo{Version: "v0.4.1-0.20261012060913-c4129c94bca0", Commit: "c4129c94"},
			server: buildInfo{Version: "v0.4.0-3-gc4129c94", Commit: "c4129c94"},
			want:   false,
		},
		{
			name:   "same commit, one dirty",
			client: buildInfo{Version: "v0.4.0", Commit: "c4129c94-dirty"},
			server: buildInfo{Version: "v0.4.0", Commit: "c4129c94"},
			want:   false,
		},
		{
			name:   "different commits, same version",
			client: buildInfo{Version: "dev", Commit: "aaaa"},
			server: buildInfo{Version: "dev", Commit: "bbbb"},
			want:   true,
		},
		{
			name:   "unknown commit, same version",
			client: buildInfo{Version: "v0.4.0", Commit: "unknown"},
			server: buildInfo{Version: "v0.4.0", Commit: "c4129c94"},
			want:   false,
		},
		{
			name:   "unstamped client is not compared",
			client: buildInfo{Version: "dev", Commit: "unknown"},
			server: buildInfo{Version: "v0.4.0-3-gc4129c94", Commit: "c4129c94"},
			want:   false,
		},
		{
			name:   "unstamped server is not compared",
			client: buildInfo{Version: "v0.4.0", Commit: "c4129c94"},
			server: buildInfo{Version: "dev", Commit: "unknown"},
			want:   false,
		},
		{
			name:   "unknown commit, different version",
			client: buildInfo{Version: "v0.3.0", Commit: "unknown"},
			server: buildInfo{Version: "v0.4.0", Commit: "c4129c94"},
			want:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := skewed(tc.client, tc.server); got != tc.want {
				t.Errorf("skewed() = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestVersionCommandArgs(t *testing.T) {
	runCommandArgsTests(t, []commandArgsTest{
		{name: "no args", command: versionCmd},
		{name: "rejects args", command: versionCmd, args: []string{"extra"}, wantErr: true},
	})
}
