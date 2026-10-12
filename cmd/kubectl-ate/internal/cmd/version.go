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
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var versionClientOnly bool

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the client and server version information",
	Long: `Print the version of kubectl-ate and of the ateapi server.

ateapi runs several replicas, which differ in version while an upgrade rolls
through them. The server version is that of the one replica that answered,
which is named in the output.`,
	Args: cobra.NoArgs,
	RunE: runVersion,
}

// buildInfo is one binary's build identity. Instance is empty for the client.
type buildInfo struct {
	Instance  string `json:"instance,omitempty"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
	Platform  string `json:"platform"`
}

type versionInfo struct {
	ClientVersion buildInfo  `json:"clientVersion"`
	ServerVersion *buildInfo `json:"serverVersion,omitempty"`
}

// VersionRunner executes the version command logic.
type VersionRunner struct {
	// getServerInfo fetches the server's build identity. Nil skips the server.
	getServerInfo func(context.Context) (*ateapipb.ServerInfo, error)
	outputFmt     string
	out           io.Writer
	errOut        io.Writer
}

// Run prints the client version even when the server cannot be reached, and
// then returns the server error.
func (r *VersionRunner) Run(ctx context.Context) error {
	info := versionInfo{ClientVersion: buildInfo{
		Version:   version.Version,
		Commit:    version.Commit,
		BuildDate: version.BuildDate,
		Platform:  version.Platform(),
	}}
	var serverErr error
	if r.getServerInfo != nil {
		var resp *ateapipb.ServerInfo
		resp, serverErr = r.getServerInfo(ctx)
		if serverErr == nil {
			info.ServerVersion = &buildInfo{
				Instance:  resp.GetInstance(),
				Version:   resp.GetVersion(),
				Commit:    resp.GetCommit(),
				BuildDate: resp.GetBuildDate(),
				Platform:  resp.GetPlatform(),
			}
		}
	}

	if err := printVersion(r.out, info, r.outputFmt); err != nil {
		return err
	}
	if serverErr != nil {
		return fmt.Errorf("failed to get server version: %w", serverErr)
	}
	if c, s := info.ClientVersion, info.ServerVersion; s != nil && skewed(c, *s) {
		fmt.Fprintf(r.errOut, "WARNING: client version %s (commit %s) differs from server version %s (commit %s)\n", c.Version, c.Commit, s.Version, s.Commit)
	}
	return nil
}

// skewed reports whether client and server were built from different source.
// The commit identifies the source exactly, whereas the version string depends
// on how a binary was built (a stamped release tag or the Go module version),
// so versions are compared only when either commit is unknown. A "-dirty"
// suffix is ignored, so a build with uncommitted changes matches one built
// from its base commit. A side with neither a version nor a commit (e.g. a
// `go run` binary) cannot be compared, so it is not reported as skewed.
func skewed(client, server buildInfo) bool {
	if unidentified(client) || unidentified(server) {
		return false
	}
	clientCommit := strings.TrimSuffix(client.Commit, "-dirty")
	serverCommit := strings.TrimSuffix(server.Commit, "-dirty")
	if knownCommit(clientCommit) && knownCommit(serverCommit) {
		return clientCommit != serverCommit
	}
	return client.Version != server.Version
}

func knownCommit(commit string) bool {
	return commit != "" && commit != "unknown"
}

// unidentified reports whether b carries only the unstamped defaults of the
// version package.
func unidentified(b buildInfo) bool {
	return (b.Version == "" || b.Version == "dev") && !knownCommit(b.Commit)
}

func printVersion(out io.Writer, info versionInfo, format string) error {
	switch format {
	case "json":
		b, err := json.MarshalIndent(info, "", "  ")
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "%s\n", b)
		return err
	case "yaml":
		b, err := yaml.Marshal(info)
		if err != nil {
			return err
		}
		_, err = out.Write(b)
		return err
	}
	c := info.ClientVersion
	fmt.Fprintf(out, "Client Version: %s\n", version.Format(c.Version, c.Commit, c.BuildDate, c.Platform))
	if s := info.ServerVersion; s != nil {
		fmt.Fprintf(out, "Server Version: %s instance=%s\n", version.Format(s.Version, s.Commit, s.BuildDate, s.Platform), s.Instance)
	}
	return nil
}

func runVersion(cmd *cobra.Command, args []string) error {
	runner := &VersionRunner{
		outputFmt: outputFmt,
		out:       cmd.OutOrStdout(),
		errOut:    cmd.ErrOrStderr(),
	}
	if !versionClientOnly {
		runner.getServerInfo = func(ctx context.Context) (*ateapipb.ServerInfo, error) {
			apiClient, err := ateclient.NewClient(ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to ate-api-server: %w", err)
			}
			defer apiClient.Close()
			return apiClient.GetServerInfo(ctx, &ateapipb.GetServerInfoRequest{})
		}
	}
	return runner.Run(cmd.Context())
}

func init() {
	versionCmd.Flags().BoolVar(&versionClientOnly, "client", false, "Print the client version only, without contacting the server")
	rootCmd.AddCommand(versionCmd)
}
