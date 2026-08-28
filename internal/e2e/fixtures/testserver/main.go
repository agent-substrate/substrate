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

// Command testserver is the one binary behind every plain helper pod the egress
// e2e suites stand up. A subcommand picks its behavior, so a single ko image
// backs all of them and a new origin-style test adds a subcommand here rather
// than a new image to build and push:
//
//	testserver grpc  --listen=:50051   a cleartext HTTP/2 gRPC echo origin
//	testserver http  --listen=:8080    a plain HTTP origin serving /healthz
//	testserver egressprobe --listen=:8080  a client that drives the egress gateway
//
// Each pod runs exactly one subcommand on one listener, so the wire behavior of
// any given pod is unchanged from when these were separate binaries -- the grpc
// pod still speaks nothing but cleartext HTTP/2, for instance.
//
// The subcommand shape follows Kubernetes' test/images/agnhost: a single
// extendable CLI whose modes are cobra subcommands, matching the rest of this
// repo's binaries (see cmd/atenet).
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "testserver",
		Short: "Multi-mode helper server for the egress e2e suites.",
	}
	root.AddCommand(newGRPCCmd(), newHTTPCmd(), newEgressProbeCmd())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
