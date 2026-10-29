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

package main

import (
	"time"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/pki"
)

func pkiCommand() *cobra.Command {
	var dir string
	var opts pki.Options
	cmd := &cobra.Command{
		Use:   "pki",
		Short: "Write the throwaway certificates the other subcommands share",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			return pki.Generate(dir, opts)
		},
	}
	cmd.Flags().StringVar(&dir, "dir", "", "directory to write the certificates to")
	cmd.Flags().StringSliceVar(&opts.WorkerHosts, "worker-hosts", []string{"atunnel-worker"}, "names and IPs atunnel's serving certificate is valid for")
	cmd.Flags().StringSliceVar(&opts.GatewayHosts, "gateway-hosts", []string{"atunnel-sink"}, "names and IPs the egress gateway's certificate is valid for")
	cmd.Flags().DurationVar(&opts.Validity, "validity", 30*24*time.Hour, "certificate lifetime")
	_ = cmd.MarkFlagRequired("dir")
	return cmd
}
