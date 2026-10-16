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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/load"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/pki"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/report"
)

func loadCommand() *cobra.Command {
	var r load.Runner
	var pkiDir, tunnelHost, scenarios, resultsPath string
	var ingressPort, connectPort int
	var rungs []float64
	cmd := &cobra.Command{
		Use:   "load",
		Short: "Drive the scenarios through atunnel and report its CPU and memory per rate",
		Long: `load drives traffic through atunnel at rising rates, ingress from here and
egress through the egress actor in an actor's sandbox, and reports, per rate,
atunnel's own CPU and memory as read from its admin endpoint.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			selected, err := load.Select(scenarios)
			if err != nil {
				return err
			}
			for _, rate := range rungs {
				if rate <= 0 {
					return fmt.Errorf("--rungs: %v is not a positive number", rate)
				}
			}
			if len(rungs) == 0 {
				return fmt.Errorf("--rungs is empty")
			}
			if r.Warmup >= r.Step {
				return fmt.Errorf("--warmup must be shorter than --step")
			}
			if r.ProfileDir != "" {
				if err := os.MkdirAll(r.ProfileDir, 0o755); err != nil {
					return err
				}
			}
			at := func(port int) string { return net.JoinHostPort(tunnelHost, strconv.Itoa(port)) }
			r.Targets = load.Targets{
				Ingress:      at(ingressPort),
				Connect:      at(connectPort),
				ServerName:   tunnelHost,
				RouterBundle: filepath.Join(pkiDir, pki.RouterBundle),
				CA:           filepath.Join(pkiDir, pki.CAFile),
			}

			ctx := cmd.Context()
			if err := r.Prepare(ctx); err != nil {
				return err
			}
			var reports []*report.Report
			for _, s := range selected {
				fmt.Fprintf(os.Stderr, "running %s...\n", s.Name)
				rep, err := r.Run(ctx, s, rungs)
				if rep != nil {
					fmt.Println(rep)
					reports = append(reports, rep)
				}
				if resultsPath != "" {
					// Rewritten after every scenario, so an interrupted
					// run keeps what it measured.
					if err := report.Write(resultsPath, reports); err != nil {
						return err
					}
				}
				if err != nil {
					return err
				}
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&pkiDir, "pki-dir", "", "directory written by atunnel-bench pki")
	f.StringVar(&tunnelHost, "tunnel-host", "atunnel-worker", "host atunnel runs on")
	f.IntVar(&ingressPort, "ingress-port", 443, "atunnel's ingress HTTPS port (--atunnel-listen-address)")
	f.IntVar(&connectPort, "connect-port", 8443, "atunnel's ingress CONNECT port (--atunnel-connect-listen-address)")
	f.StringVar(&r.TunnelAdmin, "tunnel-admin", "http://atunnel-worker:9090", "atunnel's admin base URL")
	f.StringVar(&r.SinkAdmin, "sink-admin", "http://atunnel-sink:9091", "the sink's admin base URL")
	f.StringVar(&r.SharedDir, "shared-dir", "/shared", "directory shared with the worker container, holding the egress actor's and ingress actors' sockets")
	f.StringVar(&scenarios, "scenarios", "all", "comma-separated scenarios to run, or all: "+load.Names())
	f.Float64SliceVar(&rungs, "rungs", []float64{1000, 2000, 4000, 8000}, "offered rates per second, in order; connection counts for EgressIdleConns")
	f.DurationVar(&r.Step, "step", 10*time.Second, "length of each rung")
	f.DurationVar(&r.Warmup, "warmup", 2*time.Second, "leading part of each rung left out of the measurement")
	f.IntVar(&r.Workers, "workers", 256, "units in flight at most")
	f.StringVar(&r.ProfileDir, "profile-dir", "", "save a CPU and a heap profile of atunnel per rung into this directory")
	f.StringVar(&resultsPath, "results-json", "", "write every scenario's results to this path as JSON")
	_ = cmd.MarkFlagRequired("pki-dir")
	return cmd
}
