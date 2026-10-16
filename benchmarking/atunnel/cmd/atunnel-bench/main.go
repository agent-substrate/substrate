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

// atunnel-bench measures what atunnel costs in CPU and memory as load rises.
// Every process of the benchmark is a subcommand of this one program; see
// run-docker.sh for how they fit together.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "atunnel-bench",
		Short: "Measure atunnel's CPU and memory on its own",
		Long: `atunnel-bench measures what atunnel costs in CPU and memory as load rises.
Each process of the benchmark is a subcommand; run-docker.sh shows how they
fit together.`,
		// Usage is for mistakes in the command line, not for a run that fails.
		SilenceUsage: true,
		// main prints the error itself, once.
		SilenceErrors: true,
		// Nobody types these commands often enough to want shell completion.
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	root.AddCommand(pkiCommand(), sinkCommand(), loadCommand())
	root.AddCommand(linuxCommands()...)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "atunnel-bench:", err)
		os.Exit(1)
	}
}
