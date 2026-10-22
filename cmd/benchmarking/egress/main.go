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

// benchmark-egress is the workload of the egress scale test, which holds many
// CONNECT tunnels open through the egress gateway at a fixed pace and measures
// what they cost. Each of its programs is a subcommand: the actor that opens
// the tunnels and counts what happened, and the target they call.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/cmd/actor"
)

func main() {
	root := &cobra.Command{
		Use:   "benchmark-egress",
		Short: "The actor and target of the egress scale test",
		Long: `benchmark-egress is the workload of the egress scale test. The actor holds
one tunnel per endpoint open through the egress gateway and counts what
happened; the target is the endpoint every tunnel leads to.`,
		// No usage text on an error; the error alone names what went wrong.
		SilenceUsage: true,
		// main prints the error itself, once.
		SilenceErrors: true,
		// Nobody types these commands often enough to want shell completion.
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	root.AddCommand(actor.Command())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "benchmark-egress:", err)
		os.Exit(1)
	}
}
