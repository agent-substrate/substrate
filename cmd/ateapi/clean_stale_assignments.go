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
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/internal/serverboot"
)

const cleanStaleAssignmentsCommand = "clean-stale-assignments"

// cleanStaleAssignments runs controlapi.CleanStaleAssignments against the
// database the server flags name. docs/guides/upgrade.md says when to run it.
func cleanStaleAssignments(args []string) {
	flags := pflag.NewFlagSet(cleanStaleAssignmentsCommand, pflag.ExitOnError)
	dryRun := flags.Bool("dry-run", false, "Log the stale assignments without releasing them.")
	for _, name := range []string{
		"postgres-read-write-connection-string",
		"postgres-owner-connection-string",
		"postgres-read-write-role",
		"postgres-owner-role",
		"postgres-schema",
		"postgres-pool-max-conns",
	} {
		flags.AddFlag(pflag.CommandLine.Lookup(name))
	}
	_ = flags.Parse(args) // ExitOnError

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	serverboot.InitLogger()
	if err := loadFlagsFromEnv(); err != nil {
		serverboot.Fatal(ctx, "Invalid PostgreSQL configuration", err)
	}
	persistence, err := connectStore(ctx)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to set up persistence backend", err)
	}
	defer persistence.Pool().Close()
	defer persistence.Close()

	fixed, err := controlapi.CleanStaleAssignments(ctx, persistence, *dryRun)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to clean stale assignments", err, slog.Int("fixed", fixed), slog.Bool("dry_run", *dryRun))
	}
	slog.InfoContext(ctx, "Cleaned stale assignments", slog.Int("fixed", fixed), slog.Bool("dry_run", *dryRun))
}
