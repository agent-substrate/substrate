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
	"github.com/spf13/cobra"
)

// The verb commands group the per-resource subcommands, which each resource
// file attaches in its init.
var (
	getCmd     = &cobra.Command{Use: "get", Short: "Display one or many resources"}
	createCmd  = &cobra.Command{Use: "create", Short: "Create a resource"}
	updateCmd  = &cobra.Command{Use: "update", Short: "Update a resource"}
	deleteCmd  = &cobra.Command{Use: "delete", Short: "Delete a resource"}
	pauseCmd   = &cobra.Command{Use: "pause", Short: "Pause a resource"}
	resumeCmd  = &cobra.Command{Use: "resume", Short: "Resume a resource"}
	suspendCmd = &cobra.Command{Use: "suspend", Short: "Suspend a resource"}
	logsCmd    = &cobra.Command{Use: "logs", Short: "Print the logs for a resource"}
	topCmd     = &cobra.Command{Use: "top", Short: "Display resource (CPU/Memory) usage"}
)

func init() {
	rootCmd.AddCommand(getCmd, createCmd, updateCmd, deleteCmd, pauseCmd, resumeCmd, suspendCmd, logsCmd, topCmd)
}
