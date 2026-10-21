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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

func newKillCmd() *cobra.Command {
	var contains string
	cmd := &cobra.Command{
		Use:   "kill",
		Short: "SIGKILL every process whose command line contains a string, as a crash would.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			n, err := killMatching("/proc", contains, os.Getpid())
			if err != nil {
				return err
			}
			fmt.Printf("killed %d\n", n)
			if n == 0 {
				return fmt.Errorf("no process command line contains %q", contains)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&contains, "cmdline-contains", "", "kill processes whose command line contains this")
	_ = cmd.MarkFlagRequired("cmdline-contains")
	return cmd
}

// killMatching kills the processes under procDir whose command line contains
// contains, other than self, and reports how many it killed.
func killMatching(procDir, contains string, self int) (int, error) {
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return 0, err
	}
	killed := 0
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join(procDir, e.Name(), "cmdline"))
		if err != nil || !strings.Contains(string(cmdline), contains) {
			continue
		}
		// already gone counts: a gofer or virtiofsd can exit on its own once
		// the process it serves is killed
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return killed, fmt.Errorf("killing %d: %w", pid, err)
		}
		killed++
	}
	return killed, nil
}
