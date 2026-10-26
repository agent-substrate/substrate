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
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

// sysctlCommand sets sysctls in its own network namespace, for the worker to
// give each sandbox its own TCP buffer limits. The image has no sysctl binary.
func sysctlCommand() *cobra.Command {
	return &cobra.Command{
		Use:    "sysctl NAME=VALUE...",
		Short:  "Set sysctls",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				name, value, ok := strings.Cut(arg, "=")
				if !ok {
					return fmt.Errorf("%q is not NAME=VALUE", arg)
				}
				if err := os.WriteFile(sysctlPath(name), []byte(value), 0o644); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "set %s=%s\n", name, value)
			}
			return nil
		},
	}
}

func sysctlPath(name string) string {
	return filepath.Join("/proc/sys", strings.ReplaceAll(name, ".", "/"))
}
