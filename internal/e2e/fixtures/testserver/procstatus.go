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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

// ProcessStatus is what the kernel reports for one process in /proc/<pid>/status,
// reduced to the fields the worker security tests compare. The capability sets
// are the raw 64-bit masks.
type ProcessStatus struct {
	PID     int    `json:"pid"`
	Cmdline string `json:"cmdline"`
	// NSPID lists the process's PID in each nested PID namespace it belongs
	// to, outermost first, so len(NSPID) is its PID namespace depth as seen
	// from the reader.
	NSPID          []int  `json:"nspid"`
	CapPrm         uint64 `json:"capPrm"`
	CapEff         uint64 `json:"capEff"`
	CapBnd         uint64 `json:"capBnd"`
	CapAmb         uint64 `json:"capAmb"`
	NoNewPrivs     int    `json:"noNewPrivs"`
	Seccomp        int    `json:"seccomp"`
	SeccompFilters int    `json:"seccompFilters"`
}

func newProcStatusCmd() *cobra.Command {
	var contains []string
	cmd := &cobra.Command{
		Use:   "procstatus",
		Short: "Print the capability, no_new_privs and seccomp state of PID 1 and of every process whose command line contains a string, as JSON.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			rows, err := processStatuses("/proc", contains, os.Getpid())
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(rows)
		},
	}
	cmd.Flags().StringArrayVar(&contains, "cmdline-contains", nil, "also report processes whose command line contains this (repeatable)")
	return cmd
}

// processStatuses reads PID 1 and every process under procDir, other than
// self, whose command line contains one of contains. A process that exits
// between listing and reading is skipped.
func processStatuses(procDir string, contains []string, self int) ([]ProcessStatus, error) {
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return nil, err
	}
	var rows []ProcessStatus
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(procDir, e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		cmdline := strings.TrimSpace(strings.ReplaceAll(string(raw), "\x00", " "))
		matched := pid == 1
		for _, c := range contains {
			if c != "" && strings.Contains(cmdline, c) {
				matched = true
			}
		}
		if !matched {
			continue
		}
		status, err := os.ReadFile(filepath.Join(procDir, e.Name(), "status"))
		if err != nil {
			continue
		}
		row, err := parseProcessStatus(string(status))
		if err != nil {
			return nil, fmt.Errorf("pid %d: %w", pid, err)
		}
		row.PID, row.Cmdline = pid, cmdline
		rows = append(rows, row)
	}
	return rows, nil
}

// parseProcessStatus reads the fields this command reports out of a
// /proc/<pid>/status document. A missing field is an error: every kernel this
// repo supports reports all of them.
func parseProcessStatus(status string) (ProcessStatus, error) {
	fields := map[string]string{}
	for _, line := range strings.Split(status, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			fields[k] = strings.TrimSpace(v)
		}
	}
	var row ProcessStatus
	var err error
	hex := func(name string) uint64 {
		v, ok := fields[name]
		if !ok {
			err = fmt.Errorf("no %s in status", name)
			return 0
		}
		n, perr := strconv.ParseUint(v, 16, 64)
		if perr != nil {
			err = fmt.Errorf("%s: %w", name, perr)
		}
		return n
	}
	dec := func(name string) int {
		v, ok := fields[name]
		if !ok {
			err = fmt.Errorf("no %s in status", name)
			return 0
		}
		n, perr := strconv.Atoi(v)
		if perr != nil {
			err = fmt.Errorf("%s: %w", name, perr)
		}
		return n
	}
	row.CapPrm, row.CapEff, row.CapBnd, row.CapAmb = hex("CapPrm"), hex("CapEff"), hex("CapBnd"), hex("CapAmb")
	row.NoNewPrivs, row.Seccomp, row.SeccompFilters = dec("NoNewPrivs"), dec("Seccomp"), dec("Seccomp_filters")
	for _, f := range strings.Fields(fields["NSpid"]) {
		n, perr := strconv.Atoi(f)
		if perr != nil {
			return row, fmt.Errorf("NSpid: %w", perr)
		}
		row.NSPID = append(row.NSPID, n)
	}
	if len(row.NSPID) == 0 {
		return row, fmt.Errorf("no NSpid in status")
	}
	return row, err
}
