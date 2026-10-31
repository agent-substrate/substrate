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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/internal/e2e/procstatus"
)

func newProcStatusCmd() *cobra.Command {
	var names []string
	cmd := &cobra.Command{
		Use:   "procstatus",
		Short: "Print the capabilities, no_new_privs and per-thread seccomp modes of PID 1 and of every process with a given name, as JSON.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			procs, err := readProcesses("/proc", names, os.Getpid())
			if err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(procs)
		},
	}
	cmd.Flags().StringArrayVar(&names, "name", nil, "also report processes whose argv[0] has this base name (repeatable)")
	return cmd
}

// readProcesses reads PID 1 and every process under procDir, other than self,
// whose name is one of names. A process or thread that exits while it is read
// is skipped.
func readProcesses(procDir string, names []string, self int) ([]procstatus.Process, error) {
	entries, err := os.ReadDir(procDir)
	if err != nil {
		return nil, err
	}
	var procs []procstatus.Process
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == self {
			continue
		}
		dir := filepath.Join(procDir, e.Name())
		cmdline, err := os.ReadFile(filepath.Join(dir, "cmdline"))
		if exited(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		name := processName(cmdline)
		if pid != 1 && !slices.Contains(names, name) {
			continue
		}
		p, err := readProcess(dir)
		if exited(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("pid %d: %w", pid, err)
		}
		p.PID, p.Name = pid, name
		procs = append(procs, p)
	}
	return procs, nil
}

// processName is the base name of argv[0]. It names the sandbox processes
// where /proc/<pid>/comm cannot: runsc re-executes /proc/self/exe, so its gofer
// and sandbox are both "exe" there, and comm truncates cloud-hypervisor to 15
// bytes.
func processName(cmdline []byte) string {
	argv0, _, _ := bytes.Cut(cmdline, []byte{0})
	return filepath.Base(string(argv0))
}

// readProcess reads the status of the process at dir and of each of its
// threads.
func readProcess(dir string) (procstatus.Process, error) {
	status, err := os.ReadFile(filepath.Join(dir, "status"))
	if err != nil {
		return procstatus.Process{}, err
	}
	p, err := parseProcessStatus(string(status))
	if err != nil {
		return p, err
	}
	tasks, err := os.ReadDir(filepath.Join(dir, "task"))
	if err != nil {
		return p, err
	}
	for _, task := range tasks {
		tid, err := strconv.Atoi(task.Name())
		if err != nil {
			return p, fmt.Errorf("task %q: %w", task.Name(), err)
		}
		status, err := os.ReadFile(filepath.Join(dir, "task", task.Name(), "status"))
		if exited(err) {
			continue
		}
		if err != nil {
			return p, err
		}
		thread, err := parseThreadStatus(string(status))
		if err != nil {
			return p, fmt.Errorf("thread %d: %w", tid, err)
		}
		thread.TID = tid
		p.Threads = append(p.Threads, thread)
	}
	return p, nil
}

// exited reports whether err comes from reading a process or thread that has
// exited since it was listed.
func exited(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ESRCH)
}

// parseProcessStatus reads the fields procstatus.Process reports out of a
// /proc/<pid>/status document.
func parseProcessStatus(status string) (procstatus.Process, error) {
	fields := parseStatusFields(status)
	var errs []error
	hex := func(name string) uint64 {
		n, err := fields.number(name, 16)
		errs = append(errs, err)
		return n
	}
	dec := func(name string) int {
		n, err := fields.number(name, 10)
		errs = append(errs, err)
		return int(n)
	}
	p := procstatus.Process{
		PPID:       dec("PPid"),
		CapPrm:     hex("CapPrm"),
		CapEff:     hex("CapEff"),
		CapBnd:     hex("CapBnd"),
		CapAmb:     hex("CapAmb"),
		NoNewPrivs: dec("NoNewPrivs"),
		Seccomp:    dec("Seccomp"),
	}
	return p, errors.Join(errs...)
}

// parseThreadStatus reads a thread's name and seccomp mode out of a
// /proc/<pid>/task/<tid>/status document.
func parseThreadStatus(status string) (procstatus.Thread, error) {
	fields := parseStatusFields(status)
	seccomp, err := fields.number("Seccomp", 10)
	return procstatus.Thread{Name: fields["Name"], Seccomp: int(seccomp)}, err
}

// statusFields are the "Key:\tvalue" lines of a /proc status document.
type statusFields map[string]string

func parseStatusFields(status string) statusFields {
	fields := statusFields{}
	for _, line := range strings.Split(status, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			fields[k] = strings.TrimSpace(v)
		}
	}
	return fields
}

// number parses the named field in base. A missing field is an error: every
// kernel this repo supports reports all the fields procstatus reads.
func (f statusFields) number(name string, base int) (uint64, error) {
	v, ok := f[name]
	if !ok {
		return 0, fmt.Errorf("no %s in status", name)
	}
	n, err := strconv.ParseUint(v, base, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return n, nil
}
