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

// Package procstatus holds what the e2e testserver's procstatus subcommand
// prints: the kernel's view of processes on a worker, read from /proc.
package procstatus

// Process is one process's /proc/<pid>/status, reduced to the fields the
// worker security tests compare. The capability sets are the raw 64-bit masks.
// Seccomp is the main thread's mode; Threads has every thread's.
type Process struct {
	PID  int `json:"pid"`
	PPID int `json:"ppid"`
	// Name is the base name of argv[0].
	Name       string   `json:"name"`
	CapPrm     uint64   `json:"capPrm"`
	CapEff     uint64   `json:"capEff"`
	CapBnd     uint64   `json:"capBnd"`
	CapAmb     uint64   `json:"capAmb"`
	NoNewPrivs int      `json:"noNewPrivs"`
	Seccomp    int      `json:"seccomp"`
	Threads    []Thread `json:"threads"`
}

// Thread is one thread of a process, read from /proc/<pid>/task/<tid>/status.
type Thread struct {
	TID int `json:"tid"`
	// Name is the thread's comm. cloud-hypervisor names each thread it
	// starts, such as "vmm" and "vcpu0".
	Name    string `json:"name"`
	Seccomp int    `json:"seccomp"`
}
