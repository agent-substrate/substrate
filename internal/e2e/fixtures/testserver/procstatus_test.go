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
	"os"
	"path/filepath"
	"testing"
)

const sampleStatus = `Name:	virtiofsd
Umask:	0022
State:	S (sleeping)
NSpid:	3056697	126	1
Threads:	2
CapInh:	0000000000000000
CapPrm:	00000000880000df
CapEff:	00000000880000df
CapBnd:	00000000882010df
CapAmb:	0000000000000000
NoNewPrivs:	1
Seccomp:	2
Seccomp_filters:	1
`

func TestParseProcessStatus(t *testing.T) {
	row, err := parseProcessStatus(sampleStatus)
	if err != nil {
		t.Fatal(err)
	}
	if row.CapPrm != 0x880000df || row.CapEff != 0x880000df || row.CapBnd != 0x882010df || row.CapAmb != 0 {
		t.Errorf("capabilities = %+v", row)
	}
	if row.NoNewPrivs != 1 || row.Seccomp != 2 || row.SeccompFilters != 1 {
		t.Errorf("no_new_privs/seccomp = %+v", row)
	}
	if len(row.NSPID) != 3 || row.NSPID[2] != 1 {
		t.Errorf("NSpid = %v, want [3056697 126 1]", row.NSPID)
	}
}

func TestParseProcessStatusMissingField(t *testing.T) {
	if _, err := parseProcessStatus("Name:\tx\nNSpid:\t1\nCapPrm:\t0\n"); err == nil {
		t.Fatal("missing CapEff accepted")
	}
}

func TestProcessStatusesSelectsPID1AndMatches(t *testing.T) {
	proc := t.TempDir()
	write := func(pid, cmdline string) {
		dir := filepath.Join(proc, pid)
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmdline), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "status"), []byte(sampleStatus), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("1", "/ko-app/ateworker-microvm\x00--pod-uid=abc")
	write("20", "virtiofsd\x00--shared-dir=/run/shared")
	write("30", "sleep\x0030")
	write("40", "self\x00process")
	rows, err := processStatuses(proc, []string{"--shared-dir="}, 40)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %+v, want PID 1 and the virtiofsd match", rows)
	}
	if rows[0].PID != 1 || rows[0].Cmdline != "/ko-app/ateworker-microvm --pod-uid=abc" {
		t.Errorf("row 0 = %+v", rows[0])
	}
	if rows[1].PID != 20 || rows[1].Cmdline != "virtiofsd --shared-dir=/run/shared" {
		t.Errorf("row 1 = %+v", rows[1])
	}
}
