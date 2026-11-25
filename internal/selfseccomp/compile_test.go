//go:build linux

// Copyright 2026 The Agent Substrate Authors
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

package selfseccomp

import (
	"encoding/binary"
	"testing"

	"golang.org/x/net/bpf"
	"golang.org/x/sys/unix"
)

// seccompDataWithArg0 encodes struct seccomp_data fields read by the filter:
// nr (0..3), arch (4..7), args[0] low (16..19), and args[0] high (20..23).
// Each 32-bit word is big-endian because x/net/bpf's VM decodes LoadAbsolute
// in network byte order rather than host byte order.
func seccompDataWithArg0(nr int32, arch, argLow, argHigh uint32) []byte {
	b := make([]byte, 24)
	binary.BigEndian.PutUint32(b[0:4], uint32(nr))
	binary.BigEndian.PutUint32(b[4:8], arch)
	binary.BigEndian.PutUint32(b[16:20], argLow)
	binary.BigEndian.PutUint32(b[20:24], argHigh)
	return b
}

func seccompData(nr int32, arch uint32) []byte {
	return seccompDataWithArg0(nr, arch, 0, 0)
}

func runFilter(t *testing.T, vm *bpf.VM, in []byte) uint32 {
	t.Helper()
	ret, err := vm.Run(in)
	if err != nil {
		t.Fatalf("vm.Run: %v", err)
	}
	return uint32(ret)
}

func TestCompileAllowsExactlyTheProfileList(t *testing.T) {
	for _, tt := range []struct {
		goarch string
		arch   uint32
	}{
		{"amd64", unix.AUDIT_ARCH_X86_64},
		{"arm64", unix.AUDIT_ARCH_AARCH64},
	} {
		t.Run(tt.goarch, func(t *testing.T) {
			insts, err := compileInstructions(tt.goarch)
			if err != nil {
				t.Fatalf("compileInstructions(%s): %v", tt.goarch, err)
			}
			if len(insts) > unix.BPF_MAXINSNS {
				t.Fatalf("instruction count %d exceeds BPF_MAXINSNS (%d)", len(insts), unix.BPF_MAXINSNS)
			}
			vm, err := bpf.NewVM(insts)
			if err != nil {
				t.Fatalf("bpf.NewVM: %v", err)
			}

			allowed := map[uint32]bool{}
			for _, n := range allowedSyscallsByArch[tt.goarch] {
				allowed[n] = true
			}
			if len(allowed) == 0 {
				t.Fatalf("no allowed syscalls for %s", tt.goarch)
			}

			// The sweep below passes args[0]=0, so conditional syscalls whose
			// rules allow 0 (socket, personality) also return ALLOW.
			allowedAtZeroArg := map[uint32]bool{}
			for _, c := range conditionalSyscallsByArch[tt.goarch] {
				for _, r := range c.Rules {
					if (r.Op == opLessThan && 0 < r.Value) || (r.Op == opEqual && r.Value == 0) {
						allowedAtZeroArg[c.Nr] = true
						break
					}
				}
			}

			var maxN uint32
			for n := range allowed {
				if n > maxN {
					maxN = n
				}
			}
			for n := range allowedAtZeroArg {
				if n > maxN {
					maxN = n
				}
			}

			for n := uint32(0); n <= maxN+50; n++ {
				want := uint32(retDeny)
				if allowed[n] || allowedAtZeroArg[n] {
					want = retAllow
				}
				if got := runFilter(t, vm, seccompData(int32(n), tt.arch)); got != want {
					t.Errorf("nr=%d: got %#x, want %#x", n, got, want)
				}
			}

			for _, n := range []int32{1000, 4000, -1} {
				if got := runFilter(t, vm, seccompData(n, tt.arch)); got != retDeny {
					t.Errorf("nr=%d: got %#x, want EPERM (%#x)", n, got, retDeny)
				}
			}

			wrongArch := uint32(unix.AUDIT_ARCH_AARCH64)
			if tt.goarch == "arm64" {
				wrongArch = uint32(unix.AUDIT_ARCH_X86_64)
			}
			if got := runFilter(t, vm, seccompData(0, wrongArch)); got != unix.SECCOMP_RET_KILL_PROCESS {
				t.Errorf("wrong arch: got %#x, want SECCOMP_RET_KILL_PROCESS", got)
			}
		})
	}
}

func TestCompileConditionalSyscalls(t *testing.T) {
	const (
		afUnix  = 1
		afInet  = 2
		afInet6 = 10
		afAlg   = 38
		afVsock = 40
	)
	for _, tt := range []struct {
		goarch string
		arch   uint32
	}{
		{"amd64", unix.AUDIT_ARCH_X86_64},
		{"arm64", unix.AUDIT_ARCH_AARCH64},
	} {
		t.Run(tt.goarch, func(t *testing.T) {
			insts, err := compileInstructions(tt.goarch)
			if err != nil {
				t.Fatalf("compileInstructions(%s): %v", tt.goarch, err)
			}
			vm, err := bpf.NewVM(insts)
			if err != nil {
				t.Fatalf("bpf.NewVM: %v", err)
			}

			conds := conditionalSyscallsByArch[tt.goarch]
			if len(conds) != 2 {
				t.Fatalf("conditionalSyscallsByArch[%s] has %d entries, want 2 (socket, personality)", tt.goarch, len(conds))
			}

			var socketNr, personalityNr uint32
			for _, c := range conds {
				switch c.Name {
				case "socket":
					socketNr = c.Nr
				case "personality":
					personalityNr = c.Nr
				default:
					t.Fatalf("unexpected conditional syscall %q", c.Name)
				}
			}

			for _, d := range []struct {
				domain uint32
				want   uint32
			}{
				{afUnix, retAllow},
				{afInet, retAllow},
				{afInet6, retAllow},
				{37, retAllow}, // highest domain allowed by < 38
				{afAlg, retDeny},
				{39, retAllow}, // AF_NFC
				{afVsock, retDeny},
				{41, retAllow}, // AF_KCM
				{45, retAllow}, // AF_MCTP
				{46, retDeny},
			} {
				if got := runFilter(t, vm, seccompDataWithArg0(int32(socketNr), tt.arch, d.domain, 0)); got != d.want {
					t.Errorf("socket(%d): got %#x, want %#x", d.domain, got, d.want)
				}
			}
			if got := runFilter(t, vm, seccompDataWithArg0(int32(socketNr), tt.arch, afInet, 1)); got != retDeny {
				t.Errorf("socket(AF_INET | 1<<32): got %#x, want EPERM (%#x)", got, retDeny)
			}

			for _, d := range []struct {
				flags uint32
				want  uint32
			}{
				{0x0, retAllow},        // PER_LINUX
				{0x8, retAllow},        // PER_LINUX32
				{0x20000, retAllow},    // UNAME26
				{0x20008, retAllow},    // UNAME26|PER_LINUX32
				{0xffffffff, retAllow}, // query current personality
				{0x1, retDeny},
				{0x0040000, retDeny}, // ADDR_NO_RANDOMIZE
				{0x0400000, retDeny}, // READ_IMPLIES_EXEC
			} {
				if got := runFilter(t, vm, seccompDataWithArg0(int32(personalityNr), tt.arch, d.flags, 0)); got != d.want {
					t.Errorf("personality(%#x): got %#x, want %#x", d.flags, got, d.want)
				}
			}
			if got := runFilter(t, vm, seccompDataWithArg0(int32(personalityNr), tt.arch, 0x8, 1)); got != retDeny {
				t.Errorf("personality(PER_LINUX32 | 1<<32): got %#x, want EPERM (%#x)", got, retDeny)
			}
		})
	}
}

func TestCompileUnknownArch(t *testing.T) {
	if _, err := compile("riscv64"); err == nil {
		t.Error("compile(riscv64) = nil, want error")
	}
}
