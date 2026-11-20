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
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

const kernelTestEnv = "ATE_SELFSECCOMP_KERNEL_TEST"

// TestInstallKernel installs the filter in a subprocess so the permanent
// kernel filter does not affect the parent test runner. It verifies that
// allowed/denied syscalls (including socket domain filtering) behave as
// expected on the real kernel and that TSYNC propagates the filter to OS
// threads that already existed before Install ran.
func TestInstallKernel(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestInstallKernelHelper$")
	cmd.Env = append(os.Environ(), kernelTestEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\n%s", err, out)
	}
}

func TestInstallKernelHelper(t *testing.T) {
	if os.Getenv(kernelTestEnv) != "1" {
		t.Skip("helper subprocess for TestInstallKernel")
	}

	const numThreads = 8
	var (
		ready   sync.WaitGroup
		done    sync.WaitGroup
		recheck = make(chan struct{})
		errCh   = make(chan error, numThreads)
	)
	ready.Add(numThreads)
	done.Add(numThreads)

	// Start threads before Install to exercise SECCOMP_FILTER_FLAG_TSYNC;
	// threads started after Install inherit the filter automatically via clone.
	for i := range numThreads {
		go func(id int) {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			defer done.Done()

			ready.Done()
			<-recheck

			if err := checkFiltered(); err != nil {
				errCh <- fmt.Errorf("thread %d: %w", id, err)
			}
		}(i)
	}
	ready.Wait()

	if err := Install(); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := checkFiltered(); err != nil {
		t.Fatalf("calling thread: %v", err)
	}

	close(recheck)
	done.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func checkFiltered() error {
	if _, _, errno := unix.Syscall(unix.SYS_GETPID, 0, 0, 0); errno != 0 {
		return fmt.Errorf("getpid returned %v, want 0", errno)
	}
	if _, _, errno := unix.Syscall(unix.SYS_BPF, 0, 0, 0); errno != unix.EPERM {
		return fmt.Errorf("bpf returned %v, want EPERM", errno)
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return fmt.Errorf("socket(AF_INET) returned %v, want success", err)
	}
	_ = unix.Close(fd)

	if fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM, 0); err != unix.EPERM {
		if err == nil {
			_ = unix.Close(fd)
		}
		return fmt.Errorf("socket(AF_VSOCK) returned %v, want EPERM", err)
	}
	return nil
}
