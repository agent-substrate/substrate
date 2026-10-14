//go:build linux

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

package pidfd

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// Exited returns a channel that is closed once pid exits. pid is pinned when
// Exited is called, so a later reuse of the number is never mistaken for it.
// A process already gone yields a closed channel.
func Exited(pid int) (<-chan struct{}, error) {
	done := make(chan struct{})
	fd, err := unix.PidfdOpen(pid, unix.PIDFD_NONBLOCK)
	if errors.Is(err, unix.ESRCH) {
		close(done)
		return done, nil
	}
	if err != nil {
		return nil, err
	}
	// A nonblocking fd handed to os.NewFile joins the runtime's netpoller, so
	// waiting on it parks a goroutine rather than a thread.
	f := os.NewFile(uintptr(fd), "pidfd")
	rc, err := f.SyscallConn()
	if err != nil {
		f.Close()
		return nil, err
	}
	go func() {
		defer close(done)
		defer f.Close()
		// The pidfd turns readable when the process exits. Check the fd itself
		// rather than trusting a wakeup: Read drops a readiness edge that
		// arrived before it started waiting.
		_ = rc.Read(func(fd uintptr) bool {
			fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			n, err := unix.Poll(fds, 0)
			return err == nil && n > 0
		})
	}()
	return done, nil
}
