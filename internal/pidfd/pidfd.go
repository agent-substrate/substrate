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
	"log/slog"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Exited returns a channel that is closed once pid exits. pid is pinned when
// Exited is called, so a later reuse of the number is never mistaken for it.
// A process already gone yields a closed channel.
func Exited(pid int) (<-chan struct{}, error) {
	done := make(chan struct{})
	fd, err := unix.PidfdOpen(pid, unix.PIDFD_NONBLOCK)
	if errors.Is(err, unix.EINVAL) {
		// PIDFD_NONBLOCK is Linux 5.10+; watch falls back to a blocking poll
		fd, err = unix.PidfdOpen(pid, 0)
	}
	if errors.Is(err, unix.ESRCH) {
		close(done)
		return done, nil
	}
	if err != nil {
		return nil, err
	}
	return watch(fd, done)
}

// ExitedFD is Exited for a process already pinned by pidfd. It watches a
// duplicate, so the caller keeps pidfd and may close it at any time.
func ExitedFD(pidfd int) (<-chan struct{}, error) {
	fd, err := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	// the duplicate shares the file status flags, so this makes pidfd
	// nonblocking too, which signaling it does not mind
	if err := unix.SetNonblock(fd, true); err != nil {
		unix.Close(fd)
		return nil, err
	}
	return watch(fd, make(chan struct{}))
}

// watch closes done once the nonblocking pidfd fd turns readable, and takes
// ownership of fd.
func watch(fd int, done chan struct{}) (<-chan struct{}, error) {
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
		err := rc.Read(func(fd uintptr) bool {
			return readable(int(fd), 0)
		})
		if err != nil {
			// the netpoller cannot watch this fd, so wait in a blocking poll:
			// it holds a thread, but never reports an exit that did not happen
			slog.Warn("pidfd: falling back to a blocking poll", slog.Any("err", err))
			_ = rc.Control(func(fd uintptr) {
				for !readable(int(fd), -1) {
					time.Sleep(time.Second)
				}
			})
		}
	}()
	return done, nil
}

// readable polls fd for POLLIN for up to timeoutMs, -1 meaning forever.
func readable(fd, timeoutMs int) bool {
	fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for {
		n, err := unix.Poll(fds, timeoutMs)
		if err == unix.EINTR {
			continue
		}
		return err == nil && n > 0
	}
}
