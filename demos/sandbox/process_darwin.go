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
	"syscall"
)

// processExited observes a direct child's exit without reaping it.
func processExited(pid int) (<-chan error, error) {
	fd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}
	event := syscall.Kevent_t{
		Ident: uint64(pid), Filter: syscall.EVFILT_PROC,
		Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Fflags: syscall.NOTE_EXIT,
	}
	_, err = syscall.Kevent(fd, []syscall.Kevent_t{event}, nil, nil)
	if err != nil {
		syscall.Close(fd)
		if errors.Is(err, syscall.ESRCH) {
			done := make(chan error, 1)
			done <- nil
			return done, nil
		}
		return nil, err
	}
	done := make(chan error, 1)
	go func() {
		defer syscall.Close(fd)
		var events [1]syscall.Kevent_t
		for {
			_, err := syscall.Kevent(fd, nil, events[:], nil)
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			done <- err
			return
		}
	}()
	return done, nil
}
