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

//go:build linux

package linuxsys

import (
	"fmt"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// TCPNotSentLowatControl returns a net.Dialer or net.ListenConfig Control
// function that sets TCP_NOTSENT_LOWAT to bytes: the socket accepts writes
// only while fewer than bytes wait to be sent, so a slow reader blocks the
// writer instead of filling the kernel send queue. With bytes 0 it sets
// nothing.
func TCPNotSentLowatControl(bytes int) func(network, address string, c syscall.RawConn) error {
	return func(_, _ string, c syscall.RawConn) error {
		return setTCPNotSentLowat(c, bytes)
	}
}

// SetTCPNotSentLowat sets TCP_NOTSENT_LOWAT on an open TCP connection, such as
// one returned by Accept. With bytes 0 it does nothing.
func SetTCPNotSentLowat(conn net.Conn, bytes int) error {
	if bytes == 0 {
		return nil
	}
	rawConn, err := tcpRawConn(conn)
	if err != nil {
		return err
	}
	return setTCPNotSentLowat(rawConn, bytes)
}

func setTCPNotSentLowat(c syscall.RawConn, bytes int) error {
	if bytes < 0 {
		return fmt.Errorf("linuxsys: TCP_NOTSENT_LOWAT must not be negative, got %d", bytes)
	}
	if bytes == 0 {
		return nil
	}
	var sockoptErr error
	if err := c.Control(func(fd uintptr) {
		sockoptErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, bytes)
	}); err != nil {
		return fmt.Errorf("linuxsys: accessing socket: %w", err)
	}
	if sockoptErr != nil {
		return fmt.Errorf("linuxsys: setting TCP_NOTSENT_LOWAT to %d: %w", bytes, sockoptErr)
	}
	return nil
}

func tcpRawConn(conn net.Conn) (syscall.RawConn, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("linuxsys: TCP_NOTSENT_LOWAT requires a TCP connection, got %T", conn)
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("linuxsys: acquiring TCP syscall connection: %w", err)
	}
	return rawConn, nil
}
