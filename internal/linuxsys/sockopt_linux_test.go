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
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// connPair returns a connection dialed with dialer and the one accepted for it.
func connPair(t *testing.T, dialer *net.Dialer) (client, server net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	client, err = dialer.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	server, err = listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return client, server
}

// readLowat returns the TCP_NOTSENT_LOWAT in effect on conn: 0 while the
// socket uses the net.ipv4.tcp_notsent_lowat sysctl.
func readLowat(t *testing.T, conn net.Conn) int {
	t.Helper()
	rawConn, err := tcpRawConn(conn)
	if err != nil {
		t.Fatal(err)
	}
	var got int
	var sockoptErr error
	if err := rawConn.Control(func(fd uintptr) {
		got, sockoptErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT)
	}); err != nil {
		t.Fatal(err)
	}
	if sockoptErr != nil {
		t.Fatal(sockoptErr)
	}
	return got
}

func TestTCPNotSentLowatDialAndSet(t *testing.T) {
	client, server := connPair(t, &net.Dialer{Control: TCPNotSentLowatControl(16384)})
	if got := readLowat(t, client); got != 16384 {
		t.Errorf("dialed connection TCP_NOTSENT_LOWAT = %d, want 16384", got)
	}
	// The accepted connection starts at 0, or at -1 under an MPTCP listener.
	if got := readLowat(t, server); got == 16384 {
		t.Fatal("accepted connection has TCP_NOTSENT_LOWAT 16384 before SetTCPNotSentLowat")
	}
	if err := SetTCPNotSentLowat(server, 16384); err != nil {
		t.Fatal(err)
	}
	if got := readLowat(t, server); got != 16384 {
		t.Errorf("accepted connection TCP_NOTSENT_LOWAT = %d, want 16384", got)
	}
}

func TestTCPNotSentLowatZeroSetsNothing(t *testing.T) {
	client, _ := connPair(t, &net.Dialer{Control: TCPNotSentLowatControl(0)})
	if err := SetTCPNotSentLowat(client, 0); err != nil {
		t.Fatal(err)
	}
	if got := readLowat(t, client); got != 0 {
		t.Errorf("TCP_NOTSENT_LOWAT = %d, want 0", got)
	}
}

func TestTCPNotSentLowatRejectsNegative(t *testing.T) {
	client, _ := connPair(t, &net.Dialer{})
	if err := SetTCPNotSentLowat(client, -1); err == nil {
		t.Error("SetTCPNotSentLowat accepted -1")
	}
}
