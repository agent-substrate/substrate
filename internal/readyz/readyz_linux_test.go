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

package readyz

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"golang.org/x/sys/unix"
)

// Fill a zero-backlog listener without accepting. Subsequent connections stall
// in the TCP handshake, exercising cancellation of an in-flight dial without
// depending on the host's routing to an unreachable external address.
func TestTCPBlockedConnect(t *testing.T) {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := unix.Bind(fd, &unix.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := unix.Listen(fd, 0); err != nil {
		t.Fatal(err)
	}
	sockaddr, err := unix.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	port := sockaddr.(*unix.SockaddrInet4).Port
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	occupant, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer occupant.Close()

	t.Run("attempt timeout", func(t *testing.T) {
		ok, err := tryTCP(t.Context(), address)
		var timeout net.Error
		if ok || !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("blocked connect = %v, %v; want timeout", ok, err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		timer := time.AfterFunc(30*time.Millisecond, cancel)
		defer timer.Stop()
		probe := &ateompb.Readyz{TcpSocket: &ateompb.TCPSocketAction{Port: int32(port)}}
		if err := Wait(ctx, "tcp", probe, "127.0.0.1"); !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked connect cancellation = %v", err)
		}
	})
	t.Run("overall deadline", func(t *testing.T) {
		probe := &ateompb.Readyz{TcpSocket: &ateompb.TCPSocketAction{Port: int32(port)}, TimeoutSeconds: 1}
		start := time.Now()
		if err := Wait(t.Context(), "tcp", probe, "127.0.0.1"); !errors.Is(err, ateerrors.ReasonWorkloadNotReady) {
			t.Fatalf("blocked connect deadline = %v", err)
		}
		if elapsed := time.Since(start); elapsed < time.Second || elapsed > 2*time.Second {
			t.Fatalf("blocked connect ignored overall 1s deadline: %v", elapsed)
		}
	})
}
