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

package atunnel

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"unsafe"

	"golang.org/x/sys/unix"
)

// TCPOriginalDestination reads the IPv4 destination preserved by a Linux
// REDIRECT rule. Actor networking is currently IPv4-only.
// TODO(liorlieberman) add the IPv6 IP6T_SO_ORIGINAL_DST variant
// when actor veth setup gains dual-stack support.
func TCPOriginalDestination(conn net.Conn) (string, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return "", fmt.Errorf("atunnel: original destination requires a TCP connection, got %T", conn)
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return "", fmt.Errorf("atunnel: acquiring TCP syscall connection: %w", err)
	}

	var addr unix.RawSockaddrInet4
	var sockoptErr error
	if err := rawConn.Control(func(fd uintptr) {
		size := uint32(unsafe.Sizeof(addr))
		_, _, errno := unix.Syscall6(
			unix.SYS_GETSOCKOPT,
			fd,
			unix.SOL_IP,
			unix.SO_ORIGINAL_DST,
			uintptr(unsafe.Pointer(&addr)),
			uintptr(unsafe.Pointer(&size)),
			0,
		)
		if errno != 0 {
			sockoptErr = errno
		}
	}); err != nil {
		return "", fmt.Errorf("atunnel: accessing TCP socket: %w", err)
	}
	if sockoptErr != nil {
		return "", fmt.Errorf("atunnel: reading original TCP destination: %w", sockoptErr)
	}

	portBytes := (*[2]byte)(unsafe.Pointer(&addr.Port))
	port := binary.BigEndian.Uint16(portBytes[:])
	if port == 0 {
		return "", fmt.Errorf("atunnel: original TCP destination has port zero")
	}
	return net.JoinHostPort(net.IP(addr.Addr[:]).String(), strconv.Itoa(int(port))), nil
}
