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

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"syscall"
	"unsafe"

	"github.com/agent-substrate/substrate/internal/egresscapture"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"golang.org/x/sys/unix"
)

const (
	egressCapturePort       = uint16(15001)
	egressOriginalHTTPPort  = uint16(80)
	egressOriginalHTTPSPort = uint16(443)
)

var defaultEgressCaptureRedirects = []struct {
	originalPort uint16
	capturePort  uint16
}{
	{originalPort: egressOriginalHTTPPort, capturePort: egressCapturePort},
	{originalPort: egressOriginalHTTPSPort, capturePort: egressCapturePort},
}

var defaultEgressCaptureListeners = []egresscapture.Listener{
	{Port: egressCapturePort},
}

func (s *AteomService) startEgressCaptureIfEnabled(ctx context.Context, identity egresscapture.ActorIdentity) error {
	if !egresscapture.EnabledFromEnv() {
		return nil
	}
	cfg, err := egresscapture.ConfigFromEnv(defaultEgressCaptureListeners)
	if err != nil {
		return err
	}
	capture, err := egresscapture.Start(ctx, identity, cfg, originalDestination)
	if err != nil {
		return fmt.Errorf("while starting actor egress capture: %w", err)
	}
	s.egressCapture = capture
	return nil
}

func addEgressCaptureRedirectRules(c *nftables.Conn, table *nftables.Table, prerouting *nftables.Chain, sourceIP string) {
	for _, redirect := range defaultEgressCaptureRedirects {
		c.AddRule(&nftables.Rule{
			Table: table,
			Chain: prerouting,
			Exprs: tcpRedirectExprs(sourceIP, redirect.originalPort, redirect.capturePort),
		})
	}
}

func tcpRedirectExprs(sourceIP string, originalPort, capturePort uint16) []expr.Any {
	exprs := append(ipSourceEqual(sourceIP), tcpDestinationPortEqual(originalPort)...)
	exprs = append(exprs,
		&expr.Immediate{
			Register: 1,
			Data:     binaryutil.BigEndian.PutUint16(capturePort),
		},
		&expr.Redir{
			RegisterProtoMin: 1,
		},
	)
	return exprs
}

func originalDestination(conn net.Conn) (net.Addr, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("captured connection is %T, not *net.TCPConn", conn)
	}

	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return nil, err
	}

	var addr *net.TCPAddr
	var controlErr error
	if err := rawConn.Control(func(fd uintptr) {
		addr, controlErr = originalDstFromFD(int(fd))
	}); err != nil {
		return nil, err
	}
	if controlErr != nil {
		return nil, controlErr
	}
	return addr, nil
}

func originalDstFromFD(fd int) (*net.TCPAddr, error) {
	var raw unix.RawSockaddrInet4
	size := uint32(unsafe.Sizeof(raw))
	_, _, errno := unix.Syscall6(
		unix.SYS_GETSOCKOPT,
		uintptr(fd),
		uintptr(unix.SOL_IP),
		uintptr(unix.SO_ORIGINAL_DST),
		uintptr(unsafe.Pointer(&raw)),
		uintptr(unsafe.Pointer(&size)),
		0,
	)
	if errno != 0 {
		return nil, errno
	}
	if raw.Family != syscall.AF_INET {
		return nil, fmt.Errorf("SO_ORIGINAL_DST returned address family %d", raw.Family)
	}
	return &net.TCPAddr{
		IP:   net.IPv4(raw.Addr[0], raw.Addr[1], raw.Addr[2], raw.Addr[3]),
		Port: int(binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&raw.Port))[:])),
	}, nil
}

func actorIdentityFromRun(req *ateompb.RunWorkloadRequest) egresscapture.ActorIdentity {
	return egresscapture.ActorIdentity{
		Namespace: req.GetActorTemplateNamespace(),
		Template:  req.GetActorTemplateName(),
		ActorID:   req.GetActorId(),
	}
}

func actorIdentityFromRestore(req *ateompb.RestoreWorkloadRequest) egresscapture.ActorIdentity {
	return egresscapture.ActorIdentity{
		Namespace: req.GetActorTemplateNamespace(),
		Template:  req.GetActorTemplateName(),
		ActorID:   req.GetActorId(),
	}
}
