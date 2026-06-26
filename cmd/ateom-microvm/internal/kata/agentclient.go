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

package kata

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb"
	"github.com/containerd/ttrpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// agentVsockPort is the guest port the kata-agent's ttrpc server listens on.
const agentVsockPort = 1024

// debugConsoleVsockPort is the guest port kata's debug console listens on when
// debug_console_enabled=true. It's a raw shell over the hybrid vsock.
const debugConsoleVsockPort = 1026

// DebugConsoleDump connects to the guest's kata debug console (vsock 1026) and
// runs cmd, returning its combined output. Diagnostic only (requires
// debug_console_enabled=true in the kata config). Best-effort: returns the error
// text on failure rather than failing the caller.
func DebugConsoleDump(ctx context.Context, vsockPath, cmd string) string {
	d := net.Dialer{}
	dctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	conn, err := d.DialContext(dctx, "unix", vsockPath)
	if err != nil {
		return "debug-console dial: " + err.Error()
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", debugConsoleVsockPort); err != nil {
		return "debug-console CONNECT: " + err.Error()
	}
	br := bufio.NewReader(conn)
	if _, err := br.ReadString('\n'); err != nil { // the "OK <n>" line
		return "debug-console CONNECT reply: " + err.Error()
	}
	// The kata debug console is an INTERACTIVE shell on a PTY (console.rs spawns
	// /bin/bash|/bin/sh), so it ECHOES the command line back before running it. We
	// must not let the echo trip the end sentinel: write the sentinel split by ''
	// (which the shell strips) so the echoed command contains "__ATE''_END__" (no
	// match) while the shell's OUTPUT is "__ATE_END__" (match). Read until the
	// output sentinel (or EOF/deadline).
	if _, err := fmt.Fprintf(conn, "{ %s ; } 2>&1; echo __ATE''_END__\n", cmd); err != nil {
		return "debug-console write: " + err.Error()
	}
	const sentinel = "__ATE_END__"
	var out strings.Builder
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			if strings.Contains(line, sentinel) {
				break // the shell's echo of the sentinel line (output), not its command echo
			}
			out.WriteString(line)
		}
		if err != nil {
			break
		}
	}
	return out.String()
}

// AgentClient is a thin ttrpc client for the kata-agent RPCs ateom drives
// directly. ateom owns the cloud-hypervisor boot (no kata shim) and drives the
// kata-agent over ttrpc itself: alongside UpdateInterface/UpdateRoutes for guest
// networking, it issues CreateContainer/StartContainer to assemble the container
// rootfs directly, instead of relying on the kata runtime's hooks
// (ShareRootFilesystem) to emit the storages. It dials the agent through CH's
// hybrid-vsock unix socket — the same channel the kata shim would use.
type AgentClient struct {
	conn   net.Conn
	client *ttrpc.Client
}

// DialAgent connects to the kata-agent through the hybrid-vsock socket at
// vsockPath (VsockSocketPath(id)): plain-text "CONNECT <port>" handshake with
// the VMM, then ttrpc over the stream.
func DialAgent(ctx context.Context, vsockPath string) (*AgentClient, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", vsockPath)
	if err != nil {
		return nil, fmt.Errorf("dialing hybrid vsock %q: %w", vsockPath, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", agentVsockPort); err != nil {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT: %w", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT response: %w", err)
	}
	if !strings.HasPrefix(line, "OK ") {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT refused: %q", strings.TrimSpace(line))
	}
	_ = conn.SetDeadline(time.Time{}) // ttrpc manages its own timeouts via ctx
	return &AgentClient{conn: conn, client: ttrpc.NewClient(conn)}, nil
}

// Close shuts the ttrpc client and underlying connection.
func (a *AgentClient) Close() error {
	err := a.client.Close()
	_ = a.conn.Close()
	return err
}

// CreateContainer asks the agent to create a container: mount its storages (in
// order) and build the rootfs, then fork the parked init process. This is the
// hook point — the agent mounts storages[] (here: a bind of the virtio-fs lower
// followed by the tmpfs-upper overlay) before init_rootfs consumes the rootfs.
// Mirrors grpc.AgentService/CreateContainer (returns google.protobuf.Empty).
func (a *AgentClient) CreateContainer(ctx context.Context, req *agentpb.CreateContainerRequest) error {
	if err := a.client.Call(ctx, "grpc.AgentService", "CreateContainer", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent CreateContainer: %w", err)
	}
	return nil
}

// StartContainer execs the container's init process (pivots into the rootfs the
// storages assembled). Mirrors grpc.AgentService/StartContainer.
func (a *AgentClient) StartContainer(ctx context.Context, containerID string) error {
	req := &agentpb.StartContainerRequest{ContainerId: containerID}
	if err := a.client.Call(ctx, "grpc.AgentService", "StartContainer", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent StartContainer: %w", err)
	}
	return nil
}

// CreateSandbox establishes the agent's sandbox context (sandbox id, hostname,
// sandbox pidns) before any container is created. The kata shim normally issues
// this once at VM boot; on the ateom-owned-boot path (no shim) ateom must call it
// itself so the agent has a sandbox to attach containers to. Storages is empty —
// the actor rootfs arrives as a per-container "blk" storage, not a sandbox mount.
// Mirrors grpc.AgentService/CreateSandbox (returns google.protobuf.Empty).
func (a *AgentClient) CreateSandbox(ctx context.Context, req *agentpb.CreateSandboxRequest) error {
	if err := a.client.Call(ctx, "grpc.AgentService", "CreateSandbox", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent CreateSandbox: %w", err)
	}
	return nil
}

// UpdateInterface configures a guest network interface (the kata shim's job; on
// the owned-boot path ateom does it). The agent matches the link by HwAddr, then
// applies the name/IP/MTU. Mirrors grpc.AgentService/UpdateInterface (returns the
// resulting Interface).
func (a *AgentClient) UpdateInterface(ctx context.Context, iface *agentpb.Interface) error {
	req := &agentpb.UpdateInterfaceRequest{Interface: iface}
	if err := a.client.Call(ctx, "grpc.AgentService", "UpdateInterface", req, &agentpb.Interface{}); err != nil {
		return fmt.Errorf("agent UpdateInterface: %w", err)
	}
	return nil
}

// UpdateRoutes replaces the guest's route table with routes (the agent flushes
// and re-adds). Pass the connected (scope-link) route AND the default route so
// the gateway stays reachable. Mirrors grpc.AgentService/UpdateRoutes.
func (a *AgentClient) UpdateRoutes(ctx context.Context, routes []*agentpb.Route) error {
	req := &agentpb.UpdateRoutesRequest{Routes: &agentpb.Routes{Routes: routes}}
	if err := a.client.Call(ctx, "grpc.AgentService", "UpdateRoutes", req, &agentpb.Routes{}); err != nil {
		return fmt.Errorf("agent UpdateRoutes: %w", err)
	}
	return nil
}

// AddARPNeighbors installs static ARP entries in the guest — used to pin the
// gateway (169.254.17.1) to its FIXED MAC so a restored guest's frozen neighbor
// entry stays valid across pods. Mirrors grpc.AgentService/AddARPNeighbors.
func (a *AgentClient) AddARPNeighbors(ctx context.Context, neighbors []*agentpb.ARPNeighbor) error {
	req := &agentpb.AddARPNeighborsRequest{Neighbors: &agentpb.ARPNeighbors{ARPNeighbors: neighbors}}
	if err := a.client.Call(ctx, "grpc.AgentService", "AddARPNeighbors", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent AddARPNeighbors: %w", err)
	}
	return nil
}

// ReadStdout reads up to max bytes from the container process's stdout. It is a
// unary RPC (NOT a server stream): each call returns whatever bytes the agent has
// buffered (up to max), so callers loop until it returns an error — the agent
// returns an error/EOF-like status once the stream ends (container exit / connection
// close). Mirrors grpc.AgentService/ReadStdout. The kata-agent keys the stream by
// ExecId, which the owned-boot path sets equal to ContainerId (see StartBlkWorkload).
func (a *AgentClient) ReadStdout(ctx context.Context, containerID, execID string, max uint32) ([]byte, error) {
	resp := &agentpb.ReadStreamResponse{}
	req := &agentpb.ReadStreamRequest{ContainerId: containerID, ExecId: execID, Len: max}
	if err := a.client.Call(ctx, "grpc.AgentService", "ReadStdout", req, resp); err != nil {
		return nil, err
	}
	return resp.GetData(), nil
}

// ReadStderr reads up to max bytes from the container process's stderr. Same
// semantics as ReadStdout (unary, loop-until-error). Mirrors
// grpc.AgentService/ReadStderr.
func (a *AgentClient) ReadStderr(ctx context.Context, containerID, execID string, max uint32) ([]byte, error) {
	resp := &agentpb.ReadStreamResponse{}
	req := &agentpb.ReadStreamRequest{ContainerId: containerID, ExecId: execID, Len: max}
	if err := a.client.Call(ctx, "grpc.AgentService", "ReadStderr", req, resp); err != nil {
		return nil, err
	}
	return resp.GetData(), nil
}

// StreamReader adapts the agent's repeated ReadStdout/ReadStderr unary calls into
// an io.Reader, so the consumer can pump the container's output through the shared
// actorlog forwarder like any other stream. Each Read issues one RPC with Len set
// to len(p); on RPC error (the agent signals EOF/container-exit via an error
// status) it returns io.EOF so the consuming goroutine terminates cleanly. The
// reader stops when its context is cancelled OR the underlying ttrpc connection is
// closed (both surface as RPC errors), so it never outlives the AgentClient.
type StreamReader struct {
	ctx         context.Context
	ac          *AgentClient
	containerID string
	execID      string
	stderr      bool
}

// NewStdioReader returns an io.Reader over the container's stdout (stderr=false)
// or stderr (stderr=true). execID matches the value passed to StartBlkWorkload
// (equal to containerID on the owned-boot path).
func NewStdioReader(ctx context.Context, ac *AgentClient, containerID, execID string, stderr bool) *StreamReader {
	return &StreamReader{ctx: ctx, ac: ac, containerID: containerID, execID: execID, stderr: stderr}
}

// Read issues a single ReadStdout/ReadStderr RPC for up to len(p) bytes, copying
// the returned data into p. It returns io.EOF on any RPC error so the consumer
// stops cleanly when the container exits or the connection closes.
func (r *StreamReader) Read(p []byte) (int, error) {
	var (
		data []byte
		err  error
	)
	if r.stderr {
		data, err = r.ac.ReadStderr(r.ctx, r.containerID, r.execID, uint32(len(p)))
	} else {
		data, err = r.ac.ReadStdout(r.ctx, r.containerID, r.execID, uint32(len(p)))
	}
	if err != nil {
		return 0, io.EOF
	}
	n := copy(p, data)
	return n, nil
}
