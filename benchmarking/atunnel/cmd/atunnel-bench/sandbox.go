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

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/load"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/procstats"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/sink"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/workerhost"
)

// The worker starts these inside the actors' sandboxes; they are not meant to
// be run by hand.

func netnsExecCommand() *cobra.Command {
	return &cobra.Command{
		Use:    "netns-exec NETNS COMMAND [ARGS]",
		Short:  "Run COMMAND in the network namespace at path NETNS",
		Hidden: true,
		// Everything after NETNS belongs to COMMAND.
		DisableFlagParsing: true,
		Args:               cobra.MinimumNArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return netnsExec(args[0], args[1:])
		},
	}
}

// netnsExec enters a network namespace and replaces this process with the
// command. setns applies to the calling thread only, and execve keeps just
// that thread, so the whole new program runs in the namespace.
func netnsExec(netns string, command []string) error {
	runtime.LockOSThread()
	fd, err := unix.Open(netns, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("opening %s: %w", netns, err)
	}
	if err := unix.Setns(fd, unix.CLONE_NEWNET); err != nil {
		return fmt.Errorf("entering %s: %w", netns, err)
	}
	return syscall.Exec(command[0], command, os.Environ())
}

func ingressActorCommand() *cobra.Command {
	var port, bodyBytes int
	var statsSocket string
	cmd := &cobra.Command{
		Use:   "ingress-actor",
		Short: "Stand in for an actor's program receiving ingress: an HTTP server, with its counters on a Unix socket",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runIngressActor(cmd.Context(), port, bodyBytes, statsSocket)
		},
	}
	cmd.Flags().IntVar(&port, "port", workerhost.ActorPort, "port to serve")
	cmd.Flags().IntVar(&bodyBytes, "body-bytes", 16, "response body size")
	cmd.Flags().StringVar(&statsSocket, "stats-socket", "", "Unix socket to serve the counters on")
	_ = cmd.MarkFlagRequired("stats-socket")
	return cmd
}

func runIngressActor(ctx context.Context, port, bodyBytes int, statsSocket string) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	ingressActor := sink.NewIngressActor(bodyBytes)
	statsLis, err := listenUnix(statsSocket)
	if err != nil {
		return err
	}
	statsServer := &http.Server{Handler: procstats.Handler(func() map[string]any {
		return map[string]any{"ingress_actor": ingressActor.Stats()}
	})}
	go func() { _ = statsServer.Serve(statsLis) }()
	context.AfterFunc(ctx, func() { _ = statsServer.Close() })
	return ingressActor.Serve(ctx, lis)
}

func egressActorCommand() *cobra.Command {
	var socket string
	cmd := &cobra.Command{
		Use:   "egress-actor",
		Short: "Stand in for an actor's program making outgoing connections, running egress loads for load",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runEgressActor(cmd.Context(), socket)
		},
	}
	cmd.Flags().StringVar(&socket, "socket", "", "Unix socket the load generator drives it on")
	_ = cmd.MarkFlagRequired("socket")
	return cmd
}

func runEgressActor(ctx context.Context, socket string) error {
	// The sandbox's kernel network stack stands in for gVisor's, with
	// Linux's defaults; widen its ports as the containers' are, or a long run
	// of short connections exhausts them on sockets in TIME_WAIT.
	for path, value := range map[string]string{
		"/proc/sys/net/ipv4/ip_local_port_range": "1024 65535",
		"/proc/sys/net/ipv4/tcp_tw_reuse":        "1",
	} {
		if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
			return fmt.Errorf("tuning the sandbox network: %w", err)
		}
	}
	lis, err := listenUnix(socket)
	if err != nil {
		return err
	}
	return load.ServeEgressActor(ctx, lis)
}

// listenUnix listens on a fresh Unix socket that the load generator, running
// as another user in another container, may connect to.
func listenUnix(path string) (net.Listener, error) {
	_ = os.Remove(path)
	lis, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o777); err != nil {
		_ = lis.Close()
		return nil, err
	}
	return lis, nil
}
