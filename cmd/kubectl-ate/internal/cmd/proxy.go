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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/resources"
)

const (
	defaultProxyLocalPort = 8000
	defaultActorPort      = 80
)

var (
	proxyActorAtespaceFlag string
	proxyActorAddressFlag  string
)

type portMapping struct {
	localPort  int
	remotePort int
}

type actorDialFunc func(ctx context.Context, actorRef resources.ActorRef, port int) (net.Conn, error)

var proxyCmd = &cobra.Command{Use: "proxy", Short: "Serve a local proxy to a resource"}

var proxyActorCmd = &cobra.Command{
	Use:   "actor <actor-name> [[local-port:]remote-port ...]",
	Short: "Forward one or more local ports to an actor",
	Long: `Forward one or more local TCP ports to an actor through Substrate's router,
resuming the actor if it is suspended. When no ports are
given, local port 8000 is forwarded to actor port 80.

Requires permission to create, get, and delete podcertificaterequests in
Substrate's system namespace.`,
	Args: func(cmd *cobra.Command, args []string) error {
		if err := checkSwallowedFlags(cmd); err != nil {
			return err
		}
		missingFlags := missingRequiredFlags(cmd, []string{"atespace"})
		if len(args) == 0 {
			if len(missingFlags) > 0 {
				return usageErrorf("missing required argument <actor-name> and %s", formatMissingFlagsClause(missingFlags))
			}
			return usageErrorf("missing required argument <actor-name>")
		}
		if len(missingFlags) > 0 {
			return usageErrorf("missing %s", formatMissingFlagsClause(missingFlags))
		}
		_, err := parsePortMappings(args[1:])
		return err
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		actorRef := resources.ActorRef{Atespace: proxyActorAtespaceFlag, Name: args[0]}
		mappings, err := parsePortMappings(args[1:])
		if err != nil {
			return err
		}
		// TODO: Authenticate with a JWT once atenet-router supports JWT-based
		// authentication instead of minting a client certificate via PodCertificateRequest.
		dialer, err := ateclient.NewRouterDialer(cmd.Context(), kubeconfig, k8sContext)
		if err != nil {
			return fmt.Errorf("failed to connect to atenet-router: %w", err)
		}
		listeners := make([]net.Listener, 0, len(mappings))
		defer func() {
			for _, lis := range listeners {
				_ = lis.Close()
			}
		}()
		for _, m := range mappings {
			lis, err := net.Listen("tcp", net.JoinHostPort(proxyActorAddressFlag, strconv.Itoa(m.localPort)))
			if err != nil {
				return err
			}
			listeners = append(listeners, lis)
			fmt.Fprintf(cmd.ErrOrStderr(), "Proxying %s -> %s:%d\n", lis.Addr(), actorRef, m.remotePort)
		}
		return serveActorProxy(cmd.Context(), cmd.ErrOrStderr(), listeners, mappings, actorRef, dialer.DialActor)
	},
}

func parsePortMappings(specs []string) ([]portMapping, error) {
	if len(specs) == 0 {
		return []portMapping{{localPort: defaultProxyLocalPort, remotePort: defaultActorPort}}, nil
	}
	mappings := make([]portMapping, 0, len(specs))
	seenLocal := make(map[int]bool, len(specs))
	for _, spec := range specs {
		localStr, remoteStr, hasColon := strings.Cut(spec, ":")
		if !hasColon {
			port, err := parsePortNumber(spec, 1)
			if err != nil {
				return nil, usageErrorf("invalid port %q: must be 1-65535 or [local-port:]remote-port", spec)
			}
			if seenLocal[port] {
				return nil, usageErrorf("duplicate local port %d", port)
			}
			seenLocal[port] = true
			mappings = append(mappings, portMapping{localPort: port, remotePort: port})
			continue
		}
		remotePort, err := parsePortNumber(remoteStr, 1)
		if err != nil {
			return nil, usageErrorf("invalid remote port in %q: must be 1-65535", spec)
		}
		var localPort int
		if localStr != "" {
			localPort, err = parsePortNumber(localStr, 0)
			if err != nil {
				return nil, usageErrorf("invalid local port in %q: must be 0-65535", spec)
			}
		}
		if localPort != 0 {
			if seenLocal[localPort] {
				return nil, usageErrorf("duplicate local port %d", localPort)
			}
			seenLocal[localPort] = true
		}
		mappings = append(mappings, portMapping{localPort: localPort, remotePort: remotePort})
	}
	return mappings, nil
}

func parsePortNumber(s string, minPort int) (int, error) {
	p, err := strconv.Atoi(s)
	if err != nil || p < minPort || p > 65535 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return p, nil
}

func serveActorProxy(ctx context.Context, errOut io.Writer, listeners []net.Listener, mappings []portMapping, actorRef resources.ActorRef, dial actorDialFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopAfter := context.AfterFunc(ctx, func() {
		for _, lis := range listeners {
			_ = lis.Close()
		}
	})
	defer stopAfter()

	errCh := make(chan error, len(listeners))
	var wg sync.WaitGroup
	for i, lis := range listeners {
		remotePort := mappings[i].remotePort
		wg.Add(1)
		go func(lis net.Listener, remotePort int) {
			defer wg.Done()
			for {
				clientConn, err := lis.Accept()
				if err != nil {
					if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
						errCh <- nil
						return
					}
					errCh <- err
					cancel()
					return
				}
				wg.Add(1)
				go func(clientConn net.Conn) {
					defer wg.Done()
					handleActorConn(ctx, errOut, clientConn, actorRef, remotePort, dial)
				}(clientConn)
			}
		}(lis, remotePort)
	}

	var firstErr error
	for range listeners {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	wg.Wait()
	return firstErr
}

func handleActorConn(ctx context.Context, errOut io.Writer, clientConn net.Conn, actorRef resources.ActorRef, remotePort int, dial actorDialFunc) {
	defer clientConn.Close()
	upstreamConn, err := dial(ctx, actorRef, remotePort)
	if err != nil {
		if ctx.Err() == nil && errOut != nil {
			fmt.Fprintf(errOut, "Failed to connect to %s:%d: %v\n", actorRef, remotePort, err)
		}
		return
	}
	defer upstreamConn.Close()

	stopAfter := context.AfterFunc(ctx, func() {
		_ = clientConn.Close()
		_ = upstreamConn.Close()
	})
	defer stopAfter()

	var wg sync.WaitGroup
	wg.Add(2)
	copyHalf := func(dst, src net.Conn) {
		defer wg.Done()
		_, _ = io.Copy(dst, src)
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}
	go copyHalf(upstreamConn, clientConn)
	go copyHalf(clientConn, upstreamConn)
	wg.Wait()
}

func init() {
	proxyActorCmd.Flags().StringVarP(&proxyActorAtespaceFlag, "atespace", "a", "", "the actor's atespace")
	proxyActorCmd.Flags().StringVar(&proxyActorAddressFlag, "address", "127.0.0.1", "the local address to listen on")
	proxyCmd.AddCommand(proxyActorCmd)
}
