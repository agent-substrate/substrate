// Copyright 2021 The Kubernetes Authors.
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

package ateclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/streaming/pkg/httpstream"
)

type portForwardDialer func(context.Context, ...string) (httpstream.Connection, string, error)

func newSPDYPortForwardDialer(upgrader spdy.Upgrader, client *http.Client, method string, target *url.URL) portForwardDialer {
	return func(ctx context.Context, protocols ...string) (httpstream.Connection, string, error) {
		req, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
		if err != nil {
			return nil, "", fmt.Errorf("creating port-forward request: %w", err)
		}
		return spdy.NegotiateStreaming(upgrader, client, req, protocols...)
	}
}

// dialPodPort exposes a Kubernetes port-forward data stream directly as a
// net.Conn. This avoids an intermediate localhost TCP listener and its noisy
// connection-reset errors when the gRPC client shuts down.
func dialPodPort(ctx context.Context, dialer portForwardDialer, remotePort int) (conn net.Conn, finalErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	streamConn, protocol, err := dialer(ctx, portforward.PortForwardProtocolV1Name)
	if err != nil {
		return nil, fmt.Errorf("dialing port-forward connection: %w", err)
	}
	defer func() {
		if finalErr != nil {
			_ = streamConn.Close()
		}
	}()

	setupDone := make(chan struct{})
	monitorDone := make(chan struct{})
	var setupFinished atomic.Bool
	go func() {
		defer close(monitorDone)
		select {
		case <-ctx.Done():
			if setupFinished.CompareAndSwap(false, true) {
				_ = streamConn.Close()
			}
		case <-setupDone:
		}
	}()
	defer func() {
		close(setupDone)
		<-monitorDone
	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if protocol != portforward.PortForwardProtocolV1Name {
		return nil, fmt.Errorf("port-forward protocol mismatch: server selected %q", protocol)
	}

	headers := http.Header{}
	headers.Set(corev1.StreamType, corev1.StreamTypeError)
	headers.Set(corev1.PortHeader, strconv.Itoa(remotePort))
	headers.Set(corev1.PortForwardRequestIDHeader, "0")

	errorStream, err := streamConn.CreateStream(headers)
	if err != nil {
		return nil, fmt.Errorf("creating port-forward error stream: %w", err)
	}
	// The error stream is read-only from the client's perspective.
	_ = errorStream.Close()

	headers.Set(corev1.StreamType, corev1.StreamTypeData)
	dataStream, err := streamConn.CreateStream(headers)
	if err != nil {
		return nil, fmt.Errorf("creating port-forward data stream: %w", err)
	}

	stream := &portForwardStream{
		Stream:      dataStream,
		errorStream: errorStream,
		streamConn:  streamConn,
		remotePort:  remotePort,
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !setupFinished.CompareAndSwap(false, true) {
		return nil, ctx.Err()
	}
	go stream.watchErrors()
	return stream, nil
}

type portForwardStream struct {
	httpstream.Stream
	errorStream httpstream.Stream
	streamConn  httpstream.Connection
	remotePort  int
	closeOnce   sync.Once
	closed      atomic.Bool
	closeErr    error
}

var _ net.Conn = (*portForwardStream)(nil)

func (s *portForwardStream) watchErrors() {
	message, err := io.ReadAll(s.errorStream)
	if s.closed.Load() {
		return
	}
	switch {
	case err != nil:
		slog.Error("Error reading from port-forward error stream",
			slog.Int("remotePort", s.remotePort), slog.Any("err", err))
	case len(message) > 0:
		slog.Error("Port-forward connection failed",
			slog.Int("remotePort", s.remotePort), slog.String("error", string(message)))
	default:
		return
	}
	_ = s.Close()
}

func (s *portForwardStream) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.closeErr = errors.Join(s.Stream.Close(), s.streamConn.Close())
	})
	return s.closeErr
}

func (s *portForwardStream) LocalAddr() net.Addr {
	return portForwardAddr("kubernetes-api")
}

func (s *portForwardStream) RemoteAddr() net.Addr {
	return portForwardAddr(fmt.Sprintf("pod:%d", s.remotePort))
}

// SPDY streams do not expose deadline controls. gRPC applies RPC deadlines at
// the transport layer, so these methods intentionally match Kubernetes'
// direct port-forward net.Conn adapter and remain no-ops.
func (s *portForwardStream) SetDeadline(time.Time) error {
	return nil
}

func (s *portForwardStream) SetReadDeadline(time.Time) error {
	return nil
}

func (s *portForwardStream) SetWriteDeadline(time.Time) error {
	return nil
}

type portForwardAddr string

func (a portForwardAddr) Network() string {
	return "port-forward"
}

func (a portForwardAddr) String() string {
	return string(a)
}
