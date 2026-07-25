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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/streaming/pkg/httpstream"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type fakePortForwardDialer struct {
	conn      httpstream.Connection
	protocol  string
	protocols []string
	dialCount int
}

func (d *fakePortForwardDialer) Dial(_ context.Context, protocols ...string) (httpstream.Connection, string, error) {
	d.dialCount++
	d.protocols = append(d.protocols, protocols...)
	return d.conn, d.protocol, nil
}

type fakePortForwardConnection struct {
	closeCount    int
	closeOnce     sync.Once
	closed        chan struct{}
	headers       []http.Header
	streams       []*fakePortForwardStream
	streamReaders []io.Reader
	createErrAt   int
	createErr     error
	onCreate      func(int)
}

func (c *fakePortForwardConnection) CreateStream(headers http.Header) (httpstream.Stream, error) {
	streamNumber := len(c.headers) + 1
	c.headers = append(c.headers, headers.Clone())
	if c.onCreate != nil {
		c.onCreate(streamNumber)
	}
	if c.createErrAt == streamNumber {
		return nil, c.createErr
	}
	stream := &fakePortForwardStream{}
	if len(c.streamReaders) >= streamNumber {
		stream.reader = c.streamReaders[streamNumber-1]
	}
	c.streams = append(c.streams, stream)
	return stream, nil
}

func (c *fakePortForwardConnection) Close() error {
	c.closeOnce.Do(func() {
		c.closeCount++
		if c.closed != nil {
			close(c.closed)
		}
	})
	return nil
}

func (*fakePortForwardConnection) CloseChan() <-chan bool {
	return make(chan bool)
}

func (*fakePortForwardConnection) SetIdleTimeout(time.Duration) {}

func (*fakePortForwardConnection) RemoveStreams(...httpstream.Stream) {}

type fakePortForwardStream struct {
	closeCount int
	reader     io.Reader
}

func (s *fakePortForwardStream) Read(p []byte) (int, error) {
	if s.reader == nil {
		return 0, io.EOF
	}
	return s.reader.Read(p)
}

func (s *fakePortForwardStream) Write(p []byte) (int, error) {
	return len(p), nil
}

func (s *fakePortForwardStream) Close() error {
	s.closeCount++
	return nil
}

func (*fakePortForwardStream) Reset() error {
	return nil
}

func (*fakePortForwardStream) Headers() http.Header {
	return nil
}

func (*fakePortForwardStream) Identifier() uint32 {
	return 0
}

func TestDialPodPort(t *testing.T) {
	streamConn := &fakePortForwardConnection{}
	dialer := &fakePortForwardDialer{
		conn:     streamConn,
		protocol: portforward.PortForwardProtocolV1Name,
	}

	conn, err := dialPodPort(context.Background(), dialer.Dial, 443)
	if err != nil {
		t.Fatalf("dialPodPort: %v", err)
	}

	if dialer.dialCount != 1 {
		t.Errorf("dial count = %d, want 1", dialer.dialCount)
	}
	if len(dialer.protocols) != 1 || dialer.protocols[0] != portforward.PortForwardProtocolV1Name {
		t.Errorf("dial protocols = %q, want [%q]", dialer.protocols, portforward.PortForwardProtocolV1Name)
	}
	if len(streamConn.headers) != 2 {
		t.Fatalf("created %d streams, want 2", len(streamConn.headers))
	}
	for i, streamType := range []string{corev1.StreamTypeError, corev1.StreamTypeData} {
		headers := streamConn.headers[i]
		if got := headers.Get(corev1.StreamType); got != streamType {
			t.Errorf("stream %d type = %q, want %q", i, got, streamType)
		}
		if got := headers.Get(corev1.PortHeader); got != "443" {
			t.Errorf("stream %d port = %q, want 443", i, got)
		}
		if got := headers.Get(corev1.PortForwardRequestIDHeader); got != "0" {
			t.Errorf("stream %d request ID = %q, want 0", i, got)
		}
	}

	if got := conn.LocalAddr().Network(); got != "port-forward" {
		t.Errorf("local network = %q, want port-forward", got)
	}
	if got := conn.RemoteAddr().String(); got != "pod:443" {
		t.Errorf("remote address = %q, want pod:443", got)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if streamConn.closeCount != 1 {
		t.Errorf("stream connection close count = %d, want 1", streamConn.closeCount)
	}
	if streamConn.streams[1].closeCount != 1 {
		t.Errorf("data stream close count = %d, want 1", streamConn.streams[1].closeCount)
	}
}

func TestDialPodPortCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dialer := &fakePortForwardDialer{}

	if _, err := dialPodPort(ctx, dialer.Dial, 443); err == nil {
		t.Fatal("dialPodPort: got nil error, want context cancellation")
	}
	if dialer.dialCount != 0 {
		t.Errorf("dial count = %d, want 0", dialer.dialCount)
	}
}

func TestSPDYPortForwardDialerHonorsContext(t *testing.T) {
	requestStarted := make(chan struct{})
	client := &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(requestStarted)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}

	target := &url.URL{
		Scheme: "https",
		Host:   "example.test",
	}
	dialer := newSPDYPortForwardDialer(nil, client, http.MethodPost, target)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-requestStarted
		cancel()
	}()

	if _, _, err := dialer(ctx, portforward.PortForwardProtocolV1Name); err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("dial error = %v, want context cancellation", err)
	}
}

func TestDialPodPortClosesConnectionOnSetupFailure(t *testing.T) {
	setupError := errors.New("setup failed")
	tests := []struct {
		name        string
		dialer      *fakePortForwardDialer
		streamConn  *fakePortForwardConnection
		wantCreated int
	}{
		{
			name: "protocol mismatch",
			dialer: &fakePortForwardDialer{
				protocol: "unexpected.example",
			},
			streamConn: &fakePortForwardConnection{},
		},
		{
			name: "error stream",
			dialer: &fakePortForwardDialer{
				protocol: portforward.PortForwardProtocolV1Name,
			},
			streamConn: &fakePortForwardConnection{
				createErrAt: 1,
				createErr:   setupError,
			},
			wantCreated: 1,
		},
		{
			name: "data stream",
			dialer: &fakePortForwardDialer{
				protocol: portforward.PortForwardProtocolV1Name,
			},
			streamConn: &fakePortForwardConnection{
				createErrAt: 2,
				createErr:   setupError,
			},
			wantCreated: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.dialer.conn = test.streamConn
			if _, err := dialPodPort(context.Background(), test.dialer.Dial, 443); err == nil {
				t.Fatal("dialPodPort: got nil error, want setup failure")
			}
			if test.streamConn.closeCount != 1 {
				t.Errorf("stream connection close count = %d, want 1", test.streamConn.closeCount)
			}
			if len(test.streamConn.headers) != test.wantCreated {
				t.Errorf("created stream count = %d, want %d", len(test.streamConn.headers), test.wantCreated)
			}
		})
	}
}

func TestDialPodPortClosesConnectionWhenContextCanceledDuringSetup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	closed := make(chan struct{})
	streamConn := &fakePortForwardConnection{
		closed: closed,
		onCreate: func(streamNumber int) {
			if streamNumber == 2 {
				cancel()
				<-closed
			}
		},
	}
	dialer := &fakePortForwardDialer{
		conn:     streamConn,
		protocol: portforward.PortForwardProtocolV1Name,
	}

	if _, err := dialPodPort(ctx, dialer.Dial, 443); !errors.Is(err, context.Canceled) {
		t.Fatalf("dialPodPort error = %v, want context cancellation", err)
	}
	if streamConn.closeCount != 1 {
		t.Errorf("stream connection close count = %d, want 1", streamConn.closeCount)
	}
}

func TestPortForwardErrorClosesConnection(t *testing.T) {
	closed := make(chan struct{})
	streamConn := &fakePortForwardConnection{
		closed: closed,
		streamReaders: []io.Reader{
			bytes.NewBufferString("remote port unavailable"),
			nil,
		},
	}
	dialer := &fakePortForwardDialer{
		conn:     streamConn,
		protocol: portforward.PortForwardProtocolV1Name,
	}

	conn, err := dialPodPort(context.Background(), dialer.Dial, 443)
	if err != nil {
		t.Fatalf("dialPodPort: %v", err)
	}
	defer conn.Close()

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for port-forward error to close connection")
	}
	if streamConn.streams[1].closeCount != 1 {
		t.Errorf("data stream close count = %d, want 1", streamConn.streams[1].closeCount)
	}
}
