// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package egress

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

type ActorIdentity struct {
	Namespace string
	Template  string
	ActorID   string
	Atespace  string
	// TODO: Include worker_uid once egress identity is modeled as a signed
	// first-class Substrate identity rather than plain actor metadata headers.
}

type Config struct {
	PEPAddress string
	Listeners  []Listener
}

type Listener struct {
	Port uint16
}

type OriginalDestinationFunc func(net.Conn) (net.Addr, error)

type Capture struct {
	cancel    context.CancelFunc
	listeners []net.Listener
	transport *http2.Transport
	wg        sync.WaitGroup
}

func ConfigForPEPAddress(pepAddress string, listeners []Listener) (Config, bool) {
	pepAddress = strings.TrimSpace(pepAddress)
	if pepAddress == "" {
		return Config{}, false
	}
	return Config{PEPAddress: pepAddress, Listeners: listeners}, true
}

func Start(ctx context.Context, identity ActorIdentity, cfg Config, originalDestination OriginalDestinationFunc) (*Capture, error) {
	if originalDestination == nil {
		return nil, errors.New("original destination resolver must be set")
	}

	ctx, cancel := newCaptureContext(ctx)
	// One HTTP/2 transport for the whole capture. The PEP address is fixed for
	// the actor's lifetime, so CONNECT streams to the same destination authority
	// multiplex over a pooled connection to the PEP instead of paying a fresh TCP
	// dial + h2 handshake per captured connection.
	capture := &Capture{cancel: cancel, transport: newPEPTransport(cfg.PEPAddress)}
	for _, listenerCfg := range cfg.Listeners {
		lis, err := net.Listen("tcp4", net.JoinHostPort("0.0.0.0", strconv.Itoa(int(listenerCfg.Port))))
		if err != nil {
			capture.Close()
			return nil, fmt.Errorf("while listening for captured egress on port %d: %w", listenerCfg.Port, err)
		}

		capture.listeners = append(capture.listeners, lis)
		capture.wg.Add(1)
		go capture.serve(ctx, lis, identity, cfg.PEPAddress, originalDestination)
		slog.InfoContext(ctx, "Started actor egress capture listener",
			"port", listenerCfg.Port,
			"pepAddress", cfg.PEPAddress)
	}
	return capture, nil
}

// newPEPTransport builds the shared HTTP/2 transport whose connections all dial
// the fixed PEP address, regardless of the CONNECT authority.
func newPEPTransport(pepAddress string) *http2.Transport {
	return &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, _ string, _ *tls.Config) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, pepAddress)
		},
	}
}

func newCaptureContext(ctx context.Context) (context.Context, context.CancelFunc) {
	// The setup request context can be cancelled after the actor is running, but
	// egress capture must keep serving until actor network cleanup closes it.
	return context.WithCancel(context.WithoutCancel(ctx))
}

func (c *Capture) Close() error {
	if c.cancel != nil {
		c.cancel()
	}

	var err error
	for _, lis := range c.listeners {
		if closeErr := lis.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}
	c.wg.Wait()
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	return err
}

func (c *Capture) serve(ctx context.Context, lis net.Listener, identity ActorIdentity, pepAddress string, originalDestination OriginalDestinationFunc) {
	defer c.wg.Done()
	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			slog.WarnContext(ctx, "Failed to accept captured egress connection", "err", err)
			continue
		}
		c.wg.Add(1)
		transport := c.transport
		go func() {
			defer c.wg.Done()
			handleCapturedEgress(ctx, conn, identity, transport, pepAddress, originalDestination)
		}()
	}
}

func handleCapturedEgress(ctx context.Context, actorConn net.Conn, identity ActorIdentity, transport *http2.Transport, pepAddress string, originalDestination OriginalDestinationFunc) {
	stopActorClose := context.AfterFunc(ctx, func() {
		_ = actorConn.Close()
	})
	defer stopActorClose()
	defer actorConn.Close()

	originalDst, err := originalDestination(actorConn)
	if err != nil {
		slog.WarnContext(ctx, "Failed to resolve captured egress original destination", "err", err)
		return
	}

	authority, initialBytes := deriveConnectAuthority(ctx, actorConn, originalDst)
	tunnel, err := openCONNECTTunnel(ctx, transport, pepAddress, identity, originalDst, authority)
	if err != nil {
		slog.WarnContext(ctx, "Failed to open egress tunnel",
			"originalDestination", originalDst.String(),
			"connectAuthority", authority,
			"err", err)
		return
	}
	defer tunnel.Close()

	slog.InfoContext(ctx, "Proxying captured actor egress",
		"actorID", identity.ActorID,
		"actorTemplateNamespace", identity.Namespace,
		"actorTemplateName", identity.Template,
		"originalDestination", originalDst.String(),
		"connectAuthority", authority)

	proxyByteStream(ctx, actorConn, tunnel, initialBytes)
}

func proxyByteStream(ctx context.Context, actorConn net.Conn, tunnel io.ReadWriteCloser, initialBytes []byte) {
	stopProxyClose := context.AfterFunc(ctx, func() {
		_ = actorConn.Close()
		_ = tunnel.Close()
	})
	defer stopProxyClose()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if len(initialBytes) > 0 {
			if _, err := tunnel.Write(initialBytes); err != nil {
				_ = tunnel.Close()
				return
			}
		}
		_, _ = io.Copy(tunnel, actorConn)
		_ = tunnel.Close()
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(actorConn, tunnel)
		if tcpConn, ok := actorConn.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
	}()
	wg.Wait()
}

func openCONNECTTunnel(ctx context.Context, transport *http2.Transport, pepAddress string, identity ActorIdentity, originalDst net.Addr, authority string) (io.ReadWriteCloser, error) {
	// TODO: Add a transport selector here when there is a second supported
	// egress tunnel protocol, such as TLS CONNECT or HBONE.
	req, pr, pw := newConnectRequest(ctx, identity, originalDst, authority)
	return roundTripConnect(transport, req, pr, pw, authority, pepAddress)
}

func deriveConnectAuthority(ctx context.Context, actorConn net.Conn, originalDst net.Addr) (string, []byte) {
	if tcpAddr, ok := originalDst.(*net.TCPAddr); ok {
		return classifyConnectAuthority(ctx, actorConn, tcpAddr)
	}
	return originalDst.String(), nil
}

// sniffReadTimeout bounds how long capture waits for the actor to send the
// bytes that reveal a CONNECT authority (TLS SNI or HTTP Host). The authority
// is derived from those bytes, so the tunnel cannot be opened until they
// arrive; a client that speaks only after the server (SMTP, some databases)
// waits out this deadline and then falls back to the original destination.
const sniffReadTimeout = 2 * time.Second

const maxSniffBytes = 16 * 1024

func classifyConnectAuthority(ctx context.Context, actorConn net.Conn, originalDst *net.TCPAddr) (string, []byte) {
	_ = actorConn.SetReadDeadline(time.Now().Add(sniffReadTimeout))
	defer actorConn.SetReadDeadline(time.Time{})

	var initialBytes []byte
	httpScanned := 0 // bytes already searched for the HTTP header terminator
	buf := make([]byte, 2048)
	for len(initialBytes) < maxSniffBytes {
		n, err := actorConn.Read(buf)
		if n > 0 {
			initialBytes = append(initialBytes, buf[:n]...)
			if initialBytes[0] == 0x16 {
				// tlsClientHelloSNI is O(1) on an incomplete record (it checks
				// the record length prefix before walking).
				if sni, ok, needMore := tlsClientHelloSNI(initialBytes); ok {
					return net.JoinHostPort(sni, strconv.Itoa(originalDst.Port)), initialBytes
				} else if !needMore {
					break
				}
			} else if httpHeadersComplete(initialBytes, &httpScanned) {
				// Parse only once the full header block has arrived so a slow or
				// byte-at-a-time sender does not re-parse the buffer each read.
				if host, ok, _ := httpHostHeader(initialBytes); ok {
					return authorityWithDefaultPort(host, originalDst.Port), initialBytes
				}
				break
			}
		}
		if err != nil {
			break
		}
	}
	return originalDst.String(), initialBytes
}

// httpHeadersComplete reports whether data contains the end-of-headers marker,
// searching only the bytes appended since the last call (with a small overlap
// for a marker split across reads) so the total scan cost stays linear in the
// sniffed byte count. It advances *scanned to the current length.
func httpHeadersComplete(data []byte, scanned *int) bool {
	start := *scanned - 3
	if start < 0 {
		start = 0
	}
	if bytes.Contains(data[start:], []byte("\r\n\r\n")) || bytes.Contains(data[start:], []byte("\n\n")) {
		return true
	}
	*scanned = len(data)
	return false
}

func httpHostHeader(data []byte) (string, bool, bool) {
	// Scan for the end-of-headers marker on the raw bytes so an incomplete
	// request does not copy the whole accumulated buffer into a string on every
	// sniff read; only the bounded header block below is stringified.
	headerEnd := bytes.Index(data, []byte("\r\n\r\n"))
	separator := "\r\n"
	if headerEnd == -1 {
		headerEnd = bytes.Index(data, []byte("\n\n"))
		separator = "\n"
	}
	if headerEnd == -1 {
		return "", false, len(data) < maxSniffBytes
	}

	lines := strings.Split(string(data[:headerEnd]), separator)
	if len(lines) == 0 || !strings.Contains(lines[0], " ") {
		return "", false, false
	}
	for _, line := range lines[1:] {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(name), "host") {
			host := strings.TrimSpace(value)
			return host, host != "", false
		}
	}
	return "", false, false
}

func authorityWithDefaultPort(host string, port int) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(host); err == nil {
		return host
	}
	return net.JoinHostPort(strings.Trim(host, "[]"), strconv.Itoa(port))
}

func tlsClientHelloSNI(data []byte) (string, bool, bool) {
	if len(data) < 5 {
		return "", false, true
	}
	if data[0] != 0x16 {
		return "", false, false
	}
	recordLen := int(binary.BigEndian.Uint16(data[3:5]))
	if len(data) < 5+recordLen {
		return "", false, true
	}

	record := data[5 : 5+recordLen]
	if len(record) < 4 || record[0] != 0x01 {
		return "", false, false
	}
	handshakeLen := int(record[1])<<16 | int(record[2])<<8 | int(record[3])
	if len(record) < 4+handshakeLen {
		return "", false, false
	}
	clientHello := record[4 : 4+handshakeLen]
	if len(clientHello) < 34 {
		return "", false, false
	}

	offset := 34
	if len(clientHello) < offset+1 {
		return "", false, false
	}
	sessionIDLen := int(clientHello[offset])
	offset++
	if len(clientHello) < offset+sessionIDLen+2 {
		return "", false, false
	}
	offset += sessionIDLen

	cipherSuitesLen := int(binary.BigEndian.Uint16(clientHello[offset : offset+2]))
	offset += 2
	if len(clientHello) < offset+cipherSuitesLen+1 {
		return "", false, false
	}
	offset += cipherSuitesLen

	compressionMethodsLen := int(clientHello[offset])
	offset++
	if len(clientHello) < offset+compressionMethodsLen+2 {
		return "", false, false
	}
	offset += compressionMethodsLen

	extensionsLen := int(binary.BigEndian.Uint16(clientHello[offset : offset+2]))
	offset += 2
	if len(clientHello) < offset+extensionsLen {
		return "", false, false
	}
	extensions := clientHello[offset : offset+extensionsLen]
	for len(extensions) >= 4 {
		extensionType := binary.BigEndian.Uint16(extensions[0:2])
		extensionLen := int(binary.BigEndian.Uint16(extensions[2:4]))
		extensions = extensions[4:]
		if len(extensions) < extensionLen {
			return "", false, false
		}
		extensionData := extensions[:extensionLen]
		extensions = extensions[extensionLen:]
		if extensionType != 0 {
			continue
		}
		if len(extensionData) < 2 {
			return "", false, false
		}
		serverNameListLen := int(binary.BigEndian.Uint16(extensionData[0:2]))
		if len(extensionData) < 2+serverNameListLen {
			return "", false, false
		}
		serverNames := extensionData[2 : 2+serverNameListLen]
		for len(serverNames) >= 3 {
			nameType := serverNames[0]
			nameLen := int(binary.BigEndian.Uint16(serverNames[1:3]))
			serverNames = serverNames[3:]
			if len(serverNames) < nameLen {
				return "", false, false
			}
			name := string(serverNames[:nameLen])
			serverNames = serverNames[nameLen:]
			if nameType == 0 && name != "" {
				return name, true, false
			}
		}
		return "", false, false
	}
	return "", false, false
}

func newConnectRequest(ctx context.Context, identity ActorIdentity, originalDst net.Addr, authority string) (*http.Request, *io.PipeReader, *io.PipeWriter) {
	pr, pw := io.Pipe()
	req := &http.Request{
		Method:        http.MethodConnect,
		URL:           &url.URL{Scheme: "http", Host: authority},
		Host:          authority,
		Header:        make(http.Header),
		Body:          pr,
		ContentLength: -1,
	}
	req = req.WithContext(ctx)
	// TODO: Replace these plain identity headers with a signed short-lived actor
	// identity token for the PEP. The signed claims should include sub, aud, exp,
	// iat, worker_uid, and the original destination so policy is evaluated over
	// verified request identity rather than unsigned metadata.
	req.Header.Set("x-ate-actor-id", identity.ActorID)
	req.Header.Set("x-ate-atespace", identity.Atespace)
	req.Header.Set("x-ate-actor-template", identity.Template)
	req.Header.Set("x-ate-actor-template-namespace", identity.Namespace)
	req.Header.Set("x-ate-original-destination", originalDst.String())
	if authority != originalDst.String() {
		req.Header.Set("x-ate-connect-authority", authority)
	}
	return req, pr, pw
}

func roundTripConnect(
	transport *http2.Transport,
	req *http.Request,
	pr *io.PipeReader,
	pw *io.PipeWriter,
	connectAuthority string,
	pepAddress string,
) (io.ReadWriteCloser, error) {
	// The transport is shared across all captured connections, so failures close
	// only this stream's pipes; idle connections to the PEP are reaped once in
	// Capture.Close so unrelated in-flight streams keep multiplexing.
	resp, err := transport.RoundTrip(req)
	if err != nil {
		_ = pr.CloseWithError(err)
		_ = pw.CloseWithError(err)
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_ = resp.Body.Close()
		err := fmt.Errorf("CONNECT to %s through %s returned %s", connectAuthority, pepAddress, resp.Status)
		_ = pr.CloseWithError(err)
		_ = pw.CloseWithError(err)
		return nil, err
	}
	return &connectStream{
		requestWriter: pw,
		responseBody:  resp.Body,
	}, nil
}

type connectStream struct {
	requestWriter *io.PipeWriter
	responseBody  io.ReadCloser
}

func (s *connectStream) Read(p []byte) (int, error) {
	return s.responseBody.Read(p)
}

func (s *connectStream) Write(p []byte) (int, error) {
	return s.requestWriter.Write(p)
}

func (s *connectStream) Close() error {
	return errors.Join(s.requestWriter.Close(), s.responseBody.Close())
}
