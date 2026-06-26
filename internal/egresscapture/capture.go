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

package egresscapture

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
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
	// TODO: Include worker_uid once egress identity is modeled as a signed
	// first-class Substrate identity rather than plain actor metadata headers.
}

type Config struct {
	PEPAddress string
	Protocol   string
	TLS        TLSConfig
	Listeners  []Listener
}

type TLSConfig struct {
	ServerName         string
	CAFile             string
	InsecureSkipVerify bool
}

type Listener struct {
	Port uint16
}

type OriginalDestinationFunc func(net.Conn) (net.Addr, error)

type Capture struct {
	cancel    context.CancelFunc
	listeners []net.Listener
	wg        sync.WaitGroup
}

type TunnelTransport interface {
	Open(ctx context.Context, identity ActorIdentity, originalDst net.Addr, authority string) (io.ReadWriteCloser, error)
}

type TunnelTransportFactory func(Config) (TunnelTransport, error)

var tunnelTransportFactories = map[string]TunnelTransportFactory{}

func init() {
	// Keep tunnel protocol support behind factories so additional transports
	// such as HBONE can plug in without changing capture/listener logic.
	RegisterTunnelTransport(TunnelProtocolConnect, newPlaintextCONNECTTunnelTransport)
	RegisterTunnelTransport(TunnelProtocolPlaintext, newPlaintextCONNECTTunnelTransport)
	RegisterTunnelTransport(TunnelProtocolH2C, newPlaintextCONNECTTunnelTransport)
	RegisterTunnelTransport(TunnelProtocolConnectTLS, newTLSCONNECTTunnelTransport)
	RegisterTunnelTransport(TunnelProtocolHTTPSConnect, newTLSCONNECTTunnelTransport)
	RegisterTunnelTransport(TunnelProtocolTLSConnect, newTLSCONNECTTunnelTransport)
}

func RegisterTunnelTransport(protocol string, factory TunnelTransportFactory) {
	tunnelTransportFactories[strings.ToLower(strings.TrimSpace(protocol))] = factory
}

func EnabledFromEnv() bool {
	enabled, _ := strconv.ParseBool(os.Getenv(EnvCaptureEnabled))
	return enabled
}

func ConfigFromEnv(listeners []Listener) (Config, error) {
	pepAddress := strings.TrimSpace(os.Getenv(EnvPEPAddress))
	if pepAddress == "" {
		return Config{}, fmt.Errorf("%s must be set when egress capture is enabled", EnvPEPAddress)
	}
	cfg := Config{
		PEPAddress: pepAddress,
		Protocol:   DefaultTunnelProtocol,
		TLS: TLSConfig{
			ServerName:         os.Getenv(EnvConnectTLSServerName),
			CAFile:             os.Getenv(EnvConnectTLSCAFile),
			InsecureSkipVerify: boolEnv(EnvConnectTLSInsecureSkipVerify),
		},
		Listeners: listeners,
	}
	if v := os.Getenv(EnvTunnelProtocol); v != "" {
		cfg.Protocol = v
	}
	return cfg, nil
}

func boolEnv(name string) bool {
	enabled, _ := strconv.ParseBool(os.Getenv(name))
	return enabled
}

func Start(ctx context.Context, identity ActorIdentity, cfg Config, originalDestination OriginalDestinationFunc) (*Capture, error) {
	if originalDestination == nil {
		return nil, errors.New("original destination resolver must be set")
	}
	transport, err := NewTunnelTransport(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := newCaptureContext(ctx)
	capture := &Capture{cancel: cancel}
	for _, listenerCfg := range cfg.Listeners {
		lis, err := net.Listen("tcp4", net.JoinHostPort("0.0.0.0", strconv.Itoa(int(listenerCfg.Port))))
		if err != nil {
			capture.Close()
			return nil, fmt.Errorf("while listening for captured egress on port %d: %w", listenerCfg.Port, err)
		}

		capture.listeners = append(capture.listeners, lis)
		capture.wg.Add(1)
		go capture.serve(ctx, lis, identity, transport, originalDestination)
		slog.InfoContext(ctx, "Started actor egress capture listener",
			"port", listenerCfg.Port,
			"pepAddress", cfg.PEPAddress,
			"protocol", cfg.Protocol)
	}
	return capture, nil
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
	return err
}

func (c *Capture) serve(ctx context.Context, lis net.Listener, identity ActorIdentity, transport TunnelTransport, originalDestination OriginalDestinationFunc) {
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
		go func() {
			defer c.wg.Done()
			handleCapturedEgress(ctx, conn, identity, transport, originalDestination)
		}()
	}
}

func handleCapturedEgress(ctx context.Context, actorConn net.Conn, identity ActorIdentity, transport TunnelTransport, originalDestination OriginalDestinationFunc) {
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
	tunnel, err := transport.Open(ctx, identity, originalDst, authority)
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

func NewTunnelTransport(cfg Config) (TunnelTransport, error) {
	protocol := strings.ToLower(strings.TrimSpace(cfg.Protocol))
	if protocol == "" {
		protocol = DefaultTunnelProtocol
	}
	factory, ok := tunnelTransportFactories[protocol]
	if !ok {
		return nil, fmt.Errorf("unsupported egress tunnel protocol %q", cfg.Protocol)
	}
	return factory(cfg)
}

type PlaintextCONNECTTunnelTransport struct {
	PEPAddress string
}

func newPlaintextCONNECTTunnelTransport(cfg Config) (TunnelTransport, error) {
	return &PlaintextCONNECTTunnelTransport{PEPAddress: cfg.PEPAddress}, nil
}

func (t *PlaintextCONNECTTunnelTransport) Open(ctx context.Context, identity ActorIdentity, originalDst net.Addr, authority string) (io.ReadWriteCloser, error) {
	req, pr, pw := newConnectRequest(ctx, "http", identity, originalDst, authority)
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, _ string, _ *tls.Config) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, t.PEPAddress)
		},
	}
	return roundTripConnect(transport, req, pr, pw, authority, t.PEPAddress)
}

type TLSCONNECTTunnelTransport struct {
	PEPAddress string
	TLS        TLSConfig
}

func newTLSCONNECTTunnelTransport(cfg Config) (TunnelTransport, error) {
	return &TLSCONNECTTunnelTransport{PEPAddress: cfg.PEPAddress, TLS: cfg.TLS}, nil
}

func (t *TLSCONNECTTunnelTransport) Open(ctx context.Context, identity ActorIdentity, originalDst net.Addr, authority string) (io.ReadWriteCloser, error) {
	req, pr, pw := newConnectRequest(ctx, "https", identity, originalDst, authority)
	tlsConfig, err := t.tlsConfig()
	if err != nil {
		_ = pr.CloseWithError(err)
		_ = pw.CloseWithError(err)
		return nil, err
	}
	transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, _ string, _ *tls.Config) (net.Conn, error) {
			var dialer net.Dialer
			conn, err := dialer.DialContext(ctx, network, t.PEPAddress)
			if err != nil {
				return nil, err
			}
			tlsConn := tls.Client(conn, tlsConfig)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return tlsConn, nil
		},
	}
	return roundTripConnect(transport, req, pr, pw, authority, t.PEPAddress)
}

func (t *TLSCONNECTTunnelTransport) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{
		NextProtos:         []string{"h2"},
		ServerName:         t.TLS.ServerName,
		InsecureSkipVerify: t.TLS.InsecureSkipVerify,
	}
	if t.TLS.CAFile == "" {
		return cfg, nil
	}
	rootsPEM, err := os.ReadFile(t.TLS.CAFile)
	if err != nil {
		return nil, fmt.Errorf("while reading CONNECT TLS CA file %q: %w", t.TLS.CAFile, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootsPEM) {
		return nil, fmt.Errorf("CONNECT TLS CA file %q contains no certificates", t.TLS.CAFile)
	}
	cfg.RootCAs = roots
	return cfg, nil
}

func deriveConnectAuthority(ctx context.Context, actorConn net.Conn, originalDst net.Addr) (string, []byte) {
	if tcpAddr, ok := originalDst.(*net.TCPAddr); ok && tcpAddr.Port == 443 {
		authority, initialBytes := deriveTLSConnectAuthority(ctx, actorConn, tcpAddr)
		return authority, initialBytes
	}
	if tcpAddr, ok := originalDst.(*net.TCPAddr); ok && tcpAddr.Port == 80 {
		authority, initialBytes := deriveHTTPConnectAuthority(ctx, actorConn, tcpAddr)
		return authority, initialBytes
	}
	return originalDst.String(), nil
}

func deriveTLSConnectAuthority(ctx context.Context, actorConn net.Conn, originalDst *net.TCPAddr) (string, []byte) {
	const maxClientHelloBytes = 16 * 1024
	_ = actorConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer actorConn.SetReadDeadline(time.Time{})

	var initialBytes []byte
	buf := make([]byte, 2048)
	for len(initialBytes) < maxClientHelloBytes {
		n, err := actorConn.Read(buf)
		if n > 0 {
			initialBytes = append(initialBytes, buf[:n]...)
			if sni, ok, needMore := tlsClientHelloSNI(initialBytes); ok {
				return net.JoinHostPort(sni, strconv.Itoa(originalDst.Port)), initialBytes
			} else if !needMore {
				break
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return originalDst.String(), initialBytes
			}
			break
		}
	}
	return originalDst.String(), initialBytes
}

func deriveHTTPConnectAuthority(ctx context.Context, actorConn net.Conn, originalDst *net.TCPAddr) (string, []byte) {
	const maxHeaderBytes = 16 * 1024
	_ = actorConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	defer actorConn.SetReadDeadline(time.Time{})

	var initialBytes []byte
	buf := make([]byte, 2048)
	for len(initialBytes) < maxHeaderBytes {
		n, err := actorConn.Read(buf)
		if n > 0 {
			initialBytes = append(initialBytes, buf[:n]...)
			if host, ok, needMore := httpHostHeader(initialBytes); ok {
				return authorityWithDefaultPort(host, originalDst.Port), initialBytes
			} else if !needMore {
				break
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return originalDst.String(), initialBytes
			}
			break
		}
	}
	return originalDst.String(), initialBytes
}

func httpHostHeader(data []byte) (string, bool, bool) {
	headers := string(data)
	headerEnd := strings.Index(headers, "\r\n\r\n")
	separator := "\r\n"
	if headerEnd == -1 {
		headerEnd = strings.Index(headers, "\n\n")
		separator = "\n"
	}
	if headerEnd == -1 {
		return "", false, len(data) < 16*1024
	}

	lines := strings.Split(headers[:headerEnd], separator)
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

func newConnectRequest(ctx context.Context, scheme string, identity ActorIdentity, originalDst net.Addr, authority string) (*http.Request, *io.PipeReader, *io.PipeWriter) {
	pr, pw := io.Pipe()
	req := &http.Request{
		Method:        http.MethodConnect,
		URL:           &url.URL{Scheme: scheme, Host: authority},
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
	resp, err := transport.RoundTrip(req)
	if err != nil {
		_ = pr.CloseWithError(err)
		_ = pw.CloseWithError(err)
		transport.CloseIdleConnections()
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_ = resp.Body.Close()
		err := fmt.Errorf("CONNECT to %s through %s returned %s", connectAuthority, pepAddress, resp.Status)
		_ = pr.CloseWithError(err)
		_ = pw.CloseWithError(err)
		transport.CloseIdleConnections()
		return nil, err
	}
	return &hboneStream{
		requestWriter: pw,
		responseBody:  resp.Body,
		closeIdle:     transport.CloseIdleConnections,
	}, nil
}

type hboneStream struct {
	requestWriter *io.PipeWriter
	responseBody  io.ReadCloser
	closeIdle     func()
}

func (s *hboneStream) Read(p []byte) (int, error) {
	return s.responseBody.Read(p)
}

func (s *hboneStream) Write(p []byte) (int, error) {
	return s.requestWriter.Write(p)
}

func (s *hboneStream) Close() error {
	err := errors.Join(s.requestWriter.Close(), s.responseBody.Close())
	if s.closeIdle != nil {
		s.closeIdle()
	}
	return err
}
