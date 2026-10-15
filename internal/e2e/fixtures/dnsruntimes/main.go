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

// Command dnsruntimes is the in-sandbox HTTP driver for the dnsruntimes
// E2E suite. It invokes language/resolver probes under /opt/probes/<runtime>
// and optionally runs a local multi-server mock DNS harness on loopback to
// observe query ordering, same-server retries, and failover behavior.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultProbesDir      = "/opt/probes"
	defaultResolvConfPath = "/etc/resolv.conf"
	probeTimeout          = 25 * time.Second
)

// ProbeRequest is the JSON request body accepted by POST /probe.
type ProbeRequest struct {
	Runtime     string           `json:"runtime"`
	Op          string           `json:"op"`
	Host        string           `json:"host,omitempty"`
	URL         string           `json:"url,omitempty"`
	Attempts    int              `json:"attempts,omitempty"`
	IntervalMs  int              `json:"intervalMs,omitempty"`
	EnableCache bool             `json:"enableCache,omitempty"`
	MockDNS     []MockServerSpec `json:"mockDNS,omitempty"`
	Search      []string         `json:"search,omitempty"`
}

// MockServerSpec configures one loopback mock DNS server for failover/retry testing.
type MockServerSpec struct {
	// Addr is the IP address to bind on port 53 (e.g. "127.0.0.10", "127.0.0.11").
	Addr string `json:"addr"`
	// RCode is the DNS response code ("NOERROR", "REFUSED", "SERVFAIL", "NXDOMAIN").
	RCode string `json:"rcode"`
	// AnswerIP is the IPv4 A record returned when RCode is "NOERROR" (default "127.0.0.1").
	AnswerIP string `json:"answerIP,omitempty"`
}

// ProbeAttempt records the outcome of a single lookup or HTTP GET inside a probe.
type ProbeAttempt struct {
	Success    bool     `json:"success"`
	Addresses  []string `json:"addresses,omitempty"`
	StatusCode int      `json:"statusCode,omitempty"`
	ErrorCode  string   `json:"errorCode,omitempty"`
	Error      string   `json:"error,omitempty"`
	DurationMs int64    `json:"durationMs"`
}

// DNSQueryLog records a single DNS query observed by the mock DNS harness.
type DNSQueryLog struct {
	ServerAddr  string `json:"serverAddr"`
	ServerIndex int    `json:"serverIndex"`
	Network     string `json:"network"`
	QName       string `json:"qname"`
	QType       uint16 `json:"qtype"`
	TimestampNs int64  `json:"timestampNs"`
}

// ProbeResponse is the JSON response returned by POST /probe.
type ProbeResponse struct {
	Runtime    string         `json:"runtime"`
	Attempts   []ProbeAttempt `json:"attempts"`
	DNSQueries []DNSQueryLog  `json:"dnsQueries,omitempty"`
	Error      string         `json:"error,omitempty"`
}

type server struct {
	probesDir      string
	resolvConfPath string
	mu             sync.Mutex
}

func newServer(probesDir, resolvConfPath string) *server {
	s := &server{
		probesDir:      probesDir,
		resolvConfPath: resolvConfPath,
	}
	if raw, err := os.ReadFile(resolvConfPath); err == nil {
		sanitized := sanitizeSandboxResolvConf(raw)
		if err := writeResolvConf(resolvConfPath, sanitized); err != nil {
			log.Printf("dnsruntimes-agent: initial resolv.conf sanitize (%s): %v", resolvConfPath, err)
		}
	}
	return s
}

// sanitizeSandboxResolvConf normalizes the sandbox's /etc/resolv.conf so
// workstation-inherited search domains (e.g. corp.google.com on Kind nodes) do
// not cause 11-domain search loops on blocked lookups, and sets ndots:2 so
// 4-dot Kubernetes FQDNs (<svc>.<ns>.svc.cluster.local) are queried directly as
// FQDNs while 1-dot relative names (<svc>.<ns>) still traverse search domains.
func sanitizeSandboxResolvConf(raw []byte) []byte {
	var nameservers []string
	var clusterSearch []string
	var allSearch []string

	for line := range strings.SplitSeq(string(raw), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ";") {
			continue
		}
		if rest, ok := strings.CutPrefix(trimmed, "nameserver"); ok {
			ns := strings.TrimSpace(rest)
			if ns != "" {
				nameservers = append(nameservers, ns)
			}
			continue
		}
		if rest, ok := strings.CutPrefix(trimmed, "search"); ok {
			for tok := range strings.FieldsSeq(rest) {
				allSearch = append(allSearch, tok)
				if tok == "cluster.local" || strings.HasSuffix(tok, ".cluster.local") {
					clusterSearch = append(clusterSearch, tok)
				}
			}
		}
	}

	var b strings.Builder
	for _, ns := range nameservers {
		b.WriteString("nameserver " + ns + "\n")
	}
	searchToKeep := clusterSearch
	if len(searchToKeep) == 0 {
		searchToKeep = allSearch
	}
	if len(searchToKeep) > 0 {
		b.WriteString("search " + strings.Join(searchToKeep, " ") + "\n")
	}
	b.WriteString("options ndots:2 timeout:1 attempts:2\n")
	return []byte(b.String())
}

func writeResolvConf(path string, data []byte) error {
	if err := os.WriteFile(path, data, 0o644); err == nil {
		return nil
	}
	if err := makeResolvConfWritable(path); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/resolvconf", s.handleResolvConf)
	mux.HandleFunc("/probe", s.handleProbe)
	return mux
}

func (s *server) handleResolvConf(w http.ResponseWriter, _ *http.Request) {
	b, err := os.ReadFile(s.resolvConfPath)
	resp := map[string]string{
		"path":    s.resolvConfPath,
		"content": string(b),
	}
	if err != nil {
		resp["error"] = err.Error()
	}
	writeJSON(w, resp)
}

func (s *server) handleProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var req ProbeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
		writeJSON(w, ProbeResponse{Error: "decoding request: " + err.Error()})
		return
	}
	if req.Runtime == "" || strings.Contains(req.Runtime, "/") || strings.Contains(req.Runtime, "..") {
		writeJSON(w, ProbeResponse{Runtime: req.Runtime, Error: fmt.Sprintf("invalid runtime %q", req.Runtime)})
		return
	}
	if req.Op == "" {
		req.Op = "resolve"
	}
	if req.Attempts <= 0 {
		req.Attempts = 1
	}

	ctx, cancel := context.WithTimeout(r.Context(), probeTimeout)
	defer cancel()

	var (
		harness       *mockDNSHarness
		restoreResolv func()
	)
	if len(req.MockDNS) > 0 {
		var err error
		harness, err = startMockDNSHarness(req.MockDNS)
		if err != nil {
			writeJSON(w, ProbeResponse{Runtime: req.Runtime, Error: "starting mock DNS: " + err.Error()})
			return
		}
		defer harness.Close()

		restoreResolv, err = s.overrideResolvConf(req.MockDNS, req.Search)
		if err != nil {
			writeJSON(w, ProbeResponse{Runtime: req.Runtime, Error: "overriding resolv.conf: " + err.Error()})
			return
		}
		defer restoreResolv()
	}

	probeBin := filepath.Join(s.probesDir, req.Runtime)
	reqJSON, err := json.Marshal(req)
	if err != nil {
		writeJSON(w, ProbeResponse{Runtime: req.Runtime, Error: "marshaling probe input: " + err.Error()})
		return
	}

	cmd := exec.CommandContext(ctx, probeBin, string(reqJSON))
	out, err := cmd.CombinedOutput()
	if err != nil {
		writeJSON(w, ProbeResponse{
			Runtime: req.Runtime,
			Error:   fmt.Sprintf("running probe %s: %v (output: %s)", probeBin, err, strings.TrimSpace(string(out))),
		})
		return
	}

	var resp ProbeResponse
	if err := json.Unmarshal(out, &resp); err != nil {
		writeJSON(w, ProbeResponse{
			Runtime: req.Runtime,
			Error:   fmt.Sprintf("decoding probe output: %v (raw: %s)", err, strings.TrimSpace(string(out))),
		})
		return
	}
	resp.Runtime = req.Runtime
	if harness != nil {
		resp.DNSQueries = harness.Queries()
	}
	writeJSON(w, resp)
}

func (s *server) overrideResolvConf(specs []MockServerSpec, search []string) (func(), error) {
	orig, err := os.ReadFile(s.resolvConfPath)
	if err != nil {
		return nil, err
	}
	var b strings.Builder
	for _, sp := range specs {
		host := sp.Addr
		if h, _, splitErr := net.SplitHostPort(sp.Addr); splitErr == nil {
			host = h
		}
		b.WriteString("nameserver " + host + "\n")
	}
	if len(search) > 0 {
		b.WriteString("search " + strings.Join(search, " ") + "\n")
	}
	b.WriteString("options ndots:1 timeout:1 attempts:2\n")
	if err := writeResolvConf(s.resolvConfPath, []byte(b.String())); err != nil {
		return nil, err
	}
	return func() {
		_ = writeResolvConf(s.resolvConfPath, orig)
	}, nil
}

type mockDNSHarness struct {
	mu      sync.Mutex
	queries []DNSQueryLog
	closers []io.Closer
	wg      sync.WaitGroup
}

func startMockDNSHarness(specs []MockServerSpec) (*mockDNSHarness, error) {
	h := &mockDNSHarness{}
	for idx, sp := range specs {
		listenAddr := sp.Addr
		if _, _, err := net.SplitHostPort(listenAddr); err != nil {
			listenAddr = net.JoinHostPort(listenAddr, "53")
		}
		udpConn, err := net.ListenPacket("udp", listenAddr)
		if err != nil {
			h.Close()
			return nil, fmt.Errorf("udp listen %s: %w", listenAddr, err)
		}
		h.closers = append(h.closers, udpConn)

		tcpLis, err := net.Listen("tcp", listenAddr)
		if err != nil {
			h.Close()
			return nil, fmt.Errorf("tcp listen %s: %w", listenAddr, err)
		}
		h.closers = append(h.closers, tcpLis)

		serverIdx := idx
		spec := sp
		h.wg.Add(2)
		go func() {
			defer h.wg.Done()
			h.serveUDP(udpConn, serverIdx, spec)
		}()
		go func() {
			defer h.wg.Done()
			h.serveTCP(tcpLis, serverIdx, spec)
		}()
	}
	return h, nil
}

func (h *mockDNSHarness) Close() {
	for _, c := range h.closers {
		_ = c.Close()
	}
	h.wg.Wait()
}

func (h *mockDNSHarness) Queries() []DNSQueryLog {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]DNSQueryLog, len(h.queries))
	copy(out, h.queries)
	return out
}

func (h *mockDNSHarness) serveUDP(conn net.PacketConn, idx int, spec MockServerSpec) {
	buf := make([]byte, 2048)
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			return
		}
		pkt := make([]byte, n)
		copy(pkt, buf[:n])
		resp := h.handlePacket(pkt, idx, "udp", spec)
		if len(resp) > 0 {
			_, _ = conn.WriteTo(resp, addr)
		}
	}
}

func (h *mockDNSHarness) serveTCP(lis net.Listener, idx int, spec MockServerSpec) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		h.wg.Add(1)
		go func(c net.Conn) {
			defer h.wg.Done()
			defer c.Close()
			for {
				var lenBuf [2]byte
				if _, err := io.ReadFull(c, lenBuf[:]); err != nil {
					return
				}
				msgLen := int(binary.BigEndian.Uint16(lenBuf[:]))
				pkt := make([]byte, msgLen)
				if _, err := io.ReadFull(c, pkt); err != nil {
					return
				}
				resp := h.handlePacket(pkt, idx, "tcp", spec)
				if len(resp) == 0 {
					return
				}
				frame := make([]byte, 2+len(resp))
				binary.BigEndian.PutUint16(frame[0:2], uint16(len(resp)))
				copy(frame[2:], resp)
				if _, err := c.Write(frame); err != nil {
					return
				}
			}
		}(conn)
	}
}

func (h *mockDNSHarness) handlePacket(pkt []byte, idx int, network string, spec MockServerSpec) []byte {
	qname, qtype, qEnd, ok := parseDNSQuestion(pkt)
	if !ok {
		return nil
	}
	h.mu.Lock()
	h.queries = append(h.queries, DNSQueryLog{
		ServerAddr:  spec.Addr,
		ServerIndex: idx,
		Network:     network,
		QName:       qname,
		QType:       qtype,
		TimestampNs: time.Now().UnixNano(),
	})
	h.mu.Unlock()

	return buildDNSResponse(pkt, qtype, qEnd, spec)
}

func parseDNSQuestion(pkt []byte) (qname string, qtype uint16, qEnd int, ok bool) {
	if len(pkt) < 12 {
		return "", 0, 0, false
	}
	qdCount := binary.BigEndian.Uint16(pkt[4:6])
	if qdCount == 0 {
		return "", 0, 0, false
	}
	pos := 12
	var labels []string
	for pos < len(pkt) {
		l := int(pkt[pos])
		pos++
		if l == 0 {
			break
		}
		if pos+l > len(pkt) {
			return "", 0, 0, false
		}
		labels = append(labels, string(pkt[pos:pos+l]))
		pos += l
	}
	if pos+4 > len(pkt) {
		return "", 0, 0, false
	}
	qtype = binary.BigEndian.Uint16(pkt[pos : pos+2])
	qEnd = pos + 4
	return strings.Join(labels, "."), qtype, qEnd, true
}

func buildDNSResponse(req []byte, qtype uint16, qEnd int, spec MockServerSpec) []byte {
	var rcode byte
	switch strings.ToUpper(spec.RCode) {
	case "DROP", "TIMEOUT":
		return nil
	case "NOERROR", "":
		rcode = 0
	case "SERVFAIL":
		rcode = 2
	case "NXDOMAIN":
		rcode = 3
	case "REFUSED":
		rcode = 5
	default:
		rcode = 5
	}

	rd := req[2] & 0x01
	questionSection := req[12:qEnd]

	const (
		qtypeA   = 1
		qclassIN = 1
	)

	var answer []byte
	anCount := uint16(0)
	if rcode == 0 && qtype == qtypeA {
		ansIPStr := spec.AnswerIP
		if ansIPStr == "" {
			ansIPStr = "127.0.0.1"
		}
		if ip4 := net.ParseIP(ansIPStr).To4(); ip4 != nil {
			anCount = 1
			answer = make([]byte, 16)
			// Name pointer to offset 12 (0xc00c).
			binary.BigEndian.PutUint16(answer[0:2], 0xc00c)
			binary.BigEndian.PutUint16(answer[2:4], qtypeA)
			binary.BigEndian.PutUint16(answer[4:6], qclassIN)
			binary.BigEndian.PutUint32(answer[6:10], 60) // TTL 60s
			binary.BigEndian.PutUint16(answer[10:12], 4)
			copy(answer[12:16], ip4)
		}
	}

	resp := make([]byte, 12+len(questionSection)+len(answer))
	copy(resp[0:2], req[0:2])
	resp[2] = 0x80 | rd // QR=1, AA=0, RD copied
	resp[3] = 0x80 | rcode
	binary.BigEndian.PutUint16(resp[4:6], 1)       // QDCOUNT = 1
	binary.BigEndian.PutUint16(resp[6:8], anCount) // ANCOUNT
	copy(resp[12:12+len(questionSection)], questionSection)
	copy(resp[12+len(questionSection):], answer)
	return resp
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("dnsruntimes-agent: encoding response: %v", err)
	}
}

func main() {
	probesDir := os.Getenv("DNSRUNTIMES_PROBES_DIR")
	if probesDir == "" {
		probesDir = defaultProbesDir
	}
	resolvConfPath := os.Getenv("DNSRUNTIMES_RESOLV_CONF")
	if resolvConfPath == "" {
		resolvConfPath = defaultResolvConfPath
	}
	srv := newServer(probesDir, resolvConfPath)
	const addr = ":80"
	log.Printf("dnsruntimes-agent listening on %s", addr)
	httpSrv := &http.Server{Addr: addr, Handler: srv.routes()}
	if err := httpSrv.ListenAndServe(); err != nil {
		log.Fatalf("dnsruntimes-agent server: %v", err)
	}
}
