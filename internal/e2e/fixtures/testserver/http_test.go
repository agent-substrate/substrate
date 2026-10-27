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
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestHTTPHandlerDefaultsToEmptyHealthz(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(server.httpURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading /healthz: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("GET /healthz body = %q, want empty", body)
	}
}

func TestHTTPHandlerEchoesRequestHeaders(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")

	// Long enough to catch a truncated value, the size of a JWT.
	token := strings.Repeat("eyJhbGciOiJFUzI1NiJ9.", 64)
	request, err := http.NewRequest(http.MethodGet, server.httpURL+"/echoheaders", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	// Sent lowercase on the wire; the origin reports it canonicalized.
	request.Header["authorization"] = []string{"Bearer placeholder"}
	request.Header.Set("X-Actor-Token", token)
	request.Header.Add("X-Repeated", "one")
	request.Header.Add("X-Repeated", "two")

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET /echoheaders: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /echoheaders status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if got := response.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("GET /echoheaders Content-Type = %q, want application/json", got)
	}
	var echoed echoedHeaders
	if err := json.NewDecoder(response.Body).Decode(&echoed); err != nil {
		t.Fatalf("decoding /echoheaders: %v", err)
	}
	for name, want := range map[string]string{
		"Authorization": "Bearer placeholder",
		"X-Actor-Token": token,
		"X-Repeated":    "one,two",
	} {
		if got := echoed.Headers[name]; got != want {
			t.Errorf("echoed %s = %q, want %q", name, got, want)
		}
	}
	if got, ok := echoed.Headers["X-Absent"]; ok {
		t.Errorf("echoed X-Absent = %q for a header the request did not carry, want it missing", got)
	}
}

func TestHTTPSTestResponseUsesHTTP11AndVerifiedIdentity(t *testing.T) {
	cert := writeOriginCertificate(t)
	server := startOriginServer(t, newHTTPHandler("fixture response"), cert.certFile, cert.keyFile)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert.caPEM) {
		t.Fatal("adding fixture CA to trust pool")
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			Protocols: protocols,
			TLSClientConfig: &tls.Config{
				RootCAs:    roots,
				ServerName: "127.0.0.1",
			},
		},
	}
	response, err := client.Get(server.httpsURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz over verified TLS: %v", err)
	}
	defer response.Body.Close()

	if response.Proto != "HTTP/1.1" {
		t.Errorf("HTTPS response protocol with HTTP/2 offered = %q, want HTTP/1.1", response.Proto)
	}
	if response.StatusCode != http.StatusOK {
		t.Errorf("HTTPS response status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading HTTPS response: %v", err)
	}
	if string(body) != "fixture response" {
		t.Errorf("HTTPS response body = %q, want %q", body, "fixture response")
	}
}

func TestHTTPHandlerFetch(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")
	client := &http.Client{Timeout: 2 * time.Second}

	t.Run("waits for full POST body before responding 200", func(t *testing.T) {
		pr, pw := io.Pipe()
		req, err := http.NewRequest(http.MethodPost, server.httpURL+"/fetch", pr)
		if err != nil {
			t.Fatal(err)
		}

		type result struct {
			resp *http.Response
			err  error
		}
		done := make(chan result, 1)
		go func() {
			resp, err := client.Do(req)
			done <- result{resp: resp, err: err}
		}()

		if _, err := pw.Write([]byte("part1")); err != nil {
			t.Fatal(err)
		}
		select {
		case res := <-done:
			t.Fatalf("POST /fetch responded before body EOF: resp=%v, err=%v", res.resp, res.err)
		case <-time.After(50 * time.Millisecond):
		}

		if _, err := pw.Write([]byte("part2")); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}

		res := <-done
		if res.err != nil {
			t.Fatalf("POST /fetch: %v", res.err)
		}
		defer res.resp.Body.Close()
		if res.resp.StatusCode != http.StatusOK {
			t.Errorf("POST /fetch status = %d, want %d", res.resp.StatusCode, http.StatusOK)
		}
	})

	t.Run("rejects GET with 405", func(t *testing.T) {
		resp, err := client.Get(server.httpURL + "/fetch")
		if err != nil {
			t.Fatalf("GET /fetch: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("GET /fetch status = %d, want %d", resp.StatusCode, http.StatusMethodNotAllowed)
		}
	})
}

func TestHTTPServesCleartextBesideTLS(t *testing.T) {
	cert := writeOriginCertificate(t)
	server := startOriginServer(t, newHTTPHandler("both schemes"), cert.certFile, cert.keyFile)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert.caPEM) {
		t.Fatal("adding fixture CA to trust pool")
	}
	client := &http.Client{
		Timeout:   2 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "127.0.0.1"}},
	}
	for _, url := range []string{server.httpURL + "/healthz", server.httpsURL + "/healthz"} {
		response, err := client.Get(url)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("reading %s: %v", url, err)
		}
		if response.StatusCode != http.StatusOK || string(body) != "both schemes" {
			t.Errorf("GET %s = %d %q, want 200 %q", url, response.StatusCode, body, "both schemes")
		}
	}
}

func TestHTTPWithoutCertificateServesOnlyCleartext(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")
	if server.httpsURL != "" {
		t.Errorf("origin without a certificate serves HTTPS at %s, want cleartext only", server.httpsURL)
	}
}

// getDial calls the origin's /dial with query and returns the status and, for a
// 200, the decoded result.
func getDial(t *testing.T, originURL, query string) (int, dialResult) {
	t.Helper()
	// Its own transport with keep-alives off, so each call opens one connection
	// and closes it after the response.
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	response, err := client.Get(originURL + "/dial?" + query)
	if err != nil {
		t.Fatalf("GET /dial?%s: %v", query, err)
	}
	defer response.Body.Close()
	var result dialResult
	if response.StatusCode == http.StatusOK {
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatalf("decoding /dial?%s: %v", query, err)
		}
	}
	return response.StatusCode, result
}

func TestHTTPDialReachesListener(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")
	target := listenLoopback(t)
	t.Cleanup(func() { target.Close() })

	address := target.Addr().String()
	status, result := getDial(t, server.httpURL, "address="+url.QueryEscape(address))
	if status != http.StatusOK {
		t.Fatalf("GET /dial status = %d, want %d", status, http.StatusOK)
	}
	if !result.OK || result.Error != "" {
		t.Errorf("dial %s = ok %t, error %q; want ok with no error", address, result.OK, result.Error)
	}
	if result.Address != address {
		t.Errorf("dial result address = %q, want %q", result.Address, address)
	}
}

func TestHTTPDialReportsUnreachableAddress(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")
	// A port that was just listening and is now closed refuses the connection.
	closed := listenLoopback(t)
	address := closed.Addr().String()
	closed.Close()

	status, result := getDial(t, server.httpURL, "address="+url.QueryEscape(address)+"&timeout=1s")
	if status != http.StatusOK {
		t.Fatalf("GET /dial status = %d, want %d: an unreachable address is a result, not a request error", status, http.StatusOK)
	}
	if result.OK || result.Error == "" {
		t.Errorf("dial %s = ok %t, error %q; want not ok with the reason", address, result.OK, result.Error)
	}
}

func TestHTTPDialRejectsMalformedRequest(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")
	for name, query := range map[string]string{
		"missing address":  "",
		"address w/o port": "address=127.0.0.1",
		"empty port":       "address=127.0.0.1:",
		"bad timeout":      "address=127.0.0.1:80&timeout=soon",
		"zero timeout":     "address=127.0.0.1:80&timeout=0s",
	} {
		t.Run(name, func(t *testing.T) {
			if status, _ := getDial(t, server.httpURL, query); status != http.StatusBadRequest {
				t.Errorf("GET /dial?%s status = %d, want %d", query, status, http.StatusBadRequest)
			}
		})
	}
}

// sendConnect opens a connection to originURL's host, sends it a CONNECT for
// target, and returns the status it answered with.
func sendConnect(t *testing.T, originURL, target string) int {
	t.Helper()
	parsed, err := url.Parse(originURL)
	if err != nil {
		t.Fatalf("parsing %s: %v", originURL, err)
	}
	conn, err := net.DialTimeout("tcp", parsed.Host, 2*time.Second)
	if err != nil {
		t.Fatalf("dialing %s: %v", parsed.Host, err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("setting deadline: %v", err)
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("sending CONNECT %s: %v", target, err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatalf("reading the answer to CONNECT %s: %v", target, err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

func TestHTTPAcceptConnectAnswersCONNECT(t *testing.T) {
	server := startOriginServer(t, acceptingConnect(newHTTPHandler("")), "", "")
	if status := sendConnect(t, server.httpURL, "example.com:443"); status != http.StatusOK {
		t.Errorf("CONNECT status = %d, want %d", status, http.StatusOK)
	}
	// Anything else is still the origin.
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(server.httpURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Errorf("GET /healthz status = %d, want %d", response.StatusCode, http.StatusOK)
	}
}

func TestHTTPRefusesCONNECTByDefault(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")
	if status := sendConnect(t, server.httpURL, "example.com:443"); status/100 == 2 {
		t.Errorf("CONNECT status = %d, want the origin not to open a tunnel", status)
	}
}
