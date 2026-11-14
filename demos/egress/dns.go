// Copyright 2026 The Agent Substrate Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	atunneldns "go.ate.dev/substrate/internal/atunnel/dns"
	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
)

const (
	maxDNSProbeCount       = 5000
	maxDNSProbeConcurrency = 128
	dnsProbeQueryTimeout   = 6 * time.Second
	// ednsOptionExtendedError is the EDNS option that carries an Extended DNS
	// Error (RFC 8914).
	ednsOptionExtendedError = 15
)

// dnsProbeRequest asks the demo to send Count A queries for Name to the
// actor's nameserver and time each one.
type dnsProbeRequest struct {
	// Name is the fully qualified name to query.
	Name        string `json:"name"`
	Count       int    `json:"count"`
	Concurrency int    `json:"concurrency"`
	// TCP sends each query on its own TCP connection.
	TCP bool `json:"tcp"`
	// UniqueLabel prefixes Name with a different random label on every query,
	// so no resolver can answer from a cache.
	UniqueLabel bool `json:"uniqueLabel"`
}

type dnsProbeResponse struct {
	Nameserver string `json:"nameserver,omitempty"`
	// RCodes counts the responses by response code.
	RCodes map[string]int `json:"rcodes,omitempty"`
	// ExtendedErrors counts the responses by RFC 8914 info-code.
	ExtendedErrors map[int]int `json:"extendedErrors,omitempty"`
	// LatenciesMicros holds the time each answered query took.
	LatenciesMicros []int64 `json:"latenciesMicros,omitempty"`
	// Failures counts the queries that got no response.
	Failures int    `json:"failures,omitempty"`
	Error    string `json:"error,omitempty"`
}

// handleDNS sends raw DNS queries to the actor's nameserver. Raw queries skip
// the stub resolver's search list and paired A and AAAA lookups, so each
// latency is one exchange with the DNS relay.
func handleDNS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, dnsProbeResponse{Error: "method must be POST"})
		return
	}
	var input dnsProbeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody)).Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, dnsProbeResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if _, err := dnsmessage.NewName(input.Name); err != nil || input.Name == "" || input.Name[len(input.Name)-1] != '.' {
		writeJSON(w, http.StatusBadRequest, dnsProbeResponse{Error: "name must be a fully qualified DNS name ending in a dot"})
		return
	}
	if input.Count < 1 || input.Count > maxDNSProbeCount {
		writeJSON(w, http.StatusBadRequest, dnsProbeResponse{Error: fmt.Sprintf("count must be between 1 and %d", maxDNSProbeCount)})
		return
	}
	if input.Concurrency < 1 || input.Concurrency > maxDNSProbeConcurrency {
		writeJSON(w, http.StatusBadRequest, dnsProbeResponse{Error: fmt.Sprintf("concurrency must be between 1 and %d", maxDNSProbeConcurrency)})
		return
	}
	nameservers, err := atunneldns.ResolvConfNameservers("/etc/resolv.conf")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, dnsProbeResponse{Error: err.Error()})
		return
	}

	out := dnsProbeResponse{
		Nameserver:     nameservers[0],
		RCodes:         map[string]int{},
		ExtendedErrors: map[int]int{},
	}
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		next = make(chan struct{})
	)
	for range input.Concurrency {
		wg.Go(func() {
			for range next {
				name := input.Name
				if input.UniqueLabel {
					name = fmt.Sprintf("q%016x.%s", rand.Uint64(), input.Name)
				}
				start := time.Now()
				resp, err := dnsProbeExchange(r.Context(), nameservers[0], name, input.TCP)
				elapsed := time.Since(start)

				mu.Lock()
				if err != nil {
					out.Failures++
					if out.Error == "" {
						out.Error = err.Error()
					}
				} else {
					out.LatenciesMicros = append(out.LatenciesMicros, elapsed.Microseconds())
					out.RCodes[dnsmessage.RCode(resp[3]&0x0f).String()]++
					if code, ok := extendedDNSError(resp); ok {
						out.ExtendedErrors[code]++
					}
				}
				mu.Unlock()
			}
		})
	}
	for range input.Count {
		next <- struct{}{}
	}
	close(next)
	wg.Wait()
	writeJSON(w, http.StatusOK, out)
}

// dnsProbeExchange sends one A query for name, with EDNS, and returns the
// response.
func dnsProbeExchange(ctx context.Context, nameserver, name string, tcp bool) ([]byte, error) {
	id := uint16(rand.Uint32())
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, RecursionDesired: true})
	if err := b.StartQuestions(); err != nil {
		return nil, err
	}
	qname, err := dnsmessage.NewName(name)
	if err != nil {
		return nil, err
	}
	if err := b.Question(dnsmessage.Question{Name: qname, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}); err != nil {
		return nil, err
	}
	if err := b.StartAdditionals(); err != nil {
		return nil, err
	}
	var opt dnsmessage.ResourceHeader
	if err := opt.SetEDNS0(1232, dnsmessage.RCodeSuccess, false); err != nil {
		return nil, err
	}
	if err := b.OPTResource(opt, dnsmessage.OPTResource{}); err != nil {
		return nil, err
	}
	query, err := b.Finish()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, dnsProbeQueryTimeout)
	defer cancel()
	network := "udp"
	if tcp {
		network = "tcp"
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, network, nameserver)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	var resp []byte
	if tcp {
		if err := protocol.WriteTCPFrame(conn, query); err != nil {
			return nil, err
		}
		if resp, err = protocol.ReadTCPFrame(conn); err != nil {
			return nil, err
		}
	} else {
		if _, err := conn.Write(query); err != nil {
			return nil, err
		}
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}
		resp = buf[:n]
	}
	if !protocol.ResponseSanityCheck(resp) || protocol.TxnID(resp) != id {
		return nil, errors.New("response does not match the query")
	}
	return resp, nil
}

// extendedDNSError returns the info-code of the Extended DNS Error in resp.
func extendedDNSError(resp []byte) (int, bool) {
	var p dnsmessage.Parser
	if _, err := p.Start(resp); err != nil {
		return 0, false
	}
	if p.SkipAllQuestions() != nil || p.SkipAllAnswers() != nil || p.SkipAllAuthorities() != nil {
		return 0, false
	}
	for {
		h, err := p.AdditionalHeader()
		if err != nil {
			return 0, false
		}
		if h.Type != dnsmessage.TypeOPT {
			if p.SkipAdditional() != nil {
				return 0, false
			}
			continue
		}
		opt, err := p.OPTResource()
		if err != nil {
			return 0, false
		}
		for _, o := range opt.Options {
			if o.Code == ednsOptionExtendedError && len(o.Data) >= 2 {
				return int(binary.BigEndian.Uint16(o.Data)), true
			}
		}
		return 0, false
	}
}
