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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	atunneldns "go.ate.dev/substrate/internal/atunnel/dns"
	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
)

const (
	maxDNSProbeCount     = 5000
	dnsProbeQueryTimeout = 6 * time.Second
)

// dnsProbeRequest asks the demo to send Count A queries for Name to the
// actor's nameserver, one at a time, and time each one.
type dnsProbeRequest struct {
	// Name is the fully qualified name to query.
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type dnsProbeResponse struct {
	// RCodes counts the responses by response code.
	RCodes map[string]int `json:"rcodes,omitempty"`
	// LatenciesMicros holds the time each answered query took.
	LatenciesMicros []int64 `json:"latenciesMicros,omitempty"`
	// Failures counts the queries that got no response.
	Failures int    `json:"failures,omitempty"`
	Error    string `json:"error,omitempty"`
}

// handleDNS sends raw DNS queries over UDP to the actor's nameserver. Raw
// queries skip the stub resolver's search list and paired A and AAAA lookups,
// so each latency is one exchange with the DNS relay.
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
	name, err := dnsmessage.NewName(input.Name)
	if err != nil || input.Name == "" || input.Name[len(input.Name)-1] != '.' {
		writeJSON(w, http.StatusBadRequest, dnsProbeResponse{Error: "name must be a fully qualified DNS name ending in a dot"})
		return
	}
	if input.Count < 1 || input.Count > maxDNSProbeCount {
		writeJSON(w, http.StatusBadRequest, dnsProbeResponse{Error: fmt.Sprintf("count must be between 1 and %d", maxDNSProbeCount)})
		return
	}
	nameservers, err := atunneldns.ResolvConfNameservers("/etc/resolv.conf")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, dnsProbeResponse{Error: err.Error()})
		return
	}

	out := dnsProbeResponse{RCodes: map[string]int{}}
	for range input.Count {
		start := time.Now()
		resp, err := dnsProbeExchange(r.Context(), nameservers[0], name)
		elapsed := time.Since(start)
		if err != nil {
			out.Failures++
			if out.Error == "" {
				out.Error = err.Error()
			}
			continue
		}
		out.LatenciesMicros = append(out.LatenciesMicros, elapsed.Microseconds())
		out.RCodes[dnsmessage.RCode(resp[3]&0x0f).String()]++
	}
	writeJSON(w, http.StatusOK, out)
}

// dnsProbeExchange sends one A query for name and returns the response.
func dnsProbeExchange(ctx context.Context, nameserver string, name dnsmessage.Name) ([]byte, error) {
	id := uint16(rand.Uint32())
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, RecursionDesired: true})
	if err := b.StartQuestions(); err != nil {
		return nil, err
	}
	if err := b.Question(dnsmessage.Question{Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}); err != nil {
		return nil, err
	}
	query, err := b.Finish()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, dnsProbeQueryTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "udp", nameserver)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := conn.Write(query); err != nil {
		return nil, err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	resp := buf[:n]
	if !protocol.ResponseSanityCheck(resp) || protocol.TxnID(resp) != id {
		return nil, errors.New("response does not match the query")
	}
	return resp, nil
}
