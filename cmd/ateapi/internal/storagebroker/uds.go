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

package storagebroker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

// The "uds" backend dials an out-of-process broker over a Unix domain socket.
// The broker holds the cloud identity and links the cloud SDK; ate-api-server
// links none. This is the end-state that keeps the control-plane core free of
// every cloud SDK (the sidecar is swappable per cloud), mirroring the mTLS-UDS
// pattern atelet already uses for its actor credential broker.
func init() {
	Register("uds", newUDSBroker)
}

// mintRequest and mintReply are the tiny wire contract between ate-api-server
// and the out-of-process broker sidecar.
type mintRequest struct {
	Verb        string `json:"verb"`
	SnapshotURI string `json:"snapshotUri"`
	TTLSeconds  int    `json:"ttlSeconds"`
}

type mintReply struct {
	PrefixURL      string            `json:"prefixUrl"`
	ReadToken      string            `json:"readToken,omitempty"`
	ReadObjectURLs map[string]string `json:"readObjectUrls,omitempty"`
	WriteMethod    string            `json:"writeMethod,omitempty"`
	WriteToken     string            `json:"writeToken,omitempty"`
	WriteHeaders   map[string]string `json:"writeHeaders,omitempty"`
	PostURL        string            `json:"postUrl,omitempty"`
	PostFields     map[string]string `json:"postFields,omitempty"`
}

type udsBroker struct {
	client *http.Client
}

func newUDSBroker(ctx context.Context) (Broker, error) {
	socket := os.Getenv("BROKER_UDS_PATH")
	if socket == "" {
		return nil, fmt.Errorf("uds storage broker needs BROKER_UDS_PATH")
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}
	return &udsBroker{client: &http.Client{Transport: transport}}, nil
}

func (b *udsBroker) MintRead(ctx context.Context, snapshotURI string, ttl time.Duration) (Capability, error) {
	return b.mint(ctx, "read", snapshotURI, ttl)
}

func (b *udsBroker) MintWrite(ctx context.Context, snapshotURI string, ttl time.Duration) (Capability, error) {
	return b.mint(ctx, "write", snapshotURI, ttl)
}

func (b *udsBroker) mint(ctx context.Context, verb, snapshotURI string, ttl time.Duration) (Capability, error) {
	body, err := json.Marshal(mintRequest{Verb: verb, SnapshotURI: snapshotURI, TTLSeconds: int(ttl.Seconds())})
	if err != nil {
		return Capability{}, err
	}
	// Host is ignored (the transport dials the socket); the path selects the op.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://broker/mint", bytes.NewReader(body))
	if err != nil {
		return Capability{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return Capability{}, fmt.Errorf("dialing broker over UDS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return Capability{}, fmt.Errorf("broker mint %s: status %d: %s", verb, resp.StatusCode, string(msg))
	}
	var reply mintReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return Capability{}, fmt.Errorf("decoding broker reply: %w", err)
	}
	// mintReply mirrors Capability field for field, so a struct conversion
	// copies it (struct tags are ignored in conversions).
	return Capability(reply), nil
}
