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
	"fmt"
	"io"
	"net"
	"slices"
	"testing"

	"go.ate.dev/substrate/internal/resources"
)

func TestParsePortMappings(t *testing.T) {
	tests := []struct {
		name    string
		specs   []string
		want    []portMapping
		wantErr bool
	}{
		{
			name:  "default when omitted",
			specs: nil,
			want:  []portMapping{{localPort: 8000, remotePort: 80}},
		},
		{
			name:  "single port maps local and remote to same port",
			specs: []string{"9090"},
			want:  []portMapping{{localPort: 9090, remotePort: 9090}},
		},
		{
			name:  "explicit local and remote port",
			specs: []string{"8080:80"},
			want:  []portMapping{{localPort: 8080, remotePort: 80}},
		},
		{
			name:  "random local port",
			specs: []string{":80", "0:9090"},
			want:  []portMapping{{localPort: 0, remotePort: 80}, {localPort: 0, remotePort: 9090}},
		},
		{
			name:  "multiple ports",
			specs: []string{"8000:80", "9090"},
			want:  []portMapping{{localPort: 8000, remotePort: 80}, {localPort: 9090, remotePort: 9090}},
		},
		{
			name:    "zero single port",
			specs:   []string{"0"},
			wantErr: true,
		},
		{
			name:    "non-numeric port",
			specs:   []string{"http"},
			wantErr: true,
		},
		{
			name:    "missing remote port",
			specs:   []string{"8000:"},
			wantErr: true,
		},
		{
			name:    "zero remote port",
			specs:   []string{"8000:0"},
			wantErr: true,
		},
		{
			name:    "remote port out of range",
			specs:   []string{"8000:65536"},
			wantErr: true,
		},
		{
			name:    "negative local port",
			specs:   []string{"-1:80"},
			wantErr: true,
		},
		{
			name:    "local port out of range",
			specs:   []string{"65536:80"},
			wantErr: true,
		},
		{
			name:    "extra colon",
			specs:   []string{"8000:80:80"},
			wantErr: true,
		},
		{
			name:    "duplicate local port",
			specs:   []string{"8000:80", "8000:9090"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePortMappings(tc.specs)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parsePortMappings(%v) error = %v, wantErr %t", tc.specs, err, tc.wantErr)
			}
			if !tc.wantErr && !slices.Equal(got, tc.want) {
				t.Errorf("parsePortMappings(%v) = %v, want %v", tc.specs, got, tc.want)
			}
		})
	}
}

func TestServeActorProxy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lis80, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis80.Close()
	lis9090, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis9090.Close()

	wantActor := resources.ActorRef{Atespace: "team", Name: "my-counter"}
	dial := func(_ context.Context, actorRef resources.ActorRef, port int) (net.Conn, error) {
		if actorRef != wantActor {
			return nil, fmt.Errorf("unexpected actor %s", actorRef)
		}
		backendLis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		go func() {
			defer backendLis.Close()
			serverSide, err := backendLis.Accept()
			if err != nil {
				return
			}
			defer serverSide.Close()
			payload, err := io.ReadAll(serverSide)
			if err != nil {
				return
			}
			_, _ = fmt.Fprintf(serverSide, "%s:%d:%s", actorRef, port, payload)
		}()
		return net.Dial("tcp", backendLis.Addr().String())
	}

	done := make(chan error, 1)
	go func() {
		done <- serveActorProxy(
			ctx,
			io.Discard,
			[]net.Listener{lis80, lis9090},
			[]portMapping{{remotePort: 80}, {remotePort: 9090}},
			wantActor,
			dial,
		)
	}()

	for _, tc := range []struct {
		addr string
		send string
		want string
	}{
		{addr: lis80.Addr().String(), send: "hello", want: "team/my-counter:80:hello"},
		{addr: lis9090.Addr().String(), send: "metrics", want: "team/my-counter:9090:metrics"},
	} {
		conn, err := net.Dial("tcp", tc.addr)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(conn, tc.send); err != nil {
			t.Fatal(err)
		}
		if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(conn)
		_ = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tc.want {
			t.Errorf("response from %s = %q, want %q", tc.addr, got, tc.want)
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serveActorProxy: %v", err)
	}
}
