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

package networking

import (
	"bufio"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
)

func TestActorTargetPortHeader(t *testing.T) {
	ctx := t.Context()
	_, actorName, _ := createAndResumeSubstrateActor(t, ctx, "targetport", e2e.SubstrateCounterFixture())
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	router := mustRouterClient(t, ctx)
	defer router.Close()

	t.Run("explicit port", func(t *testing.T) {
		body := waitForRouteReady(t, "target-port header", func() (*http.Response, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, router.BaseURL()+"/", nil)
			if err != nil {
				return nil, err
			}
			req.Host = "app.example.com:8443"
			req.Header.Set(atenet.TargetActorHeader, actorRef.String())
			req.Header.Set(atenet.TargetPortHeader, "9090")
			return router.DoReq(req)
		})
		if !strings.Contains(body, "extra port 9090") {
			t.Fatalf("response = %q, want extra port 9090", body)
		}
	})
	for _, values := range [][]string{{""}, {"0"}, {"65536"}, {"+80"}, {"80,9090"}, {"9090", "9090"}} {
		t.Run("header "+strings.Join(values, ","), func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, router.BaseURL()+"/", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(atenet.TargetActorHeader, actorRef.String())
			for _, value := range values {
				req.Header.Add(atenet.TargetPortHeader, value)
			}
			resp, err := router.DoReq(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			wantStatus := http.StatusBadRequest
			if len(values) == 1 && values[0] == "" {
				wantStatus = http.StatusOK
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if !counterDefaultPortIPPattern.Match(body) {
					t.Fatalf("empty header response = %q, want default port response", body)
				}
			}
			if resp.StatusCode != wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
			}
		})
	}
	for _, values := range [][]string{{"80"}, {"invalid"}, {"80", "9090"}} {
		t.Run("CONNECT ignores "+strings.Join(values, ","), func(t *testing.T) {
			router := mustRouterClient(t, ctx)
			defer router.Close()
			headers := http.Header{}
			headers.Set(atenet.TargetActorHeader, actorRef.String())
			for _, value := range values {
				headers.Add(atenet.TargetPortHeader, value)
			}
			conn, err := router.ConnectWithHeaders(ctx, actorRef, counterExtraPort, headers)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://app.example.com/", nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, value := range values {
				req.Header.Add(atenet.TargetPortHeader, value)
			}
			if err := req.Write(conn); err != nil {
				t.Fatal(err)
			}
			resp, err := http.ReadResponse(bufio.NewReader(conn), req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "extra port 9090") {
				t.Fatalf("CONNECT response = %d %q, want port 9090 response", resp.StatusCode, body)
			}
		})
	}
}
