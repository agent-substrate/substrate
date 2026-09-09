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
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/gorilla/websocket"
)

// deployWebsocketFixture installs the websocket fixture for the sandbox class
// under test and waits for its golden snapshot.
func deployWebsocketFixture(t *testing.T, ctx context.Context) e2e.SubstrateFixture {
	t.Helper()
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	atespace, _ := e2e.DeploySubstrateFixture(t, ctx, e2e.GetClients(), e2e.SubstrateFixtureManifests{
		Pool:     "internal/e2e/fixtures/testserver/websocket.yaml.tmpl",
		Template: "internal/e2e/fixtures/testserver/websocket-template.yaml.tmpl",
	}, env["BUCKET_NAME"], "websocket", false)

	return e2e.SubstrateFixture{
		Atespace:   atespace,
		Name:       "websocket",
		DeployWith: "the networking suite itself (see deployWebsocketFixture)",
	}
}

func TestWebsocketIngressPing(t *testing.T) {
	ctx := context.Background()

	fixture := deployWebsocketFixture(t, ctx)

	actorName, _ := createAndResumeSubstrateActor(t, ctx, "websocket", fixture)

	rc := mustRouterClient(t, ctx)
	defer rc.Close()

	// Convert http://127.0.0.1:<port> to ws://127.0.0.1:<port>/ws
	wsURLStr := strings.Replace(rc.BaseURL(), "http://", "ws://", 1) + "/ws"
	u, err := url.Parse(wsURLStr)
	if err != nil {
		t.Fatalf("parse ws URL: %v", err)
	}

	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	header := http.Header{}
	header.Set("Host", resources.ActorDNSName(actorRef))

	var c *websocket.Conn

	// Ride out atenet-router xDS snapshot sync lag (up to ~30s), similar to waitForRouteReady
	deadline := time.Now().Add(30 * time.Second)
	for {
		var resp *http.Response
		c, resp, err = websocket.DefaultDialer.DialContext(ctx, u.String(), header)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("websocket dial %s failed after 30s: %v", u.String(), err)
		}
		if resp != nil {
			t.Logf("websocket dial returned HTTP %d, retrying...", resp.StatusCode)
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}
	defer c.Close()

	err = c.WriteMessage(websocket.TextMessage, []byte("PING"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	mt, message, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	if mt != websocket.TextMessage {
		t.Fatalf("expected text message, got %d", mt)
	}
	if string(message) != "PONG" {
		t.Fatalf("expected PONG, got %s", message)
	}

	t.Log("Websocket PingPong succeeded")
}
