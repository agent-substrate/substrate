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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/gorilla/websocket"
)

// deployWebsocketFixture renders and applies the websocket fixture for the sandbox class.
func deployWebsocketFixture(t *testing.T) string {
	t.Helper()
	root, err := e2e.FindRepoRoot()
	if err != nil {
		t.Fatalf("FindRepoRoot: %v", err)
	}
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}

	namespace := e2e.FixtureName("ate-e2e") + "-websocket"
	bucket := env["BUCKET_NAME"]
	manifest := e2e.RenderFixtureManifest(t, "internal/e2e/fixtures/testserver/websocket.yaml.tmpl", bucket, "websocket")

	applyArgs := []string{"ko", "apply", "-f", manifest}
	if e2e.KubeContext != "" {
		applyArgs = append(applyArgs, "--", "--context="+e2e.KubeContext)
	}
	e2e.RunCmdWithEnv(t, []string{"KO_CONFIG_PATH=" + root}, filepath.Join(root, "hack/run-tool.sh"), applyArgs...)

	t.Cleanup(func() {
		delArgs := []string{"delete", "--ignore-not-found", "-f", manifest}
		if e2e.KubeContext != "" {
			delArgs = append([]string{"--context=" + e2e.KubeContext}, delArgs...)
		}
		e2e.RunCmd(t, "kubectl", delArgs...)
	})

	return namespace
}

func TestWebsocketIngressPing(t *testing.T) {
	ctx := context.Background()
	clients := e2e.GetClients()

	namespace := deployWebsocketFixture(t)

	// Wait for the websocket ActorTemplate golden snapshot to be ready
	e2e.WaitForTemplateReady(ctx, t, clients, namespace, "websocket")

	actorName, _ := createAndResumeActor(t, ctx, "websocket", e2e.Fixture{Namespace: namespace, Name: "websocket"})

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
