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

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var residentSecret string

func init() {
	// Unique ID generated in volatile RAM on process start
	residentSecret = fmt.Sprintf("SECRET-%d", time.Now().UnixNano()%10000)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/", handleRequest)

	port := os.Getenv("PORT")
	if port == "" {
		port = "80"
	}

	log.Printf("Self-Suspending Agent listening on :%s with Identity: %s", port, residentSecret)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	log.Printf("DEBUG: Request received. Path=%s Host=%s", r.URL.Path, r.Host)

	// 1. Identify Actor (Robust extraction)
	actorName := r.Header.Get("X-AgentSet-Session")
	if actorName == "" {
		actorName = r.Header.Get("x-agentset-session")
	}
	var atespace string
	if actorName == "" {
		host := r.Host
		if host == "" {
			host = r.Header.Get("Host")
		}
		atespace, actorName, _ = resources.ParseActorDNSName(host)
	}

	if actorName == "" {
		actorName = "unknown"
	}

	log.Printf("DEBUG: Identified ActorName: [%s]", actorName)

	body, _ := io.ReadAll(r.Body)
	message := string(body)
	if message == "" {
		message = "Status Check"
	}

	// 1. Respond to user
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Agent Response: [%s] | Identity: %s | Session: %s\n", message, residentSecret, actorName))
	if actorName == "" {
		sb.WriteString("DEBUG: ID Missing. Headers received:\n")
		for k, v := range r.Header {
			sb.WriteString(fmt.Sprintf("  %s: %v\n", k, v))
		}
	}
	response := sb.String()
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(response))

	// 2. Self-Suspend (Zero-Idle)
	if actorName != "" && actorName != "localhost" && !strings.Contains(actorName, ":") {
		// Use a goroutine to avoid blocking the HTTP response
		go func() {
			// We linger for 7 seconds in this demo to make the multiplexing visible in the CLI.
			time.Sleep(7 * time.Second)
			suspendSelf(atespace, actorName)
		}()
	}
}

func suspendSelf(atespace, actorName string) {
	apiAddr := os.Getenv("ATE_API_ADDR")
	if apiAddr == "" {
		apiAddr = "api.ate-system.svc.cluster.local:443"
	}

	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})
	conn, err := grpc.Dial(apiAddr, grpc.WithTransportCredentials(creds)) //nolint:staticcheck // SA1019: TODO migrate to grpc.NewClient.
	if err != nil {
		log.Printf("Failed to connect to ATE API: %v", err)
		return
	}
	defer conn.Close()

	client := ateapipb.NewControlClient(conn)

	log.Printf("Yielding compute. Requesting self-suspension for actor %s...", actorName)
	_, err = client.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName}})
	if err != nil {
		log.Printf("Failed to self-suspend: %v", err)
	}
}
