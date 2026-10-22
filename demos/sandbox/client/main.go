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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/pflag"
	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

type ProcessRequest struct {
	Command []string          `json:"command"`
	EnvVars map[string]string `json:"envvars,omitempty"`
	Cwd     string            `json:"cwd,omitempty"`
	Timeout string            `json:"timeout,omitempty"`
}

type ProcessResponse struct {
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
	Error  string `json:"error,omitempty"`
}

func main() {
	actorName := pflag.String("name", "", "Name of the sandbox actor (required)")
	atespace := pflag.String("atespace", "", "Atespace the actor lives in (required)")
	ateapiAddr := pflag.String("ateapi", "", "Address of the ateapi gRPC server; empty auto-forwards to svc/api in the substrate system namespace")
	atenetAddr := pflag.String("atenet", "localhost:8000", "Address of the atenet HTTP router")
	pflag.Parse()

	if *actorName == "" {
		log.Fatal("--name is required")
	}
	if *atespace == "" {
		log.Fatal("--atespace is required")
	}
	actorRef := resources.ActorRef{Atespace: *atespace, Name: *actorName}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nInterrupt received, shutting down...")
		cancel()
	}()

	// 1. Connect to ateapi, once, up front, so a bad kube context fails here
	// rather than at the first command. The connection is deliberately not tied
	// to ctx: ctx is canceled by the interrupt handler, and the deferred suspend
	// below still has to reach ateapi after that.
	log.Printf("Connecting to ateapi (endpoint=%q; empty = auto port-forward to svc/api)...", *ateapiAddr)
	cli, err := ateclient.NewClient(context.Background(), "", "", *ateapiAddr, "", false)
	if err != nil {
		log.Fatalf("Failed to connect to ateapi: %v", err)
	}
	defer cli.Close()

	// No explicit resume: a request through the router activates a suspended
	// actor on its own, so the first command wakes it.

	// Ensure we suspend the actor on exit.
	defer func() {
		log.Printf("Suspending actor %s...", actorRef.Name)
		suspendCtx := context.Background()
		_, err := cli.SuspendActor(suspendCtx, &ateapipb.SuspendActorRequest{Actor: actorRef.ToObjectRef()})
		if err != nil {
			log.Printf("Failed to suspend actor: %v", err)
		} else {
			log.Println("Actor suspended successfully.")
		}
	}()

	// 2. Start Input Loop (REPL)
	fmt.Println("Starting Sandbox REPL. Type 'exit' to leave.")

	lines := make(chan string)
	scanErrors := make(chan error, 1)

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			scanErrors <- err
		}
		close(lines)
	}()

	for {
		fmt.Print("sandbox> ")
		select {
		case <-ctx.Done():
			// Context was canceled (e.g., CTRL+C)
			return
		case err := <-scanErrors:
			log.Printf("Error reading standard input: %v", err)
			return
		case line, ok := <-lines:
			if !ok {
				// Stdin closed (EOF)
				return
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if line == "exit" {
				return
			}

			// Send command to atenet router
			output, err := runCommand(ctx, *atenetAddr, actorRef, line)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			if output.Error != "" {
				fmt.Printf("Command error: %s\n", output.Error)
			}
			if output.Stdout != "" {
				fmt.Print(output.Stdout)
			}
			if output.Stderr != "" {
				fmt.Print(output.Stderr)
			}
		}
	}
}

func runCommand(ctx context.Context, atenetAddr string, actorRef resources.ActorRef, command string) (*ProcessResponse, error) {
	url := fmt.Sprintf("http://%s/process", atenetAddr)

	reqBody := ProcessRequest{
		Command: []string{"sh", "-c", command},
	}
	jsonBody, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(atenet.TargetActorHeader, actorRef.String())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var processResp ProcessResponse
	if err := json.NewDecoder(resp.Body).Decode(&processResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &processResp, nil
}
