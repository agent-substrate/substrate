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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

func dialAteAPI(endpoint string) (ateapipb.ControlClient, *grpc.ClientConn, error) {
	creds := credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, err
	}

	return ateapipb.NewControlClient(conn), conn, nil
}

func main() {
	actorID := pflag.String("id", "", "ID of the sandbox actor (required)")
	atespace := pflag.String("atespace", "", "Atespace the actor lives in (required)")
	ateapiAddr := pflag.String("ateapi", "localhost:8080", "Address of the ateapi gRPC server")
	atenetAddr := pflag.String("atenet", "localhost:8000", "Address of the atenet HTTP router")
	pflag.Parse()

	if *actorID == "" {
		log.Fatal("--id is required")
	}
	if *atespace == "" {
		log.Fatal("--atespace is required")
	}

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

	// 1. Connect to ateapi and Resume Actor
	log.Printf("Connecting to ateapi at %s...", *ateapiAddr)
	cli, conn, err := dialAteAPI(*ateapiAddr)
	if err != nil {
		log.Fatalf("Failed to dial ateapi: %v", err)
	}
	defer conn.Close()

	log.Printf("Resuming actor %s...", *actorID)
	_, err = cli.ResumeActor(ctx, &ateapipb.ResumeActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: *atespace, Name: *actorID}})
	if err != nil {
		log.Fatalf("Failed to resume actor: %v", err)
	}
	log.Println("Actor resumed successfully.")

	// Ensure we suspend the actor on exit
	defer func() {
		log.Printf("Suspending actor %s...", *actorID)
		suspendCtx := context.Background()
		_, err := cli.SuspendActor(suspendCtx, &ateapipb.SuspendActorRequest{ActorRef: &ateapipb.ActorRef{Atespace: *atespace, Name: *actorID}})
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
			output, err := runCommand(ctx, *atenetAddr, *atespace, *actorID, line)
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

func runCommand(ctx context.Context, atenetAddr, atespace, actorID, command string) (*ProcessResponse, error) {
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
	req.Host = resources.ActorDNSName(atespace, actorID)

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
