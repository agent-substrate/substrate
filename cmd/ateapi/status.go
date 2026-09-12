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

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/statusz"
	"github.com/spf13/pflag"
)

const statusShutdownTimeout = 2 * time.Second

var statusEnvByFlag = map[string]string{
	"postgres-connection-string": "ATE_API_POSTGRES_CONNECTION_STRING",
	"postgres-schema":            "ATE_API_POSTGRES_SCHEMA",
}

var statusValueFlags = map[string]bool{
	"drain-delay":            true,
	"drain-timeout":          true,
	"egress-gateway-address": true,
	"grpc-listen-addr":       true,
	"log-level":              true,
	"metrics-listen-addr":    true,
	"postgres-schema":        true,
	"status-port":            true,
	"version":                true,
}

var statusPresenceFlags = map[string]bool{
	"actor-id-ca-pool":          true,
	"actor-id-jwt-pool":         true,
	"atelet-client-cred-bundle": true,
	"authentication-config":     true,
	"grpc-server-cred-bundle":   true,
	"pod-identity-ca-certs":     true,
}

func captureStatusEnvSources(flags *pflag.FlagSet) map[string]string {
	sources := make(map[string]string)
	for flagName, envName := range statusEnvByFlag {
		flag := flags.Lookup(flagName)
		if flag != nil && flag.Value.String() == "@env" {
			sources[flagName] = envName
		}
	}
	return sources
}

func projectStatusFlags(flags *pflag.FlagSet, envSources map[string]string) []statusz.Flag {
	projected := make([]statusz.Flag, 0, flags.NFlag())
	flags.VisitAll(func(flag *pflag.Flag) {
		source := "default"
		if flag.Changed {
			source = "command line"
		}
		if envName, ok := envSources[flag.Name]; ok {
			source = "environment: " + envName
			projected = append(projected, statusz.Flag{Name: flag.Name, Value: "[redacted]", Source: source})
			return
		}

		value := "[redacted]"
		switch {
		case statusPresenceFlags[flag.Name]:
			value = "configured"
			if flag.Value.String() == "" {
				value = "disabled"
			}
		case statusValueFlags[flag.Name]:
			value = flag.Value.String()
		}
		projected = append(projected, statusz.Flag{Name: flag.Name, Value: value, Source: source})
	})
	return projected
}

func statusListenAddress(port int) string {
	if port <= 0 {
		return "disabled"
	}
	return ":" + strconv.Itoa(port)
}

type listenerFactory func(network, address string) (net.Listener, error)

type runningStatusHTTPServer struct {
	server           *http.Server
	listener         net.Listener
	unexpectedErrors <-chan error
}

func startStatusHTTPServer(port int, handler http.Handler, listen listenerFactory) (*runningStatusHTTPServer, error) {
	if port <= 0 {
		return nil, nil
	}
	listener, err := listen("tcp", statusListenAddress(port))
	if err != nil {
		return nil, fmt.Errorf("binding status port %d: %w", port, err)
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	unexpectedErrors := make(chan error, 1)
	go func() {
		defer close(unexpectedErrors)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			unexpectedErrors <- err
		}
	}()
	return &runningStatusHTTPServer{server: server, listener: listener, unexpectedErrors: unexpectedErrors}, nil
}

type statusShutdownResult struct {
	Forced bool
	Err    error
}

func shutdownStatusAfter(drainDone <-chan struct{}, server *http.Server, timeout time.Duration) <-chan statusShutdownResult {
	done := make(chan statusShutdownResult, 1)
	go func() {
		defer close(done)
		<-drainDone
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			result := statusShutdownResult{Forced: true, Err: fmt.Errorf("graceful status HTTP shutdown: %w", err)}
			if closeErr := server.Close(); closeErr != nil {
				result.Err = errors.Join(result.Err, fmt.Errorf("forcing status HTTP close: %w", closeErr))
			}
			done <- result
			return
		}
		done <- statusShutdownResult{}
	}()
	return done
}
