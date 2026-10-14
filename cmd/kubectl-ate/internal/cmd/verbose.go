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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/ateclient"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/rest"
)

// Verbosity levels for -v / --verbose.
const (
	vBasic = 1 // -v: high-level info, non-fatal errors, retries, and waits
	vFlow  = 2 // -vv: step-by-step execution flow
	vTrace = 5 // -vvvvv: execution debugging and tracing
)

// slowOpThreshold is the duration above which an operation is reported at
// vBasic (-v) even if its normal completion log is at vFlow (-vv).
const slowOpThreshold = 2 * time.Second

var vlock sync.Mutex

// vlogf writes a formatted diagnostic line to w (or os.Stderr if w is nil)
// when the configured verbosity is at least level.
func vlogf(w io.Writer, level int, format string, args ...any) {
	if verbosity < level {
		return
	}
	if w == nil {
		w = os.Stderr
	}
	vlock.Lock()
	defer vlock.Unlock()
	fmt.Fprintf(w, strings.TrimSuffix(format, "\n")+"\n", args...)
}

// loadKubeConfig loads the kubeconfig and logs the resolved API server
// when verbosity is enabled.
func loadKubeConfig(errOut io.Writer, kubeconfigPath, contextName string) (*rest.Config, error) {
	vlogf(errOut, vFlow, "Loading kubeconfig (path=%q, context=%q)", kubeconfigPath, contextName)
	cfg, err := ateclient.LoadKubeConfig(kubeconfigPath, contextName)
	if err != nil {
		return nil, err
	}
	vlogf(errOut, vTrace, "Loaded kubeconfig for Kubernetes API server %s", cfg.Host)
	return cfg, nil
}

// newAPIClient connects to ate-api-server with a verbosity-aware gRPC unary
// interceptor attached.
func newAPIClient(cmd *cobra.Command) (*ateclient.Client, error) {
	ctx := cmd.Context()
	errOut := cmd.ErrOrStderr()

	if verbosity >= vFlow {
		vlogf(errOut, vFlow, "Connecting to ate-api-server (endpoint=%q, kubeconfig=%q, context=%q, token-file=%q, trace=%t)",
			endpoint, kubeconfig, k8sContext, tokenFile, traceEnabled)
	} else if endpoint != "" {
		vlogf(errOut, vBasic, "Connecting to ate-api-server at %s", endpoint)
	} else {
		vlogf(errOut, vBasic, "Connecting to ate-api-server via port-forward")
	}

	start := time.Now()
	cli, err := ateclient.NewClient(
		ctx, kubeconfig, k8sContext, endpoint, tokenFile, traceEnabled,
		grpc.WithChainUnaryInterceptor(newLoggingUnaryInterceptor(errOut)),
	)
	elapsed := time.Since(start).Round(time.Millisecond)
	if err != nil {
		vlogf(errOut, vBasic, "Connection to ate-api-server failed after %s: %v", elapsed, err)
		return nil, err
	}

	if elapsed >= slowOpThreshold {
		vlogf(errOut, vBasic, "Connected to ate-api-server in %s", elapsed)
	} else {
		vlogf(errOut, vFlow, "Connected to ate-api-server in %s", elapsed)
	}

	return cli, nil
}

func formatProtoCompact(v any) string {
	msg, ok := v.(proto.Message)
	if !ok || msg == nil {
		return "<nil>"
	}
	b, err := (protojson.MarshalOptions{Multiline: false}).Marshal(msg)
	if err != nil {
		return fmt.Sprintf("<marshal error: %v>", err)
	}
	return string(b)
}

// newLoggingUnaryInterceptor returns a gRPC unary client interceptor that logs
// RPC calls, durations, errors, and (at vTrace) request/response payloads.
func newLoggingUnaryInterceptor(errOut io.Writer) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if verbosity >= vTrace {
			vlogf(errOut, vTrace, "gRPC %s request: %s", method, formatProtoCompact(req))
		} else {
			vlogf(errOut, vFlow, "gRPC %s", method)
		}
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		elapsed := time.Since(start).Round(time.Millisecond)
		if err != nil {
			vlogf(errOut, vBasic, "gRPC %s failed after %s: %v", method, elapsed, err)
			return err
		}
		if verbosity >= vTrace {
			vlogf(errOut, vTrace, "gRPC %s response (%s): %s", method, elapsed, formatProtoCompact(reply))
		} else if elapsed >= slowOpThreshold {
			vlogf(errOut, vBasic, "gRPC %s completed in %s", method, elapsed)
		} else {
			vlogf(errOut, vFlow, "gRPC %s completed in %s", method, elapsed)
		}
		return nil
	}
}
