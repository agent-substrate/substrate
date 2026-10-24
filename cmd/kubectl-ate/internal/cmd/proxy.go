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
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/resources"
)

var (
	proxyActorAtespaceFlag string
	proxyActorAddressFlag  string
	proxyActorPortFlag     int
)

var proxyCmd = &cobra.Command{Use: "proxy", Short: "Serve a local proxy to a resource"}

var proxyActorCmd = &cobra.Command{
	Use:   "actor <actor-name>",
	Short: "Serve a local HTTP proxy that sends every request to an actor through atenet-router",
	Long: `Serve a local HTTP proxy that sends every request to an actor through
atenet-router, resuming the actor if it is suspended.

The proxy presents a client certificate for the atenet-client ServiceAccount,
which the router allows by default. Requesting it needs permission to create
PodCertificateRequests in substrate's namespace.`,
	Args: exactlyOneArg("<actor-name>", "atespace"),
	RunE: func(cmd *cobra.Command, args []string) error {
		actorRef := resources.ActorRef{Atespace: proxyActorAtespaceFlag, Name: args[0]}
		transport, err := ateclient.NewRouterTransport(cmd.Context(), kubeconfig, k8sContext)
		if err != nil {
			return fmt.Errorf("failed to connect to atenet-router: %w", err)
		}
		listener, err := net.Listen("tcp", net.JoinHostPort(proxyActorAddressFlag, strconv.Itoa(proxyActorPortFlag)))
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.ErrOrStderr(), "Proxying http://%s to actor %s\n", listener.Addr(), actorRef)
		return http.Serve(listener, newActorProxy(actorRef, transport))
	},
}

// newActorProxy sends every request to actorRef through atenet-router. The
// request keeps its Host header, so an actor that builds URLs from it builds
// ones that point back at the proxy.
func newActorProxy(actorRef resources.ActorRef, transport http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.Out.URL.Scheme = "https"
			r.Out.URL.Host = ateclient.RouterServerName()
			r.Out.Header.Set(atenet.TargetActorHeader, actorRef.String())
		},
		Transport: transport,
	}
}

func init() {
	proxyActorCmd.Flags().StringVarP(&proxyActorAtespaceFlag, "atespace", "a", "", "the actor's atespace")
	proxyActorCmd.Flags().StringVar(&proxyActorAddressFlag, "address", "127.0.0.1", "the local address to listen on")
	proxyActorCmd.Flags().IntVarP(&proxyActorPortFlag, "port", "p", 8000, "the local port to listen on")
	proxyCmd.AddCommand(proxyActorCmd)
}
