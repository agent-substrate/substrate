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

// Command driver is a Kubernetes pod that creates and communicates with counter
// actors.  In a real application, this would be your "application frontend"
// that authenticates requests from your users, then creates (and suspends)
// actors to do things on your users behalf.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/demos/counter/driver/credfolder"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	credentialFolder = flag.String("credential-folder", "", "SPIFFE credential folder")

	ateAPIConnString = flag.String("ate-api-connstring", "", "IP or DNS name to reach the Substrate control API.")
	ateRouterAddress = flag.String("ate-router-address", "atenet-router.ate-system.svc", "IP or DNS name to reach the Substrate ingress router")

	counterAtespace = flag.String("counter-atespace", "", "Atespace to operate in.")
	counterTemplate = flag.String("counter-template", "", "ActorTemplate to derive actors from.")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	if err := do(ctx); err != nil {
		slog.ErrorContext(ctx, "Error", slog.Any("err", err))
		os.Exit(1)
	}
}

func do(ctx context.Context) error {
	credLoader, err := credfolder.NewLoader(*credentialFolder)
	if err != nil {
		return fmt.Errorf("while creating credential loader: %w", err)
	}
	clientTLSConfig := &tls.Config{}
	credLoader.PrepareClientTLSConfig(clientTLSConfig)

	// Create Control API client
	controlConn, err := grpc.NewClient(
		*ateAPIConnString,
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLSConfig)),
	)
	if err != nil {
		return fmt.Errorf("while creating Substrate Control API client: %w", err)
	}
	controlClient := ateapipb.NewControlClient(controlConn)

	// Create router client.
	routerClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: clientTLSConfig,
		},
	}

	// Create actor
	wantActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: *counterAtespace,
			Name:     "counter-" + strings.ToLower(rand.Text()),
		},
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: *counterAtespace,
			Name:     *counterTemplate,
		},
	}
	actor, err := controlClient.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: wantActor,
	})
	if err != nil {
		return fmt.Errorf("while creating actor %s from template %s: %w",
			wantActor.GetMetadata().GetAtespace()+"/"+wantActor.GetMetadata().GetName(),
			wantActor.GetActorTemplate().GetAtespace()+"/"+wantActor.GetActorTemplate().GetName(),
			err,
		)
	}

	for {
		slog.InfoContext(ctx, "Sending request to actor",
			slog.String("actor", actor.GetMetadata().GetAtespace()+"/"+actor.GetMetadata().GetName()),
		)

		counterURL := &url.URL{
			Scheme: "https",
			Host:   *ateRouterAddress,
			Path:   "/",
		}
		counterReq, err := http.NewRequestWithContext(ctx, "GET", counterURL.String(), nil)
		if err != nil {
			return fmt.Errorf("while creating counter request: %w", err)
		}
		counterReq.Header.Add("ate-target-actor", actor.GetMetadata().GetAtespace()+"/"+actor.GetMetadata().GetName())
		counterResp, err := routerClient.Do(counterReq)
		if err != nil {
			return fmt.Errorf("while making request to %q: %w", actor.GetMetadata().GetAtespace()+"/"+actor.GetMetadata().GetName(), err)
		}
		defer counterResp.Body.Close()

		resp, err := io.ReadAll(counterResp.Body)
		if err != nil {
			return fmt.Errorf("while reading counter response body: %w", err)
		}

		slog.InfoContext(
			ctx, "Got counter response",
			slog.String("actor", actor.GetMetadata().GetAtespace()+"/"+actor.GetMetadata().GetName()),
			slog.String("response", string(resp)),
		)

		slog.InfoContext(ctx, "Pausing actor",
			slog.String("actor", actor.GetMetadata().GetAtespace()+"/"+actor.GetMetadata().GetName()),
		)

		_, err = controlClient.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor: &ateapipb.ObjectRef{
				Atespace: actor.GetMetadata().GetAtespace(),
				Name:     actor.GetMetadata().GetName(),
			},
		})
		if err != nil {
			return fmt.Errorf("while suspending actor: %w", err)
		}

		time.Sleep(30 * time.Second)
	}

	// unreachable
}
