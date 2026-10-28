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

// Command objectstore-plugin serves the object-store plugin API on GCS or S3 over
// a Unix socket. It runs as a sidecar: "node" next to atelet, serving snapshot
// transfers and sandbox assets, and "control" next to ate-api-server. The
// backend is chosen by ATE_STORAGE_BACKEND ("s3", or GCS by default) with the
// ambient credentials.
// "healthcheck" reports whether a running plugin is serving, for the sidecar's
// startup probe.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"cloud.google.com/go/storage"
	"github.com/spf13/pflag"
	objectstorev1 "go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/objectstore"
	"go.ate.dev/substrate/internal/objectstoreplugin"
	"go.ate.dev/substrate/internal/objectstoreplugin/exchange"
	"go.ate.dev/substrate/internal/objectstoreplugin/exchangeclient"
	"go.ate.dev/substrate/internal/pluginsocket"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/serverboot"
	"go.ate.dev/substrate/pkg/objectstorage"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const usage = `usage: objectstore-plugin <node|control|healthcheck> [flags]`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	mode := os.Args[1]
	flags := pflag.NewFlagSet(mode, pflag.ExitOnError)
	socket := flags.String("socket", "", "Unix socket to serve on, or to check in healthcheck mode")
	root := flags.String("root", nodepath.BasePath, "node mode: the only directory tree local snapshot and asset files may be read from or written to")
	uploadMemory := flags.String("upload-memory-budget", resource.NewQuantity(objectstorage.DefaultUploadMemory, resource.BinarySI).String(), "node mode: shared memory reservations for snapshot compression and upload buffers, as a Kubernetes quantity; uploads wait for capacity")
	assetStagingDir := flags.String("asset-staging-dir", "/var/lib/objectstore-plugin/asset-staging", "node mode: directory sandbox assets are downloaded into and verified before they are copied below --root; must not be below --root or readable by the caller")
	assetStagingCapacity := flags.String("asset-staging-capacity", "8Gi", "node mode: the most sandbox asset downloads in --asset-staging-dir may hold at once, as a Kubernetes quantity; downloads wait for space, and an asset larger than this is refused")
	timeout := flags.Duration("timeout", 5*time.Second, "healthcheck mode: how long to wait for the plugin to report that it is serving")
	apiHost := installdefaults.APIServiceHost(installdefaults.NamespaceFromPodEnv())
	var ncfg exchangeclient.Config
	flags.StringVar(&ncfg.Address, "exchange-address", net.JoinHostPort(apiHost, strconv.Itoa(exchange.DefaultPort)), "node mode on GCS: address of the credential exchange the control sidecar serves, where each transfer's actor JWT is traded for a token scoped to the actor")
	flags.StringVar(&ncfg.CAFile, "exchange-ca-file", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "node mode on GCS: CA bundle used to verify the exchange")
	flags.StringVar(&ncfg.ServerName, "exchange-server-name", apiHost, "node mode on GCS: DNS name expected on the exchange's certificate, the API Service in the pod's namespace")
	var xcfg exchangeConfig
	flags.StringVar(&xcfg.listen, "exchange-listen", "", "control mode: TCP address to serve the credential exchange on over TLS, for example :8445. Empty leaves it unserved.")
	flags.StringVar(&xcfg.credBundle, "exchange-cred-bundle", "/run/servicedns.podcert.ate.dev/credential-bundle.pem", "control mode: PEM credential bundle the exchange serves with, the pod's service DNS certificate")
	flags.StringVar(&xcfg.issuer, "actor-jwt-issuer", "", "control mode: issuer URL of the actor JWTs the exchange accepts, the value ate-api-server runs with")
	flags.StringVar(&xcfg.jwksFile, "actor-jwt-jwks-file", "/run/actor-id-jwt-jwks/jwks.json", "control mode: JWK set file holding the public keys actor JWTs are verified with")
	flags.StringVar(&xcfg.transferAccount, "transfer-service-account", "", "control mode on GCS: email of the Google service account snapshot transfers run as. The exchange impersonates it and downscopes its token per actor")
	flags.StringVar(&xcfg.ateapi.Address, "ateapi-address", "127.0.0.1:443", "control mode: ate-api-server gRPC address, the loopback in the shared pod")
	flags.StringVar(&xcfg.ateapi.CAFile, "ateapi-ca-file", "/run/servicedns.podcert.ate.dev/trust-bundle.pem", "control mode: CA bundle used to verify ate-api-server")
	flags.StringVar(&xcfg.ateapi.ServerName, "ateapi-server-name", installdefaults.APIServiceHost(installdefaults.NamespaceFromPodEnv()), "control mode: DNS name expected on the ate-api-server certificate, the API Service in the pod's namespace")
	flags.StringVar(&xcfg.ateapi.ClientCredBundle, "ateapi-client-cred-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "control mode: PEM credential bundle presented to ate-api-server, the pod identity certificate")
	previewFlags := flags.StringSlice("preview", nil, "Preview gates to enable.")
	_ = flags.Parse(os.Args[2:])

	ctx := context.Background()
	serverboot.InitLogger()
	if err := preview.Init(*previewFlags...); err != nil {
		serverboot.Fatal(ctx, "Invalid --preview", err)
	}
	if *socket == "" {
		serverboot.Fatal(ctx, "Missing --socket", fmt.Errorf("--socket is required"))
	}

	// healthcheck is what the sidecar's startup probe runs: the image has no
	// shell or grpc_health_probe, so the binary checks itself.
	if mode == "healthcheck" {
		if err := healthcheck(ctx, *socket, *timeout); err != nil {
			serverboot.Fatal(ctx, "Object-store plugin is not serving", err)
		}
		return
	}

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "objectstore-plugin-" + mode,
		Sampling:    serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	// stopExchange drains the credential exchange, when control mode serves it.
	stopExchange := func() {}
	switch mode {
	case "node":
		objects, err := objectstorage.NewFromEnv(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to set up the object storage backend", err)
		}
		uploadCapacity, err := parseCapacity(*uploadMemory)
		if err != nil {
			serverboot.Fatal(ctx, "Invalid --upload-memory-budget", err)
		}
		// On GCS every transfer runs on a token scoped to its actor, from the
		// exchange. S3 has no per-actor credential and keeps the pod's own.
		var nodeOpts []objectstoreplugin.NodeOption
		if !objectstorage.UsesS3() {
			creds, err := exchangeclient.New(ncfg)
			if err != nil {
				serverboot.Fatal(ctx, "Failed to set up the credential exchange client", err)
			}
			defer creds.Close()
			nodeOpts = append(nodeOpts, objectstoreplugin.WithCredentials(creds))
		}
		plugin, err := objectstoreplugin.NewNodePlugin(objects, *root, uploadCapacity, nodeOpts...)
		if err != nil {
			serverboot.Fatal(ctx, "Invalid node upload configuration", err)
		}
		objectstorev1.RegisterNodeProviderServer(srv, plugin)
		capacity, err := parseCapacity(*assetStagingCapacity)
		if err != nil {
			serverboot.Fatal(ctx, "Invalid --asset-staging-capacity", err)
		}
		if err := os.MkdirAll(*assetStagingDir, 0o700); err != nil {
			serverboot.Fatal(ctx, "Failed to create --asset-staging-dir", err)
		}
		assets, err := objectstoreplugin.NewAssetPlugin(objects, assetScheme(), *root, *assetStagingDir, capacity)
		if err != nil {
			serverboot.Fatal(ctx, "Invalid --asset-staging-dir", err)
		}
		objectstorev1.RegisterAssetProviderServer(srv, assets)
	case "control":
		store, err := newObjectStore(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to set up the object storage backend", err)
		}
		objectstorev1.RegisterControlProviderServer(srv, objectstoreplugin.NewControlPlugin(store))
		if xcfg.listen != "" {
			stop, err := serveExchange(ctx, xcfg)
			if err != nil {
				serverboot.Fatal(ctx, "Failed to serve the credential exchange", err)
			}
			stopExchange = stop
		}
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	healthpb.RegisterHealthServer(srv, health.NewServer())

	lis, err := pluginsocket.Listen(*socket)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen", err)
	}

	// Finish in-flight transfers on SIGTERM. The pod's grace period bounds
	// how long that may take. The exchange, when served, drains at the same
	// time as the socket rather than after it.
	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()
	exchangeDrained := make(chan struct{})
	go func() {
		<-sigCtx.Done()
		slog.InfoContext(ctx, "Draining object-store plugin")
		go func() {
			stopExchange()
			close(exchangeDrained)
		}()
		srv.GracefulStop()
	}()

	slog.InfoContext(ctx, "Serving object-store plugin", slog.String("mode", mode), slog.String("socket", *socket))
	if err := srv.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Serve failed", err)
	}
	// Serve returns only once GracefulStop ran, so the exchange is draining.
	<-exchangeDrained
}

// newObjectStore builds the client the control plugin manages snapshot
// prefixes with, on the same backend objectstorage.NewFromEnv selects for the
// node plugin. It builds its own GCS client because objectstorage's carries a
// retry policy scoped to that package.
func newObjectStore(ctx context.Context) (objectstore.Store, error) {
	if objectstorage.UsesS3() {
		client, err := objectstorage.NewS3ClientFromEnv(ctx)
		if err != nil {
			return nil, err
		}
		return objectstore.NewS3(client), nil
	}
	// GCS is currently the default, TODO: we assume workload identity / ADC
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}
	return objectstore.NewGCS(client), nil
}

// assetScheme is the asset URI scheme the node plugin serves: its backend's,
// as objectstorage.NewFromEnv selects it.
func assetScheme() string {
	if objectstorage.UsesS3() {
		return "s3"
	}
	return "gs"
}

// healthcheck dials the plugin at socket and waits up to timeout for it to
// report that it is serving.
func healthcheck(ctx context.Context, socket string, timeout time.Duration) error {
	conn, err := pluginsocket.Dial(socket, pluginsocket.ReadyWait)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return pluginsocket.WaitReady(ctx, conn)
}

// parseCapacity parses a positive byte count written as a Kubernetes quantity,
// such as 8Gi.
func parseCapacity(s string) (int64, error) {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0, err
	}
	n, ok := q.AsInt64()
	if !ok || n <= 0 {
		return 0, fmt.Errorf("%q is not a positive whole number of bytes", s)
	}
	return n, nil
}
