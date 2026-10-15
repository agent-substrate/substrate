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

// Command azure-blob-objectstore-plugin serves objectstore.v1 over a
// Unix socket using Azure Blob Storage: snapshot transfers in node and control
// mode, and sandbox assets in node mode when --asset-containers is set.
// "healthcheck" reports whether a running plugin is serving, for the sidecar's
// startup probe.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/agent-substrate/substrate/internal/plugins/azure-blob/internal/blobstore"
	"github.com/agent-substrate/substrate/internal/plugins/azure-blob/internal/provider"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
)

func main() {
	if err := run(context.Background(), os.Args[1:], newAzureStores); err != nil {
		slog.Error("azure-blob-objectstore-plugin exited", slog.Any("err", err))
		os.Exit(1)
	}
}

// storeFactory returns a store for each container in the account at
// accountURL.
type storeFactory func(accountURL string, containers []string) (map[string]blobstore.Store, error)

func newAzureStores(accountURL string, containers []string) (map[string]blobstore.Store, error) {
	credential, err := newWorkloadIdentityCredential()
	if err != nil {
		return nil, err
	}
	return blobstore.NewAzure(accountURL, containers, credential)
}

// workloadIdentityEnv lists the variables Microsoft Entra workload identity
// federation reads: the identity's client ID, its tenant, and the projected
// service account token exchanged for an Entra token.
var workloadIdentityEnv = []string{"AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE"}

// newWorkloadIdentityCredential returns the plugin's only credential: Microsoft
// Entra workload identity federation from the pod's projected service account
// token. It never falls back to another credential, so the plugin cannot
// authenticate as the node's managed identity or with an application secret.
func newWorkloadIdentityCredential() (azcore.TokenCredential, error) {
	var missing []string
	for _, name := range workloadIdentityEnv {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("workload identity is not configured: %s must be set", strings.Join(missing, ", "))
	}
	credential, err := azidentity.NewWorkloadIdentityCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("creating Azure workload identity credential: %w", err)
	}
	// The store reports a token request that got no response as
	// retryable, so an unreadable token file must fail here instead.
	if _, err := os.ReadFile(os.Getenv("AZURE_FEDERATED_TOKEN_FILE")); err != nil {
		return nil, fmt.Errorf("reading the Azure workload identity token file: %w", err)
	}
	return credential, nil
}

const usage = "usage: azure-blob-objectstore-plugin <node|control|healthcheck> [flags]"

func run(ctx context.Context, args []string, newStore storeFactory) error {
	if len(args) > 0 && args[0] == "healthcheck" {
		return runHealthcheck(ctx, args[1:])
	}
	if len(args) == 0 || (args[0] != "node" && args[0] != "control") {
		return errors.New(usage)
	}
	mode := args[0]
	flags := flag.NewFlagSet(mode, flag.ContinueOnError)
	socket := flags.String("socket", "", "Unix socket to serve on (required)")
	accountURL := flags.String("account-url", os.Getenv("AZURE_STORAGE_ACCOUNT_URL"), "Azure Blob account URL (or AZURE_STORAGE_ACCOUNT_URL)")
	containerList := flags.String("containers", os.Getenv("AZURE_STORAGE_CONTAINERS"), "comma-separated Azure Blob containers snapshot URIs may name (or AZURE_STORAGE_CONTAINERS)")
	prefix := flags.String("prefix", os.Getenv("AZURE_STORAGE_PREFIX"), "optional key prefix (or AZURE_STORAGE_PREFIX)")
	root := flags.String("root", "/var/lib/ate", "node mode: the only directory tree local snapshot and asset files may be read from or written to")
	assetContainerList := flags.String("asset-containers", os.Getenv("AZURE_STORAGE_ASSET_CONTAINERS"), "node mode: comma-separated Azure Blob containers sandbox asset URLs may name (or AZURE_STORAGE_ASSET_CONTAINERS); unset serves no assets")
	assetStagingDir := flags.String("asset-staging-dir", "/var/lib/objectstore-plugin/asset-staging", "node mode: directory sandbox assets are downloaded into and verified before they are copied below --root; must not be below --root or readable by the caller")
	assetStagingCapacity := flags.String("asset-staging-capacity", "8Gi", "node mode: the most sandbox asset downloads in --asset-staging-dir may hold at once, as a Kubernetes quantity; downloads wait for space, and an asset larger than this is refused")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if *socket == "" {
		return errors.New("--socket is required")
	}
	if *accountURL == "" {
		return errors.New("--account-url or AZURE_STORAGE_ACCOUNT_URL is required")
	}
	if *containerList == "" {
		return errors.New("--containers or AZURE_STORAGE_CONTAINERS is required")
	}
	containers, err := parseContainers("--containers", *containerList)
	if err != nil {
		return err
	}
	var assetContainers []string
	var stagingCapacity int64
	if *assetContainerList != "" {
		if mode != "node" {
			return errors.New("--asset-containers is only served in node mode")
		}
		if assetContainers, err = parseContainers("--asset-containers", *assetContainerList); err != nil {
			return err
		}
		if stagingCapacity, err = parseCapacity(*assetStagingCapacity); err != nil {
			return fmt.Errorf("--asset-staging-capacity: %w", err)
		}
	}

	// One store per distinct container: an asset container may also hold
	// snapshots, under prefixes FetchAsset refuses.
	all := slices.Clone(containers)
	for _, c := range assetContainers {
		if !slices.Contains(all, c) {
			all = append(all, c)
		}
	}
	stores, err := newStore(*accountURL, all)
	if err != nil {
		return err
	}
	server, err := provider.New(pick(stores, containers), *prefix, *root)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(logRPC))
	if mode == "node" {
		objectstorev1.RegisterNodeProviderServer(grpcServer, server)
		if len(assetContainers) > 0 {
			if err := os.MkdirAll(*assetStagingDir, 0o700); err != nil {
				return fmt.Errorf("creating --asset-staging-dir: %w", err)
			}
			assets, err := provider.NewAssetServer(*accountURL, pick(stores, assetContainers), *root, *assetStagingDir, stagingCapacity)
			if err != nil {
				return err
			}
			objectstorev1.RegisterAssetProviderServer(grpcServer, assets)
		}
	} else {
		objectstorev1.RegisterControlProviderServer(grpcServer, server)
	}

	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	listener, err := listen(*socket)
	if err != nil {
		return err
	}
	defer listener.Close()

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-signalCtx.Done()
		grpcServer.GracefulStop()
	}()

	slog.Info("serving Azure Blob object-store plugin", slog.String("mode", mode), slog.String("socket", *socket))
	if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("serving gRPC: %w", err)
	}
	return nil
}

// parseContainers parses the comma-separated container list given to the
// flag called name.
func parseContainers(name, list string) ([]string, error) {
	if list == "" {
		return nil, fmt.Errorf("%s is required", name)
	}
	var containers []string
	for container := range strings.SplitSeq(list, ",") {
		container = strings.TrimSpace(container)
		if err := blobstore.ValidateContainerName(container); err != nil {
			return nil, fmt.Errorf("%s %q: %w", name, list, err)
		}
		if slices.Contains(containers, container) {
			return nil, fmt.Errorf("%s %q names container %q twice", name, list, container)
		}
		containers = append(containers, container)
	}
	return containers, nil
}

// parseCapacity parses a Kubernetes quantity of bytes, such as 8Gi.
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

// pick returns the stores of the named containers.
func pick(stores map[string]blobstore.Store, containers []string) map[string]blobstore.Store {
	picked := make(map[string]blobstore.Store, len(containers))
	for _, c := range containers {
		picked[c] = stores[c]
	}
	return picked
}

// runHealthcheck is what the sidecar's startup probe runs: the image has no
// shell or grpc_health_probe, so the binary checks itself.
func runHealthcheck(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	socket := flags.String("socket", "", "Unix socket of the running plugin (required)")
	timeout := flags.Duration("timeout", 5*time.Second, "how long to wait for the plugin to report that it is serving")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *socket == "" {
		return errors.New("--socket is required")
	}
	conn, err := grpc.NewClient("unix://"+*socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return fmt.Errorf("dialing object-store plugin at %s: %w", *socket, err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("checking object-store plugin at %s: %w", *socket, err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("object-store plugin at %s is %s", *socket, resp.GetStatus())
	}
	return nil
}

func logRPC(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	slog.InfoContext(ctx, "handling RPC", slog.String("method", info.FullMethod))
	resp, err := handler(ctx, req)
	// atelet keeps only the code of a failed FetchAsset, so this line is the
	// one record of why the plugin failed. Internal and Unavailable mean it
	// failed rather than declined the URL: warn.
	level := slog.LevelInfo
	if info.FullMethod == objectstorev1.AssetProvider_FetchAsset_FullMethodName {
		switch status.Code(err) {
		case codes.Internal, codes.Unavailable:
			level = slog.LevelWarn
		}
	}
	slog.Log(ctx, level, "handled RPC",
		slog.String("method", info.FullMethod),
		slog.Duration("duration", time.Since(start)),
		slog.Any("err", err))
	return resp, err
}

func listen(socket string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		return nil, fmt.Errorf("creating socket directory: %w", err)
	}
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("removing stale socket: %w", err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", socket, err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		listener.Close()
		return nil, fmt.Errorf("restricting socket permissions: %w", err)
	}
	return listener, nil
}
