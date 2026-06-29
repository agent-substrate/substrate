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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/ategcs"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/memorypullcache"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-containerregistry/pkg/authn"
	googlecontainerauth "github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"k8s.io/utils/lru"
)

var (
	port              = pflag.Int("port", 8085, "The port to listen on")
	metricsListenAddr = pflag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")

	gcpAuthForImagePulls         = pflag.Bool("gcp-auth-for-image-pulls", true, "Use GCP application default credentials mechanism.")
	localhostRegistryReplacement = pflag.String("localhost-registry-replacement", "", "The replacement registry endpoint for localhost and/or loopback IP addresses, useful for local development. for example kind-registry:5000")

	showVersion = pflag.Bool("version", false, "Print version and exit.")
)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	ctx := context.Background()
	serverboot.InitLogger()

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "atelet",
		Sampler:     sdktrace.ParentBased(sdktrace.NeverSample()),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetrics(ctx, "atelet")
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	if err := initSnapshotSizeMetric(); err != nil {
		serverboot.Fatal(ctx, "Failed to create snapshot size metric", err)
	}

	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{Addr: *metricsListenAddr})

	ateomDialer := &AteomDialer{
		conns: lru.New(256),
	}

	var gcpRegistryAuthn authn.Authenticator
	if *gcpAuthForImagePulls {
		gcpRegistryAuthn, err = googlecontainerauth.NewEnvAuthenticator(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to create GCP registry authenticator", err)
		}
	}

	pullCache, err := memorypullcache.NewMemoryPullCache(ctx, gcpRegistryAuthn, *localhostRegistryReplacement)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create pull cache", err)
	}

	anonGCSClient, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		serverboot.Fatal(ctx, "Failed to create anonymous GCS client", err)
	}

	var gcsClient *storage.Client
	var s3Client *s3.Client
	storageBackend := os.Getenv("ATE_STORAGE_BACKEND")
	switch storageBackend {
	case "s3":
		slog.InfoContext(ctx, "Using S3 storage backend")
		// depend on standard AWS environment variables to configure the client
		// these will need to be set on the atelet pods
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to load S3 config", err)
		}
		s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			if usePathStyle := os.Getenv("AWS_S3_USE_PATH_STYLE"); usePathStyle == "true" {
				o.UsePathStyle = true
			}
		})
	// GCS is currently the default, TODO: we assume workload identity / ADC
	default:
		gcsClient, err = storage.NewClient(ctx)
		if err != nil {
			serverboot.Fatal(ctx, "Failed to create GCS client", err)
		}
	}

	var wrappedAnonGCS ategcs.ObjectStorage
	if anonGCSClient != nil {
		wrappedAnonGCS = ategcs.NewGCSClient(anonGCSClient)
	}

	var wrappedGCS ategcs.ObjectStorage
	if s3Client != nil {
		wrappedGCS = ategcs.NewS3Client(s3Client)
	} else if gcsClient != nil {
		wrappedGCS = ategcs.NewGCSClient(gcsClient)
	}

	wmService := NewService(
		ctx,
		ateomDialer,
		wrappedAnonGCS,
		wrappedGCS,
		pullCache,
	)

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen", err)
	}

	svr := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()), grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor))
	ateletpb.RegisterAteomHerderServer(svr, wmService)
	reflection.Register(svr)
	slog.InfoContext(ctx, "WorkersManagerService listening", slog.Any("address", lis.Addr()))
	if err := svr.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Failed to serve", err)
	}
}

// AteomHerder is a service that allows controlling workloads on individual
// ateoms.
type AteomHerder struct {
	ateletpb.UnimplementedAteomHerderServer

	ateomDialer   *AteomDialer
	pullCache     *memorypullcache.MemoryPullCache
	anonGCSClient ategcs.ObjectStorage
	gcsClient     ategcs.ObjectStorage
}

var _ ateletpb.AteomHerderServer = (*AteomHerder)(nil)

// NewService creates a new WorkersManagerService.
func NewService(
	ctx context.Context,
	ateomDialer *AteomDialer,
	anonGCSClient ategcs.ObjectStorage,
	gcsClient ategcs.ObjectStorage,
	pullCache *memorypullcache.MemoryPullCache,
) *AteomHerder {
	wms := &AteomHerder{
		ateomDialer:   ateomDialer,
		pullCache:     pullCache,
		anonGCSClient: anonGCSClient,
		gcsClient:     gcsClient,
	}
	return wms
}

func (s *AteomHerder) Run(ctx context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	if err := validateRunRequest(req); err != nil {
		// status.Error so the interceptor surfaces InvalidArgument and the
		// message instead of masking both as Internal.
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ns, tmpl, actorID := req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()

	sandboxRec, err := recordFromRequest(req.GetSandboxAssets())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	assetPaths, err := s.ensureSandboxAssets(ctx, sandboxRec)
	if err != nil {
		return nil, err
	}

	if err := resetActorDirs(ns, tmpl, actorID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	// Record the sandbox binaries this actor is running so a later Checkpoint
	// (whose request no longer carries the sandbox config) can re-fetch the same
	// version and pin it into the snapshot manifest.
	if err := writeSandboxRecord(ns, tmpl, actorID, sandboxRec); err != nil {
		return nil, fmt.Errorf("while recording sandbox assets: %w", err)
	}

	if err := s.prepareOCIBundles(ctx, ns, tmpl, actorID,
		req.GetSpec(), req.GetTargetAteomUid(),
	); err != nil {
		return nil, err
	}

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	// Tell ateom to start the workload. gVisor uses RunscPath; the micro-VM
	// runtime uses the full RuntimeAssetPaths set.
	if _, err := client.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: ns,
		ActorTemplateName:      tmpl,
		ActorId:                actorID,
		RunscPath:              runscPathFor(assetPaths),
		RuntimeAssetPaths:      assetPaths,
		Spec:                   buildAteomWorkloadSpec(req.GetSpec()),
	}); err != nil {
		return nil, fmt.Errorf("while calling ateom.RunWorkload: %w", err)
	}

	return &ateletpb.RunResponse{}, nil
}

var snapshotSizeBytes metric.Int64Histogram

func initSnapshotSizeMetric() error {
	var err error
	snapshotSizeBytes, err = otel.Meter("atelet").Int64Histogram(
		"atelet.snapshot.size",
		metric.WithUnit("By"),
		metric.WithDescription("Uncompressed size in bytes of each gVisor snapshot image written during checkpoint."),

		metric.WithExplicitBucketBoundaries(
			1e6, 5e6, 1e7, 2.5e7, 5e7, 1e8, 2.5e8, 5e8, 1e9, 2e9, 5e9, 1e10,
		),
	)
	return err
}

func recordSnapshotSize(ctx context.Context, kind, path, atNamespace, atName string) {
	if snapshotSizeBytes == nil {
		return
	}
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		slog.WarnContext(ctx, "Failed to stat snapshot image for size metric",
			slog.String("kind", kind), slog.String("path", path), slog.Any("err", err))
		return
	}
	snapshotSizeBytes.Record(ctx, fi.Size(), metric.WithAttributes(
		attribute.String("kind", kind),
		attribute.String("actor_template_namespace", atNamespace),
		attribute.String("actor_template_name", atName),
	))
}

func (s *AteomHerder) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	if err := validateCheckpointRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ns, tmpl, actorID := req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()

	// Checkpoint requests no longer carry the sandbox config; recover the
	// version this actor was started with from the on-node record and re-fetch
	// it (a cache hit) so ateom can drive runsc, and so we can pin it into the
	// snapshot manifest below.
	sandboxRec, err := readSandboxRecord(ns, tmpl, actorID)
	if err != nil {
		return nil, fmt.Errorf("while loading recorded sandbox assets: %w", err)
	}
	assetPaths, err := s.ensureSandboxAssets(ctx, sandboxRec)
	if err != nil {
		return nil, err
	}

	checkpointDir := ateompath.CheckpointStateDir(ns, tmpl, actorID)

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	// Tell ateom to take the checkpoint and delete containers. ateom reports the
	// exact files it wrote so we ship precisely that set (gVisor's image files,
	// cloud-hypervisor's snapshot set, ...) rather than a hardcoded list.
	resp, err := client.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		ActorTemplateNamespace: ns,
		ActorTemplateName:      tmpl,
		ActorId:                actorID,
		RunscPath:              runscPathFor(assetPaths),
		RuntimeAssetPaths:      assetPaths,
		Spec:                   buildAteomWorkloadSpec(req.GetSpec()),
		Scope:                  toAteomSnapshotScope(req.GetScope()),
	})
	if err != nil {
		return nil, fmt.Errorf("while calling ateom.CheckpointWorkload: %w", err)
	}
	sandboxRec.SnapshotFiles = resp.GetSnapshotFiles()
	if len(sandboxRec.SnapshotFiles) == 0 {
		return nil, fmt.Errorf("ateom reported no snapshot files for checkpoint")
	}

	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		if err := s.uploadExternalCheckpoint(ctx, req, checkpointDir, sandboxRec); err != nil {
			return nil, err
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		if err := s.moveLocalCheckpoint(ctx, req, checkpointDir, sandboxRec); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected checkpoint type: %v", req.GetType())
	}

	if err := resetActorDirs(ns, tmpl, actorID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	return &ateletpb.CheckpointResponse{}, nil
}

func toAteomSnapshotScope(scope ateletpb.SnapshotScope) ateompb.SnapshotScope {
	// assumption the request already been validated and scope is in the valid values set
	switch scope {
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		return ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA
	default:
		return ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL
	}
}

func (s *AteomHerder) moveLocalCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, checkpointDir string, rec *sandboxAssetsRecord) error {
	localCheckpointPath := filepath.Join(ateompath.LocalCheckpointsDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()), req.GetLocalConfig().GetSnapshotPrefix())
	if err := os.MkdirAll(localCheckpointPath, 0o700); err != nil {
		return fmt.Errorf("while creating local checkpoint directory: %w", err)
	}

	ns, tmpl := req.GetActorTemplateNamespace(), req.GetActorTemplateName()

	// Move exactly the files ateom reported.
	for _, fileName := range rec.SnapshotFiles {
		src := filepath.Join(checkpointDir, fileName)
		dst := filepath.Join(localCheckpointPath, fileName)
		recordSnapshotSize(ctx, strings.TrimSuffix(fileName, ".img"), src, ns, tmpl)

		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("failed to move %s to %s: %w", src, dst, err)
		}
	}

	// Pin the sandbox binaries + snapshot file list into a manifest beside the
	// images so a later Restore is self-describing.
	manifest, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(localCheckpointPath, sandboxManifestName), manifest, 0o600); err != nil {
		return fmt.Errorf("while writing snapshot manifest: %w", err)
	}

	return nil
}

func (s *AteomHerder) uploadExternalCheckpoint(ctx context.Context, req *ateletpb.CheckpointRequest, checkpointDir string, rec *sandboxAssetsRecord) error {
	ns, tmpl := req.GetActorTemplateNamespace(), req.GetActorTemplateName()
	prefix := strings.TrimSuffix(req.GetExternalConfig().GetSnapshotUriPrefix(), "/")

	// Upload exactly the files ateom reported (each zstd-compressed).
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range rec.SnapshotFiles {
		fileName := fileName
		local := filepath.Join(checkpointDir, fileName)
		recordSnapshotSize(ctx, strings.TrimSuffix(fileName, ".img"), local, ns, tmpl)
		g.Go(func() error {
			if err := ategcs.SendLocalFileToGCSWithZstd(gCtx, s.gcsClient, prefix+"/"+fileName+".zstd", local); err != nil {
				return fmt.Errorf("while uploading %s to GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Pin the sandbox binaries + snapshot file list into a manifest beside the
	// images, written last, so a Restore on any node is self-describing.
	manifest, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("while marshaling snapshot manifest: %w", err)
	}
	if err := ategcs.SendBytesToGCS(ctx, s.gcsClient, prefix+"/"+sandboxManifestName, manifest); err != nil {
		return fmt.Errorf("while uploading snapshot manifest: %w", err)
	}
	return nil
}

func (s *AteomHerder) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	if err := validateRestoreRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	ns, tmpl, actorID := req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()

	if err := resetActorDirs(ns, tmpl, actorID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	checkpointDir := ateompath.RestoreStateDir(ns, tmpl, actorID)

	// Per-step timing so we can attribute resume latency between the rustfs
	// download/decompress, the OCI image unpack, and ateom's own work. Logged at the end.
	tStart := time.Now()
	var dDownload, dBundles, dAteom time.Duration

	// The snapshot is self-describing: recover the sandbox binaries that created
	// it from the manifest stored beside the checkpoint images (the Restore
	// request no longer carries the sandbox config).
	// The snapshot is self-describing: recover the sandbox binaries that created
	// it from the manifest stored beside the checkpoint images (the Restore
	// request no longer carries the sandbox config). Fetch the (small) manifest
	// first — both the checkpoint download and the OCI/asset prep below need it.
	var sandboxRec *sandboxAssetsRecord
	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		prefix := req.GetExternalConfig().GetSnapshotUriPrefix()
		manifest, err := ategcs.FetchFromGCS(ctx, s.gcsClient, strings.TrimSuffix(prefix, "/")+"/"+sandboxManifestName)
		if err != nil {
			return nil, fmt.Errorf("while fetching snapshot manifest: %w", err)
		}
		if sandboxRec, err = unmarshalSandboxRecord(manifest); err != nil {
			return nil, err
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		localCheckpointDir := ateompath.LocalCheckpointsDir(ns, tmpl, actorID)
		snapshotPrefix := req.GetLocalConfig().GetSnapshotPrefix()
		manifest, err := os.ReadFile(filepath.Join(localCheckpointDir, snapshotPrefix, sandboxManifestName))
		if err != nil {
			return nil, fmt.Errorf("while reading local snapshot manifest: %w", err)
		}
		if sandboxRec, err = unmarshalSandboxRecord(manifest); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected checkpoint type: %v", req.GetType())
	}

	// Download the memory snapshot and prepare the sandbox assets + OCI bundle
	// CONCURRENTLY. They are independent — only the final ateom.RestoreWorkload
	// needs both — so overlapping the GCS download (~0.5s warm) with the asset
	// fetch + image unpack hides whichever leg is shorter, and on a cold node
	// (uncached assets + image, ~2.5s unpack) that overlap is large.
	// TODO(dberkov): the old pause checkpoint files are not deleted after they are
	// copied to checkpointDir for the LOCAL case.
	var assetPaths map[string]string
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		t := time.Now()
		switch req.GetType() {
		case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
			if err := s.downloadExternalCheckpoint(gctx, req.GetExternalConfig().GetSnapshotUriPrefix(), checkpointDir, sandboxRec.SnapshotFiles); err != nil {
				return err
			}
		case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
			if err := s.copyLocalCheckpoint(gctx, req.GetLocalConfig().GetSnapshotPrefix(), ateompath.LocalCheckpointsDir(ns, tmpl, actorID), checkpointDir, sandboxRec.SnapshotFiles); err != nil {
				return err
			}
		}
		dDownload = time.Since(t)
		return nil
	})
	g.Go(func() error {
		var err error
		if assetPaths, err = s.ensureSandboxAssets(gctx, sandboxRec); err != nil {
			return err
		}
		t := time.Now()
		if err := s.prepareOCIBundles(gctx, ns, tmpl, actorID, req.GetSpec(), req.GetTargetAteomUid()); err != nil {
			return err
		}
		dBundles = time.Since(t)
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	client, err := s.dialAteom(ctx, req.GetTargetAteomUid())
	if err != nil {
		return nil, err
	}

	// Tell ateom to do runsc create + runsc restore for pause container and
	// all application containers.
	tAteom := time.Now()
	if _, err := client.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{
		ActorTemplateNamespace: ns,
		ActorTemplateName:      tmpl,
		ActorId:                actorID,
		RunscPath:              runscPathFor(assetPaths),
		RuntimeAssetPaths:      assetPaths,
		Spec:                   buildAteomWorkloadSpec(req.GetSpec()),
		Scope:                  toAteomSnapshotScope(req.GetScope()),
	}); err != nil {
		return nil, fmt.Errorf("while calling ateom.RestoreWorkload: %w", err)
	}
	dAteom = time.Since(tAteom)

	// Record the (manifest-pinned) sandbox binaries on-node so a subsequent
	// Checkpoint of this restored actor can re-pin the same version.
	if err := writeSandboxRecord(ns, tmpl, actorID, sandboxRec); err != nil {
		return nil, fmt.Errorf("while recording sandbox assets: %w", err)
	}

	slog.InfoContext(ctx, "Restore timing breakdown", slog.String("actor", actorID),
		slog.Duration("download", dDownload),   // rustfs/GCS fetch + decompress (or local copy)
		slog.Duration("oci_unpack", dBundles),  // prepareOCIBundles: unpack the OCI image to the bundle
		slog.Duration("ateom_restore", dAteom), // ateom.RestoreWorkload (see its own breakdown)
		slog.Duration("total", time.Since(tStart)))
	return &ateletpb.RestoreResponse{}, nil
}

func (s *AteomHerder) copyLocalCheckpoint(ctx context.Context, snapshotPrefix string, srcDir, dstDir string, files []string) error {
	for _, fileName := range files {
		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		}
		src := filepath.Join(srcDir, snapshotPrefix, fileName)
		dst := filepath.Join(dstDir, fileName)
		if _, err := copyFile(src, dst); err != nil {
			return fmt.Errorf("failed to copy %s to %s: %w", src, dst, err)
		}
	}

	return nil
}

func copyFile(src, dst string) (int64, error) {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return 0, err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return 0, fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	nBytes, err := io.Copy(destination, source)
	return nBytes, err
}

func (s *AteomHerder) downloadExternalCheckpoint(ctx context.Context, snapshotUriPrefix string, dstDir string, files []string) error {
	prefix := strings.TrimSuffix(snapshotUriPrefix, "/")
	g, gCtx := errgroup.WithContext(ctx)
	for _, fileName := range files {
		fileName := fileName
		local := filepath.Join(dstDir, fileName)
		g.Go(func() error {
			if err := ategcs.FetchLocalFileFromGCSWithZstd(gCtx, s.gcsClient, prefix+"/"+fileName+".zstd", local); err != nil {
				return fmt.Errorf("while downloading %s from GCS: %w", fileName, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// prepareOCIBundles pulls images and assembles OCI bundles for the pause
// container and every application container in spec, in parallel.
func (s *AteomHerder) prepareOCIBundles(
	ctx context.Context,
	actorTemplateNamespace, actorTemplateName, actorID string,
	spec *ateletpb.WorkloadSpec,
	targetAteomUid string,
) error {
	netnsPath := ateompath.AteomNetNSPath(targetAteomUid)

	// Populate the per-actor identity directory that gets bind-mounted into
	// the application containers. Regenerated on every resume, so it carries
	// the correct per-actor ID even when restoring from the golden snapshot.
	identityDir := ateompath.ActorIdentityDirPath(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.MkdirAll(identityDir, 0o755); err != nil {
		return fmt.Errorf("while creating actor identity dir: %w", err)
	}
	if err := writeFileAtomic(filepath.Join(identityDir, ActorIDFileName), []byte(actorID), 0o644); err != nil {
		return fmt.Errorf("while writing actor identity file: %w", err)
	}

	ddVolumes := make(map[string]bool)
	// make directories for all durable-dir volumes
	for _, vol := range spec.GetVolumes() {
		if vol.GetType() == ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR {
			ddVolumes[vol.GetName()] = true
			volPath := ateompath.DurableDirVolumeMountPoint(actorTemplateNamespace, actorTemplateName, actorID, vol.GetName())
			if err := os.MkdirAll(volPath, 0o700); err != nil {
				return fmt.Errorf("while creating %q: %w", volPath, err)
			}
		}
	}

	g, gCtx := errgroup.WithContext(ctx)

	// Pause container.
	g.Go(func() error {
		annotations := map[string]string{
			"io.kubernetes.cri.container-type": "sandbox",
			"io.kubernetes.cri.container-name": "pause",
		}
		// add annotation for every durable-dir volume
		// TODO(dberkov) needs to revisit this logic once gVisor supports multiple durable-dir volumes.
		for _, vol := range spec.GetVolumes() {
			if vol.GetType() == ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR {
				annotations["dev.gvisor.spec.mount.durabledir.type"] = "bind"
				annotations["dev.gvisor.spec.mount.durabledir.share"] = "container"
				annotations["dev.gvisor.spec.mount.durabledir.source"] = ateompath.DurableDirVolumeMountPoint(actorTemplateNamespace, actorTemplateName, actorID, vol.GetName())
			}
		}

		if err := prepareOCIDirectory(
			gCtx,
			s.pullCache,
			actorTemplateNamespace, actorTemplateName, actorID,
			"pause",
			spec.GetPauseImage(),
			[]string{"/pause"},
			nil,
			annotations,
			netnsPath,
			"", // pause is sandbox infra; it gets no actor identity mount.
			nil,
		); err != nil {
			return fmt.Errorf("while creating pause OCI bundle: %w", err)
		}
		return nil
	})

	// Application containers.
	for _, ctr := range spec.GetContainers() {
		ctr := ctr
		var envs []string
		for _, env := range ctr.GetEnv() {
			envs = append(envs, fmt.Sprintf("%s=%s", env.GetName(), env.GetValue()))
		}
		var ddMounts []*ateletpb.VolumeMount
		for _, vm := range ctr.GetVolumeMounts() {
			if ddVolumes[vm.GetName()] {
				ddMounts = append(ddMounts, vm)
			}
		}
		g.Go(func() error {
			if err := prepareOCIDirectory(
				gCtx,
				s.pullCache,
				actorTemplateNamespace, actorTemplateName, actorID,
				ctr.GetName(),
				ctr.GetImage(),
				ctr.GetCommand(),
				envs,
				map[string]string{
					"io.kubernetes.cri.container-type": "container",
					"io.kubernetes.cri.sandbox-id":     "pause",
					"io.kubernetes.cri.container-name": ctr.GetName(),
				},
				netnsPath,
				identityDir,
				ddMounts,
			); err != nil {
				return fmt.Errorf("while creating %q OCI bundle: %w", ctr.GetName(), err)
			}
			return nil
		})
	}

	return g.Wait()
}

// dialAteom opens (or reuses) the gRPC connection to the target ateom
// pod and returns an ateom client.
func (s *AteomHerder) dialAteom(ctx context.Context, targetAteomUid string) (ateompb.AteomClient, error) {
	conn, err := s.ateomDialer.DialAteomPod(ctx, targetAteomUid)
	if err != nil {
		return nil, fmt.Errorf("while getting ateom conn for %s: %w", targetAteomUid, err)
	}
	return ateompb.NewAteomClient(conn), nil
}

// buildAteomWorkloadSpec projects the atelet-facing workload spec onto
// the ateom-facing one.
func buildAteomWorkloadSpec(spec *ateletpb.WorkloadSpec) *ateompb.WorkloadSpec {
	ddVolumes := make(map[string]bool)
	for _, vol := range spec.GetVolumes() {
		if vol.GetType() == ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR {
			ddVolumes[vol.GetName()] = true
		}
	}

	out := &ateompb.WorkloadSpec{}
	for _, ctr := range spec.GetContainers() {
		var ddMountPaths []string
		for _, vm := range ctr.GetVolumeMounts() {
			if ddVolumes[vm.GetName()] {
				ddMountPaths = append(ddMountPaths, vm.GetMountPath())
			}
		}
		out.Containers = append(out.Containers, &ateompb.Container{
			Name:              ctr.GetName(),
			DurableDirVolumes: ddMountPaths,
			Readyz:            toAteomReadyz(ctr.GetReadyz()),
		})
	}
	return out
}

// toAteomReadyz converts an ateletpb readyz probe into the ateompb wire
// type. Returns nil when the source is nil so containers without a probe
// stay unchanged on the wire to ateom.
func toAteomReadyz(in *ateletpb.Readyz) *ateompb.Readyz {
	if in == nil {
		return nil
	}
	out := &ateompb.Readyz{}
	if hg := in.GetHttpGet(); hg != nil {
		out.HttpGet = &ateompb.HTTPGetAction{
			Path: hg.GetPath(),
			Port: hg.GetPort(),
		}
	}
	return out
}

type AteomDialer struct {
	conns *lru.Cache
}

func (d *AteomDialer) DialAteomPod(ctx context.Context, podUID string) (*grpc.ClientConn, error) {
	key := podUID

	connAny, ok := d.conns.Get(key)
	if ok {
		return connAny.(*grpc.ClientConn), nil
	}

	conn, err := grpc.NewClient(
		"unix://"+ateompath.AteomSocketPath(podUID),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet gRPC client connection: %w", err)
	}

	d.conns.Add(key, conn)

	return conn, nil
}

// validateRunRequest, validateCheckpointRequest, and validateRestoreRequest
// validate everything in their request that atelet turns into host filesystem
// paths, plus the request-specific fields. atelet listens on an insecure
// hostPort, so any reachable caller could otherwise smuggle a path separator
// or ".." through these fields and make atelet read/RemoveAll/write outside
// the intended directory tree, or collide bundles. Each RPC validates at its
// boundary, before any path is built. The field rules live in
// internal/resources so other components can apply them at their boundaries.
func validateRunRequest(req *ateletpb.RunRequest) error {
	return validateActorRequest(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(), req.GetTargetAteomUid(), req.GetSpec())
}

func validateCheckpointRequest(req *ateletpb.CheckpointRequest) error {
	if err := validateActorRequest(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(), req.GetTargetAteomUid(), req.GetSpec()); err != nil {
		return err
	}

	if err := validateSnapshotScope(req.GetScope()); err != nil {
		return err
	}

	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		if err := resources.ValidateSnapshotURIPrefix(req.GetExternalConfig().GetSnapshotUriPrefix()); err != nil {
			return err
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		if req.GetLocalConfig().GetSnapshotPrefix() == "" {
			return fmt.Errorf("snapshot prefix must be non-empty for type %s", req.GetType().String())
		}
	default:
		return fmt.Errorf("invalid checkpoint type: %v", req.GetType())
	}
	return nil
}

func validateRestoreRequest(req *ateletpb.RestoreRequest) error {
	if err := validateActorRequest(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(), req.GetTargetAteomUid(), req.GetSpec()); err != nil {
		return err
	}

	if err := validateSnapshotScope(req.GetScope()); err != nil {
		return err
	}

	switch req.GetType() {
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL:
		if err := resources.ValidateSnapshotURIPrefix(req.GetExternalConfig().GetSnapshotUriPrefix()); err != nil {
			return err
		}
	case ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL:
		if req.GetLocalConfig().GetSnapshotPrefix() == "" {
			return fmt.Errorf("snapshot prefix must be non-empty for type %s", req.GetType().String())
		}
	default:
		return fmt.Errorf("invalid checkpoint type: %v", req.GetType())
	}
	return nil
}

func validateSnapshotScope(scope ateletpb.SnapshotScope) error {
	switch scope {
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
		ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA:
		return nil
	case ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED:
		return fmt.Errorf("snapshot scope must be non-zero")
	default:
		return fmt.Errorf("invalid snapshot scope: %v", scope)
	}
}

// validateActorRequest is the shared core for the fields common to all three
// RPCs.
func validateActorRequest(namespace, template, actorID, targetAteomUID string, spec *ateletpb.WorkloadSpec) error {
	if err := resources.ValidateActorRef(namespace, template, actorID); err != nil {
		return err
	}
	if err := resources.ValidateAteomUID(targetAteomUID); err != nil {
		return err
	}
	names := make([]string, 0, len(spec.GetContainers()))
	for _, ctr := range spec.GetContainers() {
		names = append(names, ctr.GetName())
	}
	return resources.ValidateContainerNames(names)
}

// writeFileAtomic writes data to path by writing a temp file in the same
// directory, syncing, and renaming it over the target, then syncing the
// parent directory so the rename is durable. The identity directory is
// bind-mounted into actors, so the file must change atomically: a reader
// must never observe a truncated or partially written value.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // no-op once the rename succeeds

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(perm); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}

	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func resetActorDirs(actorTemplateNamespace, actorTemplateName, actorID string) error {
	// Explicitly leave runsc logs dir untouched.

	bundleDir := ateompath.OCIBundleDir(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.RemoveAll(bundleDir); err != nil {
		return fmt.Errorf("while deleting bundle dir: %w", err)
	}
	if err := os.MkdirAll(bundleDir, 0o700); err != nil {
		return fmt.Errorf("while creating bundle dir: %w", err)
	}

	runscDir := ateompath.RunSCStateDir(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.RemoveAll(runscDir); err != nil {
		return fmt.Errorf("while deleting runsc state dir: %w", err)
	}
	if err := os.MkdirAll(runscDir, 0o700); err != nil {
		return fmt.Errorf("while creating runsc state dir: %w", err)
	}

	pidFileDir := ateompath.PIDFileDir(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.RemoveAll(pidFileDir); err != nil {
		return fmt.Errorf("while deleting PID file dir: %w", err)
	}
	if err := os.MkdirAll(pidFileDir, 0o700); err != nil {
		return fmt.Errorf("while creating PID file dir: %w", err)
	}

	checkpointDir := ateompath.CheckpointStateDir(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.RemoveAll(checkpointDir); err != nil {
		return fmt.Errorf("while deleting checkpoint-state dir: %w", err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return fmt.Errorf("while creating checkpoint-state dir: %w", err)
	}

	restoreStateDir := ateompath.RestoreStateDir(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.RemoveAll(restoreStateDir); err != nil {
		return fmt.Errorf("while deleting restore-state dir: %w", err)
	}
	if err := os.MkdirAll(restoreStateDir, 0o700); err != nil {
		return fmt.Errorf("while creating restore-state dir: %w", err)
	}

	// World-readable (0o755): bind-mounted into the actor, whose workload
	// reads it through the gofer.
	identityDir := ateompath.ActorIdentityDirPath(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.RemoveAll(identityDir); err != nil {
		return fmt.Errorf("while deleting actor identity dir: %w", err)
	}
	if err := os.MkdirAll(identityDir, 0o755); err != nil {
		return fmt.Errorf("while creating actor identity dir: %w", err)
	}

	durableDirVolumesMountDir := ateompath.DurableDirVolumeMountsDir(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.RemoveAll(durableDirVolumesMountDir); err != nil {
		return fmt.Errorf("while deleting durable-dir volumes mount dir: %w", err)
	}
	if err := os.MkdirAll(durableDirVolumesMountDir, 0o755); err != nil {
		return fmt.Errorf("while creating durable-dir volumes mount dir: %w", err)
	}

	return nil
}
