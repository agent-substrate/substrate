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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/memorypullcache"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-containerregistry/pkg/authn"
	googlecontainerauth "github.com/google/go-containerregistry/pkg/v1/google"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"k8s.io/utils/lru"
)

var (
	port              = flag.Int("port", 8085, "The port to listen on")
	metricsListenAddr = flag.String("metrics-listen-addr", ":9090", "Address and port the prometheus metrics server should listen on.")

	gcpAuthForImagePulls         = flag.Bool("gcp-auth-for-image-pulls", true, "Use GCP application default credentials mechanism.")
	localhostRegistryReplacement = flag.String("localhost-registry-replacement", "", "The replacement registry endpoint for localhost and/or loopback IP addresses, useful for local development. for example kind-registry:5000")
)

func main() {
	flag.Parse()
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

func (s *AteomHerder) fetchRunsc(ctx context.Context, cfg *ateletpb.RunscConfig) (string, error) {
	var platCfg *ateletpb.RunscPlatformConfig
	switch runtime.GOARCH {
	case "amd64":
		platCfg = cfg.GetAmd64()
	case "arm64":
		platCfg = cfg.GetArm64()
	}

	localPath := ateompath.RunSCBinaryPath(platCfg.GetSha256Hash())
	_, err := os.Stat(localPath)
	if err == nil { // EQUALS nil
		return localPath, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("while stat-ing local file: %w", err)
	}

	// Fetch the file.

	client := s.anonGCSClient
	if cfg.GetAuthentication().GetGcp().GetUse() {
		client = s.gcsClient
	}

	content, err := ategcs.FetchFromGCS(ctx, client, platCfg.GetUrl())
	if err != nil {
		return "", fmt.Errorf("while fetching %v: %w", platCfg.GetUrl(), err)
	}

	// Check hash
	sum := sha256.Sum256(content)
	wantSum, err := hex.DecodeString(platCfg.GetSha256Hash())
	if err != nil {
		return "", fmt.Errorf("while parsing sha256 hash: %w", err)
	}
	if !bytes.Equal(sum[:], wantSum) {
		return "", fmt.Errorf("sha256 mismatch; got=%s want=%s", hex.EncodeToString(sum[:]), platCfg.GetSha256Hash())
	}

	tmpFileName, err := func() (string, error) {
		localDir := filepath.Dir(localPath)
		tmpFile, err := os.CreateTemp(localDir, filepath.Base(localPath)+"-download-")
		if err != nil {
			return "", fmt.Errorf("while temp file: %w", err)
		}
		defer tmpFile.Close()

		if _, err := tmpFile.Write(content); err != nil {
			return "", fmt.Errorf("while writing content to temp file: %w", err)
		}

		if err := tmpFile.Chmod(0o755); err != nil {
			return "", fmt.Errorf("while setting file mode: %w", err)
		}

		return tmpFile.Name(), nil
	}()
	if err != nil {
		return "", fmt.Errorf("while populating temp file: %w", err)
	}

	if err := os.Rename(tmpFileName, localPath); err != nil {
		return "", fmt.Errorf("while renaming temp file to target: %w", err)
	}

	return localPath, nil
}

func (s *AteomHerder) Run(ctx context.Context, req *ateletpb.RunRequest) (*ateletpb.RunResponse, error) {
	runscPath, err := s.fetchRunscAndPrep(ctx, req.GetRunsc())
	if err != nil {
		return nil, err
	}

	if err := resetActorDirs(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	if err := s.prepareOCIBundles(ctx,
		req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId(),
		req.GetSpec(), req.GetTargetAteomNamespace(), req.GetTargetAteomName(),
	); err != nil {
		return nil, err
	}

	client, err := s.dialAteom(ctx, req.GetTargetAteomNamespace(), req.GetTargetAteomName())
	if err != nil {
		return nil, err
	}

	// Tell ateom to do runsc create + runsc start for pause container and
	// all application containers.
	if _, err := client.RunWorkload(ctx, &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: req.GetActorTemplateNamespace(),
		ActorTemplateName:      req.GetActorTemplateName(),
		ActorId:                req.GetActorId(),
		RunscPath:              runscPath,
		Spec:                   buildAteomWorkloadSpec(req.GetSpec()),
	}); err != nil {
		return nil, fmt.Errorf("while calling ateom.RunWorkload: %w", err)
	}

	return &ateletpb.RunResponse{}, nil
}

func (s *AteomHerder) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	runscPath, err := s.fetchRunscAndPrep(ctx, req.GetRunsc())
	if err != nil {
		return nil, err
	}

	checkpointDir := ateompath.CheckpointDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())

	client, err := s.dialAteom(ctx, req.GetTargetAteomNamespace(), req.GetTargetAteomName())
	if err != nil {
		return nil, err
	}

	// TODO(ateom): Once we enable background restore, we need `runsc wait
	// --restore pause` here, so that we know that gVisor is done with the
	// restore checkpoint file.

	// Delete any existing checkpoint file left over from restore.
	if err := os.RemoveAll(checkpointDir); err != nil {
		return nil, fmt.Errorf("while deleting checkpoint: %w", err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint dir: %w", err)
	}

	// Tell ateom to take checkpoint and delete containers.
	if _, err := client.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{
		ActorTemplateNamespace: req.GetActorTemplateNamespace(),
		ActorTemplateName:      req.GetActorTemplateName(),
		ActorId:                req.GetActorId(),
		RunscPath:              runscPath,
		Spec:                   buildAteomWorkloadSpec(req.GetSpec()),
	}); err != nil {
		return nil, fmt.Errorf("while calling ateom.CheckpointWorkload: %w", err)
	}

	prefix := strings.TrimSuffix(req.GetSnapshotUriPrefix(), "/")
	ns, tmpl, actorID := req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()

	// Upload checkpoint from local dir.
	if err := ategcs.SendLocalFileToGCSWithZstd(ctx, s.gcsClient,
		prefix+"/checkpoint.img.zstd",
		ateompath.CheckpointImgPath(ns, tmpl, actorID),
	); err != nil {
		return nil, fmt.Errorf("while uploading checkpoint.img to GCS: %w", err)
	}

	if err := uploadIfExists(ctx, s.gcsClient,
		prefix+"/pages.img.zstd",
		ateompath.PagesImgPath(ns, tmpl, actorID),
	); err != nil {
		return nil, err
	}
	if err := uploadIfExists(ctx, s.gcsClient,
		prefix+"/pages_meta.img.zstd",
		ateompath.PagesMetaImgPath(ns, tmpl, actorID),
	); err != nil {
		return nil, err
	}

	if err := resetActorDirs(ns, tmpl, actorID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	return &ateletpb.CheckpointResponse{}, nil
}

func (s *AteomHerder) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	runscPath, err := s.fetchRunscAndPrep(ctx, req.GetRunsc())
	if err != nil {
		return nil, err
	}

	ns, tmpl, actorID := req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()

	if err := resetActorDirs(ns, tmpl, actorID); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	prefix := strings.TrimSuffix(req.GetSnapshotUriPrefix(), "/")
	g, gCtx := errgroup.WithContext(ctx)
	for _, dl := range []struct{ remote, local string }{
		{prefix + "/checkpoint.img.zstd", ateompath.CheckpointImgPath(ns, tmpl, actorID)},
		{prefix + "/pages.img.zstd", ateompath.PagesImgPath(ns, tmpl, actorID)},
		{prefix + "/pages_meta.img.zstd", ateompath.PagesMetaImgPath(ns, tmpl, actorID)},
	} {
		dl := dl
		g.Go(func() error {
			if err := ategcs.FetchLocalFileFromGCSWithZstd(gCtx, s.gcsClient, dl.remote, dl.local); err != nil {
				return fmt.Errorf("while downloading %s from GCS: %w", filepath.Base(dl.remote), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	if err := s.prepareOCIBundles(ctx, ns, tmpl, actorID,
		req.GetSpec(), req.GetTargetAteomNamespace(), req.GetTargetAteomName(),
	); err != nil {
		return nil, err
	}

	client, err := s.dialAteom(ctx, req.GetTargetAteomNamespace(), req.GetTargetAteomName())
	if err != nil {
		return nil, err
	}

	// Tell ateom to do runsc create + runsc restore for pause container and
	// all application containers.
	if _, err := client.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{
		ActorTemplateNamespace: ns,
		ActorTemplateName:      tmpl,
		ActorId:                actorID,
		RunscPath:              runscPath,
		Spec:                   buildAteomWorkloadSpec(req.GetSpec()),
	}); err != nil {
		return nil, fmt.Errorf("while calling ateom.RestoreWorkload: %w", err)
	}

	return &ateletpb.RestoreResponse{}, nil
}

// fetchRunscAndPrep ensures the static files dir exists and downloads the
// runsc binary at the version pinned by the request. Returns the local
// runsc path.
func (s *AteomHerder) fetchRunscAndPrep(ctx context.Context, runscCfg *ateletpb.RunscConfig) (string, error) {
	if err := os.MkdirAll(ateompath.StaticFilesDir, 0o700); err != nil {
		return "", fmt.Errorf("while creating static files dir: %w", err)
	}
	runscPath, err := s.fetchRunsc(ctx, runscCfg)
	if err != nil {
		return "", fmt.Errorf("in fetchRunsc: %w", err)
	}
	return runscPath, nil
}

// prepareOCIBundles pulls images and assembles OCI bundles for the pause
// container and every application container in spec, in parallel.
func (s *AteomHerder) prepareOCIBundles(
	ctx context.Context,
	actorTemplateNamespace, actorTemplateName, actorID string,
	spec *ateletpb.WorkloadSpec,
	targetAteomNamespace, targetAteomName string,
) error {
	netnsPath := ateompath.AteomNetNSPath(targetAteomNamespace, targetAteomName)

	g, gCtx := errgroup.WithContext(ctx)

	// Pause container.
	g.Go(func() error {
		if err := prepareOCIDirectory(
			gCtx,
			s.pullCache,
			actorTemplateNamespace, actorTemplateName, actorID,
			"pause",
			spec.GetPauseImage(),
			[]string{"/pause"},
			nil,
			map[string]string{
				"io.kubernetes.cri.container-type": "sandbox",
				"io.kubernetes.cri.container-name": "pause",
			},
			netnsPath,
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
func (s *AteomHerder) dialAteom(ctx context.Context, namespace, name string) (ateompb.AteomClient, error) {
	conn, err := s.ateomDialer.DialAteomPod(ctx, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("while getting ateom conn for %s/%s: %w", namespace, name, err)
	}
	return ateompb.NewAteomClient(conn), nil
}

// buildAteomWorkloadSpec projects the atelet-facing workload spec onto
// the ateom-facing one — currently just the container names.
func buildAteomWorkloadSpec(spec *ateletpb.WorkloadSpec) *ateompb.WorkloadSpec {
	out := &ateompb.WorkloadSpec{}
	for _, ctr := range spec.GetContainers() {
		out.Containers = append(out.Containers, &ateompb.Container{Name: ctr.GetName()})
	}
	return out
}

// uploadIfExists uploads a local file to GCS (zstd-compressed) only if
// the file is present. Missing files are silently skipped — used for
// optional checkpoint side-files (pages.img, pages_meta.img).
func uploadIfExists(ctx context.Context, gcs ategcs.ObjectStorage, remoteURI, localPath string) error {
	if _, err := os.Stat(localPath); err != nil {
		return nil
	}
	if err := ategcs.SendLocalFileToGCSWithZstd(ctx, gcs, remoteURI, localPath); err != nil {
		return fmt.Errorf("while uploading %s to GCS: %w", filepath.Base(localPath), err)
	}
	return nil
}

type AteomDialer struct {
	conns *lru.Cache
}

func (d *AteomDialer) DialAteomPod(ctx context.Context, namespace, name string) (*grpc.ClientConn, error) {
	key := namespace + "/" + name

	connAny, ok := d.conns.Get(key)
	if ok {
		return connAny.(*grpc.ClientConn), nil
	}

	conn, err := grpc.NewClient(
		"unix://"+ateompath.AteomSocketPath(namespace, name),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("while creating atelet gRPC client connection: %w", err)
	}

	d.conns.Add(key, conn)

	return conn, nil
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

	checkpointDir := ateompath.CheckpointDir(actorTemplateNamespace, actorTemplateName, actorID)
	if err := os.RemoveAll(checkpointDir); err != nil {
		return fmt.Errorf("while deleting checkpoint dir: %w", err)
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return fmt.Errorf("while creating checkpoint dir: %w", err)
	}

	return nil
}
