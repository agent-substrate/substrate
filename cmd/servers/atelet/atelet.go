//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/agent-substrate/substrate/internal/ategcs"
	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/internal/memorypullcache"
	"github.com/agent-substrate/substrate/proto/ateletpb"
	"github.com/agent-substrate/substrate/proto/ateompb"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-containerregistry/pkg/authn"
	googlecontainerauth "github.com/google/go-containerregistry/pkg/v1/google"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
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
	slog.SetDefault(slog.New(contextlogging.NewHandler(slog.NewJSONHandler(os.Stdout, nil))))

	tp, err := initTracing(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize tracing", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			slog.Error("Failed to shutdown TracerProvider", slog.Any("err", err))
		}
	}()

	mp, err := initMetrics(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize metrics", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			slog.Error("Failed to shutdown MeterProvider", slog.Any("err", err))
		}
	}()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		slog.InfoContext(ctx, fmt.Sprintf("Starting Prometheus metrics server on %s", *metricsListenAddr))
		if err := http.ListenAndServe(*metricsListenAddr, mux); err != nil {
			slog.Error("Failed to start prometheus metrics server", slog.Any("err", err))
		}
	}()

	ateomDialer := &AteomDialer{
		conns: lru.New(256),
	}

	var gcpRegistryAuthn authn.Authenticator
	if *gcpAuthForImagePulls {
		gcpRegistryAuthn, err = googlecontainerauth.NewEnvAuthenticator(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create GCP registry authenticator", slog.Any("err", err))
			os.Exit(1)
		}
	}

	pullCache, err := memorypullcache.NewMemoryPullCache(ctx, gcpRegistryAuthn, *localhostRegistryReplacement)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create pull cache", slog.Any("err", err))
		os.Exit(1)
	}

	anonGCSClient, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create anonymous GCS client", slog.Any("err", err))
		os.Exit(1)
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
			slog.ErrorContext(ctx, "Failed to load S3 config", slog.Any("err", err))
			os.Exit(1)
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
			slog.ErrorContext(ctx, "Failed to create GCS client", slog.Any("err", err))
			os.Exit(1)
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
		slog.ErrorContext(ctx, "Failed to listen", slog.Any("err", err))
		os.Exit(1)
	}

	svr := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()), grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor))
	ateletpb.RegisterAteomHerderServer(svr, wmService)
	reflection.Register(svr)
	slog.InfoContext(ctx, "WorkersManagerService listening", slog.Any("address", lis.Addr()))
	if err := svr.Serve(lis); err != nil {
		slog.ErrorContext(ctx, "Failed to serve", slog.Any("err", err))
		os.Exit(1)
	}
}

func initTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx,
		// GKE managed traces doesn't support validating the TLS certs of the collector
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("atelet"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// Only trace on-demand when signaled by the client (e.g. via --trace flag)
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

func initMetrics(ctx context.Context) (*sdkmetric.MeterProvider, error) {
	// Prometheus Exporter
	promExporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus metric exporter: %w", err)
	}

	// OTLP Exporter
	otlpExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("atelet"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		// Register both readers
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return mp, nil
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
	// Create static files dir if it doesn't exist.
	if err := os.MkdirAll(ateompath.StaticFilesDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating static files dir: %w", err)
	}

	// Download correct runsc version if not already downloaded.
	runscPath, err := s.fetchRunsc(ctx, req.GetRunsc())
	if err != nil {
		return nil, fmt.Errorf("in fetchRunsc: %w", err)
	}

	// Clear actor state
	if err := resetActorDirs(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	netnsPath := ateompath.AteomNetNSPath(req.GetTargetAteomNamespace(), req.GetTargetAteomName())

	g, gCtx := errgroup.WithContext(ctx)

	// Pull pause container and assemble OCI bundle
	g.Go(func() error {
		if err := prepareOCIDirectory(
			gCtx,
			s.pullCache,
			req.GetActorTemplateNamespace(),
			req.GetActorTemplateName(),
			req.GetActorId(),
			"pause",
			req.GetSpec().GetPauseImage(),
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

	// Pull each application container and assemble OCI bundle
	for _, ctr := range req.GetSpec().GetContainers() {
		ctr := ctr
		var envs []string
		for _, env := range ctr.GetEnv() {
			envs = append(envs, fmt.Sprintf("%s=%s", env.GetName(), env.GetValue()))
		}

		g.Go(func() error {
			if err := prepareOCIDirectory(
				gCtx,
				s.pullCache,
				req.GetActorTemplateNamespace(),
				req.GetActorTemplateName(),
				req.GetActorId(),
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

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Dial correct ateom over UDS.
	ateomConn, err := s.ateomDialer.DialAteomPod(ctx, req.GetTargetAteomNamespace(), req.GetTargetAteomName())
	if err != nil {
		return nil, fmt.Errorf("while getting ateom conn for %s/%s: %w", req.GetTargetAteomNamespace(), req.GetTargetAteomName(), err)
	}
	client := ateompb.NewAteomClient(ateomConn)

	// Tell ateom to do runsc create + runsc start for pause container and
	// all application containers.
	ateomReq := &ateompb.RunWorkloadRequest{
		ActorTemplateNamespace: req.GetActorTemplateNamespace(),
		ActorTemplateName:      req.GetActorTemplateName(),
		ActorId:                req.GetActorId(),
		RunscPath:              runscPath,
		Spec:                   &ateompb.WorkloadSpec{},
	}
	for _, ctr := range req.GetSpec().GetContainers() {
		ateomCtr := &ateompb.Container{
			Name: ctr.GetName(),
		}
		ateomReq.GetSpec().Containers = append(ateomReq.GetSpec().Containers, ateomCtr)
	}
	_, err = client.RunWorkload(ctx, ateomReq)
	if err != nil {
		return nil, fmt.Errorf("while calling ateom.RunWorkload: %w", err)
	}

	return &ateletpb.RunResponse{}, nil
}

func (s *AteomHerder) Checkpoint(ctx context.Context, req *ateletpb.CheckpointRequest) (*ateletpb.CheckpointResponse, error) {
	// Create static files dir if it doesn't exist.
	if err := os.MkdirAll(ateompath.StaticFilesDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating static files dir: %w", err)
	}

	// Download correct runsc version if not already downloaded.
	runscPath, err := s.fetchRunsc(ctx, req.GetRunsc())
	if err != nil {
		return nil, fmt.Errorf("in fetchRunsc: %w", err)
	}

	checkpointDir := ateompath.CheckpointDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())

	// Dial correct ateom over UDS.
	ateomConn, err := s.ateomDialer.DialAteomPod(ctx, req.GetTargetAteomNamespace(), req.GetTargetAteomName())
	if err != nil {
		return nil, fmt.Errorf("while getting ateom conn for %s/%s: %w", req.GetTargetAteomNamespace(), req.GetTargetAteomName(), err)
	}
	client := ateompb.NewAteomClient(ateomConn)

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
	ateomReq := &ateompb.CheckpointWorkloadRequest{
		ActorTemplateNamespace: req.GetActorTemplateNamespace(),
		ActorTemplateName:      req.GetActorTemplateName(),
		ActorId:                req.GetActorId(),
		RunscPath:              runscPath,
		Spec:                   &ateompb.WorkloadSpec{},
	}
	for _, ctr := range req.GetSpec().GetContainers() {
		ateomCtr := &ateompb.Container{
			Name: ctr.GetName(),
		}
		ateomReq.GetSpec().Containers = append(ateomReq.GetSpec().Containers, ateomCtr)
	}
	_, err = client.CheckpointWorkload(ctx, ateomReq)
	if err != nil {
		return nil, fmt.Errorf("while calling ateom.CheckpointWorkload: %w", err)
	}

	// Upload checkpoint from local dir.
	err = ategcs.SendLocalFileToGCSWithZstd(
		ctx,
		s.gcsClient,
		strings.TrimSuffix(req.GetSnapshotUriPrefix(), "/")+"/checkpoint.img.zstd",
		ateompath.CheckpointImgPath(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()),
	)
	if err != nil {
		return nil, fmt.Errorf("while uploading checkpoint.img to GCS: %w", err)
	}

	pagesImgPath := ateompath.PagesImgPath(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())
	if _, err := os.Stat(pagesImgPath); err == nil { // EQUALS nil
		err = ategcs.SendLocalFileToGCSWithZstd(
			ctx,
			s.gcsClient,
			strings.TrimSuffix(req.GetSnapshotUriPrefix(), "/")+"/pages.img.zstd",
			pagesImgPath,
		)
		if err != nil {
			return nil, fmt.Errorf("while uploading pages.img to GCS: %w", err)
		}
	}

	pagesMetaImgPath := ateompath.PagesMetaImgPath(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())
	if _, err := os.Stat(pagesMetaImgPath); err == nil { // EQUALS nil
		err = ategcs.SendLocalFileToGCSWithZstd(
			ctx,
			s.gcsClient,
			strings.TrimSuffix(req.GetSnapshotUriPrefix(), "/")+"/pages_meta.img.zstd",
			pagesMetaImgPath,
		)
		if err != nil {
			return nil, fmt.Errorf("while uploading pages_meta.img to GCS: %w", err)
		}
	}

	// Clear actor state
	if err := resetActorDirs(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	return &ateletpb.CheckpointResponse{}, nil
}

func (s *AteomHerder) Restore(ctx context.Context, req *ateletpb.RestoreRequest) (*ateletpb.RestoreResponse, error) {
	// Create static files dir if it doesn't exist.
	if err := os.MkdirAll(ateompath.StaticFilesDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating static files dir: %w", err)
	}

	// Download correct runsc version if not already downloaded.
	runscPath, err := s.fetchRunsc(ctx, req.GetRunsc())
	if err != nil {
		return nil, fmt.Errorf("in fetchRunsc: %w", err)
	}

	// Clear actor state
	if err := resetActorDirs(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()); err != nil {
		return nil, fmt.Errorf("while resetting actor dirs: %w", err)
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := ategcs.FetchLocalFileFromGCSWithZstd(
			gCtx,
			s.gcsClient,
			strings.TrimSuffix(req.GetSnapshotUriPrefix(), "/")+"/checkpoint.img.zstd",
			ateompath.CheckpointImgPath(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()),
		); err != nil {
			return fmt.Errorf("while downloading checkpoint.img from GCS: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := ategcs.FetchLocalFileFromGCSWithZstd(
			gCtx,
			s.gcsClient,
			strings.TrimSuffix(req.GetSnapshotUriPrefix(), "/")+"/pages.img.zstd",
			ateompath.PagesImgPath(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()),
		); err != nil {
			return fmt.Errorf("while downloading pages.img from GCS: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		if err := ategcs.FetchLocalFileFromGCSWithZstd(
			gCtx,
			s.gcsClient,
			strings.TrimSuffix(req.GetSnapshotUriPrefix(), "/")+"/pages_meta.img.zstd",
			ateompath.PagesMetaImgPath(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId()),
		); err != nil {
			return fmt.Errorf("while downloading pages_meta.img from GCS: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	netnsPath := ateompath.AteomNetNSPath(req.GetTargetAteomNamespace(), req.GetTargetAteomName())

	g, gCtx = errgroup.WithContext(ctx)

	// Pull pause container and assemble OCI bundle
	g.Go(func() error {
		if err := prepareOCIDirectory(
			gCtx,
			s.pullCache,
			req.GetActorTemplateNamespace(),
			req.GetActorTemplateName(),
			req.GetActorId(),
			"pause",
			req.GetSpec().GetPauseImage(),
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

	// Pull each application container and assemble OCI bundle
	for _, ctr := range req.GetSpec().GetContainers() {
		ctr := ctr
		var envs []string
		for _, env := range ctr.GetEnv() {
			envs = append(envs, fmt.Sprintf("%s=%s", env.GetName(), env.GetValue()))
		}

		g.Go(func() error {
			if err := prepareOCIDirectory(
				gCtx,
				s.pullCache,
				req.GetActorTemplateNamespace(),
				req.GetActorTemplateName(),
				req.GetActorId(),
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

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Dial correct ateom over UDS.
	ateomConn, err := s.ateomDialer.DialAteomPod(ctx, req.GetTargetAteomNamespace(), req.GetTargetAteomName())
	if err != nil {
		return nil, fmt.Errorf("while getting ateom conn for %s/%s: %w", req.GetTargetAteomNamespace(), req.GetTargetAteomName(), err)
	}
	client := ateompb.NewAteomClient(ateomConn)

	// Tell ateom to do runsc create + runsc restore for pause container and
	// all application containers.
	ateomReq := &ateompb.RestoreWorkloadRequest{
		ActorTemplateNamespace: req.GetActorTemplateNamespace(),
		ActorTemplateName:      req.GetActorTemplateName(),
		ActorId:                req.GetActorId(),
		RunscPath:              runscPath,
		Spec:                   &ateompb.WorkloadSpec{},
	}
	for _, ctr := range req.GetSpec().GetContainers() {
		ateomCtr := &ateompb.Container{
			Name: ctr.GetName(),
		}
		ateomReq.GetSpec().Containers = append(ateomReq.GetSpec().Containers, ateomCtr)
	}
	_, err = client.RestoreWorkload(ctx, ateomReq)
	if err != nil {
		return nil, fmt.Errorf("while calling ateom.RestoreWorkload: %w", err)
	}

	return &ateletpb.RestoreResponse{}, nil
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
