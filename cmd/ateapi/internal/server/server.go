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

// Package server builds ate-api's gRPC server and the background work it
// depends on. ate-api's main and the functional-test harness both build the
// server here, so the tests run what production runs.
package server

import (
	"context"
	"fmt"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apiauthn"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/workercache"
	"go.ate.dev/substrate/cmd/ateapi/internal/workerservice"
	"go.ate.dev/substrate/internal/ateinterceptors"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
	"go.ate.dev/substrate/pkg/client/informers/externalversions"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// MaxRPCDeadline is the max deadline for all RPC methods exposed by this server.
const MaxRPCDeadline = 10 * time.Minute

// Config is what the server takes from its environment.
type Config struct {
	Store store.Interface
	// KubeClient is the clientset for built-in Kubernetes resources, such as
	// atelet pods and StorageClasses.
	KubeClient kubernetes.Interface
	// SubstrateCRDClient is the clientset for Agent Substrate's own CRDs, such
	// as WorkerPools and CSIDriverConfigs.
	SubstrateCRDClient versioned.Interface

	// AteletNamespace is where the atelet pods run.
	AteletNamespace string
	// AteletSPIFFEID is the identity atelet presents, both on its serving
	// certificate and on the client certificate it calls WorkerService with.
	AteletSPIFFEID string
	// AteletClientCredBundle and PodIdentityCACerts configure the mTLS ate-api
	// dials atelet with.
	AteletClientCredBundle string
	PodIdentityCACerts     string
	// AteletDialerOptions customize how atelet is dialed.
	AteletDialerOptions []controlapi.DialerOption

	ServerCreds credentials.TransportCredentials
	Authn       apiauthn.ServerConfig
	Authorizer  *authz.Authorizer

	ObjectStorePlugin objectstore.ControlProviderClient
	VolumePlugins     map[string]volume.VolumePluginControlPlane
	SnapshotLocation  string

	ActorJWTIssuer      string
	ActorJWTAuthorities localjwtauthority.Pool
	ActorIDCAs          localca.Pool

	DefaultEgressGatewayAddress string
	TemplateResyncInterval      time.Duration

	Meter metric.Meter
}

// Server is ate-api's control plane server.
type Server struct {
	GRPC *grpc.Server

	templateReconciler         *controlapi.ActorTemplateReconciler
	workerAssignmentReconciler *controlapi.WorkerAssignmentReconciler
}

// New builds the server. It starts the worker cache and the informers, which
// run until ctx ends, and waits for them to sync.
// Caller must start the reconcilers with StartReconcilers and serve with GRPC.
func New(ctx context.Context, cfg Config) (*Server, error) {
	if err := apiauthn.ValidateServerConfig(cfg.Authn); err != nil {
		return nil, fmt.Errorf("invalid auth config: %w", err)
	}

	workerCache := workercache.New(cfg.Store, 5*time.Minute)
	if err := workerCache.Start(ctx); err != nil {
		return nil, fmt.Errorf("seeding the worker cache: %w", err)
	}

	ateFactory := externalversions.NewSharedInformerFactory(cfg.SubstrateCRDClient, 0)
	workerPoolLister := ateFactory.Ate().V1().WorkerPools().Lister()
	csiDriverConfigLister := ateFactory.Ate().V1().CSIDriverConfigs().Lister()

	ateletPodInformerFactory, ateletPodInformer := controlapi.AteletInformer(cfg.KubeClient, cfg.AteletNamespace)
	scInformerFactory := informers.NewSharedInformerFactory(cfg.KubeClient, 0)
	storageClassLister := scInformerFactory.Storage().V1().StorageClasses().Lister()

	ateletPodInformerFactory.Start(ctx.Done())
	ateFactory.Start(ctx.Done())
	scInformerFactory.Start(ctx.Done())

	ateletPodInformerFactory.WaitForCacheSync(ctx.Done())
	ateFactory.WaitForCacheSync(ctx.Done())
	scInformerFactory.WaitForCacheSync(ctx.Done())

	if err := controlapi.RegisterWorkerCount(cfg.Meter, workerCache.Workers, workerPoolLister.List); err != nil {
		return nil, fmt.Errorf("registering the worker-count metric: %w", err)
	}
	instruments, err := controlapi.NewInstruments(cfg.Meter)
	if err != nil {
		return nil, fmt.Errorf("creating metric instruments: %w", err)
	}

	dialerOpts := append([]controlapi.DialerOption{
		controlapi.WithNodeGetter(func(ctx context.Context, nodeName string) error {
			_, err := cfg.KubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
			return err
		}),
	}, cfg.AteletDialerOptions...)
	ateletDialer := controlapi.NewAteletDialer(ateletPodInformer.GetIndexer(), cfg.AteletSPIFFEID, cfg.AteletClientCredBundle, cfg.PodIdentityCACerts, dialerOpts...)

	controlSrv := controlapi.NewRPCService(
		cfg.Store,
		workerCache,
		csiDriverConfigLister,
		storageClassLister,
		ateletDialer,
		instruments,
		cfg.DefaultEgressGatewayAddress,
		cfg.VolumePlugins,
		cfg.ObjectStorePlugin,
		cfg.ActorJWTIssuer,
		cfg.ActorJWTAuthorities,
		cfg.SnapshotLocation,
	)

	unaryInterceptors := []grpc.UnaryServerInterceptor{
		apiauthn.UnaryServerInterceptor(cfg.Authn),
		ateinterceptors.MaxDeadlineUnaryInterceptor(MaxRPCDeadline),
		ateinterceptors.ServerUnaryInterceptor,
		authz.UnaryServerInterceptor(cfg.Authorizer),
		ateinterceptors.RejectUnknownFieldsUnaryInterceptor,
		preview.UnaryServerInterceptor,
	}

	mux := grpc.NewServer(
		grpc.Creds(cfg.ServerCreds),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		// Close connections after an hour to allow for any
		// client that doesn't use Kubernetes endpoint resolvers
		// to eventually reobtain backend IPs. https://github.com/grpc/grpc/issues/12295
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      1 * time.Hour,
			MaxConnectionAgeGrace: MaxRPCDeadline + time.Minute,
		}),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(
			apiauthn.StreamServerInterceptor(cfg.Authn),
			authz.StreamServerInterceptor(),
			preview.StreamServerInterceptor,
		),
	)
	reflection.Register(mux)
	ateapi.RegisterControlServer(mux, controlSrv)
	ateapi.RegisterWorkerServiceServer(mux, workerservice.New(cfg.Store, controlSrv, cfg.AteletSPIFFEID, cfg.ActorIDCAs, cfg.ActorJWTIssuer, cfg.ActorJWTAuthorities))

	return &Server{
		GRPC: mux,
		// Drive stored ActorTemplates through the golden actor flow.
		templateReconciler: controlapi.NewActorTemplateReconciler(cfg.Store, controlSrv, cfg.TemplateResyncInterval),
		// Crash the Actors lost when a Worker's worker restarts.
		workerAssignmentReconciler: controlapi.NewWorkerAssignmentReconciler(cfg.Store, workerCache),
	}, nil
}

// StartReconcilers starts the reconcilers, which run until ctx ends.
func (s *Server) StartReconcilers(ctx context.Context) {
	s.templateReconciler.Start(ctx)
	s.workerAssignmentReconciler.Start(ctx)
}
