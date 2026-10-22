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

package ateclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"go.ate.dev/substrate/internal/clustertrustbundle"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/portforward"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	metricsv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned"
)

// NamespaceEnv overrides the namespace the client looks for substrate in. The
// client runs outside the cluster, so it has no downward API to read and no pod
// namespace to fall back on.
const NamespaceEnv = "ATE_NAMESPACE"

// APIServiceEnv and ClientServiceAccountEnv override the ateapi Service and the
// ServiceAccount the client mints its token from. A deployment that prefixes
// resource names changes both, and the client has no way to discover that from
// outside the cluster.
const (
	APIServiceEnv           = "ATE_API_SERVICE_NAME"
	ClientServiceAccountEnv = "ATE_CLIENT_SERVICE_ACCOUNT"
)

// apiServiceName is the Service that fronts ateapi.
func apiServiceName() string {
	if n := os.Getenv(APIServiceEnv); n != "" {
		return n
	}
	return installdefaults.APIServiceName
}

// clientServiceAccount is the ServiceAccount the bearer token is minted from.
func clientServiceAccount() string {
	if n := os.Getenv(ClientServiceAccountEnv); n != "" {
		return n
	}
	return installdefaults.ClientServiceAccount
}

// FallbackPrincipal is the principal a client authenticates as when it is
// given no token file: the ServiceAccount it mints a token from. Every caller
// relying on the fallback is this same principal.
func FallbackPrincipal() string {
	return fmt.Sprintf("system:serviceaccount:%s:%s", systemNamespace(), clientServiceAccount())
}

// DefaultServiceAccount is the ServiceAccount a client mints its token from
// when it has no other credential.
func DefaultServiceAccount() (namespace, name string) {
	return systemNamespace(), clientServiceAccount()
}

// DefaultAudience is the audience of tokens minted for ateapi: the in-cluster
// DNS name of its Service.
func DefaultAudience() string {
	return apiServerName()
}

// systemNamespace is the namespace the client expects ateapi to be running in.
func systemNamespace() string {
	if ns := os.Getenv(NamespaceEnv); ns != "" {
		return ns
	}
	return installdefaults.SystemNamespace
}

// apiServerName is the in-cluster DNS name of the ateapi Service. It is both the
// name checked on ateapi's serving cert and the audience of the bearer token
// minted for it, so it has to track the namespace ateapi actually runs in.
func apiServerName() string {
	return fmt.Sprintf("%s.%s.svc", apiServiceName(), systemNamespace())
}

const (
	// serviceDNSSignerName and liveBundleSelector mirror the
	// clusterTrustBundle projected-volume sources that in-cluster clients
	// mount to verify ateapi's serving cert.
	serviceDNSSignerName = "servicedns.podcert.ate.dev/identity"
	liveBundleSelector   = "podcert.ate.dev/canarying=live"
)

// Client wraps the gRPC ControlClient and ensures the port-forward connection is closed when done.
type Client struct {
	ateapipb.ControlClient
	conn           *grpc.ClientConn
	cancel         func()
	tracerProvider *sdktrace.TracerProvider
}

// Close closes the underlying gRPC connection and stops the port-forwarder.

// roundRobinServiceConfig spreads RPCs over every address the resolver returns.
// ateapi is a headless Service, so that is one address per replica, and gRPC's
// default of pick_first would send an entire client's traffic to whichever one
// it connected to first. internal/ateapiauth dials with the same policy.
const roundRobinServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`

func (c *Client) Close() {
	if c.tracerProvider != nil {
		// Best practice to ensure clean provider shutdown, even though we skip exporters for clients.
		_ = c.tracerProvider.Shutdown(context.Background())
	}
	if c.conn != nil {
		c.conn.Close()
	}
	if c.cancel != nil {
		c.cancel()
	}
}

// WorkerService returns a WorkerService client on the same connection. Only
// atelet is permitted to call WorkerService; this exists so tests can check
// that other callers are rejected.
func (c *Client) WorkerService() ateapipb.WorkerServiceClient {
	return ateapipb.NewWorkerServiceClient(c.conn)
}

// Authentication returns an Authentication client on the same connection.
func (c *Client) Authentication() ateapipb.AuthenticationClient {
	return ateapipb.NewAuthenticationClient(c.conn)
}

// NewClient creates a new Ate API client. If endpoint is empty, it automatically
// port-forwards to the ate-api-server pod in substrate's system namespace. It
// authenticates with tokenFile if set, and otherwise with a token minted for
// the default ServiceAccount, refreshed before it expires. extraOpts are
// appended to the options used to dial the server.
func NewClient(ctx context.Context, kubeconfigPath, k8sContext, endpoint, tokenFile string, traceEnabled bool, extraOpts ...grpc.DialOption) (*Client, error) {
	return Dial(ctx, DialOptions{
		Kubeconfig:  kubeconfigPath,
		KubeContext: k8sContext,
		Endpoint:    endpoint,
		TokenFile:   tokenFile,
		Trace:       traceEnabled,
		GRPCOptions: extraOpts,
	})
}

// DialOptions configures Dial.
type DialOptions struct {
	// Kubeconfig and KubeContext select the cluster substrate runs in. Empty
	// values use the default loading rules and the current context.
	Kubeconfig  string
	KubeContext string
	// Endpoint is dialed directly instead of port-forwarding to the ateapi
	// Service.
	Endpoint string
	// TokenFile is a bearer token file, re-read on every RPC, or "-" to read
	// one token from stdin.
	TokenFile string
	// CredentialPlugin is a kubeconfig exec credential plugin, used when
	// TokenFile is empty. Without either, the client mints a token for the
	// default ServiceAccount.
	CredentialPlugin *clientcmdapi.ExecConfig
	Trace            bool
	GRPCOptions      []grpc.DialOption
}

// Dial creates a new Ate API client as opts describes.
func Dial(ctx context.Context, opts DialOptions) (*Client, error) {
	tp, err := initTracing(ctx, opts.Trace)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tracing: %w", err)
	}
	cli, err := dial(ctx, opts)
	if err != nil {
		if tp != nil {
			_ = tp.Shutdown(ctx)
		}
		return nil, err
	}
	cli.tracerProvider = tp
	return cli, nil
}

func dial(ctx context.Context, opts DialOptions) (*Client, error) {
	config, err := LoadKubeConfig(opts.Kubeconfig, opts.KubeContext)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	// Older clusters use the deprecated beta ClusterTrustBundle API.
	// Suppress its warnings when falling back from v1.
	config.WarningHandlerWithContext = &rest.NoWarnings{}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	// Verify the server before attaching the credential below: a token must
	// never be sent over an unauthenticated channel.
	tlsCfg, err := serverTLSConfig(ctx, clientset)
	if err != nil {
		return nil, err
	}
	creds, err := clientCredentials(opts, clientset)
	if err != nil {
		return nil, err
	}

	target, stopForward := opts.Endpoint, func() {}
	if target == "" {
		// TODO: Should we special-case a LoadBalancer "api" Service and dial its
		// address directly instead of port-forwarding?
		localPort, stop, err := portforward.ServicePortForward(ctx, config, clientset, systemNamespace(), apiServiceName(), 443)
		if err != nil {
			return nil, err
		}
		target, stopForward = fmt.Sprintf("127.0.0.1:%d", localPort), stop
	}
	conn, err := connect(target, tlsCfg, creds, opts)
	if err != nil {
		stopForward()
		return nil, err
	}
	return &Client{
		ControlClient: ateapipb.NewControlClient(conn),
		conn:          conn,
		cancel:        stopForward,
	}, nil
}

// connect dials target, attaching creds to every RPC and retrying an RPC once
// when ateapi rejects a refreshable credential.
func connect(target string, tlsCfg *tls.Config, creds credentials.PerRPCCredentials, opts DialOptions) (*grpc.ClientConn, error) {
	if cc, ok := creds.(interface {
		clientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error)
	}); ok {
		tlsCfg.GetClientCertificate = cc.clientCertificate
	}
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithPerRPCCredentials(creds),
		grpc.WithChainUnaryInterceptor(retryUnauthenticated(creds)),
	}
	if opts.Endpoint != "" {
		dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(roundRobinServiceConfig))
	}
	if opts.Trace {
		dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(newTraceInterceptor()))
	}
	dialOpts = append(dialOpts, opts.GRPCOptions...)

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial ateapi at %s: %w", target, err)
	}
	return conn, nil
}

// LoadKubeConfig loads a Kubernetes client configuration from the specified kubeconfig path and context.
func LoadKubeConfig(kubeconfigPath, k8sContext string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath
	configOverrides := &clientcmd.ConfigOverrides{CurrentContext: k8sContext}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
}

func serverTLSConfig(ctx context.Context, clientset kubernetes.Interface) (*tls.Config, error) {
	bundles, err := clustertrustbundle.NewClient(clientset, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to discover ClusterTrustBundle API: %w", err)
	}
	ctbs, err := bundles.List(ctx, metav1.ListOptions{
		LabelSelector: liveBundleSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list ClusterTrustBundles: %w", err)
	}

	pool := x509.NewCertPool()
	found := false
	for _, ctb := range ctbs.Items {
		if ctb.Spec.SignerName != serviceDNSSignerName {
			continue
		}
		if !pool.AppendCertsFromPEM([]byte(ctb.Spec.TrustBundle)) {
			return nil, fmt.Errorf("ClusterTrustBundle %q contains no valid certificates", ctb.ObjectMeta.Name)
		}
		found = true
	}
	if !found {
		return nil, fmt.Errorf("no live ClusterTrustBundle found for signer %q", serviceDNSSignerName)
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    pool,
		ServerName: apiServerName(),
	}, nil
}

// clientCredentials returns the credential source opts selects.
//
// The minted token authenticates as FallbackPrincipal, not as whoever runs
// the client: anyone Kubernetes RBAC lets create a token for that
// ServiceAccount gets the same identity, and whatever it is granted.
func clientCredentials(opts DialOptions, clientset kubernetes.Interface) (credentials.PerRPCCredentials, error) {
	switch {
	case opts.TokenFile == "-":
		creds, err := readBearerToken(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("read bearer token from stdin: %w", err)
		}
		return creds, nil
	case opts.TokenFile != "":
		return fileBearerTokenCreds(opts.TokenFile), nil
	case opts.CredentialPlugin != nil:
		server := "https://" + apiServerName()
		if opts.Endpoint != "" {
			server = "https://" + opts.Endpoint
		}
		cluster, err := execCluster(server, apiServerName(), DefaultAudience())
		if err != nil {
			return nil, err
		}
		return newExecCreds(opts.CredentialPlugin, cluster)
	}
	return newMintedTokenCreds(func(ctx context.Context) (string, time.Time, error) {
		return MintServiceAccountToken(ctx, clientset, systemNamespace(), clientServiceAccount(), DefaultAudience(), time.Hour)
	}), nil
}

// initTracing returns (nil, nil) when tracing is disabled: the OTel globals
// stay noop so no traceparent is injected, the server roots the trace, and the
// server side sampling ratio applies. A NeverSample provider here would
// instead pin every ParentBased sampler downstream to not sampled.
func initTracing(ctx context.Context, enabled bool) (*sdktrace.TracerProvider, error) {
	if !enabled {
		return nil, nil
	}

	res, err := resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(
			semconv.UserAgentOriginal("kubectl-ate"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

func newTraceInterceptor() grpc.UnaryClientInterceptor {
	var once sync.Once
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		tracer := otel.Tracer("kubectl-ate")
		ctx, span := tracer.Start(ctx, method)
		defer span.End()

		once.Do(func() {
			fmt.Fprintf(os.Stderr, "Tracing enabled. Trace ID: %s\n", span.SpanContext().TraceID().String())
		})

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// NewMetricsClientset creates a new Kubernetes Metrics Clientset using the provided kubeconfig path and context.
func NewMetricsClientset(kubeconfigPath, k8sContext string) (*metricsv1beta1.Clientset, error) {
	config, err := LoadKubeConfig(kubeconfigPath, k8sContext)
	if err != nil {
		return nil, err
	}
	return metricsv1beta1.NewForConfig(config)
}
