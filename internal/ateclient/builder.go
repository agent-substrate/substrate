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
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	metricsv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned"
)

const (
	apiServerName = "api.ate-system.svc"

	// serviceDNSSignerName and liveBundleSelector mirror the
	// clusterTrustBundle projected-volume sources that in-cluster clients
	// mount to verify ateapi's serving cert.
	serviceDNSSignerName = "servicedns.podcert.ate.dev/identity"
	liveBundleSelector   = "podcert.ate.dev/canarying=live"
)

// Client wraps the gRPC ControlClient and DebugClient and ensures the port-forward connection is closed when done.
type Client struct {
	ateapipb.ControlClient
	ateapipb.DebugClient
	conn           *grpc.ClientConn
	cancel         func()
	tracerProvider *sdktrace.TracerProvider
}

// Close closes the underlying gRPC connection and stops the port-forwarder.
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

// NewClient creates a new Ate API client. If endpoint is empty, it automatically port-forwards
// to the ate-api-server pod in the ate-system namespace.
func NewClient(ctx context.Context, kubeconfigPath, k8sContext, endpoint string, traceEnabled bool) (*Client, error) {
	tp, err := initTracing(ctx, traceEnabled)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tracing: %w", err)
	}

	var cli *Client
	if endpoint != "" {
		cli, err = dialDirect(ctx, kubeconfigPath, k8sContext, endpoint, traceEnabled)
	} else {
		cli, err = dialPortForward(ctx, kubeconfigPath, k8sContext, traceEnabled)
	}

	if err != nil {
		_ = tp.Shutdown(ctx)
		return nil, err
	}

	cli.tracerProvider = tp
	return cli, nil
}

func dialDirect(ctx context.Context, kubeconfigPath, k8sContext, endpoint string, traceEnabled bool) (*Client, error) {
	clientset, err := NewK8sClientset(kubeconfigPath, k8sContext)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	// Verify the server before attaching the bearer token below: the token
	// must never be sent over an unauthenticated channel.
	tlsCfg, err := serverTLSConfig(ctx, clientset)
	if err != nil {
		return nil, err
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	tokenOpt, err := bearerTokenDialOption(ctx, clientset)
	if err != nil {
		return nil, err
	}
	opts = append(opts, tokenOpt)

	if traceEnabled {
		opts = append(opts, grpc.WithUnaryInterceptor(newTraceInterceptor()))
	}

	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial manual endpoint: %w", err)
	}
	return &Client{
		ControlClient: ateapipb.NewControlClient(conn),
		DebugClient:   ateapipb.NewDebugClient(conn),
		conn:          conn,
		cancel:        func() {},
	}, nil
}

// LoadConfig loads a Kubernetes client configuration from the specified kubeconfig path and context.
func LoadConfig(kubeconfigPath, k8sContext string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath
	configOverrides := &clientcmd.ConfigOverrides{CurrentContext: k8sContext}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
}

func dialPortForward(ctx context.Context, kubeconfigPath, k8sContext string, traceEnabled bool) (*Client, error) {
	config, err := LoadConfig(kubeconfigPath, k8sContext)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	// Look up the 'api' Service to dynamically get its pod selector
	svc, err := clientset.CoreV1().Services("ate-system").Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get api service: %w", err)
	}
	selector := labels.SelectorFromSet(svc.Spec.Selector).String()

	// Find the pods backing the service
	pods, err := clientset.CoreV1().Pods("ate-system").List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list ateapi pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no ate-api-server pods found in ate-system namespace")
	}
	targetPod := pods.Items[0]

	// Setup port-forwarding
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(targetPod.Namespace).
		Name(targetPod.Name).
		SubResource("portforward")

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create SPDY transport: %w", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, req.URL())

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})

	ports := []string{"0:443"} // Port 0 asks OS for a random available local port

	fw, err := portforward.New(dialer, ports, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("failed to create port forwarder: %w", err)
	}

	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := fw.ForwardPorts(); err != nil {
			errCh <- fmt.Errorf("port forwarding failed: %w", err)
		}
	}()

	// Wait for the tunnel to be ready, an error, or context cancellation
	select {
	case <-readyCh:
		// Tunnel is ready!
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	forwardedPorts, err := fw.GetPorts()
	if err != nil || len(forwardedPorts) == 0 {
		close(stopCh)
		return nil, fmt.Errorf("failed to get forwarded ports: %w", err)
	}

	localPort := forwardedPorts[0].Local
	localEndpoint := fmt.Sprintf("127.0.0.1:%d", localPort)

	tlsCfg, err := serverTLSConfig(ctx, clientset)
	if err != nil {
		close(stopCh)
		return nil, err
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	tokenOpt, err := bearerTokenDialOption(ctx, clientset)
	if err != nil {
		close(stopCh)
		return nil, err
	}
	opts = append(opts, tokenOpt)

	if traceEnabled {
		opts = append(opts, grpc.WithUnaryInterceptor(newTraceInterceptor()))
	}

	conn, err := grpc.NewClient(localEndpoint, opts...)
	if err != nil {
		close(stopCh)
		return nil, fmt.Errorf("failed to dial gRPC over tunnel: %w", err)
	}

	return &Client{
		ControlClient: ateapipb.NewControlClient(conn),
		DebugClient:   ateapipb.NewDebugClient(conn),
		conn:          conn,
		cancel: func() {
			close(stopCh)
			wg.Wait()
		},
	}, nil
}

func serverTLSConfig(ctx context.Context, clientset kubernetes.Interface) (*tls.Config, error) {
	ctbs, err := clientset.CertificatesV1beta1().ClusterTrustBundles().List(ctx, metav1.ListOptions{
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
		ServerName: apiServerName,
	}, nil
}

// bearerTokenDialOption attaches a ServiceAccount token for the ate-client SA
// as per-RPC credentials.
func bearerTokenDialOption(ctx context.Context, clientset *kubernetes.Clientset) (grpc.DialOption, error) {
	expirationSeconds := int64(3600)
	tokenRequest := &authv1.TokenRequest{
		Spec: authv1.TokenRequestSpec{
			Audiences:         []string{apiServerName},
			ExpirationSeconds: &expirationSeconds,
		},
	}
	token, err := clientset.CoreV1().ServiceAccounts("ate-system").CreateToken(ctx, "ate-client", tokenRequest, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to request ateapi bearer token: %w", err)
	}
	if token.Status.Token == "" {
		return nil, fmt.Errorf("failed to request ateapi bearer token: token response was empty")
	}
	return grpc.WithPerRPCCredentials(bearerTokenCreds(token.Status.Token)), nil
}

type bearerTokenCreds string

func (c bearerTokenCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	if c == "" {
		return nil, fmt.Errorf("bearer token is empty")
	}
	return map[string]string{"authorization": "Bearer " + string(c)}, nil
}

func (c bearerTokenCreds) RequireTransportSecurity() bool { return true }

func initTracing(ctx context.Context, enabled bool) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithAttributes(
			semconv.UserAgentOriginal("kubectl-ate"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	sampler := sdktrace.NeverSample()
	if enabled {
		sampler = sdktrace.AlwaysSample()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
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

// NewK8sClientset creates a new Kubernetes Clientset using the provided kubeconfig path and context.
func NewK8sClientset(kubeconfigPath, k8sContext string) (*kubernetes.Clientset, error) {
	config, err := LoadConfig(kubeconfigPath, k8sContext)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

// NewMetricsClientset creates a new Kubernetes Metrics Clientset using the provided kubeconfig path and context.
func NewMetricsClientset(kubeconfigPath, k8sContext string) (*metricsv1beta1.Clientset, error) {
	config, err := LoadConfig(kubeconfigPath, k8sContext)
	if err != nil {
		return nil, err
	}
	return metricsv1beta1.NewForConfig(config)
}
