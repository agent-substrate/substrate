// Copyright 2026 The Agent Substrate Authors
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

package harness

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"go.ate.dev/substrate/api/ateapi/v1"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	objectstorev1 "go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/authz"
	"go.ate.dev/substrate/cmd/ateapi/internal/controlapi"
	"go.ate.dev/substrate/cmd/ateapi/internal/server"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/pkg/client/clientset/versioned"
)

const (
	// bufSize is the buffer of each in-memory listener.
	bufSize = 1 << 20
	// templateResync is how often the ActorTemplateReconciler lists templates,
	// which bounds how long a new template waits for its golden snapshot.
	templateResync = 100 * time.Millisecond
	// waitTimeout bounds every wait and retry the harness does.
	waitTimeout = 10 * time.Second
	// ActorJWTIssuer is the issuer of the actor JWTs ate-api mints.
	ActorJWTIssuer = "https://idp.harness.test"
)

// testCount numbers the harnesses of the process, so each test's namespace and
// node names are its own in the shared envtest cluster.
var testCount atomic.Int64

// Harness is one ate-api and the fakes it talks to. Tests change and observe
// ate-api only through Control and the harness's methods.
type Harness struct {
	// Control is a client of ate-api's Control service. It opts in to preview
	// features only in a harness created WithPreviewClient.
	Control ateapi.ControlClient
	// ObjectStore is the blob storage ate-api and the atelets share.
	ObjectStore *ObjectStore
	// Volumes is the CSI driver behind its StorageClass.
	Volumes *Volumes

	// preview makes the harness's Control clients opt in to preview features.
	preview bool

	t         *testing.T
	ctx       context.Context
	namespace string
	pki       *pki
	k8s       kubernetes.Interface
	listener  *bufconn.Listener

	mu                    sync.Mutex
	nodesByTestName       map[string]*Node
	workersByName         map[string]*Worker
	ateletListenersByAddr map[string]*bufconn.Listener
	counter               int
}

// Option configures a harness.
type Option func(*Harness)

// WithPreviewClient makes the harness's Control clients, and so its fixtures,
// opt in to preview features, as kubectl ate --preview does. Every preview
// gate is on in ate-api, so a test needs it to use a preview feature, such as
// a template with an external volume.
func WithPreviewClient() Option {
	return func(h *Harness) { h.preview = true }
}

// New starts an empty ate-api for t, built as ate-api's main builds it, with
// its own database and fakes. Everything is torn down when t ends.
func New(t *testing.T, opts ...Option) *Harness {
	t.Helper()
	if restConfig == nil {
		t.Fatal("harness.Main must run from the package's TestMain")
	}
	ctx, cancel := context.WithCancel(context.Background())

	n := testCount.Add(1)
	h := &Harness{
		ObjectStore:           newObjectStore(),
		Volumes:               newVolumes(fmt.Sprintf("harness-%d.volumes.substrate.io", n), fmt.Sprintf("harness-%d-volumes", n)),
		t:                     t,
		ctx:                   ctx,
		namespace:             fmt.Sprintf("harness-%d", n),
		nodesByTestName:       map[string]*Node{},
		workersByName:         map[string]*Worker{},
		ateletListenersByAddr: map[string]*bufconn.Listener{},
	}
	for _, opt := range opts {
		opt(h)
	}

	var err error
	if h.pki, err = newPKI(); err != nil {
		t.Fatal(err)
	}
	if h.k8s, err = kubernetes.NewForConfig(restConfig); err != nil {
		t.Fatalf("creating the k8s client: %v", err)
	}
	ac, err := versioned.NewForConfig(restConfig)
	if err != nil {
		t.Fatalf("creating the substrate client: %v", err)
	}
	if _, err := h.k8s.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: h.namespace},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("creating namespace %s: %v", h.namespace, err)
	}
	// Before ate-api starts, so its informers see the driver's config and
	// StorageClass from the start.
	h.serveVolumes(ac)

	persistence := storetest.SetupPostgresPersistence(t)
	fgaServer, err := authz.NewOpenFGAServer(persistence.Pool())
	if err != nil {
		t.Fatalf("creating the OpenFGA server: %v", err)
	}
	t.Cleanup(fgaServer.Close)
	ns := installdefaults.SystemNamespace
	authorizer, policyManager, err := authz.New(ctx, persistence.Pool(), fgaServer, []string{ClientID(clientName)}, authz.SystemPrincipals{
		ControllerSPIFFEID:       installdefaults.ControllerSPIFFEID(ns),
		EgressGatewaySPIFFEIDs:   []string{installdefaults.EgressSPIFFEID(ns)},
		IngressRouterSPIFFEID:    installdefaults.RouterSPIFFEID(ns),
		AteletSPIFFEID:           installdefaults.AteletSPIFFEID(ns),
		APIServerSidecarSPIFFEID: installdefaults.APIServerSPIFFEID(ns),
	})
	if err != nil {
		t.Fatalf("initializing authorization: %v", err)
	}
	persistence.SetPolicyManager(policyManager)
	// Registered after the store's and OpenFGA's own cleanups, so the caches
	// and reconcilers stop before their database goes away.
	t.Cleanup(cancel)

	jwtAuthority, err := localjwtauthority.GenerateAuthority("ES256", "1")
	if err != nil {
		t.Fatalf("generating the actor JWT authority: %v", err)
	}
	actorCA, err := localca.GenerateCA("1", localca.KeyTypeECDSAP256, 24*time.Hour)
	if err != nil {
		t.Fatalf("generating the actor CA: %v", err)
	}
	serverTLS, err := h.pki.serverTLS()
	if err != nil {
		t.Fatal(err)
	}

	srv, err := server.New(ctx, server.Config{
		Store:              persistence,
		KubeClient:         h.k8s,
		SubstrateCRDClient: ac,
		AteletNamespace:    ns,
		AteletSPIFFEID:     installdefaults.AteletSPIFFEID(ns),
		// Atelets are dialed in memory and without TLS, through the real
		// lookup, dial and connection-cache path.
		AteletDialerOptions: []controlapi.DialerOption{
			controlapi.WithDialCredentials(func(string) (credentials.TransportCredentials, error) {
				return insecure.NewCredentials(), nil
			}),
			controlapi.WithContextDialer(h.dialAtelet),
		},
		ServerCreds:            credentials.NewTLS(serverTLS),
		Authn:                  noTokens(),
		Authorizer:             authorizer,
		ObjectStorePlugin:      objectstorev1.NewControlProviderClient(dialInMemory(t, serveInMemory(t, h.ObjectStore.grpcServer()))),
		SnapshotLocation:       StorageLocation,
		ActorJWTIssuer:         ActorJWTIssuer,
		ActorJWTAuthorities:    &localjwtauthority.ConcretePool{Authorities: []*localjwtauthority.Authority{jwtAuthority}, ActiveForSigning: "1"},
		ActorIDCAs:             &localca.ConcretePool{CAs: []*localca.CA{actorCA}, ActiveForSigning: "1"},
		TemplateResyncInterval: templateResync,
		Meter:                  noop.NewMeterProvider().Meter("ateapi"),
	})
	if err != nil {
		t.Fatalf("building ate-api: %v", err)
	}
	srv.StartReconcilers(ctx)
	h.listener = bufconn.Listen(bufSize)
	go func() { _ = srv.GRPC.Serve(h.listener) }()
	t.Cleanup(srv.GRPC.Stop)

	h.Control = h.ClientAs(clientName)
	return h
}

// ClientAs returns a Control client that authenticates with a certificate for
// ClientID(name). Access policies name it as Member(name). Unlike Control, it
// holds no permissions until a policy grants them.
func (h *Harness) ClientAs(name string) ateapi.ControlClient {
	h.t.Helper()
	var opts []grpc.DialOption
	if h.preview {
		opts = preview.DialOptionsWithOptIn()
	}
	return h.controlClient(ClientID(name), opts...)
}

// ControlWithoutPreview returns a Control client that authenticates as Control
// does but never opts in to preview features, for a test WithPreviewClient to
// check what a client without the opt-in can do.
func (h *Harness) ControlWithoutPreview() ateapi.ControlClient {
	h.t.Helper()
	return h.controlClient(ClientID(clientName))
}

// EgressGateway returns a Control client that authenticates as atenet-egress,
// which holds the egress_gateway role, such as for minting actor JWTs.
func (h *Harness) EgressGateway() ateapi.ControlClient {
	h.t.Helper()
	return h.controlClient(installdefaults.EgressSPIFFEID(installdefaults.SystemNamespace), preview.DialOptionsWithOptIn()...)
}

// controlClient returns a Control client that authenticates with a
// certificate for the SPIFFE ID id.
func (h *Harness) controlClient(id string, opts ...grpc.DialOption) ateapi.ControlClient {
	h.t.Helper()
	cert, err := h.pki.clientCert(id)
	if err != nil {
		h.t.Fatal(err)
	}
	return ateapi.NewControlClient(h.dialAteAPI(h.pki.clientTLS(cert), opts...))
}

// dialAteAPI connects to ate-api over TLS configured by tlsConfig. The
// connection is closed when the test ends.
func (h *Harness) dialAteAPI(tlsConfig *tls.Config, opts ...grpc.DialOption) *grpc.ClientConn {
	h.t.Helper()
	opts = append([]grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return h.listener.DialContext(ctx)
		}),
	}, opts...)
	conn, err := grpc.NewClient("passthrough:///"+serverName, opts...)
	if err != nil {
		h.t.Fatalf("connecting to ate-api: %v", err)
	}
	h.t.Cleanup(func() { conn.Close() })
	return conn
}

// serveInMemory serves srv on an in-memory listener until the test ends.
func serveInMemory(t testing.TB, srv *grpc.Server) *bufconn.Listener {
	lis := bufconn.Listen(bufSize)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis
}

// dialInMemory connects to lis without TLS. The connection is closed when the
// test ends.
func dialInMemory(t testing.TB, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///in-memory",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}))
	if err != nil {
		t.Fatalf("connecting in memory: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// serveVolumes serves the Volumes as a CSI driver on a loopback TCP port, and
// creates the CSIDriverConfig through which ate-api dials it, without TLS, and
// the StorageClass it provisions. Both are deleted when the test ends.
func (h *Harness) serveVolumes(ac versioned.Interface) {
	h.t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		h.t.Fatalf("listening for the CSI driver: %v", err)
	}
	srv := h.Volumes.grpcServer()
	go func() { _ = srv.Serve(lis) }()
	h.t.Cleanup(srv.Stop)

	driver := h.Volumes.Driver()
	if _, err := ac.AteV1().CSIDriverConfigs().Create(h.ctx, &atev1.CSIDriverConfig{
		ObjectMeta: metav1.ObjectMeta{Name: driver},
		Spec: atev1.CSIDriverConfigSpec{
			DriverName:         driver,
			ControllerEndpoint: "tcp://" + lis.Addr().String(),
		},
	}, metav1.CreateOptions{}); err != nil {
		h.t.Fatalf("creating CSIDriverConfig %s: %v", driver, err)
	}
	h.t.Cleanup(func() {
		_ = ac.AteV1().CSIDriverConfigs().Delete(context.Background(), driver, metav1.DeleteOptions{})
	})
	storageClass := h.Volumes.StorageClass()
	if _, err := h.k8s.StorageV1().StorageClasses().Create(h.ctx, &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: storageClass},
		Provisioner: driver,
	}, metav1.CreateOptions{}); err != nil {
		h.t.Fatalf("creating StorageClass %s: %v", storageClass, err)
	}
	h.t.Cleanup(func() {
		_ = h.k8s.StorageV1().StorageClasses().Delete(context.Background(), storageClass, metav1.DeleteOptions{})
	})
}

// errNoAtelet is ate-api dialing an atelet the test stopped, before ate-api
// has seen its pod deleted.
var errNoAtelet = errors.New("harness: no atelet listening")

// dialAtelet connects ate-api to the fake atelet serving addr.
func (h *Harness) dialAtelet(ctx context.Context, addr string) (net.Conn, error) {
	h.mu.Lock()
	lis, ok := h.ateletListenersByAddr[addr]
	h.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("%w at %s", errNoAtelet, addr)
	}
	return lis.DialContext(ctx)
}

// next returns a number unique within the harness, for resource names.
func (h *Harness) next() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counter++
	return h.counter
}
