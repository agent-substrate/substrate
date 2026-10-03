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

// Package config resolves the environment ate-setup installs into. It layers
// the developer's .ate-dev-env.sh (sourced through bash, so existing setups
// keep working), the ambient process environment, and command line flags into
// a single typed Config.
package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
	"github.com/agent-substrate/substrate/internal/installdefaults"
)

// Enumerated values for the install-shaping flags.
const (
	RouterEnvoy        = "envoy"
	RouterAgentgateway = "agentgateway"

	SandboxClassGvisor  = "gvisor"
	SandboxClassMicrovm = "microvm"

	// Cluster size profiles. size0 is the shipped footprint; size10 assumes a
	// dedicated node for PostgreSQL and raises the store, its client pool, and
	// the podcertificate controller's API rate limits to match.
	ClusterSizeSize0  = "size0"
	ClusterSizeSize10 = "size10"
)

// DefaultRolloutTimeout is the default wait timeout for workload rollouts.
const DefaultRolloutTimeout = 60 * time.Second

// DefaultPostgresConnectionString reaches bundled PostgreSQL over mTLS using the
// podcertificate controller's projected servicedns trust bundle and its own
// podidentity credential bundle.
const DefaultPostgresConnectionString = "postgresql://postgres@postgres.ate-system.svc:5432/atepg?sslmode=verify-full&sslrootcert=/run/servicedns.podcert.ate.dev/trust-bundle.pem&sslcert=/run/podidentity.podcert.ate.dev/credential-bundle.pem&sslkey=/run/podidentity.podcert.ate.dev/credential-bundle.pem"

// Size10PostgresPoolParams is appended to the default connection string on
// size10 clusters. pgxpool defaults MaxConns to max(4, runtime.NumCPU()), which
// under-uses the size10 server's raised max_connections; pinning the pool makes
// the client side open the sockets the server is provisioned for.
const Size10PostgresPoolParams = "&pool_max_conns=64&pool_min_conns=4"

const (
	DefaultPostgresSchema        = "substrate"
	DefaultPostgresReadWriteRole = "substrate_readwrite"
	DefaultPostgresOwnerRole     = "substrate_owner"
)

// Store backends, the values ATE_API_STORE_BACKEND accepts.
const (
	StoreBackendPostgres = "postgres"
	StoreBackendMySQL    = "mysql"
)

// Cloud SQL Auth Proxy IP types, the values ATE_API_POSTGRES_CLOUDSQL_IP_TYPE
// accepts.
const (
	CloudSQLIPTypePrivate = "private"
	CloudSQLIPTypePublic  = "public"
	CloudSQLIPTypePSC     = "psc"
)

// devEnvFile is the optional per-developer environment script at the repo root.
const devEnvFile = ".ate-dev-env.sh"

// Config is the fully resolved installation environment. Fields sourced from
// the developer environment keep their shell names in the comments so the
// mapping back to .ate-dev-env.sh stays obvious.
type Config struct {
	// Root is the repository root. All manifest paths are relative to it.
	Root string

	// Kind selects the local Kind install profile (ATE_INSTALL_KIND).
	Kind bool

	// Namespace is the namespace the control plane is installed into, from
	// ATE_NAMESPACE. It defaults to the canonical installdefaults.SystemNamespace,
	// so an install that does not set it is unaffected. The checked-in manifests
	// under manifests/ate-install/ name that namespace literally, so the
	// manifest-applying steps refuse any other value; see Env.RequireCanonicalNamespace.
	Namespace string

	// Kubeconfig and Context select the target cluster. Empty Context means
	// "use the current context" (the KUBECTL_CONTEXT convention).
	//
	// Kubeconfig is a single file, handed to client-go as its explicit path. It
	// falls back to $KUBECONFIG rather than staying empty so that ate-setup and
	// the shell scripts it delegates to agree on the cluster — but only when
	// that variable names one file. See loadKubeconfig.
	Kubeconfig string
	Context    string

	// GKE cluster coordinates, used to fetch credentials and to derive the
	// service account JWT issuer.
	ProjectID       string
	ClusterName     string
	ClusterLocation string

	// ExpectedJWTIssuer is the service account token issuer ate-api-server
	// trusts (EXPECTED_JWT_ISSUER). It overrides both the GKE derivation from
	// the coordinates above and OpenID discovery, for clusters whose issuer
	// follows neither form.
	ExpectedJWTIssuer string

	// BucketName is the snapshot bucket demos are templated with.
	BucketName string

	// KODockerRepo is where ko pushes images (KO_DOCKER_REPO).
	KODockerRepo string
	// KODefaultPlatforms constrains ko's build platforms.
	KODefaultPlatforms string

	// Images selects where container images come from. Its zero value builds
	// them from source with ko, which is what a developer install does.
	Images images.Source

	// Router selects the atenet router dataplane.
	Router string
	// StoreBackend is the database ateapi keeps its state in
	// (ATE_API_STORE_BACKEND): StoreBackendPostgres or StoreBackendMySQL.
	StoreBackend string
	// StoreBackendSet records whether ATE_API_STORE_BACKEND was given rather
	// than defaulted, so that a redeploy that does not name a backend cannot
	// silently move a MySQL install onto the bundled PostgreSQL.
	StoreBackendSet bool
	// The read/write and owner connections can use different login identities.
	// With one configured connection, both pools use it. Both empty selects
	// bundled PostgreSQL.
	PostgresReadWriteConnectionString string
	PostgresOwnerConnectionString     string
	PostgresReadWriteRole             string
	PostgresOwnerRole                 string
	// These distinguish an explicit role override from the default when
	// adopting an existing Cloud SQL installation.
	PostgresReadWriteRoleSet bool
	PostgresOwnerRoleSet     bool
	// PostgresSchema is the PostgreSQL schema for the Substrate tables
	// (ATE_API_POSTGRES_SCHEMA). Empty means DefaultPostgresSchema.
	PostgresSchema string
	// PostgresPoolMaxConns sizes the apiserver's read/write pool
	// (ATE_API_POSTGRES_POOL_MAX_CONNS). Empty leaves the DSN or pgxpool default.
	PostgresPoolMaxConns string
	// PostgresServerCAFile is a local PEM file holding the server CA of an
	// external PostgreSQL (ATE_API_POSTGRES_SERVER_CA_FILE). Its contents are
	// published as the postgres-server-ca Secret, which ate-api-server mounts
	// at /run/postgres-server-ca/server-ca.pem for sslmode=verify-ca DSNs.
	PostgresServerCAFile string
	// CloudSQL points the apiserver at a Cloud SQL instance through the Auth
	// Proxy sidecar instead of a directly reachable PostgreSQL.
	CloudSQL CloudSQLConfig

	// MySQL settings, used only when StoreBackend is StoreBackendMySQL. The
	// database is always external and must already exist. The owner DSN
	// defaults to the read/write one.
	MySQLReadWriteConnectionString string
	MySQLOwnerConnectionString     string
	// MySQLPoolMaxConns sizes the apiserver's read/write pool
	// (ATE_API_MYSQL_POOL_MAX_CONNS). Empty leaves the driver default.
	MySQLPoolMaxConns string
	// MySQLServerCAFile is a local PEM file holding the MySQL server CA
	// (ATE_API_MYSQL_SERVER_CA_FILE). Its contents are published as the
	// mysql-server-ca Secret, which ate-api-server mounts at
	// /run/mysql-server-ca/server-ca.pem.
	MySQLServerCAFile string
	// MySQL TLS files already inside the ate-api-server pod, such as the pod
	// identity credential bundle (ATE_API_MYSQL_TLS_CA_FILE,
	// ATE_API_MYSQL_TLS_CERT_FILE, ATE_API_MYSQL_TLS_KEY_FILE). They play the
	// part of the sslrootcert, sslcert and sslkey paths a PostgreSQL DSN names,
	// which a MySQL DSN cannot carry.
	MySQLTLSCAFile   string
	MySQLTLSCertFile string
	MySQLTLSKeyFile  string

	// RolloutTimeout is the timeout duration for rollout status checks.
	RolloutTimeout time.Duration
	// rolloutTimeoutSet records whether RolloutTimeout was asked for rather
	// than defaulted. See WaitTimeout.
	rolloutTimeoutSet bool

	// PodcertWorkersPerSigner overrides WORKERS_PER_SIGNER on podcertificate-controller.
	PodcertWorkersPerSigner int

	// ClusterSize is the footprint profile (ATE_INSTALL_CLUSTER_SIZE): size0
	// or size10.
	ClusterSize string

	// CordonControlPlane keeps the control plane off the worker nodes
	// (ATE_INSTALL_CORDON_CONTROL_PLANE). It assumes a small shared pool
	// labeled and tainted ate.dev/workloadType=ate-control-plane:NoSchedule,
	// and a one-node pool labeled and tainted
	// ate.dev/workloadType=ate-postgres:NoSchedule for postgres alone.
	CordonControlPlane bool

	// AdditionalEgressExtprocService is the optional NS/SVC:PORT external processor filter.
	AdditionalEgressExtprocService string

	// CredentialProviderJSON is the --credential-provider value; see
	// CredentialProvider for its schema.
	CredentialProviderJSON string

	// AnthropicAPIKey is required only by the claude-code-multiplex demo.
	AnthropicAPIKey string

	// OtlpEndpoint is where the control plane ships telemetry
	// (ATE_OTLP_ENDPOINT). Benchmark actors are pointed at it too.
	OtlpEndpoint string
	// BenchmarkActorMemory is the memory limit for benchmark actors
	// (BENCHMARK_ACTOR_MEMORY). Empty leaves the workload default in place.
	BenchmarkActorMemory string

	// kubeconfigEnv is what ScriptEnv exports as $KUBECONFIG. Unlike Kubeconfig
	// it may be a PATH-style list of files, which kubectl understands and
	// client-go's explicit path does not.
	kubeconfigEnv string

	// shellEnv is the process environment layered over .ate-dev-env.sh. It is
	// kept so that the shell scripts ate-setup still shells out to see the same
	// variables the shell installer would have exported to them.
	shellEnv map[string]string
}

// CloudSQLConfig is the operator's Cloud SQL intent, as expressed by the
// ATE_API_POSTGRES_CLOUDSQL_* variables.
//
// Every field is empty-means-unspecified except Instance, which is three-way:
// a non-empty instance selects Cloud SQL, an explicitly empty one removes it,
// and an unset one (InstanceSet false) adopts whatever the target cluster
// already records. Without that distinction a redeploy from a shell that
// simply never exported the variable would tear the proxy sidecar out from
// under a working installation.
type CloudSQLConfig struct {
	// Instance is the instance connection name, PROJECT:REGION:INSTANCE.
	Instance string
	// InstanceSet records whether ATE_API_POSTGRES_CLOUDSQL_INSTANCE was
	// present in the environment at all, empty value included.
	InstanceSet bool

	// GSA is the Google service account the proxy authenticates as, and whose
	// email (minus the .gserviceaccount.com suffix) is the IAM database user.
	GSA string
	// IAMAuth enables automatic IAM database authentication ("true" or
	// "false"). Empty defaults to enabled.
	IAMAuth string
	// IPType selects which instance address the proxy dials: one of the
	// CloudSQLIPType constants. Empty defaults to private.
	IPType string
}

// Options carries the raw flag values the root command collects, before
// defaulting and validation.
type Options struct {
	Kind                           bool
	Kubeconfig                     string
	Context                        string
	Router                         string
	RolloutTimeout                 string
	PodcertWorkersPerSigner        int
	ClusterSize                    string
	CordonControlPlane             bool
	AdditionalEgressExtprocService string
	CredentialProvider             string
	OtlpEndpoint                   string

	// Image source selection.
	ImageRepo string
	ImageTag  string

	// NoDevEnv skips sourcing .ate-dev-env.sh even when it exists.
	NoDevEnv bool
}

// Load resolves the effective configuration. Precedence, lowest to highest:
// .ate-dev-env.sh, the process environment, then flags.
func Load(opts Options) (*Config, error) {
	root, err := RepoRoot()
	if err != nil {
		return nil, err
	}

	env := environ()

	// ATE_INSTALL_KIND is read as well as --kind: hack/install-ate-kind.sh
	// selects the Kind profile by exporting it.
	kind := opts.Kind || env["ATE_INSTALL_KIND"] == "true"

	// Sourcing is skipped for Kind installs the same way the shell kind installer
	// exports NO_DEV_ENV: the GKE-shaped variables in a developer's file would
	// otherwise point a local install at a cloud project.
	if !opts.NoDevEnv && !kind && os.Getenv("NO_DEV_ENV") == "" {
		path := filepath.Join(root, devEnvFile)
		if _, statErr := os.Stat(path); statErr == nil {
			sourced, srcErr := sourceShellEnv(path, root)
			if srcErr != nil {
				return nil, fmt.Errorf("while sourcing %s: %w", devEnvFile, srcErr)
			}
			// The process environment still wins: an explicitly exported
			// variable is a deliberate override of the file.
			for k, v := range sourced {
				if _, ok := env[k]; !ok {
					env[k] = v
				}
			}
		}
	}

	timeoutStr := firstNonEmpty(opts.RolloutTimeout, env["ATE_INSTALL_ROLLOUT_TIMEOUT"])
	rolloutTimeout := DefaultRolloutTimeout
	if timeoutStr != "" {
		d, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, fmt.Errorf("invalid --rollout-timeout %q (must be a duration like 60s, 5m): %w", timeoutStr, err)
		}
		// A non-positive timeout is never what the caller meant. It would make
		// every wait a single probe against a workload that has not had time to
		// start, failing the install with a rollout timeout on the first
		// Deployment. kubectl reads --timeout=0 as "wait forever"; ate-setup
		// does not offer an unbounded wait, so say so rather than silently
		// meaning the opposite.
		if d <= 0 {
			return nil, fmt.Errorf("invalid --rollout-timeout %q: must be positive (kubectl reads 0 as no timeout, which ate-setup does not support)", timeoutStr)
		}
		rolloutTimeout = d
	}

	podcertWorkers := opts.PodcertWorkersPerSigner
	if podcertWorkers == 0 && env["ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER"] != "" {
		val := env["ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER"]
		w, err := strconv.Atoi(val)
		if err != nil || w < 1 {
			return nil, fmt.Errorf("--podcert-workers-per-signer must be a positive integer, got %q", val)
		}
		podcertWorkers = w
	}

	extproc := firstNonEmpty(opts.AdditionalEgressExtprocService, env["ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE"])
	cordon := opts.CordonControlPlane || env["ATE_INSTALL_CORDON_CONTROL_PLANE"] == "true"

	// Read with the two-value form: an exported but empty
	// ATE_API_POSTGRES_CLOUDSQL_INSTANCE means "remove Cloud SQL", which an
	// absent one does not. See CloudSQLConfig.
	cloudsqlInstance, cloudsqlInstanceSet := env["ATE_API_POSTGRES_CLOUDSQL_INSTANCE"]

	kubeconfig, kubeconfigEnv := loadKubeconfig(opts.Kubeconfig, env["KUBECONFIG"])
	readWriteConnectionString := env["ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING"]
	ownerConnectionString := firstNonEmpty(env["ATE_API_POSTGRES_OWNER_CONNECTION_STRING"], readWriteConnectionString)
	mysqlReadWriteConnectionString := env["ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING"]

	cfg := &Config{
		Root:                              root,
		Kind:                              kind,
		Namespace:                         firstNonEmpty(env["ATE_NAMESPACE"], installdefaults.SystemNamespace),
		Kubeconfig:                        kubeconfig,
		Context:                           firstNonEmpty(opts.Context, env["KUBECTL_CONTEXT"]),
		ProjectID:                         env["PROJECT_ID"],
		ClusterName:                       env["CLUSTER_NAME"],
		ClusterLocation:                   env["CLUSTER_LOCATION"],
		ExpectedJWTIssuer:                 env["EXPECTED_JWT_ISSUER"],
		BucketName:                        env["BUCKET_NAME"],
		KODockerRepo:                      env["KO_DOCKER_REPO"],
		KODefaultPlatforms:                env["KO_DEFAULTPLATFORMS"],
		Images:                            loadImageSource(opts, env),
		StoreBackend:                      firstNonEmpty(env["ATE_API_STORE_BACKEND"], StoreBackendPostgres),
		StoreBackendSet:                   env["ATE_API_STORE_BACKEND"] != "",
		PostgresReadWriteConnectionString: readWriteConnectionString,
		PostgresOwnerConnectionString:     ownerConnectionString,
		PostgresReadWriteRole:             firstNonEmpty(env["ATE_API_POSTGRES_READ_WRITE_ROLE"], DefaultPostgresReadWriteRole),
		PostgresOwnerRole:                 firstNonEmpty(env["ATE_API_POSTGRES_OWNER_ROLE"], DefaultPostgresOwnerRole),
		PostgresReadWriteRoleSet:          env["ATE_API_POSTGRES_READ_WRITE_ROLE"] != "",
		PostgresOwnerRoleSet:              env["ATE_API_POSTGRES_OWNER_ROLE"] != "",
		PostgresSchema:                    env["ATE_API_POSTGRES_SCHEMA"],
		PostgresPoolMaxConns:              env["ATE_API_POSTGRES_POOL_MAX_CONNS"],
		PostgresServerCAFile:              env["ATE_API_POSTGRES_SERVER_CA_FILE"],
		CloudSQL: CloudSQLConfig{
			Instance:    cloudsqlInstance,
			InstanceSet: cloudsqlInstanceSet,
			GSA:         env["ATE_API_POSTGRES_CLOUDSQL_GSA"],
			IAMAuth:     env["ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH"],
			IPType:      env["ATE_API_POSTGRES_CLOUDSQL_IP_TYPE"],
		},
		MySQLReadWriteConnectionString: mysqlReadWriteConnectionString,
		MySQLOwnerConnectionString:     firstNonEmpty(env["ATE_API_MYSQL_OWNER_CONNECTION_STRING"], mysqlReadWriteConnectionString),
		MySQLPoolMaxConns:              env["ATE_API_MYSQL_POOL_MAX_CONNS"],
		MySQLServerCAFile:              env["ATE_API_MYSQL_SERVER_CA_FILE"],
		MySQLTLSCAFile:                 env["ATE_API_MYSQL_TLS_CA_FILE"],
		MySQLTLSCertFile:               env["ATE_API_MYSQL_TLS_CERT_FILE"],
		MySQLTLSKeyFile:                env["ATE_API_MYSQL_TLS_KEY_FILE"],
		RolloutTimeout:                 rolloutTimeout,
		rolloutTimeoutSet:              timeoutStr != "",
		PodcertWorkersPerSigner:        podcertWorkers,
		ClusterSize:                    firstNonEmpty(opts.ClusterSize, env["ATE_INSTALL_CLUSTER_SIZE"], ClusterSizeSize0),
		CordonControlPlane:             cordon,
		AdditionalEgressExtprocService: extproc,
		CredentialProviderJSON:         firstNonEmpty(opts.CredentialProvider, env["ATE_CREDENTIAL_PROVIDER"]),
		AnthropicAPIKey:                env["ANTHROPIC_API_KEY"],
		OtlpEndpoint:                   firstNonEmpty(opts.OtlpEndpoint, env["ATE_OTLP_ENDPOINT"]),
		BenchmarkActorMemory:           env["BENCHMARK_ACTOR_MEMORY"],
		kubeconfigEnv:                  kubeconfigEnv,
		shellEnv:                       env,
	}

	if kind {
		applyKindDefaults(cfg)
	}

	cfg.Router = firstNonEmpty(opts.Router, env["ATE_ATENET_DATAPLANE"], RouterEnvoy)

	if err := validate(cfg); err != nil {
		return nil, err
	}
	if err := validateStoreBackend(cfg, env); err != nil {
		return nil, err
	}
	if cfg.StoreBackend == StoreBackendMySQL {
		// An explicitly empty instance rather than an unset one: a MySQL
		// install never adopts the cluster's recorded Cloud SQL instance, so
		// a proxy sidecar left by an earlier PostgreSQL install is removed.
		cfg.CloudSQL = CloudSQLConfig{InstanceSet: true}
	}
	return cfg, nil
}

// validateStoreBackend checks the backend choice and refuses settings for the
// other backend, which would otherwise be ignored without a word. The prefix
// check covers the Cloud SQL variables too: on MySQL they would keep a proxy
// sidecar for a database the apiserver no longer uses. An exported but empty
// ATE_API_POSTGRES_CLOUDSQL_INSTANCE is allowed, since it asks for removal.
func validateStoreBackend(cfg *Config, env map[string]string) error {
	var conflictPrefix string
	switch cfg.StoreBackend {
	case StoreBackendPostgres:
		conflictPrefix = "ATE_API_MYSQL_"
	case StoreBackendMySQL:
		conflictPrefix = "ATE_API_POSTGRES_"
		if cfg.MySQLReadWriteConnectionString == "" {
			return fmt.Errorf("ATE_API_STORE_BACKEND=%s requires ATE_API_MYSQL_READ_WRITE_CONNECTION_STRING; "+
				"there is no bundled MySQL", StoreBackendMySQL)
		}
		if v := cfg.MySQLPoolMaxConns; v != "" {
			if n, err := strconv.ParseInt(v, 10, 32); err != nil || n <= 0 {
				return fmt.Errorf("ATE_API_MYSQL_POOL_MAX_CONNS must be a positive integer, got %q", v)
			}
		}
		if cfg.MySQLServerCAFile != "" && cfg.MySQLTLSCAFile != "" {
			return fmt.Errorf("set ATE_API_MYSQL_SERVER_CA_FILE (a local file to upload) or ATE_API_MYSQL_TLS_CA_FILE (a path in the pod), not both")
		}
		if (cfg.MySQLTLSCertFile == "") != (cfg.MySQLTLSKeyFile == "") {
			return fmt.Errorf("ATE_API_MYSQL_TLS_CERT_FILE and ATE_API_MYSQL_TLS_KEY_FILE must be set together")
		}
	default:
		return fmt.Errorf("ATE_API_STORE_BACKEND must be %s or %s, got %q",
			StoreBackendPostgres, StoreBackendMySQL, cfg.StoreBackend)
	}
	var conflicts []string
	for name, value := range env {
		if strings.HasPrefix(name, conflictPrefix) && value != "" {
			conflicts = append(conflicts, name)
		}
	}
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return fmt.Errorf("ATE_API_STORE_BACKEND=%s conflicts with %s; unset them or select the other backend",
			cfg.StoreBackend, strings.Join(conflicts, ", "))
	}
	return nil
}

// loadKubeconfig splits the kubeconfig setting into the path handed to
// client-go and the value exported to the shell scripts.
//
// $KUBECONFIG is a PATH-style list, and developers who juggle clusters do set
// it to several files. client-go's explicit path is a single file, so passing
// such a value through makes every command fail with
// `stat /a.yaml:/b.yaml: no such file or directory`. The default loading rules
// already read $KUBECONFIG and merge its entries, so a list is left to them:
// the explicit path stays empty and the scripts still see the list.
func loadKubeconfig(flag, env string) (explicitPath, scriptValue string) {
	if flag != "" {
		return flag, flag
	}
	if strings.ContainsRune(env, os.PathListSeparator) {
		return "", env
	}
	return env, env
}

// loadImageSource resolves where images come from.
func loadImageSource(opts Options, env map[string]string) images.Source {
	return images.Source{
		Repo: strings.TrimSuffix(firstNonEmpty(opts.ImageRepo, env["ATE_IMAGE_REPO"]), "/"),
		Tag:  firstNonEmpty(opts.ImageTag, env["ATE_IMAGE_TAG"]),
	}
}

func applyKindDefaults(cfg *Config) {
	cfg.ProjectID = ""
	cfg.ClusterLocation = ""
	kindClusterName := firstNonEmpty(cfg.shellEnv["KIND_CLUSTER_NAME"], "kind")
	cfg.Context = firstNonEmpty(cfg.Context, "kind-"+kindClusterName)
	cfg.KODockerRepo = firstNonEmpty(cfg.KODockerRepo, "localhost:5001")
	cfg.KODefaultPlatforms = "linux/" + runtime.GOARCH
	cfg.BucketName = "ate-snapshots"
}

func validate(cfg *Config) error {
	if err := cfg.Images.Validate(); err != nil {
		return err
	}
	switch cfg.Router {
	case RouterEnvoy, RouterAgentgateway:
	default:
		return fmt.Errorf("atenet router must be %s or %s, got %q", RouterEnvoy, RouterAgentgateway, cfg.Router)
	}
	if cfg.PodcertWorkersPerSigner < 0 {
		return fmt.Errorf("--podcert-workers-per-signer must be a positive integer, got %d", cfg.PodcertWorkersPerSigner)
	}
	// Only an explicitly supplied value is checked. One adopted from the
	// cluster is derived from the recorded CSQL_PROXY_* keys and so is always
	// one of these by construction.
	switch cfg.CloudSQL.IPType {
	case "", CloudSQLIPTypePrivate, CloudSQLIPTypePublic, CloudSQLIPTypePSC:
	default:
		return fmt.Errorf("ATE_API_POSTGRES_CLOUDSQL_IP_TYPE must be %s, %s, or %s, got %q",
			CloudSQLIPTypePrivate, CloudSQLIPTypePublic, CloudSQLIPTypePSC, cfg.CloudSQL.IPType)
	}
	switch cfg.ClusterSize {
	case ClusterSizeSize0, ClusterSizeSize10:
	default:
		return fmt.Errorf("--cluster-size must be %s or %s, got %q", ClusterSizeSize0, ClusterSizeSize10, cfg.ClusterSize)
	}
	if cfg.AdditionalEgressExtprocService != "" {
		if err := validateExtprocService(cfg.AdditionalEgressExtprocService); err != nil {
			return err
		}
		if cfg.Router != RouterEnvoy {
			return fmt.Errorf("--experimental-additional-egress-extproc-service requires --atenet-dataplane=envoy")
		}
	}
	if cfg.CredentialProviderJSON != "" {
		if _, err := cfg.CredentialProvider(); err != nil {
			return err
		}
	}
	return nil
}

// The bundled Kubernetes Secrets provider, which the installer deploys with a
// NetworkPolicy that admits only the egress gateway. Its default address is
// the Service in
// manifests/egress-credential-injection/k8s-credential-provider.yaml.
const (
	K8sCredentialProviderName    = "k8s.io"
	K8sCredentialProviderAddress = "k8s-credential-provider." + installdefaults.SystemNamespace + ".svc:50051"
)

// credentialProviderUsage lists the accepted --credential-provider values.
const credentialProviderUsage = `{"name":"k8s.io"} for the bundled Kubernetes Secrets provider, ` +
	`{"enabled":false} to turn egress credential injection off, ` +
	`or {"name":"<provider>","address":"<host>:<port>"} for a provider you deploy`

// credentialProviderSpec is the schema of the --credential-provider JSON.
type credentialProviderSpec struct {
	// Enabled defaults to true when absent.
	Enabled *bool  `json:"enabled"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

// CredentialProvider is the credential provider the egress gateway is pointed
// at. The zero value means injection is off.
type CredentialProvider struct {
	// Name is the host of the ate-secret:// credential URIs the provider
	// serves, e.g. k8s.io.
	Name string
	// Address is the host:port the gateway dials the provider at.
	Address string
}

// Enabled reports whether the gateway is given a provider.
func (p CredentialProvider) Enabled() bool { return p.Name != "" }

// Kubernetes reports whether the provider is the bundled one.
func (p CredentialProvider) Kubernetes() bool { return p.Name == K8sCredentialProviderName }

// ServerName is the SAN the gateway expects on the provider's serving
// certificate: the address without its port.
func (p CredentialProvider) ServerName() string {
	if i := strings.LastIndex(p.Address, ":"); i >= 0 {
		return p.Address[:i]
	}
	return p.Address
}

// CredentialProvider parses --credential-provider, a JSON object that either
// turns injection off with enabled false or names a provider and its address.
// Only the bundled provider has a default address. A missing value is reported here rather than in validate
// because only the deploys that render the egress gateway need it.
func (c *Config) CredentialProvider() (CredentialProvider, error) {
	raw := c.CredentialProviderJSON
	if raw == "" {
		return CredentialProvider{}, fmt.Errorf("--credential-provider is required (or ATE_CREDENTIAL_PROVIDER): %s", credentialProviderUsage)
	}
	invalid := func(format string, args ...any) (CredentialProvider, error) {
		return CredentialProvider{}, fmt.Errorf("invalid --credential-provider '%s': %s", raw, fmt.Sprintf(format, args...))
	}

	var spec credentialProviderSpec
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return invalid("%v; want %s", err, credentialProviderUsage)
	}
	if _, err := dec.Token(); err != io.EOF {
		return invalid("want a single JSON object")
	}

	if spec.Enabled != nil && !*spec.Enabled {
		if spec.Name != "" || spec.Address != "" {
			return invalid("enabled false takes no name or address")
		}
		return CredentialProvider{}, nil
	}
	if spec.Name == "" {
		return invalid("name is required; want %s", credentialProviderUsage)
	}
	// The name is the host of the credential URIs the provider serves.
	if errs := validation.IsDNS1123Subdomain(spec.Name); len(errs) > 0 {
		return invalid("name %q is not a valid DNS name: %s", spec.Name, strings.Join(errs, "; "))
	}
	if c.Router != RouterEnvoy {
		return invalid(`a credential provider requires --atenet-dataplane=envoy; pass {"enabled":false} on the agentgateway dataplane`)
	}
	if spec.Address == "" {
		if spec.Name != K8sCredentialProviderName {
			return invalid("a provider you deploy needs an address as host:port")
		}
		spec.Address = K8sCredentialProviderAddress
	} else if host, port, err := net.SplitHostPort(spec.Address); err != nil || host == "" || port == "" {
		return invalid("address must be host:port, got %q", spec.Address)
	}
	return CredentialProvider{Name: spec.Name, Address: spec.Address}, nil
}

func validateExtprocService(spec string) error {
	parts := strings.Split(spec, "/")
	if len(parts) != 2 {
		return fmt.Errorf("--experimental-additional-egress-extproc-service must be <namespace>/<service>:<port>, got %q", spec)
	}
	namespace := parts[0]
	svcPort := strings.Split(parts[1], ":")
	if len(svcPort) != 2 {
		return fmt.Errorf("--experimental-additional-egress-extproc-service must be <namespace>/<service>:<port>, got %q", spec)
	}
	service := svcPort[0]
	portStr := svcPort[1]
	if namespace == "" || service == "" {
		return fmt.Errorf("--experimental-additional-egress-extproc-service must be <namespace>/<service>:<port>, got %q", spec)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("--experimental-additional-egress-extproc-service port must be 1-65535, got %q", portStr)
	}
	return nil
}

// Size10 reports whether the size10 footprint profile is selected.
func (c *Config) Size10() bool {
	return c.ClusterSize == ClusterSizeSize10
}

// PostgresSchemaName returns the configured schema, falling back to the
// shell installer's default. ate-api-server rejects an empty value.
func (c *Config) PostgresSchemaName() string {
	if c.PostgresSchema != "" {
		return c.PostgresSchema
	}
	return DefaultPostgresSchema
}

// StoreBackendName returns the configured store backend, falling back to
// PostgreSQL as ateapi does.
func (c *Config) StoreBackendName() string {
	if c.StoreBackend != "" {
		return c.StoreBackend
	}
	return StoreBackendPostgres
}

// MySQL reports whether ateapi keeps its state in MySQL.
func (c *Config) MySQL() bool {
	return c.StoreBackendName() == StoreBackendMySQL
}

// WaitTimeout returns how long to wait for a workload whose historical timeout
// was historical.
//
// The shell installer applied --rollout-timeout to the ate-system rollouts
// only; the podcertificate-controller wait and the CSI driver waits were fixed
// at 120s because they are the slow bootstrap paths — an image pull on a cold
// cluster, in the CSI case a driver being torn down and reinstalled. Honoring
// the flag everywhere would let its 60s default halve exactly those waits.
//
// So the flag now reaches every wait, but only when someone asked for it. Left
// alone, each site keeps the timeout the scripts gave it.
func (c *Config) WaitTimeout(historical time.Duration) time.Duration {
	if c.rolloutTimeoutSet {
		return c.RolloutTimeout
	}
	return historical
}

// Manifest resolves a path under manifests/ate-install.
func (c *Config) Manifest(parts ...string) string {
	return filepath.Join(append([]string{c.Root, "manifests", "ate-install"}, parts...)...)
}

// Path resolves a repo-relative path.
func (c *Config) Path(parts ...string) string {
	return filepath.Join(append([]string{c.Root}, parts...)...)
}

// kindUnsetVars are the GKE-specific variables the shell kind installer unset
// before delegating, so that a developer who had already sourced
// .ate-dev-env.sh into their shell did not end up pointing a local install at a
// cloud project.
var kindUnsetVars = []string{
	"GCE_REGION", "CLUSTER_LOCATION", "NETWORK", "SUBNETWORK",
	"MEMORYSTORE_INSTANCE", "PROJECT_ID",
}

// ScriptEnv returns the environment for the shell scripts ate-setup still
// delegates to (the benchmark and micro-VM helpers).
//
// Those scripts read the same variables the shell installer exported to them,
// so this reproduces that environment: .ate-dev-env.sh under the process
// environment, with the resolved configuration layered on top so that flags
// such as --context and --kind reach them.
func (c *Config) ScriptEnv() []string {
	merged := make(map[string]string, len(c.shellEnv)+len(kindUnsetVars)+12)
	for k, v := range c.shellEnv {
		merged[k] = v
	}

	if c.Kind {
		for _, name := range kindUnsetVars {
			delete(merged, name)
		}
		merged["ATE_INSTALL_KIND"] = "true"
		merged["NO_DEV_ENV"] = "true"
	}

	// The resolved values win: they already account for flags, the dev env,
	// and the kind profile.
	for name, value := range map[string]string{
		"KUBECTL_CONTEXT":     c.Context,
		"KUBECONFIG":          c.kubeconfigEnv,
		"BUCKET_NAME":         c.BucketName,
		"KO_DOCKER_REPO":      c.KODockerRepo,
		"KO_DEFAULTPLATFORMS": c.KODefaultPlatforms,
		"PROJECT_ID":          c.ProjectID,
		"CLUSTER_NAME":        c.ClusterName,
		"CLUSTER_LOCATION":    c.ClusterLocation,
		"ATE_OTLP_ENDPOINT":   c.OtlpEndpoint,
	} {
		if value == "" {
			// An empty value means "not configured". Leaving the variable set
			// but empty would defeat the ${VAR:-default} fallbacks the scripts
			// rely on.
			delete(merged, name)
			continue
		}
		merged[name] = value
	}

	if c.RolloutTimeout > 0 {
		merged["ATE_INSTALL_ROLLOUT_TIMEOUT"] = c.RolloutTimeout.String()
	}
	if c.PodcertWorkersPerSigner > 0 {
		merged["ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER"] = strconv.Itoa(c.PodcertWorkersPerSigner)
	}
	// Resolved values again, so a flag overrides whatever the environment
	// carried rather than layering under it.
	delete(merged, "ATE_INSTALL_CLUSTER_SIZE")
	if c.ClusterSize != "" && c.ClusterSize != ClusterSizeSize0 {
		merged["ATE_INSTALL_CLUSTER_SIZE"] = c.ClusterSize
	}
	delete(merged, "ATE_INSTALL_CORDON_CONTROL_PLANE")
	if c.CordonControlPlane {
		merged["ATE_INSTALL_CORDON_CONTROL_PLANE"] = "true"
	}
	if c.AdditionalEgressExtprocService != "" {
		merged["ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE"] = c.AdditionalEgressExtprocService
	}
	if c.CredentialProviderJSON != "" {
		merged["ATE_CREDENTIAL_PROVIDER"] = c.CredentialProviderJSON
	}

	env := make([]string, 0, len(merged))
	for k, v := range merged {
		env = append(env, k+"="+v)
	}
	sort.Strings(env)
	return env
}

// KoEnv returns the environment overrides ko needs for this configuration.
func (c *Config) KoEnv() []string {
	var env []string
	if c.KODockerRepo != "" {
		env = append(env, "KO_DOCKER_REPO="+c.KODockerRepo)
	}
	if c.KODefaultPlatforms != "" {
		env = append(env, "KO_DEFAULTPLATFORMS="+c.KODefaultPlatforms)
	}
	return env
}

func environ() map[string]string {
	env := make(map[string]string)
	for _, kv := range os.Environ() {
		if name, value, ok := strings.Cut(kv, "="); ok {
			env[name] = value
		}
	}
	return env
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
