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

package config

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/internal/installdefaults"
)

// ValueKind is the type of a setting's value.
type ValueKind int

const (
	KindString ValueKind = iota
	KindBool
	KindInt
	KindDuration
)

// Setting declares one configurable value. It is the single definition: the
// flag set, the environment reader and the file schema are all projections of
// Registry, so a setting cannot reach one channel and miss another.
type Setting struct {
	// Key is the canonical name and the path in the configuration file,
	// dot-separated: "ateapi.postgres.connectionString".
	Key string
	// Env is the environment variable that supplies this setting.
	Env string
	// Flag is the command-line name, without the leading dashes.
	Flag string
	// Kind is the value type. Resolve parses raw strings against it.
	Kind ValueKind
	// Default applies when no channel supplies a value.
	Default string
	// Secret suppresses the value wherever configuration is displayed.
	Secret bool
	// Usage is the flag help text.
	Usage string
	// Commands restricts the flag to those command paths, named as they are
	// typed: "deploy benchmarks". Empty means a persistent flag on the root.
	//
	// Only the flag channel is scoped. The environment and the file reach
	// every setting, because a configuration document describes a whole
	// install and must parse the same way whichever subcommand reads it.
	Commands []string
}

// Registry is the settings the installer itself owns. Each entry is the one
// place a setting is declared: the flag, the environment variable and the
// file key are all projections of it.
//
// Settings that belong to one command are added by RegisterCommand instead;
// All returns both.
var Registry = []Setting{
	{
		// Key is kindCluster.enabled, not kind: a top-level "kind" would collide
		// with the document's own kind header field.
		Key: "kindCluster.enabled", Env: "ATE_INSTALL_KIND", Flag: "kind", Kind: KindBool, Default: "false",
		Usage: "Target a local Kind cluster: kind overlays, the local registry, and host-architecture builds",
	},
	{
		Key: "kindCluster.name", Env: "KIND_CLUSTER_NAME", Flag: "kind-cluster-name", Kind: KindString, Default: "kind",
		Usage: "Name of the Kind cluster to target",
	},
	{
		Key: "namespace", Env: "ATE_NAMESPACE", Flag: "namespace", Kind: KindString,
		Default: installdefaults.SystemNamespace,
		Usage:   "Namespace the control plane is installed into",
	},
	{
		Key: "kubeconfig", Env: "KUBECONFIG", Flag: "kubeconfig", Kind: KindString,
		Usage: "Path to the kubeconfig file",
	},
	{
		Key: "context", Env: "KUBECTL_CONTEXT", Flag: "context", Kind: KindString,
		Usage: "Name of the kubeconfig context to use",
	},

	{
		Key: "gcp.projectID", Env: "PROJECT_ID", Flag: "gcp-project-id", Kind: KindString,
		Usage: "GCP project holding the target cluster",
	},
	{
		Key: "gcp.clusterName", Env: "CLUSTER_NAME", Flag: "gcp-cluster-name", Kind: KindString,
		Usage: "GKE cluster name, used to fetch credentials and derive the JWT issuer",
	},
	{
		Key: "gcp.clusterLocation", Env: "CLUSTER_LOCATION", Flag: "gcp-cluster-location", Kind: KindString,
		Usage: "GKE cluster location",
	},

	{
		Key: "ko.dockerRepo", Env: "KO_DOCKER_REPO", Flag: "ko-docker-repo", Kind: KindString,
		Usage: "Registry ko pushes built images to",
	},
	{
		Key: "ko.defaultPlatforms", Env: "KO_DEFAULTPLATFORMS", Flag: "ko-default-platforms", Kind: KindString,
		Usage: "Platforms ko builds for",
	},
	{
		Key: "actorJWT.algorithm", Env: "ACTOR_JWT_ALGORITHM", Flag: "actor-jwt-algorithm",
		Kind: KindString, Default: "ES256",
		Usage: "Signing algorithm of the key in a new actor JWT pool: ES256 or RS256",
	},
	{
		// Whitespace-separated, as the environment variable is. The registry
		// has no list kind, so the value is carried as written and split
		// where it is used.
		Key: "docker.buildFlags", Env: "DOCKER_BUILD_FLAGS", Flag: "docker-build-flags",
		Kind:  KindString,
		Usage: "Extra docker buildx build flags for the images ko cannot build, whitespace-separated",
	},
	{
		Key: "images.repo", Env: "ATE_IMAGE_REPO", Flag: "image-repo", Kind: KindString,
		Usage: "Install pre-built images from this registry path instead of building from source",
	},
	{
		Key: "images.tag", Env: "ATE_IMAGE_TAG", Flag: "image-tag", Kind: KindString,
		Usage: "Tag the pre-built images carry (required with --image-repo)",
	},

	{
		Key: "storage.bucketName", Env: "BUCKET_NAME", Flag: "storage-bucket-name", Kind: KindString,
		Usage: "Snapshot bucket the demos are templated with",
	},

	{
		Key: "atenet.dataplane", Env: "ATE_ATENET_DATAPLANE", Flag: "atenet-dataplane", Kind: KindString,
		Default: RouterEnvoy,
		Usage:   "Atenet ingress and egress dataplane: envoy or agentgateway",
	},
	{
		Key: "atenet.egress.additionalExtprocService", Env: "ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE",
		Flag: "experimental-additional-egress-extproc-service", Kind: KindString,
		Usage: "Run an additional ext_proc authorization filter served by NS/SVC:PORT " +
			"(requires --atenet-dataplane=envoy)",
	},

	{
		Key: "ateapi.expectedJWTIssuer", Env: "EXPECTED_JWT_ISSUER", Flag: "ateapi-expected-jwt-issuer",
		Kind:  KindString,
		Usage: "Service account token issuer ate-api-server trusts, overriding derivation and discovery",
	},
	{
		// The egress gateway's credential provider, as a JSON object. Upstream
		// takes it as one value on one flag; it is carried the same way here
		// so the two channels accept the same text.
		Key: "atenet.egress.credentialProvider", Env: "ATE_CREDENTIAL_PROVIDER",
		Flag: "credential-provider", Kind: KindString,
		Usage: "Credential provider the egress gateway injects credentials from, as JSON; " +
			"required by deploy ate-system and deploy atenet. " + credentialProviderUsage +
			". A provider requires --atenet-dataplane=envoy",
	},
	{
		// Both pools use the read-write connection when only it is set; both
		// empty selects the bundled PostgreSQL.
		Key: "ateapi.postgres.readWrite.connectionString", Env: "ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING",
		Kind: KindString, Secret: true,
		Usage: "PostgreSQL connection string ate-api-server uses for normal application access",
	},
	{
		Key: "ateapi.postgres.owner.connectionString", Env: "ATE_API_POSTGRES_OWNER_CONNECTION_STRING",
		Kind: KindString, Secret: true,
		Usage: "PostgreSQL connection string ate-api-server uses for schema changes, " +
			"defaulting to the read-write one",
	},
	{
		Key: "ateapi.postgres.readWrite.role", Env: "ATE_API_POSTGRES_READ_WRITE_ROLE",
		Flag: "ateapi-postgres-read-write-role", Kind: KindString,
		Default: DefaultPostgresReadWriteRole,
		Usage:   "PostgreSQL role owning normal application access",
	},
	{
		Key: "ateapi.postgres.owner.role", Env: "ATE_API_POSTGRES_OWNER_ROLE",
		Flag: "ateapi-postgres-owner-role", Kind: KindString,
		Default: DefaultPostgresOwnerRole,
		Usage:   "PostgreSQL role owning the schema",
	},
	{
		Key: "ateapi.postgres.schema", Env: "ATE_API_POSTGRES_SCHEMA", Flag: "ateapi-postgres-schema",
		Kind:  KindString,
		Usage: "PostgreSQL schema holding the Substrate tables",
	},
	{
		Key: "ateapi.postgres.poolMaxConns", Env: "ATE_API_POSTGRES_POOL_MAX_CONNS",
		Flag: "ateapi-postgres-pool-max-conns", Kind: KindString,
		Usage: "Maximum connections in the apiserver's pgxpool",
	},
	{
		Key: "ateapi.postgres.serverCAFile", Env: "ATE_API_POSTGRES_SERVER_CA_FILE",
		Flag: "ateapi-postgres-server-ca-file", Kind: KindString,
		Usage: "Local PEM file holding an external PostgreSQL server CA",
	},
	{
		Key: "ateapi.postgres.cloudsql.instance", Env: "ATE_API_POSTGRES_CLOUDSQL_INSTANCE",
		Flag: "ateapi-postgres-cloudsql-instance", Kind: KindString,
		Usage: "Cloud SQL instance connection name, PROJECT:REGION:INSTANCE",
	},
	{
		Key: "ateapi.postgres.cloudsql.gsa", Env: "ATE_API_POSTGRES_CLOUDSQL_GSA",
		Flag: "ateapi-postgres-cloudsql-gsa", Kind: KindString,
		Usage: "Google service account the Cloud SQL Auth Proxy authenticates as",
	},
	{
		Key: "ateapi.postgres.cloudsql.iamAuth", Env: "ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH",
		Flag: "ateapi-postgres-cloudsql-iam-auth", Kind: KindString,
		Usage: "Enable automatic IAM database authentication: true or false",
	},
	{
		Key: "ateapi.postgres.cloudsql.ipType", Env: "ATE_API_POSTGRES_CLOUDSQL_IP_TYPE",
		Flag: "ateapi-postgres-cloudsql-ip-type", Kind: KindString,
		Usage: "Instance address the Cloud SQL proxy dials: private, public or psc",
	},

	{
		Key: "podcert.workersPerSigner", Env: "ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER",
		Flag: "podcert-workers-per-signer", Kind: KindInt, Default: "0",
		Usage: "Worker goroutines per signer in podcertificate-controller",
	},

	{
		Key: "rolloutTimeout", Env: "ATE_INSTALL_ROLLOUT_TIMEOUT", Flag: "rollout-timeout",
		Kind: KindDuration, Default: DefaultRolloutTimeout.String(),
		Usage: "Timeout for workload rollouts, as a duration string",
	},
	{
		Key: "clusterSize", Env: "ATE_INSTALL_CLUSTER_SIZE", Flag: "cluster-size", Kind: KindString,
		Default: ClusterSizeSize0,
		Usage:   "Coarse-grained sizing of Substrate: size0 or size10",
	},
	{
		Key: "bundledPostgres.storageClass", Env: "ATE_INSTALL_POSTGRES_STORAGE_CLASS", Flag: "postgres-storage-class",
		Kind: KindString,
		Usage: "StorageClass for the bundled PostgreSQL volume, instead of the cluster default. " +
			"An existing install must delete the postgres StatefulSet and its claim first",
	},
	{
		Key: "cordonControlPlane", Env: "ATE_INSTALL_CORDON_CONTROL_PLANE", Flag: "cordon-control-plane",
		Kind: KindBool, Default: "false",
		Usage: "Keep the control plane off the worker nodes. Assumes a small shared pool labeled " +
			"and tainted ate.dev/workloadType=ate-control-plane:NoSchedule, and a one-node pool " +
			"labeled and tainted ate.dev/workloadType=ate-postgres:NoSchedule for postgres alone",
	},
	{
		Key: "otlpEndpoint", Env: "ATE_OTLP_ENDPOINT", Flag: "otlp-endpoint", Kind: KindString,
		Usage: "Send control plane telemetry to this OTLP collector instead of the cluster default",
	},

	{
		Key: "benchmark.actorMemory", Env: "BENCHMARK_ACTOR_MEMORY", Flag: "benchmark-actor-memory",
		Kind:  KindString,
		Usage: "Memory limit for benchmark actors",
	},
	{
		Key: "record.dir", Env: "ATE_RECORD_DIR", Flag: "record-dir", Kind: KindString,
		Usage: "Directory install records are written to (default: the user cache directory)",
	},
	{
		Key: "demo.anthropicAPIKey", Env: "ANTHROPIC_API_KEY",
		Kind: KindString, Secret: true,
		Usage: "API key required by the claude-code-multiplex demo",
	},
}

// kindEnabledKey is resolved before the rest, because whether to source the
// developer environment depends on it.
const kindEnabledKey = "kindCluster.enabled"

// scoped holds the settings commands registered for themselves, in
// registration order.
var scoped []Setting

// byKey indexes Registry and everything RegisterCommand has added.
var byKey = func() map[string]Setting {
	m := make(map[string]Setting, len(Registry))
	for _, s := range Registry {
		m[s.Key] = s
	}
	return m
}()

// RegisterCommand records settings owned by one command, to be called from the
// owning package's init. A command declares its own configuration rather than
// a central list growing a line for every command, which is how demos.Register
// already works.
//
// It does not validate. A panic here would fire during package initialization,
// before any test could observe it, and would abort the test binary instead of
// failing a test. Validate reports the same problems and is callable.
func RegisterCommand(settings ...Setting) {
	for _, s := range settings {
		scoped = append(scoped, s)
		byKey[s.Key] = s
	}
}

// All returns every setting, installer-owned and command-registered, in
// declaration order. The file and the environment read this, so a
// configuration document parses identically whichever subcommand runs.
func All() []Setting {
	out := make([]Setting, 0, len(Registry)+len(scoped))
	out = append(out, Registry...)
	return append(out, scoped...)
}

// Validate reports duplicate names and missing channels across the merged set.
//
// It cannot run inside package config's own tests: the settings it is meant to
// catch are registered by the command packages, which config does not import.
// The caller is the test in package main, which imports the whole command tree,
// and Execute, so a bad registration fails at startup.
func Validate() error {
	var problems []string
	for _, field := range []struct {
		name string
		get  func(Setting) string
		// omittable reports whether this setting is allowed to leave the
		// channel undeclared.
		omittable func(Setting) bool
	}{
		{name: "key", get: func(s Setting) string { return s.Key }},
		{name: "environment variable", get: func(s Setting) string { return s.Env }},
		{
			name: "flag", get: func(s Setting) string { return s.Flag },
			omittable: func(s Setting) bool { return s.Secret },
		},
	} {
		seen := map[string]string{}
		for _, s := range All() {
			v := field.get(s)
			if v == "" {
				if field.omittable != nil && field.omittable(s) {
					continue
				}
				problems = append(problems,
					fmt.Sprintf("setting %q has no %s", s.Key, field.name))
				continue
			}
			if prev, dup := seen[v]; dup {
				problems = append(problems,
					fmt.Sprintf("%s %q is claimed by both %s and %s", field.name, v, prev, s.Key))
			}
			seen[v] = s.Key
		}
	}
	// A command line is visible in `ps` to every user on the host and is
	// written to shell history, so a credential must not be settable there.
	// Secrets are supplied through the environment or a file an operator can
	// permission.
	for _, s := range All() {
		if s.Secret && s.Flag != "" {
			problems = append(problems,
				fmt.Sprintf("secret setting %q declares flag %q; a credential must not be a flag",
					s.Key, s.Flag))
		}
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid setting registry:\n  %s", strings.Join(problems, "\n  "))
	}
	return nil
}

// Channels names every way the setting can be supplied.
//
// Errors raised where no resolved value is at hand use it, because the reader
// may know only one of the three: telling someone who configures by file to
// set an environment variable sends them to the wrong place. A setting with
// no flag -- a secret -- names the two it has.
func (s Setting) Channels() string {
	if s.Flag == "" {
		return fmt.Sprintf("%s or config.%s", s.Env, s.Key)
	}
	return fmt.Sprintf("%s, config.%s or --%s", s.Env, s.Key, s.Flag)
}

// channelsFor names the channels of a registered key, for a caller holding
// the key rather than the setting. An unregistered key yields "", which only
// happens if a caller names a key the registry does not declare.
func channelsFor(key string) string {
	s, ok := Lookup(key)
	if !ok {
		return ""
	}
	return s.Channels()
}

// Lookup returns the setting with the given canonical key.
func Lookup(key string) (Setting, bool) {
	s, ok := byKey[key]
	return s, ok
}

// Keys returns every canonical key, sorted.
func Keys() []string {
	all := All()
	out := make([]string, 0, len(all))
	for _, s := range all {
		out = append(out, s.Key)
	}
	sort.Strings(out)
	return out
}

// BindFlags registers the settings that belong to no single command, as
// persistent flags on the root.
//
// Call it at the point flags are wired rather than capturing All() earlier: a
// command registers its settings in its own init, and package initialization
// order puts that before the command tree is built, not before this package
// loads.
func BindFlags(fs *pflag.FlagSet) {
	for _, s := range All() {
		if s.bindable() && len(s.Commands) == 0 {
			bind(fs, s)
		}
	}
}

// BindCommandFlags registers the settings scoped to one command path, on that
// command's own flag set. A setting naming several paths is bound to each: one
// setting with two bindings, not two settings.
func BindCommandFlags(path string, fs *pflag.FlagSet) {
	for _, s := range All() {
		if s.bindable() && slices.Contains(s.Commands, path) {
			bind(fs, s)
		}
	}
}

// bindable reports whether the setting gets a command-line flag.
//
// A secret never does, whatever its declaration says. Validate reports a
// secret that names a flag as a registry defect, but the rule is enforced
// here as well because here is where the exposure would happen: Validate
// runs once at startup, and a caller that builds the command tree without it
// would otherwise put a credential in `ps` and in shell history.
func (s Setting) bindable() bool { return s.Flag != "" && !s.Secret }

// bind registers one flag with the setting's declared default, so --help
// prints it. Callers skip a setting bindable rejects.
//
// This does not confuse a default with a supplied value: pflag sets Changed
// only from Set, never from registration, and Resolve reads a flag only when
// Changed reports true.
//
// Only bools get a typed flag. Ints and durations register as strings so that
// Setting.parse is the one thing deciding what they accept: pflag's Int reads
// base 0, so it takes "0x10" as 16 where parse rejects it, and its errors name
// no canonical key. One vocabulary across all three channels is worth more
// than the type shown in help.
func bind(fs *pflag.FlagSet, s Setting) {
	if s.Kind == KindBool {
		// An empty Default yields nil from parse; the assertion then gives
		// false, which is right for a bool with no declared default.
		def, _ := s.parse(s.Default)
		b, _ := def.(bool)
		fs.Bool(s.Flag, b, s.Usage)
		return
	}
	fs.String(s.Flag, s.Default, s.Usage)
}

// parse converts a raw string to the setting's kind, returning a message that
// names the setting rather than the raw value's type.
func (s Setting) parse(raw string) (any, error) {
	switch s.Kind {
	case KindBool:
		switch raw {
		case "true", "1":
			return true, nil
		case "false", "0", "":
			return false, nil
		}
		return nil, fmt.Errorf("%s must be true or false, got %q", s.Key, raw)
	case KindInt:
		// Atoi rather than Sscanf: Sscanf stops at the first character it
		// cannot read and reports success for the prefix, so "3x" would be 3
		// and "2.5" would be 2. pflag rejects both, and a value one channel
		// accepts and another rejects cannot round-trip through a record.
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("%s must be an integer, got %q", s.Key, raw)
		}
		return n, nil
	case KindDuration:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("%s must be a duration such as 60s or 5m, got %q", s.Key, raw)
		}
		return d, nil
	default:
		return raw, nil
	}
}
