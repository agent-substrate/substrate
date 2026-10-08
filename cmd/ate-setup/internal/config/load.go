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
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
)

// ConfigPathEnv names the configuration document when --config is not given.
//
// It exists so the file channel crosses hack/install-ate.sh, which forwards
// only a fixed set of flags but passes the whole environment to the child
// process. Every other setting reaches ate-setup that way already.
const ConfigPathEnv = "ATE_CONFIG"

// LoadOptions carries the inputs that select where configuration comes from,
// as opposed to the settings themselves.
type LoadOptions struct {
	// ConfigPath is the --config document. Empty falls back to $ATE_CONFIG,
	// and then to no file layer.
	ConfigPath string
	// NoDevEnv skips sourcing .ate-dev-env.sh even when it exists.
	NoDevEnv bool
}

// LoadFlags resolves the effective configuration from a bound flag set.
//
// Precedence, lowest to highest: .ate-dev-env.sh, the process environment, the
// configuration file, then flags. The file outranks the environment because it
// is the artifact an operator owns and reviews; ambient state must not silently
// outrank it.
func LoadFlags(fs *pflag.FlagSet, o LoadOptions) (*Config, error) {
	root, err := RepoRoot()
	if err != nil {
		return nil, err
	}

	env := environ()

	// The document is parsed before the Kind question is answered, because it
	// is one of the channels that answers it. Parsing reads only the file, so
	// nothing here depends on the environment it precedes.
	var file *File
	if path := configPathFrom(o); path != "" {
		if file, err = ParseFile(path); err != nil {
			return nil, err
		}
	}

	kind := resolveKind(fs, file, env)
	if err := mergeDevEnv(root, env, o.NoDevEnv, kind); err != nil {
		return nil, err
	}

	res, err := Resolve(fs, ResolveOptions{File: file, Env: env})
	if err != nil {
		return nil, err
	}
	return buildConfig(root, env, res)
}

// configPathFrom picks the configuration document, flag over environment.
//
// A relative path resolves against the working directory, which is the
// repository root when the shell installer invoked this: it does cd "$ROOT"
// before running the binary. ParseFile reports the absolute path it tried, so
// a path that meant something else from the caller's directory is visible in
// the error rather than silently missing.
func configPathFrom(o LoadOptions) string {
	if o.ConfigPath != "" {
		return o.ConfigPath
	}
	return os.Getenv(ConfigPathEnv)
}

// resolveKind answers the Kind question before the dev environment is sourced,
// because whether to source it depends on the answer.
//
// It layers the same channels in the same order Resolve does, off the same
// registry entry. Answering it from the flag and the environment alone would
// make a document that selects Kind still source .ate-dev-env.sh, pointing a
// local install at the GKE project the file names.
func resolveKind(fs *pflag.FlagSet, file *File, env map[string]string) bool {
	s, ok := Lookup(kindEnabledKey)
	if !ok {
		return false
	}
	asBool := func(raw string) (bool, bool) {
		v, err := s.parse(raw)
		if err != nil {
			// Reported by Resolve, which sees the same value moments later.
			return false, false
		}
		b, _ := v.(bool)
		return b, true
	}

	if fs != nil && fs.Changed(s.Flag) {
		// Read even when false: --kind=false is a deliberate override of an
		// inherited variable, not an absence.
		if b, err := fs.GetBool(s.Flag); err == nil {
			return b
		}
	}
	if file != nil {
		if raw, present := file.Get(s.Key); present {
			if b, valid := asBool(raw); valid {
				return b
			}
		}
	}
	if raw, present := env[s.Env]; present {
		if b, valid := asBool(raw); valid {
			return b
		}
	}
	return false
}

// mergeDevEnv layers .ate-dev-env.sh underneath the process environment.
//
// Sourcing is skipped for Kind installs the same way the shell kind installer
// exports NO_DEV_ENV: the GKE-shaped variables in a developer's file would
// otherwise point a local install at a cloud project.
func mergeDevEnv(root string, env map[string]string, noDevEnv, kind bool) error {
	if noDevEnv || kind || os.Getenv("NO_DEV_ENV") != "" {
		return nil
	}
	path := filepath.Join(root, devEnvFile)
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	sourced, err := sourceShellEnv(path, root)
	if err != nil {
		return fmt.Errorf("while sourcing %s: %w", devEnvFile, err)
	}
	// The process environment still wins: an explicitly exported variable is a
	// deliberate override of the file.
	for k, v := range sourced {
		if _, ok := env[k]; !ok {
			env[k] = v
		}
	}
	return nil
}

// buildConfig projects the resolved settings onto Config.
func buildConfig(root string, env map[string]string, r *Resolved) (*Config, error) {
	// Before the projection, so Config, the report and the record all describe
	// one install rather than Config describing a different one.
	if r.Bool(kindEnabledKey) {
		applyKindDefaults(r)
	}

	rolloutTimeout := r.Duration("rolloutTimeout")
	if r.Supplied("rolloutTimeout") && rolloutTimeout <= 0 {
		// kubectl reads --timeout=0 as "wait forever"; ate-setup does not offer
		// an unbounded wait, so say so rather than silently meaning the opposite.
		v, _ := r.Value("rolloutTimeout")
		return nil, &InvalidError{Value: v, Want: "a positive duration"}
	}

	podcertWorkers := r.Int("podcert.workersPerSigner")
	if r.Supplied("podcert.workersPerSigner") && podcertWorkers < 1 {
		v, _ := r.Value("podcert.workersPerSigner")
		return nil, &InvalidError{Value: v, Want: "a positive integer"}
	}

	kubeconfig, kubeconfigEnv := splitKubeconfig(r)

	// One configured connection serves both pools; both empty selects the
	// bundled PostgreSQL.
	readWriteConnectionString := r.String("ateapi.postgres.readWrite.connectionString")
	ownerConnectionString := r.String("ateapi.postgres.owner.connectionString")
	if ownerConnectionString == "" {
		ownerConnectionString = readWriteConnectionString
	}
	instance, _ := r.Value("ateapi.postgres.cloudsql.instance")

	cfg := &Config{
		Root:               root,
		Kind:               r.Bool("kindCluster.enabled"),
		Namespace:          r.String("namespace"),
		Kubeconfig:         kubeconfig,
		Context:            r.String("context"),
		ProjectID:          r.String("gcp.projectID"),
		ClusterName:        r.String("gcp.clusterName"),
		ClusterLocation:    r.String("gcp.clusterLocation"),
		ExpectedJWTIssuer:  r.String("ateapi.expectedJWTIssuer"),
		BucketName:         r.String("storage.bucketName"),
		KODockerRepo:       r.String("ko.dockerRepo"),
		KODefaultPlatforms: r.String("ko.defaultPlatforms"),
		ActorJWTAlgorithm:  r.String("actorJWT.algorithm"),
		DockerBuildFlags:   strings.Fields(r.String("docker.buildFlags")),
		Images: images.Source{
			Repo: strings.TrimSuffix(r.String("images.repo"), "/"),
			Tag:  r.String("images.tag"),
		},
		Router:               r.String("atenet.dataplane"),
		PostgresSchema:       r.String("ateapi.postgres.schema"),
		PostgresPoolMaxConns: r.String("ateapi.postgres.poolMaxConns"),
		PostgresServerCAFile: r.String("ateapi.postgres.serverCAFile"),
		PostgresStorageClass: r.String("ateapi.postgres.storageClass"),

		PostgresReadWriteConnectionString: readWriteConnectionString,
		PostgresOwnerConnectionString:     ownerConnectionString,
		PostgresReadWriteRole:             r.String("ateapi.postgres.readWrite.role"),
		PostgresOwnerRole:                 r.String("ateapi.postgres.owner.role"),
		// Whether a role was chosen rather than defaulted. Taken from the
		// resolver, which knows about all three channels, rather than from an
		// environment lookup that only sees one.
		PostgresReadWriteRoleSet: r.Supplied("ateapi.postgres.readWrite.role"),
		PostgresOwnerRoleSet:     r.Supplied("ateapi.postgres.owner.role"),
		CloudSQL: CloudSQLConfig{
			Instance: instance.Raw,
			// Any channel may now express "explicitly empty", not just the
			// environment, so a file can clear Cloud SQL.
			InstanceSet: instance.Supplied,
			GSA:         r.String("ateapi.postgres.cloudsql.gsa"),
			IAMAuth:     r.String("ateapi.postgres.cloudsql.iamAuth"),
			IPType:      r.String("ateapi.postgres.cloudsql.ipType"),
		},
		RolloutTimeout:                 rolloutTimeout,
		rolloutTimeoutSet:              r.Supplied("rolloutTimeout"),
		PodcertWorkersPerSigner:        podcertWorkers,
		ClusterSize:                    r.String("clusterSize"),
		CordonControlPlane:             r.Bool("cordonControlPlane"),
		AdditionalEgressExtprocService: r.String("atenet.egress.additionalExtprocService"),
		CredentialProviderJSON:         r.String("atenet.egress.credentialProvider"),
		AnthropicAPIKey:                r.String("demo.anthropicAPIKey"),
		OtlpEndpoint:                   r.String("otlpEndpoint"),
		APILogLevel:                    r.String("ateapi.logLevel"),
		BenchmarkActorMemory:           r.String("benchmark.actorMemory"),
		kubeconfigEnv:                  kubeconfigEnv,
		shellEnv:                       env,
		resolved:                       r,
	}

	if err := validateResolved(cfg, r); err != nil {
		return nil, err
	}
	return cfg, nil
}

// splitKubeconfig divides the setting into the path handed to client-go and the
// value exported to the shell scripts.
//
// $KUBECONFIG is a PATH-style list, and developers who juggle clusters do set it
// to several files. client-go's explicit path is a single file, so passing such
// a value through makes every command fail. A list is left to the default
// loading rules, which already read $KUBECONFIG and merge its entries.
func splitKubeconfig(r *Resolved) (explicitPath, scriptValue string) {
	v, _ := r.Value("kubeconfig")
	if v.From == OriginFlag || v.From == OriginFile {
		return v.Raw, v.Raw
	}
	if strings.ContainsRune(v.Raw, os.PathListSeparator) {
		return "", v.Raw
	}
	return v.Raw, v.Raw
}

// applyKindDefaults fills the Kind-only settings, leaving alone any a channel
// actually supplied.
//
// The values are written into the resolved set rather than onto Config, so
// that the configuration report and the install record name what the install
// uses. Writing them onto Config alone would report "" for four settings the
// run is about to apply, and leave a Kind record holding nothing but
// kindCluster.enabled -- which replays to whatever the defaults happen to be
// on the next machine and the next release, not to the install it recorded.
func applyKindDefaults(r *Resolved) {
	// A Kind cluster has no GCP project, so the coordinates are cleared even
	// when the developer environment named them. Only when something did name
	// them: clearing an already-empty setting would report a value as set by
	// the profile when the profile changed nothing.
	for _, key := range []string{"gcp.projectID", "gcp.clusterLocation"} {
		if r.String(key) != "" {
			r.set(key, "", OriginKindProfile)
		}
	}

	if r.String("context") == "" {
		r.set("context", "kind-"+r.String("kindCluster.name"), OriginKindProfile)
	}
	if r.String("ko.dockerRepo") == "" {
		r.set("ko.dockerRepo", "localhost:5001", OriginKindProfile)
	}
	// Ambient environment is ignored: a developer's shell or .ate-dev-env.sh
	// points these at GKE and GCS, which a local install must not inherit. A
	// value the caller named on this run in a flag or a configuration file is
	// theirs and is kept.
	if !namedExplicitly(r, "ko.defaultPlatforms") {
		r.set("ko.defaultPlatforms", "linux/"+runtime.GOARCH, OriginKindProfile)
	}
	if !namedExplicitly(r, "storage.bucketName") {
		r.set("storage.bucketName", "ate-snapshots", OriginKindProfile)
	}
}

// namedExplicitly reports whether a flag or configuration file supplied the
// setting, as opposed to it arriving from the ambient environment or a default.
func namedExplicitly(r *Resolved, key string) bool {
	v, ok := r.Value(key)
	return ok && (v.From == OriginFlag || v.From == OriginFile)
}

// validateResolved rejects invalid and conflicting settings, naming the channel
// each value came from so the reader knows which one to edit.
func validateResolved(cfg *Config, r *Resolved) error {
	// The repo/tag pairing is checked here rather than in images.Source.Validate
	// because only here is the channel that supplied each half known. A tag on
	// its own would otherwise be discarded in silence, leaving a build from
	// source that looks like the release the tag names.
	repo, _ := r.Value("images.repo")
	tag, _ := r.Value("images.tag")
	switch {
	case cfg.Images.IsPrebuilt() && cfg.Images.Tag == "":
		return &ConflictError{A: repo, B: tag, Why: "a prebuilt image repository needs a tag"}
	case !cfg.Images.IsPrebuilt() && cfg.Images.Tag != "":
		return &ConflictError{A: tag, B: repo,
			Why: "without a repository, images are built from source and the tag names nothing"}
	}

	dataplane, _ := r.Value("atenet.dataplane")
	switch cfg.Router {
	case RouterEnvoy, RouterAgentgateway:
	default:
		return &InvalidError{Value: dataplane, Want: RouterEnvoy + " or " + RouterAgentgateway}
	}

	if ipType, _ := r.Value("ateapi.postgres.cloudsql.ipType"); ipType.Raw != "" {
		switch ipType.Raw {
		case CloudSQLIPTypePrivate, CloudSQLIPTypePublic, CloudSQLIPTypePSC:
		default:
			return &InvalidError{Value: ipType, Want: strings.Join(
				[]string{CloudSQLIPTypePrivate, CloudSQLIPTypePublic, CloudSQLIPTypePSC}, ", ")}
		}
	}

	if size, _ := r.Value("clusterSize"); cfg.ClusterSize != ClusterSizeSize0 && cfg.ClusterSize != ClusterSizeSize10 {
		return &InvalidError{Value: size, Want: ClusterSizeSize0 + " or " + ClusterSizeSize10}
	}

	switch cfg.APILogLevel {
	case "", "debug", "info", "warn", "error":
	default:
		level, _ := r.Value("ateapi.logLevel")
		return &InvalidError{Value: level, Want: "debug, info, warn, or error"}
	}

	switch cfg.ActorJWTAlgorithm {
	case "ES256", "RS256":
	default:
		alg, _ := r.Value("actorJWT.algorithm")
		return &InvalidError{Value: alg, Want: "ES256 or RS256"}
	}
	if cfg.AdditionalEgressExtprocService != "" {
		extproc, _ := r.Value("atenet.egress.additionalExtprocService")
		if err := validateExtprocService(extproc); err != nil {
			return err
		}
		if cfg.Router != RouterEnvoy {
			return &ConflictError{A: extproc, B: dataplane,
				Why: "an additional ext_proc filter requires dataplane " + RouterEnvoy}
		}
	}
	// Parsed here so a malformed provider is rejected with the rest of the
	// configuration rather than part-way through a deploy. Whether one is
	// required at all is the deploy's question, not this one's.
	if cfg.CredentialProviderJSON != "" {
		if _, err := cfg.CredentialProvider(); err != nil {
			// The parser's own message says which part is wrong and what to
			// write instead; only the channel is added. Replacing it with the
			// generic "must be <usage>" would drop the reason.
			provider, _ := r.Value("atenet.egress.credentialProvider")
			return fmt.Errorf("%w (from %s)", err, provider.From.Describe(provider.Setting))
		}
	}
	return nil
}
