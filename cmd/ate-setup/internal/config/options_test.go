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
	"strconv"

	"github.com/spf13/pflag"
)

// Options and Load let a test state a configuration as a struct rather than
// building a flag set. Nothing in the binary uses them: the root command binds
// the registry to cobra's flags and calls LoadFlags, so a struct of raw flag
// values has no production caller. They live in a test file to keep that
// true -- production code cannot reach a second entry point that quietly
// drops ConfigPath.

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
	IngressAuthMode                string
	AdditionalEgressExtprocService string
	CredentialProvider             string
	OtlpEndpoint                   string

	// Image source selection.
	ImageRepo string
	ImageTag  string

	// NoDevEnv skips sourcing .ate-dev-env.sh even when it exists.
	NoDevEnv bool
}

// Load resolves the effective configuration from the legacy Options struct.
//
// It delegates to LoadFlags so there is one resolution path: Options is
// projected onto a flag set, then layered with the environment and any
// configuration file exactly as the command line is.
func Load(opts Options) (*Config, error) {
	fs, err := optionsFlagSet(opts)
	if err != nil {
		return nil, err
	}
	return LoadFlags(fs, LoadOptions{NoDevEnv: opts.NoDevEnv})
}

// optionsFlagSet converts the legacy Options struct into a bound flag set,
// marking as supplied every field holding a non-zero value.
//
// This is what Load uses to reach the same resolver the command line does. It
// cannot represent a boolean explicitly set to false, which is the limitation
// Options already had: firstNonEmpty and || treated a false the same as an
// absent one.
func optionsFlagSet(opts Options) (*pflag.FlagSet, error) {
	fs := pflag.NewFlagSet("options", pflag.ContinueOnError)
	BindFlags(fs)

	set := func(flag, val string) error {
		if val == "" {
			return nil
		}
		return fs.Set(flag, val)
	}
	for _, kv := range []struct{ flag, val string }{
		{"kubeconfig", opts.Kubeconfig},
		{"context", opts.Context},
		{"atenet-dataplane", opts.Router},
		{"rollout-timeout", opts.RolloutTimeout},
		{"cluster-size", opts.ClusterSize},
		{"ingress-auth-mode", opts.IngressAuthMode},
		{"experimental-additional-egress-extproc-service", opts.AdditionalEgressExtprocService},
		{"credential-provider", opts.CredentialProvider},
		{"otlp-endpoint", opts.OtlpEndpoint},
		{"image-repo", opts.ImageRepo},
		{"image-tag", opts.ImageTag},
	} {
		if err := set(kv.flag, kv.val); err != nil {
			return nil, err
		}
	}
	for _, kv := range []struct {
		flag string
		val  bool
	}{
		{"kind", opts.Kind},
		{"cordon-control-plane", opts.CordonControlPlane},
	} {
		if kv.val {
			if err := fs.Set(kv.flag, "true"); err != nil {
				return nil, err
			}
		}
	}
	if opts.PodcertWorkersPerSigner != 0 {
		if err := fs.Set("podcert-workers-per-signer", strconv.Itoa(opts.PodcertWorkersPerSigner)); err != nil {
			return nil, err
		}
	}
	return fs, nil
}
