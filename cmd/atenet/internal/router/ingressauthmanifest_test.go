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

package router

import (
	"slices"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

// TestRouterIngressAuthManifestIsValid checks the router would accept the
// flags in its manifest. The TLS listeners need the client auth settings, so
// a new listener in the manifest, or a new startup check here, would otherwise
// surface only as a router that refuses to start in the e2e cluster. The
// manifest carries no allowlist, so the test appends one as ate-setup does
// (TestRenderRouterAllowedSPIFFEIDs pins that ate-setup appends it).
func TestRouterIngressAuthManifestIsValid(t *testing.T) {
	var args []string
	var envoyMounts []string
	for _, c := range findDeployment(t, routerManifestPath, "atenet-router").Spec.Template.Spec.Containers {
		switch c.Name {
		case "atenet-router":
			if len(c.Args) == 0 || c.Args[0] != "router" {
				t.Fatalf("the atenet-router container's args are %v; they should invoke the router subcommand", c.Args)
			}
			args = c.Args[1:]
		case "envoy":
			for _, m := range c.VolumeMounts {
				envoyMounts = append(envoyMounts, m.MountPath)
			}
		}
	}
	args = append(args, "--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/test/sa/client")

	var cfg routerConfig
	fs := pflag.NewFlagSet("router", pflag.ContinueOnError)
	bindRouterFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		t.Fatalf("the atenet-router flags are not ones the binary accepts: %v", err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("the router would refuse the manifest's flags: %v", err)
	}
	// The router reads the CA file, but Envoy is what enforces it, so it has
	// to be mounted in the envoy container at the same path.
	if !slices.ContainsFunc(envoyMounts, func(dir string) bool {
		return strings.HasPrefix(cfg.IngressAuth.ClientCAFile, dir+"/")
	}) {
		t.Errorf("--ingress-client-ca-file=%s is not under any of the envoy container's mounts %v", cfg.IngressAuth.ClientCAFile, envoyMounts)
	}
}
