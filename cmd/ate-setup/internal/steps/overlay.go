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

package steps

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

// installDir is the manifest root, relative to the repository root.
const installDir = "manifests/ate-install"

// SystemOverlay picks the manifest source for a full control plane install.
//
// The choice is a product of two switches: kind vs GKE, and the atenet router dataplane.
// An empty return means "no overlay": apply the base manifests/ate-install
// directory directly, which is what a plain GKE envoy install does.
func SystemOverlay(cfg *config.Config) string {
	switch {
	case cfg.Router == config.RouterAgentgateway && cfg.Kind:
		return installDir + "/kind-agentgateway"
	case cfg.Router == config.RouterAgentgateway:
		return installDir + "/agentgateway"
	case cfg.Kind:
		return installDir + "/kind"
	default:
		return ""
	}
}

// renderSystemManifests produces the full control plane manifest with all
// image references resolved.
func (e *Env) renderSystemManifests(ctx context.Context) ([]byte, error) {
	if overlay := SystemOverlay(e.Cfg); overlay != "" {
		return e.KustomizeResolve(ctx, overlay)
	}
	return e.ResolveManifest(ctx, e.Cfg.Manifest())
}

// renderAtenetRouterManifest produces the atenet router manifest for the
// selected dataplane.
func (e *Env) renderAtenetRouterManifest(ctx context.Context) ([]byte, error) {
	if e.Cfg.Router == config.RouterAgentgateway {
		return e.KustomizeResolve(ctx, installDir+"/agentgateway-router")
	}
	return e.ResolveManifest(ctx, e.Cfg.Manifest("atenet-router.yaml"))
}

// atenetEgressManifestPath returns the egress manifest path based on configuration.
func (e *Env) atenetEgressManifestPath() string {
	if e.Cfg.ExperimentalUseSDSMint {
		return e.Cfg.Manifest("atenet-egress-with-sdsmint.yaml")
	}
	return e.Cfg.Manifest("atenet-egress.yaml")
}

// renderAtenetEgressManifest produces the atenet egress manifest.
func (e *Env) renderAtenetEgressManifest(ctx context.Context) ([]byte, error) {
	general := e.Cfg.AdditionalEgressExtprocService != ""
	injection := e.Cfg.ExperimentalEgressCredentialInjection

	if e.Cfg.Router == config.RouterAgentgateway {
		if general {
			return nil, fmt.Errorf("--experimental-additional-egress-extproc-service requires --atenet-dataplane=envoy")
		}
		if injection {
			return nil, fmt.Errorf("--experimental-egress-credential-injection requires --atenet-dataplane=envoy")
		}
		return e.KustomizeResolve(ctx, installDir+"/agentgateway-egress")
	}

	if !general && !injection {
		return e.ResolveManifest(ctx, e.atenetEgressManifestPath())
	}

	// The general additional-ext_proc filter and egress credential injection are
	// independent splices with their own markers, so compose them.
	var raw []byte
	var err error
	if general {
		raw, err = e.patchAtenetEgressManifest()
	} else {
		raw, err = os.ReadFile(e.atenetEgressManifestPath())
	}
	if err != nil {
		return nil, err
	}
	if injection {
		raw, err = e.patchAtenetEgressInject(raw)
		if err != nil {
			return nil, err
		}
	}
	return e.ResolveManifestBytes(ctx, raw)
}

// patchAtenetEgressInject splices the credential-provider flags into the egress
// sidecar over the #ATE_EGRESS_INJECT_FLAGS marker. It takes the manifest bytes
// rather than reading the file so it can run after the general patch. Mirrors
// hack/experimental-egress-credential-injection.sh; the two must stay in sync.
func (e *Env) patchAtenetEgressInject(raw []byte) ([]byte, error) {
	if !e.Cfg.ExperimentalUseSDSMint {
		return nil, fmt.Errorf("--experimental-egress-credential-injection requires --experimental-use-sdsmint")
	}

	name := e.Cfg.CredentialProviderName
	if name == "" {
		name = "ate-secret://k8s.io"
	}
	address := e.Cfg.CredentialProviderAddress
	if address == "" {
		address = "k8s-credential-provider.ate-system.svc:50051"
	}
	serverName := address
	if i := strings.LastIndex(address, ":"); i >= 0 {
		serverName = address[:i]
	}

	flagsBlock := emitEgressInjectFlags(name, address, serverName)

	var out []string
	flagsReplaced := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#ATE_EGRESS_INJECT_FLAGS") {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			for _, l := range strings.Split(flagsBlock, "\n") {
				if l == "" {
					out = append(out, "")
				} else {
					out = append(out, indent+l)
				}
			}
			flagsReplaced++
			continue
		}
		out = append(out, line)
	}
	if flagsReplaced != 1 {
		return nil, fmt.Errorf("expected 1 #ATE_EGRESS_INJECT_FLAGS marker in %s, found %d",
			e.atenetEgressManifestPath(), flagsReplaced)
	}
	return []byte(strings.Join(out, "\n")), nil
}

func emitEgressInjectFlags(name, address, serverName string) string {
	return fmt.Sprintf(`- --credential-provider-name=%s
- --credential-provider-address=%s
- --credential-provider-ca-file=/run/servicedns.podcert.ate.dev/trust-bundle.pem
- --credential-provider-client-cert=/run/podidentity.podcert.ate.dev/credential-bundle.pem
- --credential-provider-server-name=%s`, name, address, serverName)
}

func (e *Env) patchAtenetEgressManifest() ([]byte, error) {
	if !e.Cfg.ExperimentalUseSDSMint {
		return nil, fmt.Errorf("--experimental-additional-egress-extproc-service requires --experimental-use-sdsmint")
	}
	raw, err := os.ReadFile(e.atenetEgressManifestPath())
	if err != nil {
		return nil, fmt.Errorf("reading egress manifest: %w", err)
	}

	spec := e.Cfg.AdditionalEgressExtprocService
	parts := strings.Split(spec, "/")
	namespace := parts[0]
	svcPort := strings.Split(parts[1], ":")
	service := svcPort[0]
	port := svcPort[1]

	address := fmt.Sprintf("%s.%s.svc.cluster.local", service, namespace)
	serverName := fmt.Sprintf("%s.%s.svc", service, namespace)

	filterBlock := emitAdditionalEgressExtprocFilter()
	clusterBlock := emitAdditionalEgressExtprocCluster(address, port, serverName)

	lines := strings.Split(string(raw), "\n")
	var out []string
	filtersReplaced := 0
	clustersReplaced := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#ATE_MITM_EXTPROC_FILTER") {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			for _, fLine := range strings.Split(filterBlock, "\n") {
				if fLine == "" {
					out = append(out, "")
				} else {
					out = append(out, indent+fLine)
				}
			}
			filtersReplaced++
			continue
		}
		if strings.HasPrefix(trimmed, "#ATE_MITM_EXTPROC_CLUSTER") {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			for _, cLine := range strings.Split(clusterBlock, "\n") {
				if cLine == "" {
					out = append(out, "")
				} else {
					out = append(out, indent+cLine)
				}
			}
			clustersReplaced++
			continue
		}
		out = append(out, line)
	}

	if filtersReplaced != 2 || clustersReplaced != 1 {
		return nil, fmt.Errorf("expected 2 filter markers and 1 cluster marker in %s, found %d and %d",
			e.atenetEgressManifestPath(), filtersReplaced, clustersReplaced)
	}

	return []byte(strings.Join(out, "\n")), nil
}

const additionalEgressExtprocCluster = "additional_egress_ext_proc"

func emitAdditionalEgressExtprocFilter() string {
	return `- name: envoy.filters.http.ext_proc
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
    grpc_service:
      envoy_grpc:
        cluster_name: additional_egress_ext_proc
      timeout: 2s
    failure_mode_allow: false
    message_timeout: 2s
    request_attributes:
    - filter_state['dev.ate.actor.identity']
    processing_mode:
      request_header_mode: SEND
      response_header_mode: SKIP
      request_body_mode: NONE
      response_body_mode: NONE
      request_trailer_mode: SKIP
      response_trailer_mode: SKIP
    mutation_rules:
      disallow_system: true
      disallow_is_error: true`
}

func emitAdditionalEgressExtprocCluster(address, port, serverName string) string {
	return fmt.Sprintf(`- name: %s
  type: STRICT_DNS
  lb_policy: ROUND_ROBIN
  connect_timeout: 1s
  typed_extension_protocol_options:
    envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
      "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
      explicit_http_config:
        http2_protocol_options: {}
  transport_socket:
    name: envoy.transport_sockets.tls
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
      sni: %s
      common_tls_context:
        tls_params:
          tls_minimum_protocol_version: TLSv1_3
          tls_maximum_protocol_version: TLSv1_3
        tls_certificate_sds_secret_configs:
        - name: podidentity_client_cert
          sds_config:
            resource_api_version: V3
            path_config_source:
              path: /etc/envoy/sds-podidentity-cert.yaml
        combined_validation_context:
          default_validation_context:
            match_typed_subject_alt_names:
            - san_type: DNS
              matcher:
                exact: %s
          validation_context_sds_secret_config:
            name: servicedns_validation_context
            sds_config:
              resource_api_version: V3
              path_config_source:
                path: /etc/envoy/sds-servicedns-validation.yaml
  load_assignment:
    cluster_name: %s
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address:
              address: %s
              port_value: %s`, additionalEgressExtprocCluster, serverName, serverName, additionalEgressExtprocCluster, address, port)
}

func (e *Env) applyAtenetEgress(ctx context.Context) error {
	manifests, err := e.renderAtenetEgressManifest(ctx)
	if err != nil {
		return err
	}

	running, err := e.Kube.DeploymentExists(ctx, NamespaceAteSystem, "atenet-egress")
	if err != nil {
		return err
	}

	if err := e.Kube.ApplyBytes(ctx, manifests); err != nil {
		return err
	}

	if running && (e.Cfg.AdditionalEgressExtprocService != "" || e.Cfg.ExperimentalEgressCredentialInjection) {
		if err := e.Kube.RolloutRestartDeployment(ctx, NamespaceAteSystem, "atenet-egress", time.Now()); err != nil {
			return err
		}
	}
	return nil
}

// otelConfigPath returns the environment's ate-otel-config ConfigMap.
//
// Every control plane component pulls this ConfigMap in via envFrom. A full
// install gets it as part of the rendered bundle, but the single-component
// redeploys apply raw manifests with no kustomize, so they have to select the
// right copy themselves: applying the base file on a kind cluster would
// overwrite it with the GKE endpoint and break telemetry everywhere at once.
func (e *Env) otelConfigPath() string {
	if e.Cfg.Kind {
		return e.Cfg.Manifest("kind", "ate-otel-config.yaml")
	}
	return e.Cfg.Manifest("ate-otel-config.yaml")
}

// applyOtelConfig applies the environment's ate-otel-config ConfigMap.
func (e *Env) applyOtelConfig(ctx context.Context) error {
	return e.Kube.ApplyPath(ctx, e.otelConfigPath())
}
