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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sigs.k8s.io/kustomize/kyaml/kio"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kustomize"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

const (
	// installDir is the manifest root, relative to the repository root.
	installDir = "manifests/ate-install"
)

// cordonSystemInfraComponent is the kustomize component that pins each
// system infra workload to its own node pool, layered over every system
// infra apply under --cordon-system-infra-components.
const cordonSystemInfraComponent = installDir + "/components/cordon-system-infra-components"

// e2eComponent is the kustomize component that tunes the control plane and
// data plane for end-to-end test runs, layered over every control plane apply
// under --e2e-tuning.
const e2eComponent = installDir + "/components/e2e"

// fakeDataPlaneComponent is the kustomize component that puts fake-atelet in
// atelet's DaemonSet and fake-workersync in place of ate-controller, layered
// over the atelet and ate-controller applies under --fake-data-plane.
const fakeDataPlaneComponent = installDir + "/components/fake-data-plane"

const agentgatewayStaticMTLSComponent = installDir + "/components/agentgateway-static-mtls"

// storageExternalComponent is the kustomize component that removes the in-tree
// object store settings from the objectstore-plugin sidecars, layered over
// every control plane apply under --storage-backend=external.
const storageExternalComponent = installDir + "/components/storage-external"

// components lists the kustomize components every control plane apply is
// composed with: ours, then the operator's --component directories. Each patch
// targets its workloads by name, and kustomize leaves a stream alone when
// nothing in it matches, so composing a manifest that carries none of them is
// harmless.
func (e *Env) components() []string {
	// Operator components come last, so they patch the result of ours.
	return append(e.inTreeComponents(), e.Cfg.Components...)
}

// inTreeComponents lists the installer's own components in components.
func (e *Env) inTreeComponents() []string {
	var components []string
	// e2e must come before fake-data-plane: e2e appends to atelet's args,
	// and fake-data-plane removes args entirely.
	if e.Cfg.E2ETuning {
		components = append(components, e.Cfg.Path(e2eComponent))
	}
	if e.Cfg.FakeDataPlane {
		components = append(components, e.Cfg.Path(fakeDataPlaneComponent))
	}
	if e.Cfg.Router == config.RouterAgentgateway {
		components = append(components, e.Cfg.Path(agentgatewayStaticMTLSComponent))
	}
	if e.Cfg.CordonSystemInfraComponents {
		components = append(components, e.Cfg.Path(cordonSystemInfraComponent))
	}
	if e.Cfg.StorageBackend == config.StorageBackendExternal {
		components = append(components, e.Cfg.Path(storageExternalComponent))
	}
	return components
}

// SystemOverlay picks the kustomization for a full control plane install.
//
// The choice is a product of two switches: kind vs GKE, and the atenet router
// dataplane. The plain GKE envoy install renders the base kustomization rather
// than the raw manifests/ate-install directory: the directory would also
// re-apply pod-certificate-controller.yaml (reverting the size10 flags and the
// WORKERS_PER_SIGNER value set earlier in the install), both atenet-egress
// variants, and the sandboxconfig files, all of which have their own apply
// steps.
func SystemOverlay(cfg *config.Config) string {
	switch {
	case cfg.Router == config.RouterAgentgateway && cfg.Kind:
		return installDir + "/kind-agentgateway"
	case cfg.Router == config.RouterAgentgateway:
		return installDir + "/agentgateway"
	case cfg.Kind:
		return installDir + "/kind"
	default:
		return installDir + "/base"
	}
}

// kustomization directory, before image resolution. It composes path with the
// selected components (see components), so the same patches reach every
// control plane workload whichever apply path delivers it.
func (e *Env) render(path string) ([]byte, error) {
	manifest, err := composeManifest(path, e.components())
	if err != nil {
		return nil, err
	}
	if e.Cfg.StorageBackend == config.StorageBackendExternal {
		if err := e.checkProviderPluginImages(manifest); err != nil {
			return nil, err
		}
	}
	return manifest, nil
}

// composeManifest renders path, a manifest file or a kustomization directory,
// composed with components.
func composeManifest(path string, components []string) ([]byte, error) {
	if len(components) > 0 {
		return kustomize.Compose(path, components...)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	if info.IsDir() {
		return kustomize.Build(path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	return data, nil
}

// checkProviderPluginImages fails if an objectstore-plugin container in
// manifest still runs the in-tree image. Kustomize skips a patch whose target
// matches nothing, so a --component that names the wrong workload would
// otherwise leave the in-tree sidecar in place with no storage backend set.
func (e *Env) checkProviderPluginImages(manifest []byte) error {
	inTree := images.KoReference("cmd/objectstore-plugin")
	nodes, err := (&kio.ByteReader{
		Reader:                bytes.NewReader(manifest),
		OmitReaderAnnotations: true,
	}).Read()
	if err != nil {
		return fmt.Errorf("while parsing manifest: %w", err)
	}
	for _, node := range nodes {
		for _, field := range []string{"initContainers", "containers"} {
			image, err := node.Pipe(yaml.Lookup("spec", "template", "spec", field, "[name=objectstore-plugin]", "image"))
			if err != nil {
				return err
			}
			if image != nil && yaml.GetValue(image) == inTree {
				return fmt.Errorf("storage.backend is %s, but container objectstore-plugin in %s %s still runs the in-tree image %s; check that a patch in --component %s targets it",
					config.StorageBackendExternal, node.GetKind(), node.GetName(), inTree, strings.Join(e.Cfg.Components, ", "))
			}
		}
	}
	return nil
}

// checkComponentTargets fails unless each patch in each --component selects a
// resource in some manifest that deploy ate-system composes the components
// into. Kustomize skips a patch whose target matches nothing, so a misspelled
// target would otherwise leave the in-tree workload in place with no error.
// It renders those manifests without the later splices and image resolution,
// which edit fields of existing objects and add none. Rendering also runs the
// render-time checks, so they fail here before any cluster write.
//
// A patch matches if its target selects a resource either before or after the
// operator components apply. The first covers a patch that deletes or renames
// its target, and the second a resource that a component adds. A patch whose
// target only a later --component adds, or an earlier one removes, passes here
// but kustomize skips it.
func (e *Env) checkComponentTargets(providers []config.CredentialProvider) error {
	if len(e.Cfg.Components) == 0 {
		return nil
	}
	var manifests [][]byte
	for _, path := range e.ateSystemSources(providers) {
		composed, err := e.render(path)
		if err != nil {
			return err
		}
		inTree, err := composeManifest(path, e.inTreeComponents())
		if err != nil {
			return err
		}
		manifests = append(manifests, composed, inTree)
	}
	var errs []error
	for _, dir := range e.Cfg.Components {
		patches, err := kustomize.ComponentPatches(dir)
		if err != nil {
			return fmt.Errorf("--component %s: %w", dir, err)
		}
		unmatched, err := kustomize.Unmatched(patches, manifests)
		if err != nil {
			return fmt.Errorf("--component %s: %w", dir, err)
		}
		for _, p := range unmatched {
			errs = append(errs, fmt.Errorf("--component %s: patch %s targets %s, which matches no control plane resource",
				dir, p.Name, kustomize.DescribeSelector(p.Target)))
		}
	}
	return errors.Join(errs...)
}

// ateSystemSources lists each manifest DeployAteSystem composes the components
// into. Keep it in step with the render calls there. It includes the bundled
// PostgreSQL even when the install uses an external database, which only
// takes a cluster read to know.
func (e *Env) ateSystemSources(providers []config.CredentialProvider) []string {
	paths := []string{
		e.podCertificateControllerPath(),
		e.postgresManifestPath(),
		e.Cfg.Path(SystemOverlay(e.Cfg)),
	}
	if config.HasKubernetesProvider(providers) {
		paths = append(paths, e.k8sCredentialProviderPath(k8sCredentialProviderManifest))
	}
	if e.Cfg.Router == config.RouterAgentgateway {
		return append(paths, e.Cfg.Path(installDir+"/agentgateway-egress"))
	}
	return append(paths, e.atenetEgressManifestPath())
}

// renderBytes is render for a manifest already held in memory.
func (e *Env) renderBytes(manifest []byte) ([]byte, error) {
	if components := e.components(); len(components) > 0 {
		return kustomize.ComposeBytes(manifest, components...)
	}
	return manifest, nil
}

// renderResolve renders path and resolves its image references.
func (e *Env) renderResolve(ctx context.Context, path string) ([]byte, error) {
	manifest, err := e.render(path)
	if err != nil {
		return nil, err
	}
	return e.ResolveManifestBytes(ctx, manifest)
}

// renderResolveApply renders path, resolves its images, and applies the result.
func (e *Env) renderResolveApply(ctx context.Context, path string) error {
	manifest, err := e.renderResolve(ctx, path)
	if err != nil {
		return err
	}
	return e.Kube.ApplyBytes(ctx, manifest)
}

// renderSystemManifests produces the full control plane manifest with all
// image references resolved.
func (e *Env) renderSystemManifests(ctx context.Context) ([]byte, error) {
	manifest, err := e.render(e.Cfg.Path(SystemOverlay(e.Cfg)))
	if err != nil {
		return nil, err
	}
	if manifest, err = setIDPServerIssuer(manifest, e.Cfg.ActorJWTIssuer); err != nil {
		return nil, err
	}
	if manifest, err = e.setAgentgatewayIngressAllowedSPIFFEIDs(manifest); err != nil {
		return nil, err
	}
	if e.Cfg.Router != config.RouterAgentgateway {
		manifest, err = e.patchAtenetRouterAllowedSPIFFEIDs(manifest)
		if err != nil {
			return nil, err
		}
		imageReference, err := e.dockerfileImage(ctx, images.EnvoyDataplane)
		if err != nil {
			return nil, err
		}
		manifest = e.patchEnvoyDataplaneImage(manifest, imageReference)
	}
	return e.ResolveManifestBytes(ctx, manifest)
}

// renderAtenetRouterManifest produces the atenet router manifest for the
// selected dataplane.
func (e *Env) renderAtenetRouterManifest(ctx context.Context) ([]byte, error) {
	if e.Cfg.Router == config.RouterAgentgateway {
		manifest, err := e.render(e.Cfg.Path(installDir + "/agentgateway-router"))
		if err != nil {
			return nil, err
		}
		manifest, err = e.setAgentgatewayIngressAllowedSPIFFEIDs(manifest)
		if err != nil {
			return nil, err
		}
		return e.ResolveManifestBytes(ctx, manifest)
	}
	manifest, err := e.render(e.Cfg.Manifest("atenet-router.yaml"))
	if err != nil {
		return nil, err
	}
	manifest, err = e.patchAtenetRouterAllowedSPIFFEIDs(manifest)
	if err != nil {
		return nil, err
	}
	imageReference, err := e.dockerfileImage(ctx, images.EnvoyDataplane)
	if err != nil {
		return nil, err
	}
	manifest = e.patchEnvoyDataplaneImage(manifest, imageReference)
	return e.ResolveManifestBytes(ctx, manifest)
}

func (e *Env) setAgentgatewayIngressAllowedSPIFFEIDs(raw []byte) ([]byte, error) {
	if e.Cfg.Router != config.RouterAgentgateway {
		return raw, nil
	}
	// nil would marshal as null, which agentgateway's networkAuthorization CEL rule can't evaluate.
	allowed := e.Cfg.IngressAllowedSPIFFEIDs
	if allowed == nil {
		allowed = []string{}
	}
	ids, err := json.Marshal(allowed)
	if err != nil {
		return nil, fmt.Errorf("encoding AgentGateway ingress SPIFFE IDs: %w", err)
	}
	marker := []byte("[ATE_INGRESS_ALLOWED_SPIFFE_IDS]")
	if count := bytes.Count(raw, marker); count != 1 {
		return nil, fmt.Errorf("expected 1 AgentGateway ingress SPIFFE IDs placeholder, found %d", count)
	}
	return bytes.Replace(raw, marker, ids, 1), nil
}

// atenetEgressManifestPath returns the envoy egress gateway manifest.
func (e *Env) atenetEgressManifestPath() string {
	return e.Cfg.Manifest("atenet-egress.yaml")
}

// renderAtenetEgressManifest produces the atenet egress manifest.
func (e *Env) renderAtenetEgressManifest(ctx context.Context, providers []config.CredentialProvider) ([]byte, error) {
	general := e.Cfg.AdditionalEgressExtprocService != ""

	if e.Cfg.Router == config.RouterAgentgateway {
		// Rejected during configuration loading, which names the channel the
		// value came from. This guards the invariant for a caller that built
		// a Config directly; an operator never sees it.
		if general {
			return nil, fmt.Errorf("internal: additional ext_proc filter reached rendering with dataplane %s", config.RouterAgentgateway)
		}
		raw, err := e.render(e.Cfg.Path(installDir + "/agentgateway-egress"))
		if err != nil {
			return nil, err
		}
		raw, err = e.setAgentgatewayIngressAllowedSPIFFEIDs(raw)
		if err != nil {
			return nil, err
		}
		raw, err = patchAgentgatewayEgressInject(raw, providers)
		if err != nil {
			return nil, err
		}
		return e.ResolveManifestBytes(ctx, raw)
	}

	imageReference, err := e.dockerfileImage(ctx, images.EnvoyDataplane)
	if err != nil {
		return nil, err
	}

	// The general additional-ext_proc filter and egress credential injection are
	// independent splices with their own markers, so compose them. The cordon
	// render comes last, over the spliced stream, so the node pinning reaches
	// every variant.
	var raw []byte
	if general {
		raw, err = e.patchAtenetEgressManifest()
	} else {
		raw, err = os.ReadFile(e.atenetEgressManifestPath())
	}
	if err != nil {
		return nil, err
	}
	raw, err = e.patchAtenetEgressInject(raw, providers)
	if err != nil {
		return nil, err
	}
	raw = e.patchEnvoyDataplaneImage(raw, imageReference)
	if e.Cfg.EnvoyConcurrency > 0 {
		raw, err = patchAtenetEgressEnvoyConcurrency(raw, e.Cfg.EnvoyConcurrency)
		if err != nil {
			return nil, err
		}
	}
	rendered, err := e.renderBytes(raw)
	if err != nil {
		return nil, err
	}
	return e.ResolveManifestBytes(ctx, rendered)
}

// patchAtenetEgressEnvoyConcurrency appends --concurrency <concurrency> to the
// envoy container's args in the atenet-egress Deployment.
func patchAtenetEgressEnvoyConcurrency(raw []byte, concurrency int) ([]byte, error) {
	return appendContainerArgs(raw, "atenet-egress", "envoy", "--concurrency", strconv.Itoa(concurrency))
}

// patchAtenetRouterAllowedSPIFFEIDs appends --ingress-allowed-spiffe-ids to the
// atenet-router container's args in the Envoy router Deployment. The base
// manifest carries no allowlist, because which clients may reach actors is the
// install's to decide. Without one the router serves no traffic.
func (e *Env) patchAtenetRouterAllowedSPIFFEIDs(raw []byte) ([]byte, error) {
	if len(e.Cfg.IngressAllowedSPIFFEIDs) == 0 {
		return raw, nil
	}
	return appendContainerArgs(raw, "atenet-router", "atenet-router",
		"--ingress-allowed-spiffe-ids="+strings.Join(e.Cfg.IngressAllowedSPIFFEIDs, ","))
}

// appendContainerArgs uses a kustomize JSON6902 patch to append args to the
// first container of the named Deployment in ate-system. The patch first
// tests that the first container is the one named, so a reordered manifest
// fails the render instead of patching the wrong container.
func appendContainerArgs(raw []byte, deployment, container string, args ...string) ([]byte, error) {
	dir, err := os.MkdirTemp("", "ate-setup-kustomize-")
	if err != nil {
		return nil, fmt.Errorf("while creating a scratch kustomization: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), raw, 0o600); err != nil {
		return nil, fmt.Errorf("while staging a manifest for kustomize: %w", err)
	}
	var kustomization strings.Builder
	fmt.Fprintf(&kustomization, `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - manifest.yaml
patches:
  - target:
      group: apps
      version: v1
      kind: Deployment
      name: %s
      namespace: ate-system
    patch: |-
      - op: test
        path: /spec/template/spec/containers/0/name
        value: %q
`, deployment, container)
	for _, arg := range args {
		fmt.Fprintf(&kustomization, `      - op: add
        path: /spec/template/spec/containers/0/args/-
        value: %q
`, arg)
	}
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization.String()), 0o600); err != nil {
		return nil, fmt.Errorf("while writing a scratch kustomization: %w", err)
	}
	return kustomize.Build(dir)
}

// patchEnvoyDataplaneImage replaces the ${ENVOY_DATAPLANE_IMAGE} placeholder in
// the manifest with imageRef.
func (e *Env) patchEnvoyDataplaneImage(raw []byte, imageRef string) []byte {
	return bytes.ReplaceAll(raw, []byte("${ENVOY_DATAPLANE_IMAGE}"), []byte(imageRef))
}

// patchAtenetEgressInject replaces the #ATE_EGRESS_INJECT_FLAGS marker in the
// egress sidecar's args with the credential-provider flags, or removes it when
// injection is off, which leaves the gateway with no provider. It takes the
// manifest bytes so it can run after the general patch.
func (e *Env) patchAtenetEgressInject(raw []byte, providers []config.CredentialProvider) ([]byte, error) {
	var flagsBlock string
	if len(providers) > 0 {
		flagsBlock = emitEgressInjectFlags(providers)
	}
	return replaceManifestMarker(raw, "#ATE_EGRESS_INJECT_FLAGS", flagsBlock)
}

// The HTTP and HTTPS listeners share the egress policy and its providers.
func patchAgentgatewayEgressInject(raw []byte, providers []config.CredentialProvider) ([]byte, error) {
	var block string
	for i, p := range providers {
		if i == 0 {
			block = "credentialProviders:"
		}
		block += fmt.Sprintf(`
- uriAuthority: %q
  target:
    host: %q
    policies:
      backendTLS:
        hostname: %q
        cert: /run/podidentity.podcert.ate.dev/credential-bundle.pem
        key: /run/podidentity.podcert.ate.dev/credential-bundle.pem
        root: /run/servicedns-ca/trust-bundle.pem`, p.Name, p.Address, p.ServerName())
	}
	return replaceManifestMarker(raw, "#ATE_AGENTGATEWAY_CREDENTIAL_PROVIDERS", block)
}

func replaceManifestMarker(raw []byte, marker, block string) ([]byte, error) {
	var out []string
	replaced := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), marker) {
			indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
			if block != "" {
				for _, l := range strings.Split(block, "\n") {
					out = append(out, indent+l)
				}
			}
			replaced++
			continue
		}
		out = append(out, line)
	}
	if replaced != 1 {
		return nil, fmt.Errorf("expected 1 %s marker, found %d", marker, replaced)
	}
	return []byte(strings.Join(out, "\n")), nil
}

// emitEgressInjectFlags writes the name, address and server name flags of each
// provider in order, then the CA and client certificate every provider shares.
func emitEgressInjectFlags(providers []config.CredentialProvider) string {
	var flags string
	for _, p := range providers {
		flags += fmt.Sprintf(`- --credential-provider-name=%s
- --credential-provider-address=%s
- --credential-provider-server-name=%s
`, p.Name, p.Address, p.ServerName())
	}
	return flags + `- --credential-provider-ca-file=/run/servicedns.podcert.ate.dev/trust-bundle.pem
- --credential-provider-client-cert=/run/podidentity.podcert.ate.dev/credential-bundle.pem`
}

func (e *Env) patchAtenetEgressManifest() ([]byte, error) {
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

func (e *Env) applyAtenetEgress(ctx context.Context, providers []config.CredentialProvider) error {
	manifests, err := e.renderAtenetEgressManifest(ctx, providers)
	if err != nil {
		return err
	}

	running, err := e.Kube.DeploymentExists(ctx, e.Namespace(), "atenet-egress")
	if err != nil {
		return err
	}

	if err := e.Kube.ApplyBytes(ctx, manifests); err != nil {
		return err
	}

	if running && (e.Cfg.AdditionalEgressExtprocService != "" || len(providers) > 0) {
		if err := e.Kube.RolloutRestartDeployment(ctx, e.Namespace(), "atenet-egress", time.Now()); err != nil {
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

// otelConfigMap is the ConfigMap every control plane component reads its
// telemetry settings from through envFrom.
const otelConfigMap = "ate-otel-config"

// otelEndpointKey is the collector address inside it.
const otelEndpointKey = "OTEL_EXPORTER_OTLP_ENDPOINT"

// otelOverrideDeployments are the control plane Deployments that read
// ate-otel-config. ate-controller additionally copies the values onto the
// worker pods it creates, so one patch reaches the whole system.
var otelOverrideDeployments = []string{"ate-api-server", "ate-controller", "atenet-router", "atenet-egress"}

// applyOtelEndpointOverride points all control plane telemetry at a different
// collector for the duration of a measurement. See
// benchmarking/telemetry/README.md.
//
// Call this AFTER every apply: the ate-system bundle carries its own copy of
// ate-otel-config, so applying it replaces an earlier patch and the endpoint
// silently returns to the cluster default.
//
// A ConfigMap change starts no rollout, because the pod template stays the
// same, so the consumers have to be restarted. Only on an actual change: a
// restart during the bundle's rollout makes the two compete, and the rollout
// wait can then exceed its timeout. An absent workload is not an error,
// because a single-component deploy has only that component.
func (e *Env) applyOtelEndpointOverride(ctx context.Context) error {
	endpoint := e.Cfg.OtlpEndpoint
	if endpoint == "" {
		return nil
	}

	cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), otelConfigMap)
	if err != nil {
		return err
	}
	if cm != nil && cm.Data[otelEndpointKey] == endpoint {
		return nil
	}

	log.Infof("Overriding %s with %s", otelEndpointKey, endpoint)
	if err := e.Kube.MergePatchConfigMap(ctx, e.Namespace(), otelConfigMap,
		map[string]string{otelEndpointKey: endpoint}); err != nil {
		return err
	}

	now := time.Now()
	for _, name := range otelOverrideDeployments {
		if err := e.Kube.RolloutRestartDeployment(ctx, e.Namespace(), name, now); err != nil {
			return err
		}
	}
	return e.Kube.RolloutRestart(ctx, e.Namespace(), "atelet", now)
}
