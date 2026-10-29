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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.ate.dev/substrate/cmd/ate-setup/internal/config"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

// renderedFile is one file RenderAteSystem writes.
type renderedFile struct {
	name     string
	manifest []byte
}

// RenderAteSystem writes the manifests deploy ate-system applies to a fresh
// cluster into dir, one file per apply step. The file names sort in apply
// order. The CRDs in 01-crds.yaml must be established before the rest is
// applied, because 03-sandboxconfig.yaml holds a SandboxConfig.
//
// It contacts no cluster, so it leaves out what deploy ate-system generates or
// reads there:
//
//   - The CA and JWT pool Secrets: actor-id-jwt-pool, actor-id-ca-pool and
//     egress-mitm-ca-pool in the install namespace, and service-dns-ca-pool,
//     pod-identity-ca-pool and postgres-ca-pool in
//     podcertificate-controller-system. Each holds a private key generated for
//     one cluster. Written here, the keys would end up wherever the output
//     goes, and every apply would replace the pools, invalidating every
//     certificate and token issued from them. deploy creates each pool only
//     when it is absent, which a manifest cannot express.
//   - actor-id-ca-certs, the root certificate copied from the actor-id-ca-pool
//     Secret in the cluster.
//   - The k8s-credential-provider namespace policy. Operators edit it, so
//     deploy creates it only when it is absent.
//   - The bundled PostgreSQL identities, which deploy creates by running SQL
//     in the postgres pod.
//   - The CSI drivers.
//
// It also skips deploy's checks against an existing install. The service
// account issuer is the one configuredJWTIssuer names, or the in-cluster
// default. An external database's connection strings stay out of dir:
// ate-api-server-secret-envvars is written only for the bundled database,
// whose connection strings hold no credentials.
func (e *Env) RenderAteSystem(ctx context.Context, dir string) error {
	log.Step("render_ate_system")

	if err := e.RequireCanonicalNamespace("render ate-system"); err != nil {
		return err
	}
	if !e.Cfg.Images.IsPrebuilt() {
		return errors.New("render ate-system pins published images; set --image-repo and --image-tag (ATE_IMAGE_REPO, ATE_IMAGE_TAG)")
	}
	provider, err := e.renderCredentialProvider()
	if err != nil {
		return err
	}
	if err := e.Cfg.RequireTransferServiceAccount(); err != nil {
		return err
	}
	snapshotLocation, err := clusterSnapshotLocation(e.Cfg.SnapshotLocation, e.Cfg.PreviewGates)
	if err != nil {
		return err
	}
	if err := requireEmptyDir(dir); err != nil {
		return err
	}

	var files []renderedFile
	add := func(name string, manifest []byte) {
		files = append(files, renderedFile{name: name, manifest: manifest})
	}

	namespace, err := kube.ReadPath(e.Cfg.Manifest("ate-system-namespace.yaml"))
	if err != nil {
		return err
	}
	add("00-namespace.yaml", namespace)

	crds, err := e.ResolveManifest(ctx, e.Cfg.Manifest("generated"))
	if err != nil {
		return err
	}
	add("01-crds.yaml", crds)

	podcert, err := e.podCertificateControllerObjects(ctx)
	if err != nil {
		return err
	}
	if err := addObjects(add, "02-podcertificate-controller.yaml", podcert); err != nil {
		return err
	}

	sandboxConfig, err := e.sandboxConfigManifest()
	if err != nil {
		return err
	}
	add("03-sandboxconfig.yaml", sandboxConfig)

	system, err := e.renderSystemManifests(ctx)
	if err != nil {
		return err
	}
	systemObjs, err := kube.DecodeManifestBytes(system)
	if err != nil {
		return err
	}
	// Every workload reads ate-otel-config through a required envFrom, so it
	// goes in with the settings, ahead of them, as applyOtelConfig does.
	otelConfig, systemObjs, err := e.takeOtelConfig(systemObjs)
	if err != nil {
		return err
	}

	settings, hash, err := e.apiServerSettingsObjects(snapshotLocation)
	if err != nil {
		return err
	}
	if err := addObjects(add, "04-settings.yaml", append([]*unstructured.Unstructured{otelConfig}, settings...)); err != nil {
		return err
	}

	if e.Cfg.PostgresReadWriteConnectionString == "" {
		postgres, err := e.postgresObjects()
		if err != nil {
			return err
		}
		if err := addObjects(add, "05-postgres.yaml", postgres); err != nil {
			return err
		}
	}

	if err := e.setAPIServerDeploymentFields(systemObjs, hash); err != nil {
		return err
	}
	if err := addObjects(add, "06-system.yaml", systemObjs); err != nil {
		return err
	}

	if provider.Kubernetes() {
		credentialProvider, err := e.renderResolve(ctx, e.k8sCredentialProviderPath(k8sCredentialProviderManifest))
		if err != nil {
			return err
		}
		add("07-k8s-credential-provider.yaml", credentialProvider)
	}

	egress, err := e.renderAtenetEgressManifest(ctx, provider)
	if err != nil {
		return err
	}
	add("08-atenet-egress.yaml", egress)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("while creating %s: %w", dir, err)
	}
	for _, f := range files {
		path := filepath.Join(dir, f.name)
		if err := os.WriteFile(path, f.manifest, 0o644); err != nil {
			return fmt.Errorf("while writing %s: %w", path, err)
		}
		log.Infof("wrote %s", path)
	}
	return nil
}

// renderCredentialProvider is the configured credential provider, or the
// bundled Kubernetes Secrets provider when none is set. The bundled provider
// runs in the install namespace and needs nothing outside the cluster, which
// is why kind installs use it too.
func (e *Env) renderCredentialProvider() (config.CredentialProvider, error) {
	if e.Cfg.CredentialProviderJSON == "" {
		log.Infof("no credential provider set; rendering the bundled %s provider", config.K8sCredentialProviderName)
		return config.CredentialProvider{
			Name:    config.K8sCredentialProviderName,
			Address: config.K8sCredentialProviderAddress,
		}, nil
	}
	return e.Cfg.CredentialProvider()
}

// takeOtelConfig removes the ate-otel-config ConfigMap from objs and returns
// it, with the --otlp-endpoint override applyOtelEndpointOverride makes.
func (e *Env) takeOtelConfig(objs []*unstructured.Unstructured) (*unstructured.Unstructured, []*unstructured.Unstructured, error) {
	for i, obj := range objs {
		if obj.GetKind() != "ConfigMap" || obj.GetName() != otelConfigMap {
			continue
		}
		if endpoint := e.Cfg.OtlpEndpoint; endpoint != "" {
			if err := unstructured.SetNestedField(obj.Object, endpoint, "data", otelEndpointKey); err != nil {
				return nil, nil, fmt.Errorf("while setting %s on %s: %w", otelEndpointKey, kube.Describe(obj), err)
			}
		}
		return obj, append(objs[:i:i], objs[i+1:]...), nil
	}
	return nil, nil, fmt.Errorf("the system manifest has no configmap/%s", otelConfigMap)
}

// addObjects encodes objs and adds them as the file name.
func addObjects(add func(string, []byte), name string, objs []*unstructured.Unstructured) error {
	manifest, err := kube.EncodeManifest(objs)
	if err != nil {
		return err
	}
	add(name, manifest)
	return nil
}

// requireEmptyDir refuses a dir that holds files, so a file left by an earlier
// render with other settings cannot be applied along with this one.
func requireEmptyDir(dir string) error {
	if dir == "" {
		return errors.New("--output-dir (ATE_RENDER_OUTPUT_DIR) must be set")
	}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while reading %s: %w", dir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("%s is not empty; render into a new or empty directory", dir)
	}
	return nil
}

// sandboxConfigManifest is the SandboxConfig admission policy, then the
// default gVisor SandboxConfig, the order DeploySandboxConfig applies them in.
func (e *Env) sandboxConfigManifest() ([]byte, error) {
	var out bytes.Buffer
	for _, name := range []string{"sandboxconfig-validation.yaml", "sandboxconfig-gvisor.yaml"} {
		manifest, err := kube.ReadPath(e.Cfg.Manifest(name))
		if err != nil {
			return nil, err
		}
		out.WriteString("---\n")
		out.Write(manifest)
	}
	return out.Bytes(), nil
}

// apiServerSettingsObjects builds the settings ConfigMaps and Secrets
// EnsureAPIServerPrerequisites writes to a fresh cluster, from the settings
// alone, and the digest of ate-api-server's environment they give it. With an
// external database the digest leaves out the Secret, which is not rendered.
func (e *Env) apiServerSettingsObjects(snapshotLocation string) ([]*unstructured.Unstructured, string, error) {
	apiConfig, err := e.apiConfig()
	if err != nil {
		return nil, "", err
	}
	issuer := e.configuredJWTIssuer()
	if issuer == "" {
		issuer = inClusterIssuer
	}
	configVars := e.apiServerConfigVars(e.Cfg.PostgresReadWriteRole, e.Cfg.PostgresOwnerRole,
		e.Cfg.PostgresPoolMaxConns, snapshotLocation)

	objs := []*unstructured.Unstructured{
		e.configMapObject(ConfigMapAPIConfig, map[string]string{"apiconfig.textproto": apiConfig}),
		e.configMapObject(ConfigMapAPIAuthn, map[string]string{"authentication.yaml": buildAuthenticationConfig(issuer)}),
		e.configMapObject(ConfigMapAPIEnvVars, configVars),
	}

	var secretData map[string][]byte
	if e.Cfg.PostgresReadWriteConnectionString == "" {
		readWriteDSN, ownerDSN, err := e.postgresReadWriteConnectionStrings()
		if err != nil {
			return nil, "", err
		}
		data := buildAPIServerEnvVars(readWriteDSN, ownerDSN, e.Cfg.PostgresSchemaName())
		objs = append(objs, e.secretObject(SecretAPIEnvVars, data))
		secretData = make(map[string][]byte, len(data))
		for k, v := range data {
			secretData[k] = []byte(v)
		}
	} else {
		log.Infof("%s is not rendered: it holds the external database's connection strings. "+
			"Create it before applying, with keys ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING, "+
			"ATE_API_POSTGRES_OWNER_CONNECTION_STRING and ATE_API_POSTGRES_SCHEMA", SecretAPIEnvVars)
	}

	if path := e.Cfg.PostgresServerCAFile; path != "" {
		pem, err := os.ReadFile(path)
		if err != nil {
			return nil, "", fmt.Errorf("reading ATE_API_POSTGRES_SERVER_CA_FILE: %w", err)
		}
		objs = append(objs, e.secretObject(SecretPostgresServerCA, map[string]string{"server-ca.pem": string(pem)}))
	}
	return objs, envHash(configVars, secretData), nil
}

// configMapObject is the ConfigMap ApplyConfigMap would write.
func (e *Env) configMapObject(name string, data map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"data":       stringMap(data),
	}}
	obj.SetName(name)
	obj.SetNamespace(e.Namespace())
	return obj
}

// secretObject is the Secret ApplySecret would write.
func (e *Env) secretObject(name string, data map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"type":       "Opaque",
		"stringData": stringMap(data),
	}}
	obj.SetName(name)
	obj.SetNamespace(e.Namespace())
	return obj
}

// stringMap converts data to the map type unstructured objects hold.
func stringMap(data map[string]string) map[string]any {
	out := make(map[string]any, len(data))
	for k, v := range data {
		out[k] = v
	}
	return out
}
