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

package steps

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/prototext"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/internal/apiconfig"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/localjwtauthority"
	"go.ate.dev/substrate/pkg/proto/ateconfigpb"
)

// Secret and ConfigMap names the control plane reads.
const (
	SecretActorIDJWTPool   = "actor-id-jwt-pool"
	SecretActorIDCAPool    = "actor-id-ca-pool"
	SecretActorIDCACerts   = "actor-id-ca-certs"
	SecretServiceDNSCA     = "service-dns-ca-pool"
	SecretPodIdentityCA    = "pod-identity-ca-pool"
	SecretPostgresCA       = "postgres-ca-pool"
	SecretEgressMITMCAPool = "egress-mitm-ca-pool"
	SecretAPIEnvVars       = "ate-api-server-secret-envvars"
	SecretPostgresServerCA = "postgres-server-ca"
	ConfigMapAPIEnvVars    = "ate-api-server-envvars"
	ConfigMapAPIAuthn      = "ate-api-authentication"
	ConfigMapAPIConfig     = "ate-api-config"
	// poolKeyID is the identifier given to the first CA in a new pool,
	// matching the --ca-id the shell scripts passed.
	poolKeyID = "1"
)

// caValidity is how long a generated root is good for, the same year
// kubectl-ate admin make-ca-pool asks for.
const caValidity = 365 * 24 * time.Hour

// CreateJWTAuthorityPoolSecret generates the actor-identity JWT signing pool.
// An existing pool is kept.
// This is the make-jwt-pool call create_jwt_authority_pool_secret shelled out
// to kubectl-ate for.
func (e *Env) CreateJWTAuthorityPoolSecret(ctx context.Context) error {
	log.Step("create_jwt_authority_pool_secret")
	return e.createJWTPool(ctx, e.Namespace(), SecretActorIDJWTPool)
}

// CreateActorIDCAPoolSecret generates the actor-identity CA pool. External
// relying parties trust this root, so it uses ECDSA P-256, which is more
// broadly accepted than Ed25519.
func (e *Env) CreateActorIDCAPoolSecret(ctx context.Context) error {
	log.Step("create_actor_id_ca_pool_secret")
	return e.createCAPool(ctx, e.Namespace(), SecretActorIDCAPool, localca.KeyTypeECDSAP256)
}

// CreateEgressMITMCAPoolSecret generates the egress MITM CA pool.
func (e *Env) CreateEgressMITMCAPoolSecret(ctx context.Context) error {
	log.Step("create_egress_mitm_ca_pool_secret")
	exists, err := e.Kube.SecretExists(ctx, e.Namespace(), SecretEgressMITMCAPool)
	if err != nil {
		return err
	}
	if exists {
		log.Infof("  CA pool %s/%s already exists; keeping it", e.Namespace(), SecretEgressMITMCAPool)
		return nil
	}

	data, err := newCAPoolSecretData(poolKeyID, localca.KeyTypeECDSAP256)
	if err != nil {
		return fmt.Errorf("while generating the CA pool for %s/%s: %w", e.Namespace(), SecretEgressMITMCAPool, err)
	}
	return e.createPoolSecret(ctx, e.Namespace(), SecretEgressMITMCAPool, corev1.SecretTypeTLS, data)
}

// EnsureEgressMITMCAPoolSecret creates the egress MITM CA pool secret. Both
// dataplanes need it: the agentgateway-egress overlay mounts the same
// Secret the envoy egress does.
func (e *Env) EnsureEgressMITMCAPoolSecret(ctx context.Context) error {
	return e.ensureSecret(ctx, e.Namespace(), SecretEgressMITMCAPool, e.CreateEgressMITMCAPoolSecret)
}

// CreatePodCertificateControllerCAs generates the signer pools the
// podcertificate controller issues from.
func (e *Env) CreatePodCertificateControllerCAs(ctx context.Context) error {
	log.Step("create_podcertificate_controller_cas")
	if err := e.Kube.EnsureNamespace(ctx, NamespacePodCert); err != nil {
		return err
	}
	if err := e.createCAPool(ctx, NamespacePodCert, SecretServiceDNSCA, localca.KeyTypeED25519); err != nil {
		return err
	}
	if err := e.createCAPool(ctx, NamespacePodCert, SecretPodIdentityCA, localca.KeyTypeED25519); err != nil {
		return err
	}
	return e.createCAPool(ctx, NamespacePodCert, SecretPostgresCA, localca.KeyTypeED25519)
}

// CreateActorIDCACertsSecret derives a certificate-only trust bundle from the
// actor-identity CA pool.
//
// The egress gateway verifies actor client certificates and so needs the root,
// but actor-id-ca-pool also holds the CA signing key. This publishes just the
// root.
func (e *Env) CreateActorIDCACertsSecret(ctx context.Context) error {
	log.Step("create_actor_id_ca_certs_secret")
	root, err := e.Kube.CAPoolRootPEM(ctx, e.Namespace(), SecretActorIDCAPool)
	if err != nil {
		return fmt.Errorf("while building %s: %w", SecretActorIDCACerts, err)
	}
	return e.Kube.ApplySecret(ctx, e.Namespace(), SecretActorIDCACerts, map[string]string{
		"ca.crt": string(root),
	})
}

// defaultAPILogLevel is the ate-api-server log level a new install gets when
// none is asked for.
const defaultAPILogLevel = "info"

// CreateAPIConfig writes the ate-api-server config file.
func (e *Env) CreateAPIConfig(ctx context.Context) error {
	log.Step("create_api_config")
	if err := e.Kube.EnsureNamespace(ctx, e.Namespace()); err != nil {
		return err
	}

	logLevel := e.Cfg.APILogLevel
	if logLevel == "" {
		logLevel = defaultAPILogLevel
	}
	apiConfig, err := buildAPIConfig(logLevel)
	if err != nil {
		return err
	}
	log.Infof("%s apiconfig.textproto:", ConfigMapAPIConfig)
	for _, line := range strings.Split(strings.TrimRight(apiConfig, "\n"), "\n") {
		log.Infof("  | %s", line)
	}
	return e.Kube.ApplyConfigMap(ctx, e.Namespace(), ConfigMapAPIConfig, map[string]string{
		"apiconfig.textproto": apiConfig,
	})
}

// UpdateAPIConfig is CreateAPIConfig for an install that is not being
// redeployed: it also stamps the new digest on the running Deployment,
// which rolls ate-api-server onto the new config.
func (e *Env) UpdateAPIConfig(ctx context.Context) error {
	if err := e.CreateAPIConfig(ctx); err != nil {
		return err
	}
	return e.annotateAPIServerEnvHash(ctx)
}

// buildAPIConfig renders apiconfig.textproto.
func buildAPIConfig(logLevel string) (string, error) {
	b, err := prototext.MarshalOptions{Multiline: true}.Marshal(&ateconfigpb.APIConfig{
		Logging: &ateconfigpb.APILogging{Level: logLevel},
	})
	if err != nil {
		return "", fmt.Errorf("while marshaling the ate-api-server config: %w", err)
	}
	return string(b), nil
}

// CreateAPIAuthenticationConfig writes the default ate-api-server
// authentication config, pointing it at the cluster's service account issuer.
func (e *Env) CreateAPIAuthenticationConfig(ctx context.Context) error {
	return e.applyAPIAuthenticationConfig(ctx, &apiconfig.AuthenticationConfig{
		JWTProviders: []apiconfig.JWTProviderConfig{kubernetesJWTProvider(e.jwtIssuer(ctx), e.kubeAPIServerURL())},
	})
}

func (e *Env) applyAPIAuthenticationConfig(ctx context.Context, cfg *apiconfig.AuthenticationConfig) error {
	log.Step("create_api_authentication_config")
	if err := e.Kube.EnsureNamespace(ctx, e.Namespace()); err != nil {
		return err
	}

	authnConfig, err := marshalAuthenticationConfig(cfg)
	if err != nil {
		return err
	}
	// The issuer decides which tokens the apiserver accepts at all, and a
	// wrong one fails as an opaque 401 much later, so show what was written.
	log.Infof("%s authentication.yaml:", ConfigMapAPIAuthn)
	for _, line := range strings.Split(authnConfig, "\n") {
		log.Infof("  | %s", line)
	}
	return e.Kube.ApplyConfigMap(ctx, e.Namespace(), ConfigMapAPIAuthn, map[string]string{
		"authentication.yaml": authnConfig,
	})
}

// UpdateAPIAuthenticationConfig is CreateAPIAuthenticationConfig for an install
// that is not being redeployed: it also stamps the new digest on the running
// Deployment, which rolls ate-api-server onto the new config.
func (e *Env) UpdateAPIAuthenticationConfig(ctx context.Context) error {
	if err := e.CreateAPIAuthenticationConfig(ctx); err != nil {
		return err
	}
	return e.annotateAPIServerEnvHash(ctx)
}

// jwtIssuer determines the service account token issuer to trust.
//
// ate-api-server accepts a token only if its iss claim equals this string
// exactly. EXPECTED_JWT_ISSUER, when set, is that string; the rest is for
// clusters whose issuer follows a standard form — derived from the cluster
// coordinates on GKE, otherwise read from the cluster's OpenID discovery
// document, falling back to the in-cluster default.
func (e *Env) jwtIssuer(ctx context.Context) string {
	cfg := e.Cfg
	if cfg.ExpectedJWTIssuer != "" {
		return cfg.ExpectedJWTIssuer
	}
	if cfg.ProjectID != "" && cfg.ClusterLocation != "" && cfg.ClusterName != "" {
		return fmt.Sprintf("https://container.googleapis.com/v1/projects/%s/locations/%s/clusters/%s",
			cfg.ProjectID, cfg.ClusterLocation, cfg.ClusterName)
	}
	if issuer := e.Kube.OIDCIssuer(ctx); issuer != "" {
		return issuer
	}
	return inClusterIssuer
}

// inClusterIssuer is the default issuer for a cluster that does not publish a
// discoverable one.
const inClusterIssuer = "https://kubernetes.default.svc"

// kubeAPIServerURL is the kube-apiserver URL from the rest.Config the Env's
// client was built with. Empty when there is no client, as in unit tests that
// construct a fake Client without Config, or under NewLocalEnv.
func (e *Env) kubeAPIServerURL() string {
	if e == nil || e.Kube == nil || e.Kube.Config == nil {
		return ""
	}
	return e.Kube.Config.Host
}

// kubernetesJWTProvider builds the default kubernetes JWT provider entry.
//
// An in-cluster issuer is not reachable over public discovery, so the apiserver
// is pointed at its own projected service account CA and token to complete the
// OIDC discovery handshake. A GKE or otherwise external issuer needs neither.
//
// The cluster's own issuer is either of the two kubernetes.default.svc names,
// or any issuer whose host and port match apiServerURL, the kube-apiserver
// URL from the rest config ate-setup uses. apiServerURL may be empty.
func kubernetesJWTProvider(issuer, apiServerURL string) apiconfig.JWTProviderConfig {
	p := apiconfig.JWTProviderConfig{
		Name:      "kubernetes",
		Issuer:    issuer,
		Audiences: []string{"api.ate-system.svc"},
	}
	if issuerNeedsInClusterDiscovery(issuer, apiServerURL) {
		p.CertificateAuthorityFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
		p.DiscoveryTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
	return p
}

// issuerNeedsInClusterDiscovery reports whether ate-api-server must use the
// pod's service account CA and token to fetch the issuer's signing keys.
//
// The two kubernetes.default.svc names are always in-cluster. Any other
// issuer is in-cluster only when its host and port match the kube-apiserver
// URL, so a Talos-style https://<control-plane-ip>:6443 issuer is covered
// and a public issuer such as GKE is not.
func issuerNeedsInClusterDiscovery(issuer, apiServerURL string) bool {
	switch issuer {
	case inClusterIssuer, inClusterIssuer + ".cluster.local":
		return true
	}
	return sameHostPort(issuer, apiServerURL)
}

// sameHostPort reports whether a and b name the same host and port.
// A missing scheme is treated as https, and a missing port as the scheme
// default, so a rest.Config Host of 192.0.2.10:6443 matches
// https://192.0.2.10:6443, and https://apiserver.example matches
// https://apiserver.example:443.
func sameHostPort(a, b string) bool {
	ha, okA := hostPort(a)
	hb, okB := hostPort(b)
	return okA && okB && strings.EqualFold(ha, hb)
}

func hostPort(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "", false
	}
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "https":
			port = "443"
		case "http":
			port = "80"
		default:
			return "", false
		}
	}
	return net.JoinHostPort(u.Hostname(), port), true
}

// setKubernetesJWTProvider replaces the kubernetes entry in cfg, or prepends
// one if none exists, preserving any additional JWT providers.
func setKubernetesJWTProvider(cfg *apiconfig.AuthenticationConfig, issuer, apiServerURL string) {
	k8sProvider := kubernetesJWTProvider(issuer, apiServerURL)
	for i := range cfg.JWTProviders {
		if cfg.JWTProviders[i].Name == k8sProvider.Name {
			cfg.JWTProviders[i] = k8sProvider
			return
		}
	}
	cfg.JWTProviders = append([]apiconfig.JWTProviderConfig{k8sProvider}, cfg.JWTProviders...)
}

// buildAuthenticationConfig renders the default authentication.yaml for issuer.
// apiServerURL is the kube-apiserver URL used to decide whether issuer needs
// in-cluster discovery credentials; it may be empty.
func buildAuthenticationConfig(issuer, apiServerURL string) (string, error) {
	return marshalAuthenticationConfig(&apiconfig.AuthenticationConfig{
		JWTProviders: []apiconfig.JWTProviderConfig{kubernetesJWTProvider(issuer, apiServerURL)},
	})
}

func marshalAuthenticationConfig(cfg *apiconfig.AuthenticationConfig) (string, error) {
	if err := apiconfig.ValidateAuthenticationConfig(cfg); err != nil {
		return "", fmt.Errorf("while validating the ate-api-server authentication config: %w", err)
	}
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("while marshaling the ate-api-server authentication config: %w", err)
	}
	return strings.TrimRight(string(b), "\n"), nil
}

// createCAPool generates a CA pool and stores it in a Secret.
//
// An existing pool is left alone. Regenerating it would rotate the root out
// from under every certificate already issued from it, and the callers in the
// shell script all guarded these calls with an existence check for that reason.
func (e *Env) createCAPool(ctx context.Context, namespace, name string, keyType localca.KeyType) error {
	exists, err := e.Kube.SecretExists(ctx, namespace, name)
	if err != nil {
		return err
	}
	if exists {
		log.Infof("  CA pool %s/%s already exists; keeping it", namespace, name)
		return nil
	}

	data, err := newCAPoolSecretData(poolKeyID, keyType)
	if err != nil {
		return fmt.Errorf("while generating the CA pool for %s/%s: %w", namespace, name, err)
	}
	return e.createPoolSecret(ctx, namespace, name, corev1.SecretTypeTLS, data)
}

// newCAPoolSecretData generates a pool holding one freshly minted CA and
// returns the Secret contents kubectl-ate admin make-ca-pool writes for it: the
// marshaled pool, where the new CA is named and marked active for signing, plus
// the root certificate chain and its private key under the standard TLS keys.
// A pool with no active CA still signs with its first entry, but only as a
// backwards-compatibility fallback.
//
// The certificate and key are not redundant with the pool. Consumers that speak
// TLS rather than the pool format mount them directly — the
// agentgateway-egress overlay mounts tls.crt and tls.key from
// egress-mitm-ca-pool non-optionally, so a pool Secret holding only "pool"
// leaves atenet-egress stuck in ContainerCreating.
func newCAPoolSecretData(id string, keyType localca.KeyType) (map[string][]byte, error) {
	ca, err := localca.GenerateCA(id, keyType, caValidity)
	if err != nil {
		return nil, fmt.Errorf("while generating CA %q: %w", id, err)
	}
	poolBytes, err := localca.Marshal(&localca.ConcretePool{
		CAs:              []*localca.CA{ca},
		ActiveForSigning: id,
	})
	if err != nil {
		return nil, fmt.Errorf("while marshaling the pool for CA %q: %w", id, err)
	}
	certificateChain, err := ca.TLSCertificateChainPEM()
	if err != nil {
		return nil, fmt.Errorf("while encoding the certificate chain for CA %q: %w", id, err)
	}
	privateKey, err := ca.TLSPrivateKeyPEM()
	if err != nil {
		return nil, fmt.Errorf("while encoding the private key for CA %q: %w", id, err)
	}
	return map[string][]byte{
		"pool":                  poolBytes,
		corev1.TLSCertKey:       certificateChain,
		corev1.TLSPrivateKeyKey: privateKey,
	}, nil
}

// createJWTPool generates a JWT authority pool and stores it in a Secret. As
// with the CA pools, an existing pool is preserved: rotating the signing key
// would invalidate every token already minted from it.
func (e *Env) createJWTPool(ctx context.Context, namespace, name string) error {
	existing, err := e.Kube.GetSecret(ctx, namespace, name)
	if err != nil {
		return err
	}
	if existing != nil {
		log.Infof("  JWT authority pool %s/%s already exists; keeping it", namespace, name)
		return nil
	}

	data, err := newJWTPoolSecretData(e.Cfg.ActorJWTAlgorithm)
	if err != nil {
		return fmt.Errorf("while building the JWT pool for %s/%s: %w", namespace, name, err)
	}
	return e.createPoolSecret(ctx, namespace, name, corev1.SecretTypeOpaque, data)
}

// newJWTPoolSecretData generates a pool with one active authority for
// algorithm, keyed by its thumbprint.
func newJWTPoolSecretData(algorithm string) (map[string][]byte, error) {
	poolBytes, _, err := localjwtauthority.GeneratePool(algorithm, "")
	if err != nil {
		return nil, fmt.Errorf("while generating the JWT pool: %w", err)
	}
	return map[string][]byte{"pool": poolBytes}, nil
}

// createPoolSecret writes pool state.
//
// Create, not apply: these Secrets hold generated private key material, and
// creation is guarded by an existence check above. Using apply would let a
// concurrent run overwrite a pool another run just generated.
func (e *Env) createPoolSecret(ctx context.Context, namespace, name string, secretType corev1.SecretType, data map[string][]byte) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Type:       secretType,
		Data:       data,
	}
	if _, err := e.Kube.Typed.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("while creating pool secret %s/%s: %w", namespace, name, err)
	}
	log.Infof("  created pool secret %s/%s", namespace, name)
	return nil
}
