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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
)

// Secret and ConfigMap names the control plane reads.
const (
	SecretActorIDJWTPool   = "actor-id-jwt-pool"
	SecretActorIDCAPool    = "actor-id-ca-pool"
	SecretActorIDCACerts   = "actor-id-ca-certs"
	SecretServiceDNSCA     = "service-dns-ca-pool"
	SecretPodIdentityCA    = "pod-identity-ca-pool"
	SecretEgressMITMCAPool = "egress-mitm-ca-pool"
	ConfigMapAPIEnvVars    = "ate-api-server-envvars"
	ConfigMapAPIAuthn      = "ate-api-authentication"
	// poolKeyID is the identifier given to the first CA and JWT key in a new
	// pool, matching the --ca-id/--key-id the shell scripts passed.
	poolKeyID = "1"
)

// caValidity is how long a generated root is good for, the same year
// kubectl-ate admin make-ca-pool asks for.
const caValidity = 365 * 24 * time.Hour

// CreateJWTAuthorityPoolSecret generates the actor-identity JWT signing pool.
// This is the make-jwt-pool call create_jwt_authority_pool_secret shelled out
// to kubectl-ate for.
func (e *Env) CreateJWTAuthorityPoolSecret(ctx context.Context) error {
	log.Step("create_jwt_authority_pool_secret")
	return e.createJWTPool(ctx, NamespaceAteSystem, SecretActorIDJWTPool)
}

// CreateActorIDCAPoolSecret generates the actor-identity CA pool.
func (e *Env) CreateActorIDCAPoolSecret(ctx context.Context) error {
	log.Step("create_actor_id_ca_pool_secret")
	return e.createCAPool(ctx, NamespaceAteSystem, SecretActorIDCAPool)
}

// CreateEgressMITMCAPoolSecret generates the egress MITM CA pool.
func (e *Env) CreateEgressMITMCAPoolSecret(ctx context.Context) error {
	log.Step("create_egress_mitm_ca_pool_secret")
	exists, err := e.Kube.SecretExists(ctx, NamespaceAteSystem, SecretEgressMITMCAPool)
	if err != nil {
		return err
	}
	if exists {
		log.Infof("  CA pool %s/%s already exists; keeping it", NamespaceAteSystem, SecretEgressMITMCAPool)
		return nil
	}

	poolBytes, err := newCAPoolBytes(poolKeyID, localca.KeyTypeECDSAP256)
	if err != nil {
		return fmt.Errorf("while generating the CA pool for %s/%s: %w", NamespaceAteSystem, SecretEgressMITMCAPool, err)
	}
	return e.createPoolSecret(ctx, NamespaceAteSystem, SecretEgressMITMCAPool, poolBytes)
}

// EnsureEgressMITMCAPoolSecret creates the egress MITM CA pool secret if sdsmint is enabled.
func (e *Env) EnsureEgressMITMCAPoolSecret(ctx context.Context) error {
	if e.Cfg.Router == config.RouterAgentgateway || !e.Cfg.ExperimentalUseSDSMint {
		return nil
	}
	return e.ensureSecret(ctx, NamespaceAteSystem, SecretEgressMITMCAPool, e.CreateEgressMITMCAPoolSecret)
}

// CreatePodCertificateControllerCAs generates the two signer pools the
// podcertificate controller issues from.
func (e *Env) CreatePodCertificateControllerCAs(ctx context.Context) error {
	log.Step("create_podcertificate_controller_cas")
	if err := e.Kube.EnsureNamespace(ctx, NamespacePodCert); err != nil {
		return err
	}
	if err := e.createCAPool(ctx, NamespacePodCert, SecretServiceDNSCA); err != nil {
		return err
	}
	return e.createCAPool(ctx, NamespacePodCert, SecretPodIdentityCA)
}

// CreateActorIDCACertsSecret derives a certificate-only trust bundle from the
// actor-identity CA pool.
//
// The egress gateway verifies actor client certificates and so needs the root,
// but actor-id-ca-pool also holds the CA signing key. This publishes just the
// root.
func (e *Env) CreateActorIDCACertsSecret(ctx context.Context) error {
	log.Step("create_actor_id_ca_certs_secret")
	root, err := e.Kube.CAPoolRootPEM(ctx, NamespaceAteSystem, SecretActorIDCAPool)
	if err != nil {
		return fmt.Errorf("while building %s: %w", SecretActorIDCACerts, err)
	}
	return e.Kube.ApplySecret(ctx, NamespaceAteSystem, SecretActorIDCACerts, map[string]string{
		"ca.crt": string(root),
	})
}

// CreateAPIServerEnvVars writes the ConfigMap that tells ate-api-server how to
// reach its PostgreSQL store. ate-api-server.yaml pulls it in via an optional
// envFrom and resolves --postgres-connection-string=@env and
// --postgres-schema=@env from it.
func (e *Env) CreateAPIServerEnvVars(ctx context.Context) error {
	log.Step("create_api_server_env_vars")
	if err := e.Kube.EnsureNamespace(ctx, NamespaceAteSystem); err != nil {
		return err
	}

	connString := e.Cfg.PostgresConnString()
	log.Infof("POSTGRES_CONNECTION_STRING: %s", connString)

	return e.Kube.ApplyConfigMap(ctx, NamespaceAteSystem, ConfigMapAPIEnvVars,
		buildAPIServerEnvVars(connString, e.Cfg.PostgresSchemaName()))
}

// buildAPIServerEnvVars is the ConfigMap payload. ate-api-server takes the
// connection string and the schema from it, and exits on an empty schema; an
// unrecognized key here reaches the container as a stray environment variable,
// so the set stays exactly what the shell installer's
// create_api_server_env_vars writes.
func buildAPIServerEnvVars(connString, schema string) map[string]string {
	return map[string]string{
		"ATE_API_POSTGRES_CONNECTION_STRING": connString,
		"ATE_API_POSTGRES_SCHEMA":            schema,
	}
}

// CreateAPIAuthenticationConfig writes the default ate-api-server
// authentication config, pointing it at the cluster's service account issuer.
func (e *Env) CreateAPIAuthenticationConfig(ctx context.Context) error {
	log.Step("create_api_authentication_config")
	if err := e.Kube.EnsureNamespace(ctx, NamespaceAteSystem); err != nil {
		return err
	}

	authnConfig := buildAuthenticationConfig(e.jwtIssuer(ctx))
	return e.Kube.ApplyConfigMap(ctx, NamespaceAteSystem, ConfigMapAPIAuthn, map[string]string{
		"authentication.yaml": authnConfig,
	})
}

// jwtIssuer determines the service account token issuer to trust. On GKE it is
// derived from the cluster coordinates; otherwise it comes from the cluster's
// OpenID discovery document, falling back to the in-cluster default.
func (e *Env) jwtIssuer(ctx context.Context) string {
	cfg := e.Cfg
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

// buildAuthenticationConfig renders authentication.yaml.
//
// An in-cluster issuer is not reachable over public discovery, so the apiserver
// is pointed at its own projected service account CA and token to complete the
// OIDC discovery handshake. A GKE or otherwise external issuer needs neither.
func buildAuthenticationConfig(issuer string) string {
	config := fmt.Sprintf(
		"actorIdentityJWTProvider: kubernetes\njwtProviders:\n- name: kubernetes\n  issuer: %s\n  audiences: [api.ate-system.svc]\n",
		issuer)
	switch issuer {
	case inClusterIssuer, inClusterIssuer + ".cluster.local":
		config += "  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n" +
			"  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token\n"
	}
	// The shell script built this inside $(...), which strips trailing
	// newlines. Matching that keeps the ConfigMap byte-identical, so switching
	// between the two installers does not rewrite it.
	return strings.TrimRight(config, "\n")
}

// createCAPool generates a CA pool and stores it in a Secret.
//
// An existing pool is left alone. Regenerating it would rotate the root out
// from under every certificate already issued from it, and the callers in the
// shell script all guarded these calls with an existence check for that reason.
func (e *Env) createCAPool(ctx context.Context, namespace, name string) error {
	exists, err := e.Kube.SecretExists(ctx, namespace, name)
	if err != nil {
		return err
	}
	if exists {
		log.Infof("  CA pool %s/%s already exists; keeping it", namespace, name)
		return nil
	}

	poolBytes, err := newCAPoolBytes(poolKeyID, localca.KeyTypeED25519)
	if err != nil {
		return fmt.Errorf("while generating the CA pool for %s/%s: %w", namespace, name, err)
	}
	return e.createPoolSecret(ctx, namespace, name, poolBytes)
}

// newCAPoolBytes generates a pool holding one freshly minted CA, marshaled the
// way kubectl-ate admin make-ca-pool writes it: the new CA is named and marked
// active for signing. A pool with no active CA still signs with its first entry,
// but only as a backwards-compatibility fallback.
func newCAPoolBytes(id string, keyType localca.KeyType) ([]byte, error) {
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
	return poolBytes, nil
}

// createJWTPool generates a JWT authority pool and stores it in a Secret. As
// with the CA pools, an existing pool is preserved: rotating the signing key
// would invalidate every token already minted from it.
func (e *Env) createJWTPool(ctx context.Context, namespace, name string) error {
	exists, err := e.Kube.SecretExists(ctx, namespace, name)
	if err != nil {
		return err
	}
	if exists {
		log.Infof("  JWT authority pool %s/%s already exists; keeping it", namespace, name)
		return nil
	}

	authority, err := localjwtauthority.GenerateECDSAP256Authority(poolKeyID)
	if err != nil {
		return fmt.Errorf("while generating the JWT authority for %s/%s: %w", namespace, name, err)
	}
	poolBytes, err := localjwtauthority.Marshal(&localjwtauthority.Pool{
		Authorities: []*localjwtauthority.Authority{authority},
	})
	if err != nil {
		return fmt.Errorf("while marshaling the JWT pool for %s/%s: %w", namespace, name, err)
	}
	return e.createPoolSecret(ctx, namespace, name, poolBytes)
}

// createPoolSecret writes pool state.
//
// Create, not apply: these Secrets hold generated private key material, and
// creation is guarded by an existence check above. Using apply would let a
// concurrent run overwrite a pool another run just generated.
func (e *Env) createPoolSecret(ctx context.Context, namespace, name string, poolBytes []byte) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Data:       map[string][]byte{"pool": poolBytes},
	}
	if _, err := e.Kube.Typed.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("while creating pool secret %s/%s: %w", namespace, name, err)
	}
	log.Infof("  created pool secret %s/%s", namespace, name)
	return nil
}
