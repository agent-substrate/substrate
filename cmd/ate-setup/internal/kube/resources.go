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

package kube

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applyconfigcorev1 "k8s.io/client-go/applyconfigurations/core/v1"
)

// applyOptions are shared by the typed apply helpers below.
var applyOptions = metav1.ApplyOptions{FieldManager: FieldManager, Force: true}

// EnsureNamespace applies a namespace, replacing the
// `kubectl create namespace <ns> --dry-run=client -o yaml | kubectl apply -f -`
// idiom the shell scripts used to make creation idempotent.
func (c *Client) EnsureNamespace(ctx context.Context, name string) error {
	ns := applyconfigcorev1.Namespace(name)
	if _, err := c.Typed.CoreV1().Namespaces().Apply(ctx, ns, applyOptions); err != nil {
		return fmt.Errorf("while applying namespace %s: %w", name, err)
	}
	return nil
}

// ApplySecret applies an opaque Secret from string data.
func (c *Client) ApplySecret(ctx context.Context, namespace, name string, data map[string]string) error {
	secret := applyconfigcorev1.Secret(name, namespace).
		WithType(corev1.SecretTypeOpaque).
		WithStringData(data)
	if _, err := c.Typed.CoreV1().Secrets(namespace).Apply(ctx, secret, applyOptions); err != nil {
		return fmt.Errorf("while applying secret %s/%s: %w", namespace, name, err)
	}
	return nil
}

// ApplyConfigMap applies a ConfigMap from string data.
func (c *Client) ApplyConfigMap(ctx context.Context, namespace, name string, data map[string]string) error {
	cm := applyconfigcorev1.ConfigMap(name, namespace).WithData(data)
	if _, err := c.Typed.CoreV1().ConfigMaps(namespace).Apply(ctx, cm, applyOptions); err != nil {
		return fmt.Errorf("while applying configmap %s/%s: %w", namespace, name, err)
	}
	return nil
}

// GetSecret returns a Secret, or nil when it does not exist.
func (c *Client) GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	secret, err := c.Typed.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("while getting secret %s/%s: %w", namespace, name, err)
	}
	return secret, nil
}

// SecretExists reports whether a Secret is present.
func (c *Client) SecretExists(ctx context.Context, namespace, name string) (bool, error) {
	secret, err := c.GetSecret(ctx, namespace, name)
	return secret != nil, err
}

// ConfigMapExists reports whether a ConfigMap is present.
func (c *Client) ConfigMapExists(ctx context.Context, namespace, name string) (bool, error) {
	_, err := c.Typed.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("while getting configmap %s/%s: %w", namespace, name, err)
	}
	return true, nil
}

// DeploymentExists reports whether a Deployment is present. delete_demo_actors
// used this to decide whether the control plane is still up before trying to
// talk to it.
func (c *Client) DeploymentExists(ctx context.Context, namespace, name string) (bool, error) {
	_, err := c.Typed.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("while getting deployment %s/%s: %w", namespace, name, err)
	}
	return true, nil
}

// OIDCIssuer reads the cluster's OpenID configuration and returns its issuer.
// An empty string means the endpoint is unavailable or has no issuer, which
// callers treat as "fall back to the in-cluster default".
func (c *Client) OIDCIssuer(ctx context.Context) string {
	raw, err := c.Typed.Discovery().RESTClient().
		Get().AbsPath("/.well-known/openid-configuration").
		DoRaw(ctx)
	if err != nil {
		return ""
	}
	var doc struct {
		Issuer string `json:"issuer"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return ""
	}
	return doc.Issuer
}
