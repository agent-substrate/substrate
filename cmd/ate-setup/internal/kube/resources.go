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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	applyconfigcorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/yaml"
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

// GetConfigMap returns a ConfigMap, or nil when it does not exist.
func (c *Client) GetConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	cm, err := c.Typed.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("while getting configmap %s/%s: %w", namespace, name, err)
	}
	return cm, nil
}

// MergePatchConfigMap merges keys into a ConfigMap's data, leaving the rest of
// it alone. Unlike an apply this claims no ownership of the keys it does not
// name, which is what lets the otel endpoint override amend a ConfigMap the
// bundle owns.
func (c *Client) MergePatchConfigMap(ctx context.Context, namespace, name string, data map[string]string) error {
	patch, err := json.Marshal(map[string]any{"data": data})
	if err != nil {
		return fmt.Errorf("while building the patch for configmap %s/%s: %w", namespace, name, err)
	}
	if _, err := c.Typed.CoreV1().ConfigMaps(namespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("while patching configmap %s/%s: %w", namespace, name, err)
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

// GetDeployment returns a Deployment, or nil when it does not exist.
func (c *Client) GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error) {
	dep, err := c.Typed.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("while getting deployment %s/%s: %w", namespace, name, err)
	}
	return dep, nil
}

// DeploymentExists reports whether a Deployment is present. delete_demo_actors
// used this to decide whether the control plane is still up before trying to
// talk to it.
func (c *Client) DeploymentExists(ctx context.Context, namespace, name string) (bool, error) {
	dep, err := c.GetDeployment(ctx, namespace, name)
	return dep != nil, err
}

// PatchDeployment applies a strategic merge patch to a Deployment. This is the
// `kubectl patch deployment` of the shell installer, including its
// --patch-file form: patch may be JSON or YAML.
func (c *Client) PatchDeployment(ctx context.Context, namespace, name string, patch []byte) error {
	asJSON, err := yaml.YAMLToJSON(patch)
	if err != nil {
		return fmt.Errorf("while parsing the patch for deployment %s/%s: %w", namespace, name, err)
	}
	if _, err := c.Typed.AppsV1().Deployments(namespace).Patch(
		ctx, name, types.StrategicMergePatchType, asJSON, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("while patching deployment %s/%s: %w", namespace, name, err)
	}
	return nil
}

// DaemonSetNames lists the DaemonSets in a namespace matching a label
// selector. The atelet DaemonSet name carries a substrate version suffix, so
// the installed versions can only be found by label.
func (c *Client) DaemonSetNames(ctx context.Context, namespace, selector string) ([]string, error) {
	list, err := c.Typed.AppsV1().DaemonSets(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("while listing daemonsets in %s matching %q: %w", namespace, selector, err)
	}
	names := make([]string, 0, len(list.Items))
	for _, ds := range list.Items {
		names = append(names, ds.Name)
	}
	return names, nil
}

// SetServiceAccountAnnotation adds or removes one annotation on a
// ServiceAccount; an empty value removes it. A missing ServiceAccount is not
// an error, matching the `|| true` the shell installer removed one under.
func (c *Client) SetServiceAccountAnnotation(ctx context.Context, namespace, name, key, value string) error {
	var annotation any
	if value != "" {
		annotation = value
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{key: annotation}},
	})
	if err != nil {
		return fmt.Errorf("while building the patch for serviceaccount %s/%s: %w", namespace, name, err)
	}
	if _, err := c.Typed.CoreV1().ServiceAccounts(namespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("while annotating serviceaccount %s/%s: %w", namespace, name, err)
	}
	return nil
}

// ServiceAccountAnnotation reads one annotation from a ServiceAccount. A
// missing ServiceAccount or annotation yields the empty string.
func (c *Client) ServiceAccountAnnotation(ctx context.Context, namespace, name, key string) (string, error) {
	sa, err := c.Typed.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("while getting serviceaccount %s/%s: %w", namespace, name, err)
	}
	return sa.Annotations[key], nil
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
