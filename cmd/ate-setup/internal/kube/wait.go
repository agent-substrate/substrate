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
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// Workload kinds accepted by RolloutStatus.
const (
	KindDeployment  = "deployment"
	KindDaemonSet   = "daemonset"
	KindStatefulSet = "statefulset"
)

// clusterTrustBundleGVK identifies the certificates.k8s.io ClusterTrustBundle
// the podcertificate controller publishes.
var clusterTrustBundleGVK = schema.GroupVersionKind{
	Group:   "certificates.k8s.io",
	Version: "v1beta1",
	Kind:    "ClusterTrustBundle",
}

// RolloutStatus blocks until a workload has finished rolling out, replacing
// `kubectl rollout status <kind>/<name> -n <ns> --timeout=<t>`. The readiness
// conditions match kubectl's own status viewers.
func (c *Client) RolloutStatus(ctx context.Context, kind, namespace, name string, timeout time.Duration) error {
	var lastMsg string
	var notFoundCount int
	err := poll(ctx, timeout, func(ctx context.Context) (bool, error) {
		var done bool
		var msg string
		var err error
		switch kind {
		case KindDeployment:
			done, msg, err = c.deploymentRolledOut(ctx, namespace, name)
		case KindDaemonSet:
			done, msg, err = c.daemonSetRolledOut(ctx, namespace, name)
		case KindStatefulSet:
			done, msg, err = c.statefulSetRolledOut(ctx, namespace, name)
		default:
			return false, fmt.Errorf("cannot wait on unsupported kind %q", kind)
		}
		if err != nil {
			// The workload may not exist yet when its manifest was applied
			// moments ago; keep waiting briefly rather than failing immediately.
			if apierrors.IsNotFound(err) {
				notFoundCount++
				if notFoundCount > 5 {
					return false, fmt.Errorf("%s/%s not found in namespace %s: %w", kind, name, namespace, err)
				}
				lastMsg = fmt.Sprintf("%s/%s not created yet", kind, name)
				return false, nil
			}
			return false, err
		}
		lastMsg = msg
		return done, nil
	})
	if err != nil {
		return fmt.Errorf("waiting for %s/%s in %s: %w (last status: %s)", kind, name, namespace, err, lastMsg)
	}
	return nil
}

func (c *Client) deploymentRolledOut(ctx context.Context, namespace, name string) (bool, string, error) {
	d, err := c.Typed.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, "", err
	}
	if d.Generation > d.Status.ObservedGeneration {
		return false, "waiting for the controller to observe the update", nil
	}
	for _, cond := range d.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing && cond.Reason == "ProgressDeadlineExceeded" {
			return false, "", fmt.Errorf("deployment %s/%s exceeded its progress deadline", namespace, name)
		}
	}
	desired := int32(1)
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	if d.Status.UpdatedReplicas < desired {
		return false, fmt.Sprintf("%d/%d replicas updated", d.Status.UpdatedReplicas, desired), nil
	}
	if d.Status.Replicas > d.Status.UpdatedReplicas {
		return false, fmt.Sprintf("%d old replicas pending termination", d.Status.Replicas-d.Status.UpdatedReplicas), nil
	}
	if d.Status.AvailableReplicas < d.Status.UpdatedReplicas {
		return false, fmt.Sprintf("%d/%d replicas available", d.Status.AvailableReplicas, d.Status.UpdatedReplicas), nil
	}
	return true, "rolled out", nil
}

func (c *Client) daemonSetRolledOut(ctx context.Context, namespace, name string) (bool, string, error) {
	ds, err := c.Typed.AppsV1().DaemonSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, "", err
	}
	if ds.Spec.UpdateStrategy.Type != appsv1.RollingUpdateDaemonSetStrategyType {
		return true, "not a rolling update strategy", nil
	}
	if ds.Generation > ds.Status.ObservedGeneration {
		return false, "waiting for the controller to observe the update", nil
	}
	if ds.Status.UpdatedNumberScheduled < ds.Status.DesiredNumberScheduled {
		return false, fmt.Sprintf("%d/%d nodes updated",
			ds.Status.UpdatedNumberScheduled, ds.Status.DesiredNumberScheduled), nil
	}
	if ds.Status.NumberAvailable < ds.Status.DesiredNumberScheduled {
		return false, fmt.Sprintf("%d/%d nodes available",
			ds.Status.NumberAvailable, ds.Status.DesiredNumberScheduled), nil
	}
	return true, "rolled out", nil
}

func (c *Client) statefulSetRolledOut(ctx context.Context, namespace, name string) (bool, string, error) {
	ss, err := c.Typed.AppsV1().StatefulSets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, "", err
	}
	if ss.Spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		return true, "not a rolling update strategy", nil
	}
	if ss.Status.ObservedGeneration < ss.Generation {
		return false, "waiting for the controller to observe the update", nil
	}
	desired := int32(1)
	if ss.Spec.Replicas != nil {
		desired = *ss.Spec.Replicas
	}
	if ss.Status.ReadyReplicas < desired {
		return false, fmt.Sprintf("%d/%d replicas ready", ss.Status.ReadyReplicas, desired), nil
	}
	// With a partitioned rolling update only the pods at or above the
	// partition are expected to be updated.
	if ru := ss.Spec.UpdateStrategy.RollingUpdate; ru != nil && ru.Partition != nil {
		expected := desired - *ru.Partition
		if ss.Status.UpdatedReplicas < expected {
			return false, fmt.Sprintf("%d/%d replicas updated", ss.Status.UpdatedReplicas, expected), nil
		}
		return true, "partitioned rollout complete", nil
	}
	if ss.Status.UpdateRevision != ss.Status.CurrentRevision {
		return false, "waiting for the update revision to become current", nil
	}
	return true, "rolled out", nil
}

// WaitNamespaceActive replaces
// `kubectl wait --for=jsonpath='{.status.phase}'=Active namespace/<name>`.
func (c *Client) WaitNamespaceActive(ctx context.Context, name string, timeout time.Duration) error {
	err := poll(ctx, timeout, func(ctx context.Context) (bool, error) {
		ns, err := c.Typed.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		return ns.Status.Phase == corev1.NamespaceActive, nil
	})
	if err != nil {
		return fmt.Errorf("waiting for namespace %s to become Active: %w", name, err)
	}
	return nil
}

// WaitCondition blocks until a custom resource reports the named status
// condition as True, replacing `kubectl wait --for=condition=<type>`.
func (c *Client) WaitCondition(ctx context.Context, gvk schema.GroupVersionKind, namespace, name, condition string, timeout time.Duration) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(namespace)
	obj.SetName(name)

	ri, err := c.resourceFor(obj)
	if err != nil {
		return err
	}

	err = poll(ctx, timeout, func(ctx context.Context) (bool, error) {
		current, err := ri.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		conds, found, err := unstructured.NestedSlice(current.Object, "status", "conditions")
		if err != nil || !found {
			return false, nil
		}
		for _, raw := range conds {
			cond, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if cond["type"] == condition {
				return cond["status"] == string(corev1.ConditionTrue), nil
			}
		}
		return false, nil
	})
	if err != nil {
		return fmt.Errorf("waiting for %s %s/%s to report %s=True: %w", gvk.Kind, namespace, name, condition, err)
	}
	return nil
}

// WaitClusterTrustBundles blocks until the podcertificate controller has
// published the identity bundles the rest of the install depends on.
func (c *Client) WaitClusterTrustBundles(ctx context.Context, names []string, timeout time.Duration) error {
	for _, name := range names {
		var lastErr error
		err := poll(ctx, timeout, func(ctx context.Context) (bool, error) {
			ok, err := c.Exists(ctx, clusterTrustBundleGVK, "", name)
			if err != nil {
				lastErr = err
				return false, nil
			}
			return ok, nil
		})
		if err != nil {
			if lastErr != nil {
				return fmt.Errorf("waiting for ClusterTrustBundle %s: %w (last discovery error: %v)", name, err, lastErr)
			}
			return fmt.Errorf("waiting for ClusterTrustBundle %s: %w", name, err)
		}
	}
	return nil
}

// WaitDeleted blocks until the named object is no longer present.
func (c *Client) WaitDeleted(ctx context.Context, gvk schema.GroupVersionKind, namespace, name string, timeout time.Duration) error {
	err := poll(ctx, timeout, func(ctx context.Context) (bool, error) {
		exists, err := c.Exists(ctx, gvk, namespace, name)
		if err != nil {
			return false, err
		}
		return !exists, nil
	})
	if err != nil {
		return fmt.Errorf("waiting for %s %s/%s to be deleted: %w", gvk.Kind, namespace, name, err)
	}
	return nil
}

// RolloutRestartDeployment triggers a restart of a Deployment by stamping the pod
// template with the restartedAt annotation.
func (c *Client) RolloutRestartDeployment(ctx context.Context, namespace, name string, now time.Time) error {
	patch := fmt.Sprintf(
		`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`,
		now.Format(time.RFC3339))
	_, err := c.Typed.AppsV1().Deployments(namespace).Patch(
		ctx, name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("while restarting deployment/%s in %s: %w", name, namespace, err)
	}
	return nil
}

// RolloutRestart triggers a restart of a DaemonSet by stamping the pod
// template, the same annotation `kubectl rollout restart` writes. A missing
// DaemonSet is not an error: the CSI setup restarts atelet only if present.
func (c *Client) RolloutRestart(ctx context.Context, namespace, name string, now time.Time) error {
	patch := fmt.Sprintf(
		`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`,
		now.Format(time.RFC3339))
	_, err := c.Typed.AppsV1().DaemonSets(namespace).Patch(
		ctx, name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("while restarting daemonset/%s in %s: %w", name, namespace, err)
	}
	return nil
}
