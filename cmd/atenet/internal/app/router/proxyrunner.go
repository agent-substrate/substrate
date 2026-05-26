//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package router

import (
	"context"
	"fmt"
	"log/slog"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ProxyDeploymentName = "atenet-router-proxy"
	ProxyServiceName    = "atenet-router-proxy"
	ProxyConfigMapName  = "atenet-router-proxy-config"
)

// proxyrunner manages the dynamic deployment and lifecycle of the configured
// networking proxy instance running inside Kubernetes.
type proxyrunner struct {
	k8sClient client.Client
	cfg       RouterConfig
	provider  proxyProvider
}

func newProxyRunner(k8sClient client.Client, cfg RouterConfig, provider proxyProvider) *proxyrunner {
	return &proxyrunner{
		k8sClient: k8sClient,
		cfg:       cfg,
		provider:  provider,
	}
}

func (r *proxyrunner) reconcile(ctx context.Context) error {
	if err := r.reconcileProxyConfigMap(ctx); err != nil {
		return fmt.Errorf("failed configmap reconciliation: %w", err)
	}

	if err := r.reconcileProxyDeployment(ctx); err != nil {
		return fmt.Errorf("failed deployment reconciliation: %w", err)
	}

	if err := r.reconcileProxyService(ctx); err != nil {
		return fmt.Errorf("failed service reconciliation: %w", err)
	}

	return nil
}

func (r *proxyrunner) reconcileProxyConfigMap(ctx context.Context) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ProxyConfigMapName,
			Namespace: r.cfg.Namespace,
		},
		Data: r.provider.ConfigMapData(),
	}

	var existing corev1.ConfigMap
	err := r.k8sClient.Get(ctx, client.ObjectKey{Namespace: r.cfg.Namespace, Name: ProxyConfigMapName}, &existing)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.InfoContext(ctx, "Creating proxy ConfigMap",
				slog.String("namespace", r.cfg.Namespace),
				slog.String("name", ProxyConfigMapName),
				slog.String("provider", r.provider.Name()))
			return r.k8sClient.Create(ctx, cm)
		}
		return err
	}

	existing.Data = cm.Data
	return r.k8sClient.Update(ctx, &existing)
}

func (r *proxyrunner) reconcileProxyDeployment(ctx context.Context) error {
	replicas := int32(1)
	labels := map[string]string{
		"app":                      "atenet-router-proxy",
		"substrate.ate.dev/proxy":  r.provider.Name(),
		"substrate.ate.dev/router": "atenet-router",
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ProxyDeploymentName,
			Namespace: r.cfg.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{r.provider.Container()},
					Volumes: []corev1.Volume{
						{
							Name: "proxy-config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: ProxyConfigMapName,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	var existing appsv1.Deployment
	err := r.k8sClient.Get(ctx, client.ObjectKey{Namespace: r.cfg.Namespace, Name: ProxyDeploymentName}, &existing)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.InfoContext(ctx, "Creating managed router proxy Deployment",
				slog.String("namespace", r.cfg.Namespace),
				slog.String("provider", r.provider.Name()))
			return r.k8sClient.Create(ctx, dep)
		}
		return err
	}

	existing.Spec = dep.Spec
	return r.k8sClient.Update(ctx, &existing)
}

func (r *proxyrunner) reconcileProxyService(ctx context.Context) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ProxyServiceName,
			Namespace: r.cfg.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				"app":                      "atenet-router-proxy",
				"substrate.ate.dev/proxy":  r.provider.Name(),
				"substrate.ate.dev/router": "atenet-router",
			},
			Ports: r.provider.ServicePorts(),
		},
	}

	var existing corev1.Service
	err := r.k8sClient.Get(ctx, client.ObjectKey{Namespace: r.cfg.Namespace, Name: ProxyServiceName}, &existing)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.InfoContext(ctx, "Creating managed router proxy ClusterIP service",
				slog.String("namespace", r.cfg.Namespace),
				slog.String("provider", r.provider.Name()))
			return r.k8sClient.Create(ctx, svc)
		}
		return err
	}

	existing.Spec.Ports = svc.Spec.Ports
	existing.Spec.Selector = svc.Spec.Selector
	return r.k8sClient.Update(ctx, &existing)
}
