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
	"context"
	"fmt"
	"log/slog"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	EnvoyDeploymentName = "atenet-router-envoy"
	EnvoyServiceName    = "atenet-router-envoy"
	EnvoyConfigMapName  = "atenet-router-envoy-config"
)

// envoyrunner manages the dynamic deployment and lifecycle of the underlying
// Envoy proxy instance running inside Kubernetes.
type envoyrunner struct {
	k8sClient client.Client
	cfg       RouterConfig
}

func newEnvoyRunner(k8sClient client.Client, cfg RouterConfig) *envoyrunner {
	return &envoyrunner{
		k8sClient: k8sClient,
		cfg:       cfg,
	}
}

func (r *envoyrunner) reconcile(ctx context.Context) error {
	if err := r.reconcileEnvoyConfigMap(ctx); err != nil {
		return fmt.Errorf("failed configmap reconciliation: %w", err)
	}

	if err := r.reconcileEnvoyDeployment(ctx); err != nil {
		return fmt.Errorf("failed deployment reconciliation: %w", err)
	}

	if err := r.reconcileEnvoyService(ctx); err != nil {
		return fmt.Errorf("failed service reconciliation: %w", err)
	}

	return nil
}

func (r *envoyrunner) reconcileEnvoyConfigMap(ctx context.Context) error {
	envoyYaml := fmt.Sprintf(`admin:
  address:
    socket_address:
      address: 0.0.0.0
      port_value: 9901

node:
  id: %s
  cluster: substrate-router-cluster

dynamic_resources:
  lds_config:
    resource_api_version: V3
    ads: {}
  cds_config:
    resource_api_version: V3
    ads: {}
  ads_config:
    api_type: GRPC
    transport_api_version: V3
    grpc_services:
    - envoy_grpc:
        cluster_name: xds_cluster

static_resources:
  clusters:
  - name: xds_cluster
    connect_timeout: 0.25s
    type: STRICT_DNS
    lb_policy: ROUND_ROBIN
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config:
          http2_protocol_options: {}
    load_assignment:
      cluster_name: xds_cluster
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: atenet-router
                port_value: %d
`, NodeID, r.cfg.XdsPort)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      EnvoyConfigMapName,
			Namespace: r.cfg.Namespace,
		},
		Data: map[string]string{
			"envoy.yaml": envoyYaml,
		},
	}

	var existing corev1.ConfigMap
	err := r.k8sClient.Get(ctx, client.ObjectKey{Namespace: r.cfg.Namespace, Name: EnvoyConfigMapName}, &existing)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.InfoContext(ctx, "Creating Envoy bootstrap ConfigMap",
				slog.String("namespace", r.cfg.Namespace),
				slog.String("name", EnvoyConfigMapName))
			return r.k8sClient.Create(ctx, cm)
		}
		return err
	}

	existing.Data = cm.Data
	return r.k8sClient.Update(ctx, &existing)
}

func (r *envoyrunner) reconcileEnvoyDeployment(ctx context.Context) error {
	replicas := int32(1)

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      EnvoyDeploymentName,
			Namespace: r.cfg.Namespace,
			Labels: map[string]string{
				"app": "atenet-router-envoy",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "atenet-router-envoy",
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "atenet-router-envoy",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "envoy",
							Image: r.cfg.EnvoyImage,
							Command: []string{
								"/usr/local/bin/envoy",
								"-c",
								"/etc/envoy/envoy.yaml",
								"--component-log-level",
								"upstream:debug,router:debug,ext_proc:debug",
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "http",
									ContainerPort: int32(r.cfg.HttpPort),
								},
								{
									Name:          "admin",
									ContainerPort: 9901,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "envoy-config",
									MountPath: "/etc/envoy",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "envoy-config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: EnvoyConfigMapName,
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
	err := r.k8sClient.Get(ctx, client.ObjectKey{Namespace: r.cfg.Namespace, Name: EnvoyDeploymentName}, &existing)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.InfoContext(ctx, "Creating managed Envoy router Deployment", slog.String("namespace", r.cfg.Namespace))
			return r.k8sClient.Create(ctx, dep)
		}
		return err
	}

	existing.Spec = dep.Spec
	return r.k8sClient.Update(ctx, &existing)
}

func (r *envoyrunner) reconcileEnvoyService(ctx context.Context) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      EnvoyServiceName,
			Namespace: r.cfg.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				"app": "atenet-router-envoy",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       int32(r.cfg.HttpPort),
					TargetPort: intstr.FromString("http"),
				},
			},
		},
	}

	var existing corev1.Service
	err := r.k8sClient.Get(ctx, client.ObjectKey{Namespace: r.cfg.Namespace, Name: EnvoyServiceName}, &existing)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.InfoContext(ctx, "Creating managed Envoy router ClusterIP service", slog.String("namespace", r.cfg.Namespace))
			return r.k8sClient.Create(ctx, svc)
		}
		return err
	}

	existing.Spec.Ports = svc.Spec.Ports
	existing.Spec.Selector = svc.Spec.Selector
	return r.k8sClient.Update(ctx, &existing)
}
