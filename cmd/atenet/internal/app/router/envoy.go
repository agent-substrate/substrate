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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type envoyProvider struct {
	cfg RouterConfig
}

func (p envoyProvider) Name() string {
	return NetworkingModeEnvoy
}

func (p envoyProvider) RequiresXDS() bool {
	return true
}

func (p envoyProvider) ConfigMapData() map[string]string {
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
`, NodeID, p.cfg.XdsPort)

	return map[string]string{"envoy.yaml": envoyYaml}
}

func (p envoyProvider) Container() corev1.Container {
	return corev1.Container{
		Name:  "envoy",
		Image: p.cfg.EnvoyImage,
		Command: []string{
			"/usr/local/bin/envoy",
			"-c",
			"/etc/envoy/envoy.yaml",
			"--component-log-level",
			"upstream:debug,router:debug,ext_proc:debug",
		},
		Ports: []corev1.ContainerPort{
			{Name: "http", ContainerPort: int32(p.cfg.HttpPort)},
			{Name: "https", ContainerPort: int32(p.cfg.HttpsPort)},
			{Name: "admin", ContainerPort: 9901},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "proxy-config", MountPath: "/etc/envoy"},
		},
	}
}

func (p envoyProvider) ServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: "http", Port: int32(p.cfg.HttpPort), TargetPort: intstr.FromString("http")},
		{Name: "https", Port: int32(p.cfg.HttpsPort), TargetPort: intstr.FromString("https")},
	}
}

func (p envoyProvider) CheckReady(ctx context.Context) (bool, string) {
	return checkHTTPReady(ctx, "http://127.0.0.1:9901/ready", "LIVE")
}
