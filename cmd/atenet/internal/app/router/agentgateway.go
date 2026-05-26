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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type agentgatewayProvider struct {
	cfg RouterConfig
}

func (p agentgatewayProvider) Name() string {
	return NetworkingModeAgentgateway
}

func (p agentgatewayProvider) RequiresXDS() bool {
	return false
}

func (p agentgatewayProvider) ConfigMapData() map[string]string {
	return map[string]string{"config.yaml": p.localConfig()}
}

func (p agentgatewayProvider) Container() corev1.Container {
	ports := []corev1.ContainerPort{
		{Name: "http", ContainerPort: int32(p.cfg.HttpPort)},
		{Name: "readiness", ContainerPort: 15021},
		{Name: "metrics", ContainerPort: 15020},
	}
	if p.cfg.HttpsPort > 0 && tlsCertPath(p.cfg) != "" {
		ports = append(ports, corev1.ContainerPort{Name: "https", ContainerPort: int32(p.cfg.HttpsPort)})
	}

	return corev1.Container{
		Name:  "agentgateway",
		Image: p.cfg.AgentgatewayImage,
		Args:  []string{"-f", "/etc/agentgateway/config.yaml"},
		Ports: ports,
		VolumeMounts: []corev1.VolumeMount{
			{Name: "proxy-config", MountPath: "/etc/agentgateway"},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz/ready",
					Port: intstr.FromInt32(15021),
				},
			},
			PeriodSeconds: 10,
		},
	}
}

func (p agentgatewayProvider) ServicePorts() []corev1.ServicePort {
	ports := []corev1.ServicePort{
		{Name: "http", Port: int32(p.cfg.HttpPort), TargetPort: intstr.FromString("http")},
	}
	if p.cfg.HttpsPort > 0 && tlsCertPath(p.cfg) != "" {
		ports = append(ports, corev1.ServicePort{Name: "https", Port: int32(p.cfg.HttpsPort), TargetPort: intstr.FromString("https")})
	}
	return ports
}

func (p agentgatewayProvider) CheckReady(ctx context.Context) (bool, string) {
	return checkHTTPReady(ctx, "http://127.0.0.1:15021/healthz/ready", "")
}

func (p agentgatewayProvider) localConfig() string {
	httpRoute := p.routeBlock("substrate-http")
	config := fmt.Sprintf(`# yaml-language-server: $schema=https://agentgateway.dev/schema/config
config:
  adminAddr: "127.0.0.1:15000"
  readinessAddr: "0.0.0.0:15021"
  statsAddr: "0.0.0.0:15020"
binds:
- port: %d
  listeners:
  - name: http
    protocol: HTTP
    routes:
%s`, p.cfg.HttpPort, indent(httpRoute, 4))

	if p.cfg.HttpsPort > 0 && tlsCertPath(p.cfg) != "" {
		cert := tlsCertPath(p.cfg)
		key := tlsKeyPath(p.cfg)
		config += fmt.Sprintf(`- port: %d
  listeners:
  - name: https
    protocol: HTTPS
    tls:
      cert: %q
      key: %q
    routes:
%s`, p.cfg.HttpsPort, cert, key, indent(p.routeBlock("substrate-https"), 4))
	}

	return config
}

func (p agentgatewayProvider) routeBlock(name string) string {
	return fmt.Sprintf(`- name: %s
  matches:
  - path:
      pathPrefix: /
  policies:
    extProc:
      host: %q
      failureMode: failClosed
  backends:
  - dynamic: {}
`, name, fmt.Sprintf("%s:%d", p.cfg.ExtprocAddr, p.cfg.ExtprocPort))
}

func indent(s string, spaces int) string {
	prefix := strings.Repeat(" ", spaces)
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = prefix + line
		}
	}
	return strings.Join(lines, "\n")
}
