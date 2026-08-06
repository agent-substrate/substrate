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

// Package portforward tunnels to the pods behind a Service. The
// Kubernetes port-forward API is pod-scoped, so "forwarding to a
// Service" means resolving its selector to one ready pod and its port
// mapping to a container port, then forwarding to that pod.
// This is the same thing kubectl does for `port-forward svc/<service-name>`.
package portforward

import (
	"context"
	"fmt"
	"io"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// ServicePortForward forwards a random local port to servicePort of the
// named Service. It picks a ready backing pod, resolves servicePort to
// the pod's container port (including named targetPorts), and tunnels
// to that pod. It returns the chosen local port and a stop func the
// caller must invoke to tear the tunnel down. The tunnel is pinned to
// the picked pod; it does not fail over if that pod later dies.
func ServicePortForward(ctx context.Context, config *rest.Config, clientset kubernetes.Interface, namespace, service string, servicePort int32) (int, func(), error) {
	pod, svc, err := firstReadyPodForService(ctx, clientset, namespace, service)
	if err != nil {
		return 0, nil, err
	}
	targetPort, err := resolveTargetPort(svc, pod, servicePort)
	if err != nil {
		return 0, nil, err
	}
	return podPortForward(ctx, config, clientset, namespace, pod.Name, targetPort)
}

func firstReadyPodForService(ctx context.Context, clientset kubernetes.Interface, namespace, service string) (*corev1.Pod, *corev1.Service, error) {
	svc, err := clientset.CoreV1().Services(namespace).Get(ctx, service, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("getting %s service: %w", service, err)
	}
	if len(svc.Spec.Selector) == 0 {
		return nil, nil, fmt.Errorf("service %s has no selector", service)
	}
	selector := labels.SelectorFromSet(svc.Spec.Selector).String()
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, nil, fmt.Errorf("listing %s pods: %w", service, err)
	}
	for i := range pods.Items {
		if IsPodReady(&pods.Items[i]) {
			return &pods.Items[i], svc, nil
		}
	}
	return nil, nil, fmt.Errorf("no ready %s pods in %s", service, namespace)
}

func resolveTargetPort(svc *corev1.Service, pod *corev1.Pod, servicePort int32) (int32, error) {
	for _, sp := range svc.Spec.Ports {
		if sp.Port != servicePort {
			continue
		}
		var port int32
		switch sp.TargetPort.Type {
		case intstr.Int:
			port = sp.TargetPort.IntVal
			if port == 0 {
				// targetPort defaults to port when omitted.
				port = sp.Port
			}
		case intstr.String:
			for _, c := range pod.Spec.Containers {
				for _, cp := range c.Ports {
					if cp.Name == sp.TargetPort.StrVal {
						port = cp.ContainerPort
					}
				}
			}
			if port == 0 {
				return 0, fmt.Errorf("named targetPort %q not found on pod %s", sp.TargetPort.StrVal, pod.Name)
			}
		}
		return port, nil
	}
	return 0, fmt.Errorf("service %s has no port %d", svc.Name, servicePort)
}

func podPortForward(ctx context.Context, config *rest.Config, clientset kubernetes.Interface, namespace, podName string, targetPort int32) (int, func(), error) {
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(podName).
		SubResource("portforward")

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return 0, nil, fmt.Errorf("creating SPDY transport: %w", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, req.URL())

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("0:%d", targetPort)}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return 0, nil, fmt.Errorf("creating port forwarder: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := fw.ForwardPorts(); err != nil {
			errCh <- err
		}
	}()
	select {
	case <-readyCh:
	case err := <-errCh:
		return 0, nil, fmt.Errorf("port forwarding: %w", err)
	case <-ctx.Done():
		close(stopCh)
		return 0, nil, ctx.Err()
	}

	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		close(stopCh)
		return 0, nil, fmt.Errorf("getting forwarded ports: %w", err)
	}
	return int(ports[0].Local), func() { close(stopCh) }, nil
}

func IsPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
