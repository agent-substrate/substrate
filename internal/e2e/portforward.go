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

package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// firstReadyPodForService returns a ready pod backing service, plus the Service
// itself (callers need its port mapping). It refuses a selectorless Service
// rather than forward to an arbitrary pod in the namespace.
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
		if isPodReady(&pods.Items[i]) {
			return &pods.Items[i], svc, nil
		}
	}
	return nil, nil, fmt.Errorf("no ready %s pods in %s", service, namespace)
}

// podPortForward forwards a random local port to targetPort on the pod (local
// port 0 asks the OS for a free one), returning the chosen local port and a stop
// func the caller must invoke to tear the tunnel down.
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
