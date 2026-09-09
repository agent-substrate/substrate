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

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/ateclient"
	"go.ate.dev/substrate/internal/clustertrustbundle"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/podidentity"
	"go.ate.dev/substrate/internal/portforward"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	serviceDNSSignerName = "servicedns.podcert.ate.dev/identity"
	liveBundleSelector   = "podcert.ate.dev/canarying=live"
	routerPort           = 443
)

type routerClient struct {
	baseURL    string
	httpClient *http.Client
	stop       func()
}

func setupRouter(
	ctx context.Context,
	kubeconfig, k8sContext string,
	atenetAddr string,
	routerCertFile, routerKeyFile, routerCAFile string,
	routerClientSA, routerClientNS string,
) (*routerClient, error) {
	if strings.HasPrefix(atenetAddr, "http://") {
		log.Printf("Connecting to atenet over plain HTTP at %s...", atenetAddr)
		return &routerClient{
			baseURL:    strings.TrimRight(atenetAddr, "/"),
			httpClient: &http.Client{Timeout: 30 * time.Second},
		}, nil
	}
	if atenetAddr == "localhost:8000" || atenetAddr == "127.0.0.1:8000" {
		log.Printf("Connecting to atenet proxy at http://%s...", atenetAddr)
		return &routerClient{
			baseURL:    "http://" + atenetAddr,
			httpClient: &http.Client{Timeout: 30 * time.Second},
		}, nil
	}

	config, err := ateclient.LoadKubeConfig(kubeconfig, k8sContext)
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("creating k8s client: %w", err)
	}

	var stop func()
	var baseURL string
	if atenetAddr == "" {
		log.Print("Connecting to atenet-router (auto port-forward to svc/atenet-router)...")
		localPort, pfStop, err := portforward.ServicePortForward(
			ctx,
			config,
			clientset,
			installdefaults.SystemNamespace,
			installdefaults.RouterServiceName,
			routerPort,
		)
		if err != nil {
			return nil, fmt.Errorf("port-forwarding to %s: %w", installdefaults.RouterServiceName, err)
		}
		stop = pfStop
		baseURL = fmt.Sprintf("https://127.0.0.1:%d", localPort)
	} else {
		log.Printf("Connecting to atenet at %s...", atenetAddr)
		if strings.HasPrefix(atenetAddr, "https://") {
			baseURL = strings.TrimRight(atenetAddr, "/")
		} else {
			baseURL = "https://" + atenetAddr
		}
	}

	var rootCAs *x509.CertPool
	if routerCAFile != "" {
		caBytes, err := os.ReadFile(routerCAFile)
		if err != nil {
			if stop != nil {
				stop()
			}
			return nil, fmt.Errorf("reading router CA file %q: %w", routerCAFile, err)
		}
		rootCAs = x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caBytes) {
			if stop != nil {
				stop()
			}
			return nil, fmt.Errorf("router CA file %q contains no valid PEM certificates", routerCAFile)
		}
	} else {
		pool, err := serviceDNSTrustPool(ctx, clientset)
		if err != nil {
			if stop != nil {
				stop()
			}
			return nil, fmt.Errorf("loading servicedns trust bundle for atenet-router: %w", err)
		}
		rootCAs = pool
	}

	var clientCert tls.Certificate
	if routerCertFile != "" && routerKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(routerCertFile, routerKeyFile)
		if err != nil {
			if stop != nil {
				stop()
			}
			return nil, fmt.Errorf("loading router client certificate and key: %w", err)
		}
		clientCert = cert
	} else {
		if err := ensureRouterClientNamespace(ctx, clientset, routerClientNS); err != nil {
			log.Printf("Warning: failed to ensure namespace %s: %v", routerClientNS, err)
		}
		cert, err := podidentity.RequestCertificate(ctx, clientset, "sandbox-client", routerClientNS, routerClientSA, 2*time.Minute)
		if err != nil {
			if stop != nil {
				stop()
			}
			return nil, fmt.Errorf("requesting a podidentity certificate for %s/%s: %w", routerClientNS, routerClientSA, err)
		}
		clientCert = cert
	}

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      rootCAs,
		ServerName:   fmt.Sprintf("%s.%s.svc", installdefaults.RouterServiceName, installdefaults.SystemNamespace),
		Certificates: []tls.Certificate{clientCert},
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig

	return &routerClient{
		baseURL:    baseURL,
		httpClient: &http.Client{Transport: transport, Timeout: 30 * time.Second},
		stop:       stop,
	}, nil
}

func ensureRouterClientNamespace(ctx context.Context, clientset kubernetes.Interface, namespace string) error {
	_, err := clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func serviceDNSTrustPool(ctx context.Context, clientset kubernetes.Interface) (*x509.CertPool, error) {
	bundles, err := clustertrustbundle.NewClient(clientset, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to discover ClusterTrustBundle API: %w", err)
	}
	ctbs, err := bundles.List(ctx, metav1.ListOptions{
		LabelSelector: liveBundleSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list ClusterTrustBundles: %w", err)
	}

	pool := x509.NewCertPool()
	found := false
	for _, ctb := range ctbs.Items {
		if ctb.Spec.SignerName != serviceDNSSignerName {
			continue
		}
		if !pool.AppendCertsFromPEM([]byte(ctb.Spec.TrustBundle)) {
			return nil, fmt.Errorf("ClusterTrustBundle %q contains no valid certificates", ctb.ObjectMeta.Name)
		}
		found = true
	}
	if !found {
		return nil, fmt.Errorf("no live ClusterTrustBundle found for signer %q", serviceDNSSignerName)
	}
	return pool, nil
}
