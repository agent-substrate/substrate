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

// Command podcertcontroller is a pod certificate controller that implements two signers.
//   - servicedns.ate.dev/identity: Issues certificate for Kubernetes service DNS names, backed by a
//     local CA.
//   - podid.ate.dev/identity: Issues certificates equivalent to KSA tokens, backed by a local CA.
//
// These signers are not unique to Agent Substrate, and will eventually be replaced by signers that
// are being developed as part of upstream Kubernetes.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/agent-substrate/substrate/internal/localca"
	"github.com/agent-substrate/substrate/internal/podidentitysigner"
	"github.com/agent-substrate/substrate/internal/rendezvous"
	"github.com/agent-substrate/substrate/internal/servicednssigner"
	"github.com/agent-substrate/substrate/internal/signercontroller"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/utils/clock"
)

var kubeConfigDefault string

func init() {
	if home := homedir.HomeDir(); home != "" {
		kubeConfigDefault = filepath.Join(home, ".kube", "config")
	}
}

var (
	kubeconfig = flag.String("kubeconfig", kubeConfigDefault, "absolute path to the kubeconfig file")
	inCluster  = flag.Bool("in-cluster", false, "Is the controller running in the cluster it should connect to?")

	shardingNamespace       = flag.String("sharding-pod-namespace", "", "(Work Sharding) The namespace the controller is running in")
	shardingPodName         = flag.String("sharding-pod-name", "", "(Work Sharding) The pod name of the controller")
	shardingPodUID          = flag.String("sharding-pod-uid", "", "(Work Sharding) The pod UID of the controller")
	shardingApplicationName = flag.String("sharding-application-name", "", "(Work Sharding) The application name to disambiguate Leases")

	serviceDNSCAPoolFile = flag.String(
		"service-dns-ca-pool",
		"",
		"File that contains the CA pool state for "+servicednssigner.Name,
	)

	podCAPoolFile = flag.String(
		"pod-identity-ca-pool",
		"",
		"File that contains the CA pool state for "+podidentitysigner.Name,
	)
)

func main() {
	ctx := context.Background()

	flag.Parse()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	var kconfig *rest.Config
	var err error
	if *inCluster {
		kconfig, err = rest.InClusterConfig()
		if err != nil {
			slog.ErrorContext(ctx, "Error creating in-cluster config", slog.Any("err", err))
			os.Exit(1)
		}
	} else {
		// use the current context in kubeconfig
		kconfig, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			slog.ErrorContext(ctx, "Error reading kubeconfig", slog.Any("err", err))
			os.Exit(1)
		}
	}

	kc, err := kubernetes.NewForConfig(kconfig)
	if err != nil {
		slog.ErrorContext(ctx, "Error creating Kubernetes client", slog.Any("err", err))
		os.Exit(1)
	}

	hasher := rendezvous.New(
		kc,
		*shardingNamespace,
		*shardingApplicationName,
		*shardingPodName,
		types.UID(*shardingPodUID),
		clock.RealClock{},
	)
	go hasher.Run(ctx)

	// Create a signer for servicedns.ate.dev/identity
	serviceDNSCAPoolBytes, err := os.ReadFile(*serviceDNSCAPoolFile)
	if err != nil {
		slog.ErrorContext(ctx, "Error reading servicedns.ate.dev/identity CA pool state", slog.Any("err", err))
		os.Exit(1)
	}
	serviceDNSCAPool, err := localca.Unmarshal(serviceDNSCAPoolBytes)
	if err != nil {
		slog.ErrorContext(ctx, "Error unmarshing servicedns.ate.dev/identity CA pool state", slog.Any("err", err))
		os.Exit(1)
	}
	serviceDNSSignerController := signercontroller.New(clock.RealClock{}, servicednssigner.NewImpl(kc, serviceDNSCAPool, clock.RealClock{}), kc, hasher)
	go serviceDNSSignerController.Run(ctx)

	// Create a signer for podidentity.podcert.ate.dev/identity
	podIdentityCAPoolBytes, err := os.ReadFile(*podCAPoolFile)
	if err != nil {
		slog.ErrorContext(ctx, "Error reading podidentity.podcert.ate.dev/identity CA pool state", slog.Any("err", err))
		os.Exit(1)
	}
	podIdentityCAPool, err := localca.Unmarshal(podIdentityCAPoolBytes)
	if err != nil {
		slog.ErrorContext(ctx, "Error unmarshing podidentity.podcert.ate.dev/identity CA pool state", slog.Any("err", err))
		os.Exit(1)
	}
	podIdentitySignerController := signercontroller.New(clock.RealClock{}, podidentitysigner.NewImpl(kc, podIdentityCAPool, clock.RealClock{}), kc, hasher)
	go podIdentitySignerController.Run(ctx)

	// TODO: Reload when the file changes.

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	<-signalCh
}
