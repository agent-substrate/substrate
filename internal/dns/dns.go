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

package dns

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// serviceName is the name of the CoreDNS service.
	serviceName     = "dns"
	systemNamespace = "ate-system"
)

// Controller manages the DNS configuration for the ATE.
type Controller struct {
	Client       client.Client
	Interval     time.Duration
	CorefilePath string
	Reloader     ConfigReloader
}

// Run the DNS orchestration loop until ctx is canceled.
func (c *Controller) Run(ctx context.Context) error {
	slog.InfoContext(ctx, "DNS Controller started", slog.Duration("interval", c.Interval), slog.String("corefile", c.CorefilePath))
	slog.InfoContext(ctx, "Using template", "template", corefileTemplate)

	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "DNS Controller stopped")
			return nil
		case <-ticker.C:
			if err := c.reconcile(ctx); err != nil {
				slog.ErrorContext(ctx, "Error during DNS reconciliation", slog.Any("error", err))
			}
		}
	}
}

func (c *Controller) reconcile(ctx context.Context) error {
	slog.DebugContext(ctx, "Reconciling DNS orchestration configuration...")

	// 1. Get the ClusterIP of atenet-router in ate-system namespace
	routerSvc := &corev1.Service{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: "atenet-router", Namespace: systemNamespace}, routerSvc); err != nil {
		if errors.IsNotFound(err) {
			slog.WarnContext(ctx, "atenet-router service not found, skipping until it is available")
			return nil
		}
		return fmt.Errorf("failed to get atenet-router service: %w", err)
	}

	routerIP := routerSvc.Spec.ClusterIP
	if routerIP == "" || routerIP == "None" {
		slog.WarnContext(ctx, "atenet-router service has no ClusterIP yet, waiting...")
		return nil
	}

	// 2. Get the ClusterIP of dns service in ate-system namespace
	dnsSvc := &corev1.Service{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: systemNamespace}, dnsSvc); err != nil {
		if errors.IsNotFound(err) {
			slog.WarnContext(ctx, "dns service not found, skipping until it is available")
			return nil
		}
		return fmt.Errorf("failed to get dns service: %w", err)
	}

	dnsIP := dnsSvc.Spec.ClusterIP
	if dnsIP == "" || dnsIP == "None" {
		slog.WarnContext(ctx, "dns service has no ClusterIP yet, waiting...")
		return nil
	}

	// 3. Reconcile CoreDNS Corefile on shared volume
	if err := c.reconcileCoreDNSConfig(ctx, routerIP); err != nil {
		return fmt.Errorf("failed to reconcile CoreDNS config file: %w", err)
	}

	// 4. Reconcile GKE kube-dns ConfigMap with dns service IP
	if err := c.reconcileKubeDNSConfig(ctx, dnsIP); err != nil {
		return fmt.Errorf("failed to reconcile kube-dns configmap: %w", err)
	}

	return nil
}

func (c *Controller) reconcileCoreDNSConfig(ctx context.Context, routerIP string) error {
	expectedCorefile := makeCoreFile(routerIP)

	// Read Corefile from local shared volume path to see if it needs updating
	corefileBytes, err := os.ReadFile(c.CorefilePath)
	if err == nil && string(corefileBytes) == expectedCorefile {
		slog.DebugContext(ctx, "CoreDNS Corefile is up-to-date", slog.String("routerIP", routerIP))
		return nil
	}

	// Write updated Corefile back to shared volume in its entirety
	if err := os.WriteFile(c.CorefilePath, []byte(expectedCorefile), 0644); err != nil {
		return fmt.Errorf("failed to write updated Corefile to %s: %w", c.CorefilePath, err)
	}
	slog.InfoContext(ctx, "CoreDNS Corefile updated", slog.String("routerIP", routerIP))

	// Signal CoreDNS process to reload
	if err := c.Reloader.Reload(ctx); err != nil {
		return fmt.Errorf("failed to reload CoreDNS: %w", err)
	}

	return nil
}

// reconcileKubeDNSConfig ensures that the kube-dns ConfigMap has a stub domain for ate-system.
func (c *Controller) reconcileKubeDNSConfig(ctx context.Context, dnsIP string) error {
	cm := &corev1.ConfigMap{}
	if err := c.Client.Get(ctx, types.NamespacedName{Name: "kube-dns", Namespace: "kube-system"}, cm); err != nil {
		if errors.IsNotFound(err) {
			slog.WarnContext(ctx, "kube-dns ConfigMap not found in kube-system namespace, skipping stub resolver configuration")
			return nil
		}
		return fmt.Errorf("failed to retrieve kube-dns ConfigMap: %w", err)
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	stubDomainsStr := cm.Data["stubDomains"]
	var stubDomains map[string][]string

	if stubDomainsStr != "" {
		if err := json.Unmarshal([]byte(stubDomainsStr), &stubDomains); err != nil {
			return fmt.Errorf("failed to parse stubDomains JSON: %w", err)
		}
	} else {
		stubDomains = make(map[string][]string)
	}

	ips, exists := stubDomains[resources.ActorDNSSuffix]
	if exists && len(ips) == 1 && ips[0] == dnsIP {
		slog.DebugContext(ctx, "kube-dns stubDomains are already up-to-date", slog.String("dnsIP", dnsIP))
		return nil
	}

	stubDomains[resources.ActorDNSSuffix] = []string{dnsIP}

	newStubDomainsBytes, err := json.Marshal(stubDomains)
	if err != nil {
		return fmt.Errorf("failed to marshal stubDomains JSON: %w", err)
	}

	cm.Data["stubDomains"] = string(newStubDomainsBytes)
	if err := c.Client.Update(ctx, cm); err != nil {
		return fmt.Errorf("failed to update kube-dns ConfigMap: %w", err)
	}

	slog.InfoContext(ctx, "kube-dns stubDomains successfully updated with custom DNS IP", slog.String("dnsIP", dnsIP))
	return nil
}

// ConfigReloader defines an interface for dynamically signaling CoreDNS to reload its configuration.
type ConfigReloader interface {
	Reload(ctx context.Context) error
}

type procConfigReloader struct{}

func NewConfigReloader() ConfigReloader {
	return &procConfigReloader{}
}

func (r *procConfigReloader) Reload(ctx context.Context) error {
	pid, err := findPID("coredns")
	if err != nil {
		slog.ErrorContext(ctx, "findPID error", slog.Any("error", err))
		return nil
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("FindProcess %d: %w", pid, err)
	}

	// CoreDNS catches SIGUSR1 or SIGHUP to trigger dynamic reload of Corefile
	if err := process.Signal(syscall.SIGUSR1); err != nil {
		return fmt.Errorf("SendProcess SIGUSR1 %d: %w", pid, err)
	}

	slog.InfoContext(ctx, "Successfully signaled reload", slog.Int("pid", pid))
	return nil
}

func findPID(cmdName string) (int, error) {
	files, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("ReadDir /proc: %w", err)
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		pid, err := strconv.Atoi(file.Name())
		if err != nil {
			continue
		}

		commBytes, err := os.ReadFile(filepath.Join("/proc", file.Name(), "comm"))
		if err != nil {
			// Process might have terminated
			continue
		}

		commGot := strings.TrimSpace(string(commBytes))
		if commGot == cmdName {
			return pid, nil
		}
	}

	return 0, fmt.Errorf("%q process not found", cmdName)
}
