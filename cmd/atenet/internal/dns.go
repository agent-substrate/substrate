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

package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/dns"
)

type DnsConfig struct {
	LogLevel          string
	Kubeconfig        string
	ReconcileInterval time.Duration
	CorefilePath      string
}

func NewDnsCmd() *cobra.Command {
	var cfg DnsConfig

	cmd := &cobra.Command{
		Use:   "dns",
		Short: "Orchestrates CoreDNS and GKE stub resolver configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			var level slog.Level
			switch strings.ToLower(cfg.LogLevel) {
			case "debug":
				level = slog.LevelDebug
			case "warn":
				level = slog.LevelWarn
			case "error":
				level = slog.LevelError
			default:
				level = slog.LevelInfo
			}
			slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigChan
				cancel()
			}()

			k8sCfg, err := config.GetConfig()
			if err != nil {
				if cfg.Kubeconfig != "" {
					k8sCfg, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
					if err != nil {
						return fmt.Errorf("failed to read config from path %s: %w", cfg.Kubeconfig, err)
					}
				} else {
					return fmt.Errorf("unable to establish Kubernetes configuration parameters: %w", err)
				}
			}

			k8sClient, err := client.New(k8sCfg, client.Options{})
			if err != nil {
				return fmt.Errorf("failed to initialize cluster client: %w", err)
			}

			dnsController := &dns.Controller{
				Client:       k8sClient,
				Interval:     cfg.ReconcileInterval,
				CorefilePath: cfg.CorefilePath,
				Reloader:     dns.NewConfigReloader(),
			}

			slog.InfoContext(ctx, "Starting DNS Controller subsystem")
			return dnsController.Run(ctx)
		},
	}

	cmd.Flags().StringVar(&cfg.LogLevel, "log-level", "info", "Log level: debug, info, warn, error")
	cmd.Flags().StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Absolute path to the kubeconfig configuration file")
	cmd.Flags().DurationVar(&cfg.ReconcileInterval, "interval", 10*time.Second, "Interval for reconciling DNS configurations")
	cmd.Flags().StringVar(&cfg.CorefilePath, "corefile-path", "/etc/coredns/Corefile", "Path to the local Corefile configuration on shared volume")

	return cmd
}
