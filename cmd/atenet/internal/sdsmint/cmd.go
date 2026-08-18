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

package sdsmint

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

type config struct {
	UDSPath     string
	CAPoolPath  string
	CAID        string
	LeafCertTTL time.Duration
	LogLevel    string
}

func NewSdsmintCmd() *cobra.Command {
	var cfg config

	cmd := &cobra.Command{
		Use:   "sdsmint",
		Short: "Minting SDS server that issues a TLS leaf for the SNI Envoy asks for",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.UDSPath, "uds-path", "", "unix socket to listen on; required, and the only transport offered, because leaf private keys transit this channel")
	cmd.Flags().StringVar(&cfg.CAPoolPath, "ca-pool-path", "", "path to a localca pool JSON holding the MITM CA, the format substrate mounts its other CAs in")
	cmd.Flags().StringVar(&cfg.CAID, "ca-id", "", "which CA in the pool to sign with; empty takes the first")
	cmd.Flags().DurationVar(&cfg.LeafCertTTL, "leaf-cert-ttl", defaultTTL, "leaf certificate lifetime; the xDS resource TTL is derived from it at half its length, so an actively used name is re-minted about twice per lifetime and an idle one is dropped and not minted again")
	cmd.Flags().StringVar(&cfg.LogLevel, "log-level", "info", "one of debug, info, warn, error")

	return cmd
}

func (c config) validateTTL() error {
	ttl := c.LeafCertTTL
	if ttl <= 0 {
		return fmt.Errorf("--leaf-cert-ttl must be positive, got %s", ttl)
	}

	// The band outside which --leaf-cert-ttl is refused. Both edges are cases
	// where the flag still starts a server but stops describing what it does,
	// which is worse than not starting: the operator has no signal that the
	// number they set is not the number in effect.
	const (
		minSensibleTTL = 2 * time.Minute
		maxSensibleTTL = 24 * time.Hour
	)
	switch {
	case ttl < minSensibleTTL:
		return fmt.Errorf("--leaf-cert-ttl %s is below %s; leaves are back-dated 5m for clock skew, so most of each certificate's validity would already be in the past and --leaf-cert-ttl would not describe how long clients accept it", ttl, minSensibleTTL)
	case ttl > maxSensibleTTL:
		return fmt.Errorf("--leaf-cert-ttl %s is above %s; a leaf is served until the resource TTL derived from this drops it, so a name in steady use would carry the same certificate for half of that, which is not what short-lived MITM leaves are for", ttl, maxSensibleTTL)
	}
	return nil
}
