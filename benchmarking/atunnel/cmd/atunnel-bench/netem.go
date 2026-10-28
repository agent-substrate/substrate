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

//go:build linux

package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/vishvananda/netlink"
)

// netemCommand delays every packet leaving a device, with a root netem qdisc,
// for run-docker.sh to put a round trip between atunnel and the router and
// gateway. The image has no tc binary.
func netemCommand() *cobra.Command {
	var dev string
	var delay time.Duration
	cmd := &cobra.Command{
		Use:    "netem",
		Short:  "Delay packets leaving a network device",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			link, err := netlink.LinkByName(dev)
			if err != nil {
				return err
			}
			qdisc := netlink.NewNetem(netlink.QdiscAttrs{
				LinkIndex: link.Attrs().Index,
				Handle:    netlink.MakeHandle(1, 0),
				Parent:    netlink.HANDLE_ROOT,
			}, netlink.NetemQdiscAttrs{Latency: uint32(delay.Microseconds()), Limit: 100000})
			if err := netlink.QdiscReplace(qdisc); err != nil {
				return fmt.Errorf("adding netem to %s: %w", dev, err)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&dev, "dev", "eth0", "network device")
	cmd.Flags().DurationVar(&delay, "delay", time.Millisecond, "delay added to every packet leaving the device")
	return cmd
}
