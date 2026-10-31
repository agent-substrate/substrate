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

package router

import (
	"strings"
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/google/go-cmp/cmp"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNotSentLowatSocketOptions(t *testing.T) {
	if got := notSentLowatSocketOptions(0); got != nil {
		t.Errorf("notSentLowatSocketOptions(0) = %v, want nil", got)
	}
	if got := notSentLowatBindConfig(0); got != nil {
		t.Errorf("notSentLowatBindConfig(0) = %v, want nil", got)
	}
	want := []*corev3.SocketOption{
		{Description: "TCP_NOTSENT_LOWAT", Level: 6, Name: 25, Value: &corev3.SocketOption_IntValue{IntValue: 16384}, State: corev3.SocketOption_STATE_PREBIND},
	}
	if diff := cmp.Diff(want, notSentLowatSocketOptions(16384), protocmp.Transform()); diff != "" {
		t.Errorf("notSentLowatSocketOptions(16384) mismatch (-want +got):\n%s", diff)
	}
}

// tcpOptionsSnapshot builds the default ingress snapshot, with both TLS
// listeners and upstream CONNECT, under the given TCP_NOTSENT_LOWAT.
func tcpOptionsSnapshot(t *testing.T, notSentLowat int, upstreamConnect bool) (map[string]*listenerv3.Listener, map[string]*clusterv3.Cluster) {
	t.Helper()
	server := NewXdsServer(18000)
	server.SetConfig(50053, "127.0.0.1")
	server.SetConnectTLSPort(8444)
	setIngressTLS(server)
	server.SetUpstreamTls("/run/bundle.pem", "/run/trust.pem")
	server.SetUpstreamConnect(upstreamConnect)
	server.SetTCPNotSentLowat(notSentLowat)
	listeners := snapshotListeners(t, server)
	res, err := server.snapshot.GetSnapshot(NodeID)
	if err != nil {
		t.Fatal(err)
	}
	clusters := map[string]*clusterv3.Cluster{}
	for name, raw := range res.GetResources(resourcev3.ClusterType) {
		clusters[name] = raw.(*clusterv3.Cluster)
	}
	return listeners, clusters
}

func TestXdsServer_TCPNotSentLowat(t *testing.T) {
	const notSentLowat = 16384

	for _, upstreamConnect := range []bool{true, false} {
		listeners, clusters := tcpOptionsSnapshot(t, notSentLowat, upstreamConnect)

		// Only listeners with kernel sockets get the options.
		for name, l := range listeners {
			want := []*corev3.SocketOption(nil)
			if name == IngressHTTPSListener || name == "connect_terminate_tls" {
				want = notSentLowatSocketOptions(notSentLowat)
			}
			if diff := cmp.Diff(want, l.GetSocketOptions(), protocmp.Transform()); diff != "" {
				t.Errorf("upstreamConnect=%t: listener %s socket options mismatch (-want +got):\n%s", upstreamConnect, name, diff)
			}
			// The dual-stack address inherits the listener's options only
			// while it sets none of its own.
			for _, a := range l.GetAdditionalAddresses() {
				if a.GetSocketOptions() != nil {
					t.Errorf("upstreamConnect=%t: listener %s additional address overrides the socket options", upstreamConnect, name)
				}
			}
		}

		// Only the clusters that dial atunnel get the options.
		actorCluster := OriginalDstClusterName
		if upstreamConnect {
			actorCluster = ActorConnectName
		}
		if _, ok := clusters[actorCluster]; !ok {
			t.Fatalf("upstreamConnect=%t: snapshot has no %s cluster", upstreamConnect, actorCluster)
		}
		for name, c := range clusters {
			want := (*corev3.BindConfig)(nil)
			if name == actorCluster {
				want = notSentLowatBindConfig(notSentLowat)
			}
			if diff := cmp.Diff(want, c.GetUpstreamBindConfig(), protocmp.Transform()); diff != "" {
				t.Errorf("upstreamConnect=%t: cluster %s bind config mismatch (-want +got):\n%s", upstreamConnect, name, diff)
			}
		}
	}
}

func TestTCPNotSentLowatFlag(t *testing.T) {
	fs := pflag.NewFlagSet("router", pflag.ContinueOnError)
	cfg := &routerConfig{}
	bindRouterFlags(fs, cfg)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if cfg.TCPNotSentLowat != 16384 {
		t.Errorf("default TCPNotSentLowat = %d, want 16384", cfg.TCPNotSentLowat)
	}
	if err := fs.Parse([]string{"--router-tcp-notsent-lowat=1"}); err != nil {
		t.Fatal(err)
	}
	if cfg.TCPNotSentLowat != 1 {
		t.Errorf("TCPNotSentLowat = %d, want 1", cfg.TCPNotSentLowat)
	}

	// Egress mode skips the ingress TLS checks that come first.
	off := routerConfig{Mode: ModeEgress, TCPNotSentLowat: 0}
	if err := off.validate(); err != nil {
		t.Errorf("validate() with 0, the kernel default = %v", err)
	}
	bad := routerConfig{Mode: ModeEgress, TCPNotSentLowat: -1}
	if err := bad.validate(); err == nil || !strings.Contains(err.Error(), "--router-tcp-notsent-lowat") {
		t.Errorf("validate() with -1 = %v, want a --router-tcp-notsent-lowat error", err)
	}
}
