// Copyright 2026 The Agent Substrate Authors
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
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestNotSentLowatSocketOptions(t *testing.T) {
	want := []*corev3.SocketOption{
		{Description: "TCP_NOTSENT_LOWAT", Level: 6, Name: 25, Value: &corev3.SocketOption_IntValue{IntValue: 16384}, State: corev3.SocketOption_STATE_PREBIND},
	}
	if diff := cmp.Diff(want, notSentLowatSocketOptions(), protocmp.Transform()); diff != "" {
		t.Errorf("notSentLowatSocketOptions() mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(&corev3.BindConfig{SocketOptions: want}, notSentLowatBindConfig(), protocmp.Transform()); diff != "" {
		t.Errorf("notSentLowatBindConfig() mismatch (-want +got):\n%s", diff)
	}
}

// tcpOptionsSnapshot builds the default ingress snapshot, with both TLS
// listeners and upstream CONNECT.
func tcpOptionsSnapshot(t *testing.T, upstreamConnect bool) (map[string]*listenerv3.Listener, map[string]*clusterv3.Cluster) {
	t.Helper()
	server := NewXdsServer(18000)
	server.SetConfig(50053, "127.0.0.1")
	server.SetConnectTLSPort(8444)
	setIngressTLS(server)
	server.SetUpstreamTls("/run/bundle.pem", "/run/trust.pem")
	server.SetUpstreamConnect(upstreamConnect)
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
	for _, upstreamConnect := range []bool{true, false} {
		listeners, clusters := tcpOptionsSnapshot(t, upstreamConnect)

		// Only listeners with kernel sockets get the options.
		for name, l := range listeners {
			want := []*corev3.SocketOption(nil)
			if name == IngressHTTPSListener || name == "connect_terminate_tls" {
				want = notSentLowatSocketOptions()
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
				want = notSentLowatBindConfig()
			}
			if diff := cmp.Diff(want, c.GetUpstreamBindConfig(), protocmp.Transform()); diff != "" {
				t.Errorf("upstreamConnect=%t: cluster %s bind config mismatch (-want +got):\n%s", upstreamConnect, name, diff)
			}
		}
	}
}
