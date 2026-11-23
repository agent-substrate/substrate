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
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
)

// TCP_NOTSENT_LOWAT's socket option level and name. Envoy takes them as
// numbers, and the router also builds on platforms whose syscall package lacks
// them.
const (
	ipprotoTCP      = 6  // IPPROTO_TCP
	tcpNotsentLowat = 25 // TCP_NOTSENT_LOWAT
)

// notSentLowatBytes is the TCP_NOTSENT_LOWAT of Envoy's client and atunnel
// sockets, so a slow reader's backlog stays in Envoy's buffers, not in the
// kernel send queue.
const notSentLowatBytes = 16384

// notSentLowatSocketOptions returns the Envoy socket options that set
// TCP_NOTSENT_LOWAT to notSentLowatBytes. Connections a listening socket
// accepts inherit them. Internal listeners and clusters have no kernel
// sockets and get none.
func notSentLowatSocketOptions() []*corev3.SocketOption {
	return []*corev3.SocketOption{{
		Description: "TCP_NOTSENT_LOWAT",
		Level:       ipprotoTCP,
		Name:        tcpNotsentLowat,
		Value:       &corev3.SocketOption_IntValue{IntValue: notSentLowatBytes},
		State:       corev3.SocketOption_STATE_PREBIND,
	}}
}

// notSentLowatBindConfig returns the cluster bind config that sets
// TCP_NOTSENT_LOWAT to notSentLowatBytes.
func notSentLowatBindConfig() *corev3.BindConfig {
	return &corev3.BindConfig{SocketOptions: notSentLowatSocketOptions()}
}
