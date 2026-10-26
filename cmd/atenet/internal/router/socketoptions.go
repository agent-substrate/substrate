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
	"fmt"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	"github.com/spf13/pflag"
)

// Linux socket option levels and names. Envoy takes them as numbers, and the
// router also builds on platforms whose syscall package lacks them.
const (
	solSocket       = 1  // SOL_SOCKET
	soSndbuf        = 7  // SO_SNDBUF
	soRcvbuf        = 8  // SO_RCVBUF
	ipprotoTCP      = 6  // IPPROTO_TCP
	tcpNotsentLowat = 25 // TCP_NOTSENT_LOWAT
)

// tcpBufferOptions bound how much one of Envoy's TCP sockets can queue in the
// kernel. A slow peer otherwise lets each socket's queues grow to the node's
// tcp_wmem and tcp_rmem maximums, memory charged to the Envoy container that
// no heap statistic shows. A zero field leaves the kernel default.
type tcpBufferOptions struct {
	// NotSentLowat is TCP_NOTSENT_LOWAT: the socket reports writable only
	// while fewer than this many bytes wait to be sent, so Envoy's write
	// buffer watermarks push back on the other side instead of the kernel
	// queueing up to tcp_wmem's maximum.
	NotSentLowat int
	// SendBuffer and ReceiveBuffer are SO_SNDBUF and SO_RCVBUF. Setting one
	// turns off the kernel's autotuning of that buffer, which caps a
	// connection's throughput at about the buffer size per round trip. The
	// kernel doubles the value and caps it at net.core.wmem_max or rmem_max.
	SendBuffer    int
	ReceiveBuffer int
}

// defaultTCP is what the router sets on its TCP sockets: the ones its TLS
// listeners accept from clients and the ones it dials to atunnel. A client
// that stops reading otherwise lets each of its sockets queue up to
// tcp_wmem's maximum, 4 MiB on GKE nodes; with this, the backlog stays in
// Envoy's buffers and upstream. Fixed buffer sizes cap throughput at the
// buffer size per round trip and are left off. See
// docs/network-socket-memory.md.
var defaultTCP = tcpBufferOptions{NotSentLowat: 16384}

// bindTCPBufferFlags registers the router's socket option flags.
func bindTCPBufferFlags(fs *pflag.FlagSet, o *tcpBufferOptions) {
	fs.IntVar(&o.NotSentLowat, "router-tcp-notsent-lowat", defaultTCP.NotSentLowat, "TCP_NOTSENT_LOWAT in bytes on Envoy's client and atunnel sockets, so a slow reader backs data up behind Envoy's buffer limits instead of in the kernel. 0 leaves the kernel default")
	fs.IntVar(&o.SendBuffer, "router-tcp-send-buffer", defaultTCP.SendBuffer, "SO_SNDBUF in bytes on Envoy's client and atunnel sockets. It turns off send-buffer autotuning, capping throughput at about this many bytes per round trip. 0 leaves the kernel default")
	fs.IntVar(&o.ReceiveBuffer, "router-tcp-receive-buffer", defaultTCP.ReceiveBuffer, "SO_RCVBUF in bytes on Envoy's client and atunnel sockets. It turns off receive-buffer autotuning, capping throughput at about this many bytes per round trip. 0 leaves the kernel default")
}

func (o tcpBufferOptions) validate() error {
	if o.NotSentLowat < 0 || o.SendBuffer < 0 || o.ReceiveBuffer < 0 {
		return fmt.Errorf("--router-tcp-* options must not be negative, got %+v", o)
	}
	return nil
}

// envoy returns the options as Envoy socket options, set before the socket
// binds. Connections a listening socket accepts get its buffer sizes, and
// TCP_NOTSENT_LOWAT on kernels that copy it, as GKE's COS 6.12 does. It
// returns nil when every option is zero.
func (o tcpBufferOptions) envoy() []*corev3.SocketOption {
	var opts []*corev3.SocketOption
	add := func(description string, level, name int64, value int) {
		if value <= 0 {
			return
		}
		opts = append(opts, &corev3.SocketOption{
			Description: description,
			Level:       level,
			Name:        name,
			Value:       &corev3.SocketOption_IntValue{IntValue: int64(value)},
			State:       corev3.SocketOption_STATE_PREBIND,
		})
	}
	add("TCP_NOTSENT_LOWAT", ipprotoTCP, tcpNotsentLowat, o.NotSentLowat)
	add("SO_SNDBUF", solSocket, soSndbuf, o.SendBuffer)
	add("SO_RCVBUF", solSocket, soRcvbuf, o.ReceiveBuffer)
	return opts
}

// upstreamBindConfig carries the options to a cluster's upstream sockets. It
// binds no source address, so the kernel picks one as it otherwise would.
func (o tcpBufferOptions) upstreamBindConfig() *corev3.BindConfig {
	opts := o.envoy()
	if opts == nil {
		return nil
	}
	return &corev3.BindConfig{SocketOptions: opts}
}
