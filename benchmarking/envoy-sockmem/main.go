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

// Command envoy-sockmem measures how much memory Envoy's kernel socket queues
// take when its peers read slowly, with and without per-socket limits. It is
// the peers of an Envoy configured like atenet-router (ingress) or the egress
// gateway (egress):
//
//	envoy-sockmem pki --dir D           write the certificates
//	envoy-sockmem config --kind K ...   print Envoy's configuration
//	envoy-sockmem origin                serve Envoy's upstream
//	envoy-sockmem client --mode M ...   hold slow connections, or move bytes,
//	                                    through Envoy and print what it cost
//	envoy-sockmem netem --delay D       delay packets leaving eth0
//	envoy-sockmem report < results.jsonl
//
// run-docker.sh runs them; see README.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: envoy-sockmem pki|config|origin|client|netem|report [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	var err error
	switch os.Args[1] {
	case "pki":
		dir := fs.String("dir", ".", "directory to write the certificates to")
		_ = fs.Parse(os.Args[2:])
		err = writePKI(*dir)
	case "config":
		kind := fs.String("kind", "", "ingress, like atenet-router, or egress, like the egress gateway")
		var tcp tcpOptions
		fs.IntVar(&tcp.notSentLowat, "tcp-notsent-lowat", 0, "TCP_NOTSENT_LOWAT on every Envoy socket; 0 leaves the kernel default")
		fs.IntVar(&tcp.sendBuffer, "tcp-send-buffer", 0, "SO_SNDBUF on every Envoy socket; 0 leaves the kernel default")
		fs.IntVar(&tcp.receiveBuffer, "tcp-receive-buffer", 0, "SO_RCVBUF on every Envoy socket; 0 leaves the kernel default")
		window := fs.Int("upstream-stream-window", 0, "ingress only: the upstream HTTP/2 stream window in bytes, with a connection window 100 times as large; 0 leaves Envoy's defaults, as the router does")
		_ = fs.Parse(os.Args[2:])
		err = writeConfig(os.Stdout, *kind, tcp, *window)
	case "origin":
		_ = fs.Parse(os.Args[2:])
		err = runOrigin(ctx)
	case "client":
		c := clientConfig{}
		fs.StringVar(&c.kind, "kind", "egress", "which Envoy: ingress or egress")
		fs.StringVar(&c.mode, "mode", "", "what to measure: slow-downstream, slow-upstream, download or upload")
		fs.StringVar(&c.pkiDir, "pki-dir", "/pki", "directory written by pki")
		fs.IntVar(&c.conns, "conns", 100, "connections to hold, or to stream over")
		fs.Int64Var(&c.drain, "drain", 4<<20, "bytes a stalled reader reads at full speed before it stalls, so the kernel has grown its queues")
		fs.DurationVar(&c.hold, "hold", 20*time.Second, "how long to hold the connections, or to stream")
		fs.DurationVar(&c.fill, "fill", 3*time.Second, "how long after the last open to sample Envoy, so the queues are full")
		fs.Float64Var(&c.openRate, "open-rate", 200, "connections opened per second")
		fs.StringVar(&c.envoyCgroup, "envoy-cgroup", "/proc/1/root/sys/fs/cgroup", "Envoy's cgroup directory")
		fs.StringVar(&c.envoyAdmin, "envoy-admin", "http://envoy:15000", "Envoy's admin address")
		fs.StringVar(&c.originAdmin, "origin-admin", "http://origin:9090", "the origin's admin address")
		fs.StringVar(&c.label, "label", "", "name for the result row, the settings it ran with")
		_ = fs.Parse(os.Args[2:])
		err = runClient(ctx, c)
	case "netem":
		dev := fs.String("dev", "eth0", "network interface")
		delay := fs.Duration("delay", 0, "delay added to every packet leaving the interface")
		_ = fs.Parse(os.Args[2:])
		err = addNetem(*dev, *delay)
	case "report":
		_ = fs.Parse(os.Args[2:])
		err = writeReport(os.Stdout, os.Stdin)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "envoy-sockmem:", err)
		os.Exit(1)
	}
}
