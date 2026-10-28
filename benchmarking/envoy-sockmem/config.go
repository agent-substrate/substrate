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

package main

import (
	_ "embed"
	"fmt"
	"io"
	"text/template"
)

var (
	//go:embed egress.yaml.tmpl
	egressTemplate string
	//go:embed ingress.yaml.tmpl
	ingressTemplate string
)

// tcpOptions are the socket options set on every Envoy socket; zero leaves the
// kernel default.
type tcpOptions struct {
	notSentLowat, sendBuffer, receiveBuffer int
}

// socketOption is an Envoy SocketOption, as the templates write it.
type socketOption struct {
	Description string
	Level, Name int
	Value       int
}

func (o tcpOptions) envoy() []socketOption {
	var opts []socketOption
	add := func(description string, level, name, value int) {
		if value > 0 {
			opts = append(opts, socketOption{description, level, name, value})
		}
	}
	add("TCP_NOTSENT_LOWAT", 6, 25, o.notSentLowat)
	add("SO_SNDBUF", 1, 7, o.sendBuffer)
	add("SO_RCVBUF", 1, 8, o.receiveBuffer)
	return opts
}

type origin struct {
	Name string
	Port int
}

// writeConfig writes the Envoy configuration of kind, ingress or egress, with
// tcp's options on every socket: the listener's and the clusters'.
//
// A positive upstreamStreamWindow sets the ingress Envoy's upstream HTTP/2 stream
// window, with a connection window 100 times as large, so one stalled stream
// cannot hold up the others; zero leaves Envoy's defaults, as the router does.
func writeConfig(w io.Writer, kind string, tcp tcpOptions, upstreamStreamWindow int) error {
	text := map[string]string{"egress": egressTemplate, "ingress": ingressTemplate}[kind]
	if text == "" {
		return fmt.Errorf("--kind must be ingress or egress, got %q", kind)
	}
	tmpl, err := template.New(kind).Parse(text)
	if err != nil {
		return err
	}
	return tmpl.Execute(w, map[string]any{
		"SocketOptions": tcp.envoy(),
		"Origins":       []origin{{"origin_source", sourcePort}, {"origin_stall", stallPort}, {"origin_discard", discardPort}},
		"HTTPPort":      httpPort,

		"UpstreamStreamWindow":     upstreamStreamWindow,
		"UpstreamConnectionWindow": 100 * upstreamStreamWindow,
	})
}
