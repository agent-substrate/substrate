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

package glutton

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// initMetrics builds every instrument the service reports and stores the
// synchronous ones on s. The two observable gauges stay local: only the
// callback registered here reads them.
func (s *Service) initMetrics() error {
	m := otel.Meter(Name)

	var err error
	s.ramWriteBytes, err = m.Int64Counter(
		"glutton.ram.write.bytes",
		metric.WithUnit("By"),
		metric.WithDescription("Total bytes written to RAM via WriteRAM over the process lifetime."),
	)
	if err != nil {
		return fmt.Errorf("create glutton.ram.write.bytes counter: %w", err)
	}
	s.ramReadBytes, err = m.Int64Counter(
		"glutton.ram.read.bytes",
		metric.WithUnit("By"),
		metric.WithDescription("Total bytes walked by ReadRAM over the process lifetime."),
	)
	if err != nil {
		return fmt.Errorf("create glutton.ram.read.bytes counter: %w", err)
	}
	s.diskWriteBytes, err = m.Int64Counter(
		"glutton.disk.write.bytes",
		metric.WithUnit("By"),
		metric.WithDescription("Total bytes written to disk via WriteDisk over the process lifetime."),
	)
	if err != nil {
		return fmt.Errorf("create glutton.disk.write.bytes counter: %w", err)
	}
	s.diskReadBytes, err = m.Int64Counter(
		"glutton.disk.read.bytes",
		metric.WithUnit("By"),
		metric.WithDescription("Total bytes read from disk via ReadDisk over the process lifetime."),
	)
	if err != nil {
		return fmt.Errorf("create glutton.disk.read.bytes counter: %w", err)
	}
	s.pingsReceived, err = m.Int64Counter(
		"glutton.ping.requests",
		metric.WithDescription("Number of Ping requests received."),
	)
	if err != nil {
		return fmt.Errorf("create glutton.ping.requests counter: %w", err)
	}
	s.gossipSent, err = m.Int64Counter(
		"glutton.gossip.requests.sent",
		metric.WithDescription("Number of gossip Ping requests sent per peer."),
	)
	if err != nil {
		return fmt.Errorf("create glutton.gossip.requests.sent counter: %w", err)
	}
	s.gossipLatency, err = m.Float64Histogram(
		"glutton.gossip.latency",
		metric.WithUnit("s"),
		metric.WithDescription("Latency of gossip Ping requests per peer."),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		),
	)
	if err != nil {
		return fmt.Errorf("create glutton.gossip.latency histogram: %w", err)
	}

	fdsOpen, err := m.Int64ObservableGauge(
		"glutton.fds.open",
		metric.WithDescription("File descriptors currently held open by OpenFD."),
	)
	if err != nil {
		return fmt.Errorf("create glutton.fds.open gauge: %w", err)
	}
	peerDelay, err := m.Int64ObservableGauge(
		"glutton.gossip.delay",
		metric.WithUnit("ms"),
		metric.WithDescription("Configured gossip delay per peer."),
	)
	if err != nil {
		return fmt.Errorf("create glutton.gossip.delay gauge: %w", err)
	}

	if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		o.ObserveInt64(fdsOpen, int64(len(s.fds)))
		for host, p := range s.peers {
			o.ObserveInt64(peerDelay, int64(p.delayMs), metric.WithAttributes(attribute.String("host", host)))
		}
		return nil
	}, fdsOpen, peerDelay); err != nil {
		return fmt.Errorf("register glutton observable callback: %w", err)
	}

	return nil
}
