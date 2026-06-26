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

// glutton is a small benchmarking workload that exposes a gRPC API for
// consuming RAM, disk, and file descriptors, and for gossiping with
// other glutton instances. See internal/proto/glutton/glutton.proto.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/pflag"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/glutton"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
)

const meterName = "glutton"

var (
	listenAddr        = pflag.String("grpc-listen-addr", ":8080", "Address and port the gRPC server should listen on.")
	metricsListenAddr = pflag.String("metrics-listen-addr", ":9090", "Address and port the Prometheus metrics server should listen on.")
	dataDir           = pflag.String("data-dir", "", "Directory under which WriteDisk files are stored. Required.")

	showVersion = pflag.Bool("version", false, "Print version and exit.")
)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "--data-dir is required")
		os.Exit(2)
	}

	ctx := context.Background()
	serverboot.InitLogger()

	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: "glutton",
		Sampler:     sdktrace.ParentBased(sdktrace.NeverSample()),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetrics(ctx, "glutton")
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	if err := os.MkdirAll(*dataDir, 0o700); err != nil {
		serverboot.Fatal(ctx, "Failed to create data directory", fmt.Errorf("%s: %w", *dataDir, err))
	}

	svc, err := newGluttonService(*dataDir)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to construct glutton service", err)
	}
	defer svc.Close()

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to start listener", fmt.Errorf("%s: %w", *listenAddr, err))
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	glutton.RegisterGluttonServer(srv, svc)
	reflection.Register(srv)

	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{
		Addr:         *metricsListenAddr,
		EnableReadyz: true,
	})

	slog.InfoContext(ctx, "glutton starting",
		slog.String("grpc-listen-addr", *listenAddr),
		slog.String("metrics-listen-addr", *metricsListenAddr),
		slog.String("data-dir", *dataDir),
	)
	if err := srv.Serve(lis); err != nil {
		serverboot.Fatal(ctx, "Failed to serve", err)
	}
}

// diskKeyRE rejects anything that could escape the data dir or hit a
// hidden file: only alphanumerics, underscore, and dash are permitted.
var diskKeyRE = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type gluttonService struct {
	glutton.UnimplementedGluttonServer

	dataDir string

	// TODO: split this into per-resource locks (ram, fds, peers). A single
	// global mutex serializes unrelated operations across all three.
	mu    sync.Mutex
	ram   map[string][]byte
	fds   []*os.File
	peers map[string]*peerGossip

	ramWriteBytes  metric.Int64Counter
	diskWriteBytes metric.Int64Counter
	pingsReceived  metric.Int64Counter
	gossipSent     metric.Int64Counter
	gossipLatency  metric.Float64Histogram
}

type peerGossip struct {
	host    string
	delayMs int32
	cancel  context.CancelFunc
	done    chan struct{}
}

func newGluttonService(dir string) (*gluttonService, error) {
	s := &gluttonService{
		dataDir: dir,
		ram:     make(map[string][]byte),
		peers:   make(map[string]*peerGossip),
	}

	m := otel.Meter(meterName)

	var err error
	s.ramWriteBytes, err = m.Int64Counter(
		"glutton.ram.write.bytes",
		metric.WithUnit("By"),
		metric.WithDescription("Total bytes written to RAM via WriteRAM over the process lifetime."),
	)
	if err != nil {
		return nil, fmt.Errorf("create glutton.ram.write.bytes counter: %w", err)
	}
	s.diskWriteBytes, err = m.Int64Counter(
		"glutton.disk.write.bytes",
		metric.WithUnit("By"),
		metric.WithDescription("Total bytes written to disk via WriteDisk over the process lifetime."),
	)
	if err != nil {
		return nil, fmt.Errorf("create glutton.disk.write.bytes counter: %w", err)
	}
	s.pingsReceived, err = m.Int64Counter(
		"glutton.ping.requests",
		metric.WithDescription("Number of Ping requests received."),
	)
	if err != nil {
		return nil, fmt.Errorf("create glutton.ping.requests counter: %w", err)
	}
	s.gossipSent, err = m.Int64Counter(
		"glutton.gossip.requests.sent",
		metric.WithDescription("Number of gossip Ping requests sent per peer."),
	)
	if err != nil {
		return nil, fmt.Errorf("create glutton.gossip.requests.sent counter: %w", err)
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
		return nil, fmt.Errorf("create glutton.gossip.latency histogram: %w", err)
	}

	fdsOpen, err := m.Int64ObservableGauge(
		"glutton.fds.open",
		metric.WithDescription("File descriptors currently held open by OpenFD."),
	)
	if err != nil {
		return nil, fmt.Errorf("create glutton.fds.open gauge: %w", err)
	}
	peerDelay, err := m.Int64ObservableGauge(
		"glutton.gossip.delay",
		metric.WithUnit("ms"),
		metric.WithDescription("Configured gossip delay per peer."),
	)
	if err != nil {
		return nil, fmt.Errorf("create glutton.gossip.delay gauge: %w", err)
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
		return nil, fmt.Errorf("register glutton observable callback: %w", err)
	}

	return s, nil
}

// Close cancels every running gossip goroutine and waits for them to exit.
func (s *gluttonService) Close() {
	s.mu.Lock()
	peers := s.peers
	s.peers = make(map[string]*peerGossip)
	s.mu.Unlock()
	for _, p := range peers {
		p.cancel()
		<-p.done
	}
}

// Write to RAM, either overwriting previously-used RAM or allocating additional RAM
// per request instructions. Data written will be random bytes.
func (s *gluttonService) WriteRAM(ctx context.Context, req *glutton.WriteRAMRequest) (*glutton.WriteRAMResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if req.GetSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "size must be non-negative")
	}
	size := int(req.GetSize())

	switch req.GetWriteMode() {
	case glutton.WriteMode_WRITE_MODE_TRUNCATE:
		buf, err := randomBytes(size)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "generate random bytes: %v", err)
		}
		s.mu.Lock()
		s.ram[req.GetKey()] = buf
		s.mu.Unlock()
	case glutton.WriteMode_WRITE_MODE_OVERWRITE:
		s.mu.Lock()
		existing := s.ram[req.GetKey()]
		if size > len(existing) {
			existing = make([]byte, size)
			s.ram[req.GetKey()] = existing
		}
		if _, err := rand.Read(existing[:size]); err != nil {
			s.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "generate random bytes: %v", err)
		}
		s.mu.Unlock()
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown write_mode %v", req.GetWriteMode())
	}

	s.ramWriteBytes.Add(ctx, int64(size))
	return &glutton.WriteRAMResponse{}, nil
}

// Write to disk using the specified mode. Data written will be random bytes.
func (s *gluttonService) WriteDisk(ctx context.Context, req *glutton.WriteDiskRequest) (*glutton.WriteDiskResponse, error) {
	if !diskKeyRE.MatchString(req.GetKey()) {
		return nil, status.Errorf(codes.InvalidArgument, "key %q must match %s", req.GetKey(), diskKeyRE)
	}
	if req.GetSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "size must be non-negative")
	}

	path := filepath.Join(s.dataDir, req.GetKey())

	var flag int
	switch req.GetWriteMode() {
	case glutton.WriteMode_WRITE_MODE_TRUNCATE:
		flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	case glutton.WriteMode_WRITE_MODE_OVERWRITE:
		// No O_TRUNC: writes go from offset 0 but any bytes beyond size remain.
		flag = os.O_WRONLY | os.O_CREATE
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown write_mode %v", req.GetWriteMode())
	}

	f, err := os.OpenFile(path, flag, 0o600)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open %s: %v", path, err)
	}
	defer f.Close()

	if err := streamRandomBytes(f, int64(req.GetSize())); err != nil {
		return nil, status.Errorf(codes.Internal, "write %s: %v", path, err)
	}

	s.diskWriteBytes.Add(ctx, int64(req.GetSize()))
	return &glutton.WriteDiskResponse{}, nil
}

// Make sure it has the specified number of file descriptors open. It will open or
// close file descriptors to hit the desired count (note this count is in addition to the other
// FDs needed to run the process).
func (s *gluttonService) OpenFD(_ context.Context, req *glutton.OpenFDRequest) (*glutton.OpenFDResponse, error) {
	if req.GetCount() < 0 {
		return nil, status.Error(codes.InvalidArgument, "count must be non-negative")
	}
	target := int(req.GetCount())

	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.fds) > target {
		last := len(s.fds) - 1
		if err := s.fds[last].Close(); err != nil {
			slog.Warn("Failed to close glutton fd", slog.Any("err", err))
		}
		s.fds[last] = nil
		s.fds = s.fds[:last]
	}
	for len(s.fds) < target {
		f, err := os.Open(os.DevNull)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "open %s: %v", os.DevNull, err)
		}
		s.fds = append(s.fds, f)
	}
	return &glutton.OpenFDResponse{}, nil
}

// Receive a ping request, echoing the same response back.
func (s *gluttonService) Ping(ctx context.Context, req *glutton.PingRequest) (*glutton.PingResponse, error) {
	s.pingsReceived.Add(ctx, 1)
	return &glutton.PingResponse{Message: req.GetMessage()}, nil
}

// Sends network traffic to a peer glutton. Messages will be sent
// on regular intervals separated by delay_ms.
func (s *gluttonService) Gossip(_ context.Context, req *glutton.GossipRequest) (*glutton.GossipResponse, error) {
	want := make(map[string]*glutton.Peer, len(req.GetPeers()))
	for _, p := range req.GetPeers() {
		if p.GetHost() == "" {
			return nil, status.Error(codes.InvalidArgument, "peer host is required")
		}
		if p.GetDelayMs() <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "peer %q delay_ms must be positive", p.GetHost())
		}
		want[p.GetHost()] = p
	}

	s.mu.Lock()
	var toStop []*peerGossip
	for host, existing := range s.peers {
		w, ok := want[host]
		if !ok || w.GetDelayMs() != existing.delayMs {
			toStop = append(toStop, existing)
			delete(s.peers, host)
		}
	}
	var toStart []*glutton.Peer
	for host, w := range want {
		if _, ok := s.peers[host]; !ok {
			toStart = append(toStart, w)
		}
	}
	s.mu.Unlock()

	for _, p := range toStop {
		p.cancel()
		<-p.done
	}

	for _, w := range toStart {
		gctx, cancel := context.WithCancel(context.Background())
		pg := &peerGossip{
			host:    w.GetHost(),
			delayMs: w.GetDelayMs(),
			cancel:  cancel,
			done:    make(chan struct{}),
		}
		s.mu.Lock()
		s.peers[w.GetHost()] = pg
		s.mu.Unlock()
		go s.runGossip(gctx, pg)
	}

	return &glutton.GossipResponse{}, nil
}

func (s *gluttonService) runGossip(ctx context.Context, pg *peerGossip) {
	defer close(pg.done)

	// grpc.NewClient resolves and connects lazily; the first RPC surfaces
	// any failure, so the peer doesn't have to be reachable at start time.
	conn, err := grpc.NewClient(pg.host,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to dial gossip peer", slog.String("host", pg.host), slog.Any("err", err))
		return
	}
	defer conn.Close()
	client := glutton.NewGluttonClient(conn)

	hostAttr := attribute.String("host", pg.host)
	ticker := time.NewTicker(time.Duration(pg.delayMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		msg := uuid.NewString()
		start := time.Now()
		resp, err := client.Ping(ctx, &glutton.PingRequest{Message: msg})
		latency := time.Since(start).Seconds()
		outcome := "ok"
		cancelled := err != nil && errors.Is(ctx.Err(), context.Canceled)
		switch {
		case cancelled:
			outcome = "cancelled"
		case err != nil:
			outcome = "error"
		}
		attrs := metric.WithAttributes(hostAttr, attribute.String("outcome", outcome))
		s.gossipSent.Add(ctx, 1, attrs)
		s.gossipLatency.Record(ctx, latency, attrs)
		if cancelled {
			return
		}
		if err != nil {
			slog.WarnContext(ctx, "Gossip ping failed", slog.String("host", pg.host), slog.Any("err", err))
			continue
		}
		if resp.GetMessage() != msg {
			slog.WarnContext(ctx, "Gossip ping returned unexpected message",
				slog.String("host", pg.host),
				slog.String("sent", msg),
				slog.String("received", resp.GetMessage()),
			)
		}
	}
}

func randomBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// streamRandomBytesChunk caps per-syscall random fill and write size so a
// multi-gigabyte WriteDisk doesn't have to materialize in RAM.
const streamRandomBytesChunk = 1 << 20 // 1 MiB

// streamRandomBytes writes total random bytes to w sequentially, in
// streamRandomBytesChunk-sized chunks. The caller is responsible for the
// file's open mode and starting offset; this writes from the current
// position forward.
func streamRandomBytes(w io.Writer, total int64) error {
	if total <= 0 {
		return nil
	}
	buf := make([]byte, streamRandomBytesChunk)
	var written int64
	for written < total {
		chunk := buf
		if remaining := total - written; remaining < int64(len(chunk)) {
			chunk = buf[:remaining]
		}
		if _, err := rand.Read(chunk); err != nil {
			return fmt.Errorf("generate random bytes: %w", err)
		}
		n, err := w.Write(chunk)
		if err != nil {
			return err
		}
		written += int64(n)
	}
	return nil
}
