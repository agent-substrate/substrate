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

// boomer-worker is the Go re-implementation of the GluttonUser locust test.
// It speaks the locust worker protocol via myzhan/boomer, so it appears as a
// regular worker to the Python locust master while sidestepping gevent's
// scheduling tax.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	btrace "github.com/agent-substrate/substrate/internal/benchmarking/boomer/trace"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/myzhan/boomer"

	// Register user classes via init():
	_ "github.com/agent-substrate/substrate/internal/benchmarking/boomer/glutton"
)

func main() {
	var (
		apiEndpoint             = flag.String("api-endpoint", "k8s:///api.ate-system.svc.cluster.local:443", "ateapi gRPC dial target.")
		routerURL               = flag.String("router-url", "http://atenet-router.ate-system.svc.cluster.local", "atenet HTTP router base URL (no trailing slash).")
		atespace                = flag.String("atespace", "benchmark", "Atespace every actor this worker creates lives in. Ensured (CreateAtespace, AlreadyExists is ok) at startup.")
		promAddr                = flag.String("prometheus-addr", ":8001", "Address for the Prometheus /metrics endpoint.")
		configJSON              = flag.String("config-json", "", "Initial dynconfig as a JSON object (keys: trace_probability, min_wait_time, max_wait_time, min_live_time, max_live_time in seconds, durdir_file_size_bytes, resume_mode, durdir_read_mode, durdir_template, mem_target, mem_churn, mem_read, max_pings_per_wake). Unset fields keep their built-in defaults.")
		masterWebPort           = flag.Int("master-web-port", 0, "If non-zero, fetch dynconfig from http://{master-host}:{master-web-port}/boomer-config on each spawn message. Exits if the first fetch fails; later failures keep the last fetched values. {master-host} comes from boomer's existing --master-host flag.")
		configPollInterval      = flag.Duration("config-poll-interval", 10*time.Second, "With --master-web-port, also fetch dynconfig on this interval. A spawn message comes only when the number of users or the spawn rate changes, thus a load shape that changes the sample rate alone needs this. Zero stops the polling.")
		userClass               = flag.String("user-class", "glutton", fmt.Sprintf("Locust user class to run, lowercase; one of %s.", strings.Join(userclass.Names(), "|")))
		actorsPerUser           = flag.Int("actors-per-user", 1, "Number of actors each user (VU) creates and cycles through in round-robin: on iteration i, the user targets actor i%actors-per-user. Startup creates all actors; shutdown suspends+deletes them.")
		httpMaxIdleConnsPerHost = flag.Int("http-max-idle-conns-per-host", 10000, "Idle HTTP connections the router client keeps per host. Set it to at least the number of users this worker runs, so each VU reuses its connection to the router across wakes instead of opening a new one per request.")
	)
	// boomer.Run will call flag.Parse() if we haven't yet; calling here so
	// our flag-derived values are usable before that.
	flag.Parse()

	class := strings.ToLower(*userClass)

	if *actorsPerUser < 1 {
		slog.Error("fatal: --actors-per-user must be >= 1",
			slog.Int("actors_per_user", *actorsPerUser))
		os.Exit(1)
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	initialCfg, err := dynconfig.Parse([]byte(*configJSON), dynconfig.Config{
		MaxWait:         500 * time.Millisecond,
		MaxPingsPerWake: 1,
	})
	if err != nil {
		slog.Error("failed to parse --config-json", slog.String("err", err.Error()))
		os.Exit(1)
	}

	ctx := context.Background()
	sampler := btrace.NewUpdatableSampler(initialCfg.TraceProbability)
	tp, err := btrace.Init(ctx, "substrate-boomer", sampler)
	if err != nil {
		slog.Error("failed to initialize tracing", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(shutdownCtx)
	}()

	conn, apiStub, err := boomerutil.DialControl(*apiEndpoint)
	if err != nil {
		slog.Error("failed to dial ateapi", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer conn.Close()

	// Every VU talks to the one router address, so the transport must keep
	// as many idle connections as there are VUs. The default keeps two per
	// host; at thousands of VUs each request then opens and closes its own
	// connection, and the pod exhausts its ephemeral ports on TIME_WAIT
	// ("connect: cannot assign requested address"). The idle timeout must
	// outlast a VU's gap between requests, which is the wake window plus the
	// suspend/resume round trip.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 0 // no total cap; the per-host cap governs
	transport.MaxIdleConnsPerHost = *httpMaxIdleConnsPerHost
	transport.IdleConnTimeout = 5 * time.Minute
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: transport}

	dyn := dynconfig.NewHolder(initialCfg)

	if *masterWebPort > 0 {
		masterHost := flag.Lookup("master-host").Value.String()
		configURL := fmt.Sprintf("http://%s:%d/boomer-config", masterHost, *masterWebPort)
		// The first fetch must succeed: without it the worker runs on its
		// command-line defaults, not the operator's values. Later spawn
		// fetches can fail without ending the run. A busy master misses the
		// 5s deadline during a large ramp, and the worker already holds the
		// last value it served.
		if err := dynconfig.SubscribeSpawn(configURL, dyn, sampler, 5*time.Second, func(err error, fetched bool) {
			if fetched {
				slog.Warn("dynconfig spawn fetch failed; keeping the current values",
					slog.String("url", configURL), slog.String("err", err.Error()))
				return
			}
			slog.Error("fatal: first dynconfig fetch failed; exiting so the pod restarts",
				slog.String("url", configURL), slog.String("err", err.Error()))
			os.Exit(1)
		}); err != nil {
			slog.Error("fatal: failed to subscribe to boomer:spawn",
				slog.String("err", err.Error()))
			os.Exit(1)
		}
		// A spawn message is not sufficient by itself: locust sends one only
		// when the number of users or the spawn rate changes. Poll also, thus
		// a step of a load shape that changes only the sample rate reaches
		// this worker. A failed poll is not fatal, because the worker holds
		// the last good value.
		dynconfig.StartPoll(ctx, configURL, dyn, sampler,
			*configPollInterval, 5*time.Second, func(err error) {
				slog.Warn("dynconfig poll failed; keeping the current values",
					slog.String("url", configURL), slog.String("err", err.Error()))
			})
		slog.Info("dynconfig fetch enabled",
			slog.String("url", configURL),
			slog.Duration("poll_interval", *configPollInterval))
	}

	cfg := &userclass.Config{
		APIStub:       apiStub,
		HTTPClient:    httpClient,
		RouterURL:     *routerURL,
		Atespace:      *atespace,
		Dyn:           dyn,
		ActorsPerUser: *actorsPerUser,
	}

	entry, ok := userclass.Lookup(class)
	if !ok {
		slog.Error("fatal: unknown --user-class value",
			slog.String("user_class", *userClass),
			slog.String("known", strings.Join(userclass.Names(), ",")))
		os.Exit(1)
	}
	taskFn, shutdownFn := entry.Init(cfg)

	slog.Info("registered boomer task",
		slog.String("user_class", entry.UserClass),
		slog.String("user_class_flag", class),
	)

	metricsCtx, metricsCancel := context.WithCancel(context.Background())
	defer metricsCancel()
	go func() {
		if err := bmetrics.Serve(metricsCtx, *promAddr); err != nil {
			slog.Error("prometheus server stopped", slog.String("err", err.Error()))
		}
	}()

	// Blocks until SIGINT/SIGTERM or master quit. Boomer registers its own
	// signal handlers; we do cleanup after it returns.
	boomer.Run(&boomer.Task{
		Name:   entry.UserClass,
		Weight: 1,
		Fn:     taskFn,
	})

	slog.Info("boomer exited; suspending+deleting actors")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	shutdownFn(shutdownCtx)
}
