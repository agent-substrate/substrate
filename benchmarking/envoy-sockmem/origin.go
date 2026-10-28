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
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Origin ports. Behind the egress-like Envoy, each CONNECT authority's port
// picks one of the TCP ports; behind the router-like Envoy, the HTTP port's
// paths do the same.
const (
	sourcePort  = 9001 // writes without end
	stallPort   = 9002 // answers one byte, drains, then reads nothing
	discardPort = 9003 // reads and drops everything
	httpPort    = 9100 // the same three, as HTTP/2 cleartext paths
	originAdmin = 9090 // POST /stall sets the stall
)

// chunk is what the origin and the client write at a time.
const chunk = 64 << 10

// stallSetting is what a stalled reader reads at full speed before it stalls,
// and when it stops stalling and closes. Behind an HTTP/2 hop, a stalled
// stream can use up the connection's receive window, which would stop the
// settings if they had to travel through the tunnel; so the client sets them
// here, out of band.
type stallSetting struct {
	mu    sync.Mutex
	until time.Time
	drain int64
}

func (s *stallSetting) get() (time.Time, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.until, s.drain
}

func (s *stallSetting) set(until time.Time, drain int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.until, s.drain = until, drain
}

// stall reads drain bytes from r, then nothing until until or until ctx ends.
func stall(ctx context.Context, r io.Reader, until time.Time, drain int64) {
	if _, err := io.CopyN(io.Discard, r, drain); err != nil {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Until(until)):
	}
}

// serveStall answers at once, reads drain bytes of the body, and then nothing
// until until. The until and drain query parameters override setting, so a
// client can set them on its request.
func serveStall(setting *stallSetting) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		until, drain := setting.get()
		if v, err := strconv.ParseInt(r.URL.Query().Get("until"), 10, 64); err == nil {
			until = time.Unix(v, 0)
		}
		if v, err := strconv.ParseInt(r.URL.Query().Get("drain"), 10, 64); err == nil {
			drain = v
		}
		rc := http.NewResponseController(w)
		// Read the body after answering, as a server streaming both ways
		// does.
		_ = rc.EnableFullDuplex()
		w.WriteHeader(http.StatusOK)
		_ = rc.Flush()
		stall(r.Context(), r.Body, until, drain)
	}
}

func writeForever(w io.Writer, flush func() error) {
	buf := make([]byte, chunk)
	for {
		if _, err := w.Write(buf); err != nil {
			return
		}
		if flush != nil {
			if err := flush(); err != nil {
				return
			}
		}
	}
}

func runOrigin(ctx context.Context) error {
	setting := &stallSetting{}
	tcp := map[int]func(net.Conn){
		sourcePort: func(c net.Conn) { writeForever(c, nil) },
		stallPort: func(c net.Conn) {
			if _, err := c.Write([]byte{'k'}); err != nil {
				return
			}
			until, drain := setting.get()
			stall(ctx, c, until, drain)
		},
		discardPort: func(c net.Conn) { _, _ = io.Copy(io.Discard, c) },
	}
	errs := make(chan error, len(tcp)+2)
	for port, handle := range tcp {
		lis, err := net.Listen("tcp", ":"+strconv.Itoa(port))
		if err != nil {
			return err
		}
		context.AfterFunc(ctx, func() { _ = lis.Close() })
		go func() {
			for {
				c, err := lis.Accept()
				if err != nil {
					errs <- err
					return
				}
				go func() {
					defer c.Close()
					handle(c)
				}()
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /stream", func(w http.ResponseWriter, _ *http.Request) {
		rc := http.NewResponseController(w)
		writeForever(w, rc.Flush)
	})
	mux.HandleFunc("POST /stall", serveStall(setting))
	mux.HandleFunc("POST /discard", func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, strconv.FormatInt(n, 10))
	})
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	servers := []*http.Server{
		{Addr: ":" + strconv.Itoa(httpPort), Handler: mux, Protocols: protocols},
		{Addr: ":" + strconv.Itoa(originAdmin), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			until, err1 := strconv.ParseInt(r.URL.Query().Get("until"), 10, 64)
			drain, err2 := strconv.ParseInt(r.URL.Query().Get("drain"), 10, 64)
			if r.Method != http.MethodPost || r.URL.Path != "/stall" || err1 != nil || err2 != nil {
				http.Error(w, "POST /stall?until=<unix seconds>&drain=<bytes>", http.StatusBadRequest)
				return
			}
			setting.set(time.Unix(until, 0), drain)
		})},
	}
	for _, s := range servers {
		context.AfterFunc(ctx, func() { _ = s.Close() })
		go func() { errs <- s.ListenAndServe() }()
	}
	log.Printf("origin serving TCP %d (source), %d (stall), %d (discard); HTTP %d; admin %d",
		sourcePort, stallPort, discardPort, httpPort, originAdmin)
	err := <-errs
	if ctx.Err() != nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
