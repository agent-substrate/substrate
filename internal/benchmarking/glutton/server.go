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
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

// Handler builds the request handler for the given wire mode. ModeGRPC serves
// gRPC alongside the readiness probe on a single listener; ModeHTTP serves the
// protobuf-over-HTTP route table. An unknown mode comes back as an error so
// the caller decides how to fail.
func Handler(mode string, svc *Service) (http.Handler, error) {
	var handler http.Handler
	switch mode {
	case ModeGRPC:
		srv := grpc.NewServer(
			grpc.StatsHandler(otelgrpc.NewServerHandler()),
		)
		gluttonpb.RegisterGluttonServer(srv, svc)
		reflection.Register(srv)
		// The readiness probe is an HTTP GET, so gRPC mode serves it next to
		// the gRPC handler on the same listener.
		handler = splitGRPC(srv, readyzMux())
	case ModeHTTP:
		// otelhttp at the mux level + per-handler span follows
		// docs/dev/best-practices/tracing.md: extract incoming context,
		// then name the span after the operation in each handler.
		handler = otelhttp.NewHandler(newMux(svc), "/")
	default:
		return nil, fmt.Errorf("must be %s or %s: %q", ModeGRPC, ModeHTTP, mode)
	}
	return handler, nil
}

// NewServer enables unencrypted HTTP/2 so gRPC works on the plaintext
// listener, alongside HTTP/1.1 for the readyz probe.
func NewServer(handler http.Handler) *http.Server {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Server{Handler: handler, Protocols: protocols}
}

// splitGRPC serves gRPC and plain HTTP on one listener: requests with a
// gRPC content-type go to grpcSrv, everything else to rest. All glutton
// RPCs are unary, which is what grpc.Server.ServeHTTP supports.
func splitGRPC(grpcSrv, rest http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcSrv.ServeHTTP(w, r)
			return
		}
		rest.ServeHTTP(w, r)
	})
}

// readyzMux serves the readiness probe both modes need.
func readyzMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc(ReadyzRoute, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// newMux builds the HTTP-mode route table on top of the readiness probe.
func newMux(svc *Service) *http.ServeMux {
	mux := readyzMux()
	mux.HandleFunc(PingRoute, protoRoute("Ping", svc.Ping))
	mux.HandleFunc(WriteDiskRoute, protoRoute("WriteDisk", svc.WriteDisk))
	mux.HandleFunc(ReadDiskRoute, protoRoute("ReadDisk", svc.ReadDisk))
	mux.HandleFunc(WriteRAMRoute, protoRoute("WriteRAM", svc.WriteRAM))
	mux.HandleFunc(ReadRAMRoute, protoRoute("ReadRAM", svc.ReadRAM))
	return mux
}

// protoRoute wraps a protobuf handler with POST-only routing, protobuf
// unmarshaling, status code mapping, and server-timing headers.
func protoRoute[Req any, Resp proto.Message, PtrReq interface {
	*Req
	proto.Message
}](spanName string, handler func(context.Context, PtrReq) (Resp, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var req Req
		ptrReq := PtrReq(&req)
		if err := proto.Unmarshal(body, ptrReq); err != nil {
			http.Error(w, "unmarshal: "+err.Error(), http.StatusBadRequest)
			return
		}
		ctx, span := otel.Tracer(Name).Start(r.Context(), spanName)
		defer span.End()
		resp, err := handler(ctx, ptrReq)
		if err != nil {
			if st, ok := status.FromError(err); ok {
				switch st.Code() {
				case codes.InvalidArgument:
					http.Error(w, st.Message(), http.StatusBadRequest)
				case codes.NotFound:
					http.Error(w, st.Message(), http.StatusNotFound)
				default:
					http.Error(w, st.Message(), http.StatusInternalServerError)
				}
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		out, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Glutton does not run ateinterceptors, so without this the serve path has no
		// server-side timing at all. Mirrors the control-plane gRPC trailer so boomer's
		// elapsedFromMD logic (source=server) works identically over HTTP.
		w.Header().Set(ateinterceptors.ServerElapsedTrailer,
			strconv.FormatInt(time.Since(start).Microseconds(), 10))
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(out)
	}
}
