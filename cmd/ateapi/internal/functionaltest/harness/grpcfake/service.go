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

package grpcfake

import (
	"context"
	"slices"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Service faults and records the calls to the instrumented methods of a fake
// gRPC service. It does so in a server interceptor, so the fake's
// implementation holds only its state and behavior.
type Service[M ~string] struct {
	faults map[M]*Fault
	log    callLog[M]
}

// NewService returns a Service for methods.
func NewService[M ~string](methods ...M) *Service[M] {
	s := &Service[M]{faults: map[M]*Fault{}}
	for _, method := range methods {
		s.faults[method] = &Fault{}
	}
	return s
}

// NewServer returns a gRPC server that applies s to its calls.
func (s *Service[M]) NewServer() *grpc.Server {
	return grpc.NewServer(grpc.UnaryInterceptor(s.intercept))
}

// On returns the Fault deciding how calls to method behave.
func (s *Service[M]) On(method M) *Fault {
	return s.faults[method]
}

// Record starts a recording of the calls to s's methods from now on.
func (s *Service[M]) Record() *Recording[M] {
	return s.log.record()
}

// intercept records a call to one of s's methods and makes it as the method's
// Fault says.
func (s *Service[M]) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	method := M(info.FullMethod)
	fault, isInstrumented := s.faults[method]
	if !isInstrumented {
		return handler(ctx, req)
	}
	s.log.add(method, req.(proto.Message))
	return fault.run(ctx, func() (any, error) { return handler(ctx, req) })
}

// Call is one call a fake received, faulted or not.
type Call[M ~string] struct {
	Method  M
	Request proto.Message
}

// callLog records the calls a fake receives, in order.
type callLog[M ~string] struct {
	mu    sync.Mutex
	calls []Call[M]
}

// add records a call to method with a copy of req.
func (l *callLog[M]) add(method M, req proto.Message) {
	req = proto.Clone(req)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, Call[M]{Method: method, Request: req})
}

// record starts a recording of the calls l receives from now on.
func (l *callLog[M]) record() *Recording[M] {
	l.mu.Lock()
	defer l.mu.Unlock()
	return &Recording[M]{log: l, start: len(l.calls)}
}

// Recording holds the calls a fake receives from the moment it is started.
type Recording[M ~string] struct {
	log   *callLog[M]
	start int
}

// Calls returns the calls the fake received since r started, in order.
func (r *Recording[M]) Calls() []Call[M] {
	r.log.mu.Lock()
	defer r.log.mu.Unlock()
	return slices.Clone(r.log.calls[r.start:])
}
