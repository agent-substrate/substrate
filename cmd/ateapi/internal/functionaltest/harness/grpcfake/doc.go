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

// Package grpcfake adds fault injection and call recording to fake gRPC
// services, for tests that call a fake through a real gRPC client.
//
// A fake creates a Service for the methods it instruments, named by their
// generated full method names, and registers its own implementation on the
// server NewServer returns:
//
//	svc := grpcfake.NewService(pb.Foo_Bar_FullMethodName)
//	srv := svc.NewServer()
//	pb.RegisterFooServer(srv, impl)
//
// The server's unary interceptor handles every call to an instrumented
// method. It adds the call, with a copy of its request, to every Recording
// started with Record. Then it makes the call as the method's Fault says. The
// faults apply to the next calls, one each, in the order they were added:
//   - FailNext fails the call with a status, without making it.
//   - Block holds the call before it reaches the implementation.
//   - BlockReply makes the call and holds its reply.
//
// Block and BlockReply return a Gate. Gate.Wait waits until the held call
// arrives, and Gate.Release lets it go on. A held call whose caller gives up
// ends with the caller's context error. Calls to other methods, and streaming
// calls, reach the implementation unchanged and are not recorded.
//
// A fake usually names its methods with its own string type, such as
// type FooMethod string, so the compiler rejects another fake's method in On
// or in a Recording.
package grpcfake
