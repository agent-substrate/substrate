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

// Package glutton implements the benchmarking workload served by
// cmd/benchmarking/glutton: an API for consuming RAM, disk, and file
// descriptors, and for gossiping with other glutton instances, over either
// gRPC or protobuf-over-HTTP. See internal/proto/glutton/glutton.proto.
package glutton

// Name is the workload's identity to the telemetry SDK: the tracer and meter
// scope, and the service name the binary reports. Instrument names are written
// out in full at each call site so they match docs/metrics/registry/metrics.yaml
// literally.
const Name = "glutton"

// Wire modes the main listener can serve, selected by the binary's --mode flag.
const (
	ModeGRPC = "grpc"
	ModeHTTP = "http"
)

// Routes the HTTP-mode mux serves. ReadyzRoute is served in both modes: ateom
// blocks RestoreWorkload until it answers 200, so ResumeActor cannot report
// success before the listener is reachable. The fake in ./fake aliases these,
// which is what keeps the stand-in and the real mux from drifting apart.
const (
	ReadyzRoute    = "/readyz"
	PingRoute      = "/ping"
	WriteDiskRoute = "/writedisk"
	ReadDiskRoute  = "/readdisk"
	WriteRAMRoute  = "/writeram"
	ReadRAMRoute   = "/readram"
)
