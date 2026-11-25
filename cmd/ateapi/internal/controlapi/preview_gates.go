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

package controlapi

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
)

// PreviewMethodGates maps each method of services whose (method) annotation
// names a preview gate to that gate. The keys are gRPC full method names,
// such as "/substrate.ateapi.v1.Control/GetActor".
func PreviewMethodGates(services protoreflect.ServiceDescriptors) (map[string]preview.Gate, error) {
	gates := map[string]preview.Gate{}
	for i := range services.Len() {
		sd := services.Get(i)
		methods := sd.Methods()
		for j := range methods.Len() {
			md := methods.Get(j)
			gate, err := methodPreviewGate(md)
			if err != nil {
				return nil, err
			}
			if gate != "" {
				gates[grpcFullMethodName(sd, md)] = gate
			}
		}
	}
	return gates, nil
}

// grpcFullMethodName returns the gRPC full method name for md, such as
// "/substrate.ateapi.v1.Control/GetActor".
func grpcFullMethodName(sd protoreflect.ServiceDescriptor, md protoreflect.MethodDescriptor) string {
	return fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
}

func methodPreviewGate(md protoreflect.MethodDescriptor) (preview.Gate, error) {
	gate := proto.GetExtension(md.Options(), ateapi.E_Method).(*ateapi.MethodAnnotation).GetPreviewGate()
	if gate == "" {
		return "", nil
	}
	if md.IsStreamingClient() || md.IsStreamingServer() {
		return "", fmt.Errorf("%s: streaming methods cannot use preview_gate", md.FullName())
	}
	if gate == preview.All {
		return "", fmt.Errorf("%s: preview gate %q names every gate", md.FullName(), gate)
	}
	if err := preview.Verify(gate); err != nil {
		return "", fmt.Errorf("%s: %w", md.FullName(), err)
	}
	return preview.Gate(gate), nil
}
