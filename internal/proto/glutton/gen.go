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

//go:generate bash -c "../../../hack/protoc.sh --plugin=protoc-gen-go=$(bash ../../../hack/run-tool.sh --print-bin-path protoc-gen-go) --plugin=protoc-gen-go-grpc=$(bash ../../../hack/run-tool.sh --print-bin-path protoc-gen-go-grpc) --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. glutton.proto"
