// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1alpha1

//go:generate bash ../../../hack/run-tool.sh controller-gen crd:headerFile=../../../hack/boilerplate/sh.txt object:headerFile=../../../hack/boilerplate/go.txt paths="./" output:crd:dir="../../../manifests/ate-install/generated/"
//go:generate bash ../../../hack/run-tool.sh client-gen --go-header-file=../../../hack/boilerplate/go.txt --clientset-name versioned --input-base github.com/agent-substrate/substrate --input pkg/api/v1alpha1 --output-pkg github.com/agent-substrate/substrate/pkg/client/clientset --output-dir ../../../pkg/client/clientset
//go:generate bash ../../../hack/run-tool.sh lister-gen --go-header-file=../../../hack/boilerplate/go.txt --output-pkg github.com/agent-substrate/substrate/pkg/client/listers --output-dir ../../../pkg/client/listers github.com/agent-substrate/substrate/pkg/api/v1alpha1
//go:generate bash ../../../hack/run-tool.sh informer-gen --go-header-file=../../../hack/boilerplate/go.txt --versioned-clientset-package github.com/agent-substrate/substrate/pkg/client/clientset/versioned --listers-package github.com/agent-substrate/substrate/pkg/client/listers --output-pkg github.com/agent-substrate/substrate/pkg/client/informers --output-dir ../../../pkg/client/informers github.com/agent-substrate/substrate/pkg/api/v1alpha1
