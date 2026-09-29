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

package resources

// DurableDirTarFile is the snapshot file holding the tar of the actor's
// durable-dir volumes. atelet uploads it alone when a paused actor's FULL
// capture is suspended as DATA.
//
// TODO: atelet should ask for the scope it wants and upload whatever files the
// snapshot has, without knowing this name.
const DurableDirTarFile = "durable-dir.tar"
