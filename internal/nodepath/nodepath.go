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

// Package nodepath holds the node-level paths more than one component needs:
// the host directory atelet and the ateoms both mount, the sockets and netns
// they find each other by, the per-actor parent directory, and where atelet
// stages runtime binaries.
package nodepath

import "path/filepath"

// BasePath is the root shared folder on the host filesystem, mounted at the
// same path into the atelet and ateom containers.
const BasePath = "/var/lib/ateom-gvisor"

// ActorsDir is the parent of the per-actor directories atelet prepares.
var ActorsDir = filepath.Join(BasePath, "actors")

// StaticFilesDir holds things like runsc binaries.
var StaticFilesDir = filepath.Join(BasePath, "static-files")

// AteomSupportSocket is the node-local atelet socket used by atunnel
// to request credentials for the worker's current actor assignment.
var AteomSupportSocket = filepath.Join(BasePath, "ateom-support.sock")

// AteletOTLPSocketPath is the node-scoped unix socket atelet serves the OTLP
// relay on (see internal/otlprelay). It is node-scoped rather than per-pod
// because every ateom on the node pushes into the same relay: atelet is a
// DaemonSet, so one socket collapses N per-pod collector connections into one
// per-node connection.
//
// It sits directly under BasePath, which is the host directory already mounted
// at the same path into atelet and into every ateom pod, so no new volume is
// needed for ateom to reach it. Note that BasePath is mounted writable
// (workerpool_apply.go) and shared with AteomSupportSocket and the image
// cache, so a worker pod can unlink or replace this socket. Confining
// atelet-owned sockets to a subdirectory mounted read-only would be an
// improvement, but it is a property of the whole BasePath mount rather than of
// this socket — a read-only subdir needs its own volume and mount, and the pod
// keeps CAP_SYS_ADMIN. Tracked separately rather than solved here.
func AteletOTLPSocketPath() string {
	return filepath.Join(
		BasePath,
		"atelet-otlp.sock",
	)
}

// AteomsDir is the parent of every per-ateom directory. Each ateom creates
// AteomPath(podUID) under it when it boots, so listing this directory is how a
// scraper with no prior knowledge discovers the node's ateoms.
func AteomsDir() string {
	return filepath.Join(BasePath, "ateoms")
}

func AteomPath(podUID string) string {
	return filepath.Join(AteomsDir(), podUID)
}

func AteomSocketPath(podUID string) string {
	return filepath.Join(
		AteomPath(podUID),
		"ateom.sock",
	)
}

// ActorNetNSName names an actor's sandbox network namespace.
func ActorNetNSName(actorUID string) string {
	return "ateom-actor:" + actorUID
}

// ActorNetNSPath is the mount path of the actor's named namespace.
func ActorNetNSPath(actorUID string) string {
	return filepath.Join("/run/netns", ActorNetNSName(actorUID))
}
