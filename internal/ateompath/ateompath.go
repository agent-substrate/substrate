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

// Ateom and atelet need to agree on many filesystem paths.  They are defined in this package.
package ateompath

import (
	"fmt"
	"path/filepath"
)

// MaxUnixSocketPathLen is the practical Linux limit for unix-domain socket
// paths. The kernel's sockaddr_un.sun_path is 108 bytes including the trailing
// NUL, leaving 107 usable bytes. Bind fails with EINVAL above this.
const MaxUnixSocketPathLen = 107

const (
	// The base path.  This is both the path of the root shared folder on the
	// host filesystem, and when it is mounted into ateom and atelet containers.
	BasePath = "/run/ateom-gvisor"
)

var (
	// StaticFilesDir holds things like downloaded runsc binaries.
	StaticFilesDir = filepath.Join(BasePath, "static-files")
)

func RunSCBinaryPath(sha256 string) string {
	return filepath.Join(StaticFilesDir, "runsc-"+sha256)
}

func AteomPath(ateomNamespace, ateomName string) string {
	return filepath.Join(
		BasePath,
		"ateoms",
		ateomNamespace+":"+ateomName,
	)
}

func AteomSocketPath(ateomNamespace, ateomName string) string {
	return filepath.Join(
		AteomPath(ateomNamespace, ateomName),
		"ateom.sock",
	)
}

// ValidateAteomSocketPath returns a descriptive error when the socket path
// derived from ateomNamespace and ateomName would exceed Linux's unix-socket
// limit. Calling net.Listen("unix", ...) with an over-limit path otherwise
// fails with the cryptic "bind: invalid argument".
func ValidateAteomSocketPath(ateomNamespace, ateomName string) error {
	p := AteomSocketPath(ateomNamespace, ateomName)
	if len(p) > MaxUnixSocketPathLen {
		return fmt.Errorf(
			"ateom socket path %q is %d bytes, exceeds Linux unix-socket limit of %d: shorten the namespace or pod name (%d + %d = %d chars used for namespace + name)",
			p, len(p), MaxUnixSocketPathLen,
			len(ateomNamespace), len(ateomName), len(ateomNamespace)+len(ateomName),
		)
	}
	return nil
}

func AteomNetNSName(ateomNamespace, ateomName string) string {
	return "ateom:" + ateomNamespace + ":" + ateomName
}

func AteomNetNSPath(ateomNamespace, ateomName string) string {
	return filepath.Join(
		"/run/netns",
		AteomNetNSName(ateomNamespace, ateomName),
	)
}

func ActorPath(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		BasePath,
		"actors",
		actorTemplateNamespace+":"+actorTemplateName+":"+actorID,
	)
}

func RunSCStateDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"runsc-state",
	)
}

func OCIBundleDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"bundles",
	)
}

func OCIBundlePath(actorTemplateNamespace, actorTemplateName, actorID, containerName string) string {
	return filepath.Join(
		OCIBundleDir(actorTemplateNamespace, actorTemplateName, actorID),
		containerName,
	)
}

func RunscDebugLogDir(actorTemplateNamespace, actorTemplateName, actorID, containerName string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"runsc-debug-logs",
		containerName,
	)
}

func CheckpointDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"checkpoint",
	)
}

func CheckpointImgPath(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		CheckpointDir(actorTemplateNamespace, actorTemplateName, actorID),
		"checkpoint.img", // gvisor implementation detail, technically.
	)
}

func PagesImgPath(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		CheckpointDir(actorTemplateNamespace, actorTemplateName, actorID),
		"pages.img", // gvisor implementation detail, technically.
	)
}

func PagesMetaImgPath(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		CheckpointDir(actorTemplateNamespace, actorTemplateName, actorID),
		"pages_meta.img", // gvisor implementation detail, technically.
	)
}

func PIDFileDir(actorTemplateNamespace, actorTemplateName, actorID string) string {
	return filepath.Join(
		ActorPath(actorTemplateNamespace, actorTemplateName, actorID),
		"pidfiles",
	)
}

func PIDFilePath(actorTemplateNamespace, actorTemplateName, actorID, containerName string) string {
	return filepath.Join(
		PIDFileDir(actorTemplateNamespace, actorTemplateName, actorID),
		containerName+".pid",
	)
}
