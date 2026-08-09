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

package controlapi

import (
	"regexp"
	"strings"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/templateversion"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	k8sresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Limits and defaults ported from the ActorTemplate CRD's kubebuilder markers
// (pkg/api/v1alpha1/actortemplate_types.go); keep the two in sync while both
// exist. This validator is a deliberate superset of the CRD: following
// upstream Kubernetes pod validation it also requires a non-empty containers
// list and unique mount paths within a container, which the CRD does not.
const (
	maxContainers        = 10
	maxCommandItems      = 64
	maxArgsItems         = 64
	maxEnvItems          = 32
	maxVolumes           = 32
	maxVolumeMounts      = 32
	maxMountPathLen      = 4096
	maxReadyzPathLen     = 1024
	maxReadyzTimeout     = 3600
	defaultReadyzTimeout = 30
	defaultReadyzPath    = "/readyz"
)

const pinnedImageMsg = "must be pinned by digest and contain '@' (changing the image invalidates snapshots)"

var (
	// envNameRegexp accepts any printable ASCII character except '='.
	envNameRegexp = regexp.MustCompile(`^[ -<>-~]+$`)
	// readyzPathRegexp accepts RFC 3986 path segments with well-formed
	// percent-escapes; query strings and fragments are excluded.
	readyzPathRegexp = regexp.MustCompile(`^/([A-Za-z0-9\-._~!$&'()*+,;=:@/]|%[0-9A-Fa-f]{2})*$`)
)

// defaultActorTemplateVersionSpec materializes server-applied defaults into
// the spec before validation and storage; shared with kubectl-ate, which
// must default identically before comparing manifests on re-apply.
func defaultActorTemplateVersionSpec(spec *ateapipb.ActorTemplateVersionSpec) {
	templateversion.DefaultSpec(spec)
}

// validateActorTemplateVersionSpec ports the ActorTemplate CRD's CEL and
// kubebuilder invariants onto the proto spec. Call
// defaultActorTemplateVersionSpec first: readyz fields are validated at their
// effective (defaulted) values.
func validateActorTemplateVersionSpec(spec *ateapipb.ActorTemplateVersionSpec, fldPath *field.Path) field.ErrorList {
	if spec == nil {
		return field.ErrorList{field.Required(fldPath, "")}
	}

	allErrs := field.ErrorList{}

	pauseImagePath := fldPath.Child("pause_image")
	if spec.GetPauseImage() == "" {
		allErrs = append(allErrs, field.Required(pauseImagePath, ""))
	} else if !strings.Contains(spec.GetPauseImage(), "@") {
		allErrs = append(allErrs, field.Invalid(pauseImagePath, spec.GetPauseImage(), pinnedImageMsg))
	}

	microvm := spec.GetSandboxConfig().GetSandboxClass() == ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM

	volumeNames, volumeErrs := validateVolumes(spec.GetVolumes(), microvm, fldPath.Child("volumes"))
	allErrs = append(allErrs, volumeErrs...)

	allErrs = append(allErrs, validateContainers(spec.GetContainers(), volumeNames, fldPath.Child("containers"))...)

	// Every declared volume must be mounted by at least one container.
	mounted := mountedVolumeNames(spec.GetContainers(), volumeNames)
	for i, v := range spec.GetVolumes() {
		if name := v.GetName(); name != "" && !mounted[name] {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("volumes").Index(i).Child("name"), name,
				"all volumes must be mounted by at least one container"))
		}
	}

	allErrs = append(allErrs, validateSnapshotsConfig(spec.GetSnapshotsConfig(), microvm, fldPath.Child("snapshots_config"))...)

	// sandbox_config is required in full: the server never defaults the class
	// or falls back to a cluster-default SandboxConfig for versions.
	cfgPath := fldPath.Child("sandbox_config")
	if cfg := spec.GetSandboxConfig(); cfg == nil {
		allErrs = append(allErrs, field.Required(cfgPath, ""))
	} else {
		classPath := cfgPath.Child("sandbox_class")
		switch class := cfg.GetSandboxClass(); class {
		case ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR, ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM:
		case ateapipb.SandboxClass_SANDBOX_CLASS_UNSPECIFIED:
			allErrs = append(allErrs, field.Required(classPath, ""))
		default:
			allErrs = append(allErrs, field.NotSupported(classPath, class, []string{
				ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR.String(),
				ateapipb.SandboxClass_SANDBOX_CLASS_MICROVM.String(),
			}))
		}

		configNamePath := cfgPath.Child("config_name")
		if cfg.GetConfigName() == "" {
			allErrs = append(allErrs, field.Required(configNamePath, ""))
		} else {
			for _, msg := range content.IsDNS1123Subdomain(cfg.GetConfigName()) {
				allErrs = append(allErrs, field.Invalid(configNamePath, cfg.GetConfigName(), msg))
			}
		}
	}

	return allErrs
}

// validateVolumes checks the declared volumes and returns the set of valid
// volume names for the mount cross-checks.
func validateVolumes(volumes []*ateapipb.Volume, microvm bool, fldPath *field.Path) (map[string]bool, field.ErrorList) {
	allErrs := field.ErrorList{}
	if len(volumes) > maxVolumes {
		allErrs = append(allErrs, field.TooMany(fldPath, len(volumes), maxVolumes))
	}

	volumeNames := make(map[string]bool, len(volumes))
	for i, v := range volumes {
		idxPath := fldPath.Index(i)

		namePath := idxPath.Child("name")
		if name := v.GetName(); name == "" {
			allErrs = append(allErrs, field.Required(namePath, ""))
		} else {
			allErrs = append(allErrs, resources.ValidateResourceName(name, namePath)...)
			if volumeNames[name] {
				allErrs = append(allErrs, field.Duplicate(namePath, name))
			}
			volumeNames[name] = true
		}

		switch source := v.GetSource().(type) {
		case *ateapipb.Volume_DurableDir:
			// No fields to validate.
		case *ateapipb.Volume_ExternalVolumeTemplate:
			allErrs = append(allErrs, validateExternalVolumeTemplate(source.ExternalVolumeTemplate, microvm, idxPath.Child("external_volume_template"))...)
		default:
			allErrs = append(allErrs, field.Required(idxPath.Child("source"), "exactly one volume source must be set"))
		}
	}
	return volumeNames, allErrs
}

func validateExternalVolumeTemplate(evt *ateapipb.ExternalVolumeTemplate, microvm bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if microvm {
		allErrs = append(allErrs, field.Forbidden(fldPath, "external volumes are not supported when sandbox_class is SANDBOX_CLASS_MICROVM"))
	}

	capacityPath := fldPath.Child("capacity")
	if capacity := evt.GetCapacity(); capacity == "" {
		allErrs = append(allErrs, field.Required(capacityPath, ""))
	} else if _, err := k8sresource.ParseQuantity(capacity); err != nil {
		allErrs = append(allErrs, field.Invalid(capacityPath, capacity, `must be a Kubernetes resource quantity (e.g. "10Gi")`))
	}

	storageClassPath := fldPath.Child("storage_class_name")
	if name := evt.GetStorageClassName(); name == "" {
		allErrs = append(allErrs, field.Required(storageClassPath, ""))
	} else {
		for _, msg := range content.IsDNS1123Subdomain(name) {
			allErrs = append(allErrs, field.Invalid(storageClassPath, name, msg))
		}
	}
	return allErrs
}

// validateContainers checks the containers list; per-container rules live in
// validateContainer.
func validateContainers(containers []*ateapipb.Container, volumeNames map[string]bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(containers) == 0 {
		return append(allErrs, field.Required(fldPath, ""))
	}
	if len(containers) > maxContainers {
		allErrs = append(allErrs, field.TooMany(fldPath, len(containers), maxContainers))
	}

	allNames := make(map[string]bool, len(containers))
	for i, c := range containers {
		idxPath := fldPath.Index(i)

		allErrs = append(allErrs, validateContainer(c, volumeNames, idxPath)...)

		// Container names must be unique within the spec.
		if name := c.GetName(); name != "" {
			if allNames[name] {
				allErrs = append(allErrs, field.Duplicate(idxPath.Child("name"), name))
			}
			allNames[name] = true
		}
	}
	return allErrs
}

// validateContainer checks a single container. Name rules match
// resources.ValidateContainerNames: a DNS label, not the reserved "pause"
// sandbox-infra name (uniqueness is checked by validateContainers).
func validateContainer(c *ateapipb.Container, volumeNames map[string]bool, path *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	namePath := path.Child("name")
	if name := c.GetName(); name == "" {
		allErrs = append(allErrs, field.Required(namePath, ""))
	} else {
		allErrs = append(allErrs, resources.ValidateResourceName(name, namePath)...)
		if name == "pause" {
			allErrs = append(allErrs, field.Invalid(namePath, name, "reserved for sandbox infrastructure"))
		}
	}

	imagePath := path.Child("image")
	if image := c.GetImage(); image == "" {
		allErrs = append(allErrs, field.Required(imagePath, ""))
	} else if !strings.Contains(image, "@") {
		allErrs = append(allErrs, field.Invalid(imagePath, image, pinnedImageMsg))
	}

	if n := len(c.GetCommand()); n > maxCommandItems {
		allErrs = append(allErrs, field.TooMany(path.Child("command"), n, maxCommandItems))
	}
	if n := len(c.GetArgs()); n > maxArgsItems {
		allErrs = append(allErrs, field.TooMany(path.Child("args"), n, maxArgsItems))
	}

	allErrs = append(allErrs, validateEnv(c.GetEnv(), path.Child("env"))...)

	if readyz := c.GetReadyz(); readyz != nil {
		allErrs = append(allErrs, validateReadyz(readyz, path.Child("readyz"))...)
	}

	allErrs = append(allErrs, validateVolumeMounts(c.GetVolumeMounts(), volumeNames, path.Child("volume_mounts"))...)

	return allErrs
}

func validateEnv(env []*ateapipb.EnvVar, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(env) > maxEnvItems {
		allErrs = append(allErrs, field.TooMany(fldPath, len(env), maxEnvItems))
	}

	for i, e := range env {
		idxPath := fldPath.Index(i)

		namePath := idxPath.Child("name")
		if name := e.GetName(); name == "" {
			allErrs = append(allErrs, field.Required(namePath, ""))
		} else if !envNameRegexp.MatchString(name) {
			allErrs = append(allErrs, field.Invalid(namePath, name, "may contain any printable ASCII character except '='"))
		}

		if e.GetSource() == nil {
			allErrs = append(allErrs, field.Required(idxPath.Child("value"), "exactly one source must be set"))
		}
	}
	return allErrs
}

// validateVolumeMounts checks one container's mounts against the declared
// volume names. Mount paths must be unique within the container.
func validateVolumeMounts(mounts []*ateapipb.VolumeMount, volumeNames map[string]bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(mounts) > maxVolumeMounts {
		allErrs = append(allErrs, field.TooMany(fldPath, len(mounts), maxVolumeMounts))
	}

	mountPaths := make(map[string]bool, len(mounts))
	for i, vm := range mounts {
		idxPath := fldPath.Index(i)

		namePath := idxPath.Child("name")
		if name := vm.GetName(); name == "" {
			allErrs = append(allErrs, field.Required(namePath, ""))
		} else if !volumeNames[name] {
			allErrs = append(allErrs, field.Invalid(namePath, name, "must match the name of a volume in spec.volumes"))
		}

		mountPathPath := idxPath.Child("mount_path")
		allErrs = append(allErrs, validateMountPath(vm.GetMountPath(), mountPathPath)...)
		if mountPaths[vm.GetMountPath()] {
			allErrs = append(allErrs, field.Invalid(mountPathPath, vm.GetMountPath(), "must be unique"))
		}
		mountPaths[vm.GetMountPath()] = true
	}
	return allErrs
}

// mountedVolumeNames returns the declared volume names referenced by at least
// one valid mount, for the every-volume-must-be-mounted cross-check.
func mountedVolumeNames(containers []*ateapipb.Container, volumeNames map[string]bool) map[string]bool {
	mounted := make(map[string]bool, len(volumeNames))
	for _, c := range containers {
		for _, vm := range c.GetVolumeMounts() {
			if volumeNames[vm.GetName()] {
				mounted[vm.GetName()] = true
			}
		}
	}
	return mounted
}

// validateReadyz checks a container's readiness probe at its effective
// (defaulted) values.
func validateReadyz(readyz *ateapipb.ContainerReadyz, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if httpGet := readyz.GetHttpGet(); httpGet == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("http_get"), ""))
	} else {
		allErrs = append(allErrs, validateHTTPGet(httpGet, fldPath.Child("http_get"))...)
	}

	timeoutPath := fldPath.Child("timeout_seconds")
	if timeout := readyz.GetTimeoutSeconds(); timeout < 1 || timeout > maxReadyzTimeout {
		allErrs = append(allErrs, field.Invalid(timeoutPath, timeout, "must be between 1 and 3600"))
	}

	return allErrs
}

func validateHTTPGet(httpGet *ateapipb.HTTPGetAction, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	portPath := fldPath.Child("port")
	if port := httpGet.GetPort(); port < 1 || port > 65535 {
		allErrs = append(allErrs, field.Invalid(portPath, port, "must be between 1 and 65535"))
	}

	pathPath := fldPath.Child("path")
	if path := httpGet.GetPath(); len(path) > maxReadyzPathLen {
		allErrs = append(allErrs, field.TooLong(pathPath, path, maxReadyzPathLen))
	} else if !readyzPathRegexp.MatchString(path) {
		allErrs = append(allErrs, field.Invalid(pathPath, path, "must be a valid URL path starting with '/', without query strings or fragments"))
	}

	return allErrs
}

// validateSnapshotsConfig checks the snapshots configuration, including the
// on_commit-is-a-subset-of-on_pause rule (UNSPECIFIED means FULL) and the
// microvm gate on golden resume.
func validateSnapshotsConfig(sc *ateapipb.SnapshotsConfig, microvm bool, fldPath *field.Path) field.ErrorList {
	if sc == nil {
		return field.ErrorList{field.Required(fldPath, "")}
	}

	allErrs := field.ErrorList{}

	locationPath := fldPath.Child("storage_location")
	if location := sc.GetStorageLocation(); location == "" {
		allErrs = append(allErrs, field.Required(locationPath, ""))
	} else if err := resources.ValidateSnapshotLocation(location); err != nil {
		allErrs = append(allErrs, field.Invalid(locationPath, location, err.Error()))
	}

	scopeNames := []string{
		ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL.String(),
		ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA.String(),
	}
	if onPause := sc.GetOnPause(); ateapipb.SnapshotContentScope_name[int32(onPause)] == "" {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("on_pause"), onPause, scopeNames))
	}
	onCommitPath := fldPath.Child("on_commit")
	if onCommit := sc.GetOnCommit(); ateapipb.SnapshotContentScope_name[int32(onCommit)] == "" {
		allErrs = append(allErrs, field.NotSupported(onCommitPath, onCommit, scopeNames))
	} else if effectiveScope(sc.GetOnPause()) == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA &&
		effectiveScope(onCommit) != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_DATA {
		allErrs = append(allErrs, field.Invalid(onCommitPath, onCommit, "must be a subset of on_pause"))
	}

	fromDataPath := fldPath.Child("on_resume", "from_data")
	if fromData := sc.GetOnResume().GetFromData(); ateapipb.ResumeSource_name[int32(fromData)] == "" {
		allErrs = append(allErrs, field.NotSupported(fromDataPath, fromData, []string{
			ateapipb.ResumeSource_RESUME_SOURCE_COLD_BOOT.String(),
			ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN.String(),
		}))
	} else if fromData == ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN && !microvm {
		allErrs = append(allErrs, field.Invalid(fromDataPath, fromData, "RESUME_SOURCE_GOLDEN requires SANDBOX_CLASS_MICROVM"))
	}

	return allErrs
}

// effectiveScope resolves the proto zero value to the documented FULL
// default.
func effectiveScope(s ateapipb.SnapshotContentScope) ateapipb.SnapshotContentScope {
	if s == ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_UNSPECIFIED {
		return ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL
	}
	return s
}

// validateMountPath ports the CRD's mount-path CEL rule: a clean absolute
// Unix path that cannot escape or alias the mount tree.
func validateMountPath(path string, fldPath *field.Path) field.ErrorList {
	const mountPathMsg = "must be a clean absolute Unix path: must start with '/', not be '/', and contain no ':', '..', '.', '//', trailing '/', or control characters"

	if path == "" {
		return field.ErrorList{field.Required(fldPath, "")}
	}

	allErrs := field.ErrorList{}
	if len(path) > maxMountPathLen {
		allErrs = append(allErrs, field.TooLong(fldPath, path, maxMountPathLen))
	}

	ok := strings.HasPrefix(path, "/") && len(path) > 1 &&
		!strings.HasSuffix(path, "/") && !strings.Contains(path, "//") &&
		!strings.Contains(path, ":")
	if ok {
		for i := 0; i < len(path); i++ {
			if path[i] <= 0x1f || path[i] == 0x7f {
				ok = false
				break
			}
		}
	}
	if ok {
		for _, seg := range strings.Split(path[1:], "/") {
			if seg == "." || seg == ".." {
				ok = false
				break
			}
		}
	}
	if !ok {
		allErrs = append(allErrs, field.Invalid(fldPath, path, mountPathMsg))
	}
	return allErrs
}
