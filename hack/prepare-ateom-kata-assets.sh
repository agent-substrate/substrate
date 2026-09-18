#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_root="${KATA_ROOT:-/opt/kata}"
output_root="${ATEOM_KATA_ROOT:-/opt/ateom-kata}"
source_config=""
shim_path=""
qemu_data_root=""
profile="qemu-cr"
enable_template="false"
template_source=""
template_path="/run/vc/vm/ateom-kata-template"
source_commit="${KATA_SOURCE_COMMIT:-}"

usage() {
	cat <<'EOF'
Usage: prepare-ateom-kata-assets.sh [options]

Options:
  --source-root PATH       Kata static or an existing verified bundle (default: /opt/kata)
  --source-config PATH     Kata configuration used as the profile base (required)
  --shim-path PATH         ateom-kata shim override built from the matching Kata source
  --qemu-data-root PATH    QEMU firmware/data directory (auto-detected for Kata static)
  --output-root PATH       Substrate-owned asset root (default: /opt/ateom-kata)
  --profile NAME           Profile name (default: qemu-cr)
  --enable-template BOOL   true or false (default: false)
  --template-source PATH   Existing QEMU template copied to the runtime template path
  --template-path PATH     Runtime template path (default: /run/vc/vm/ateom-kata-template)
  --source-commit SHA      Public Kata source commit recorded in the profile
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--source-root) source_root=${2:?missing value}; shift 2 ;;
	--source-config) source_config=${2:?missing value}; shift 2 ;;
	--shim-path) shim_path=${2:?missing value}; shift 2 ;;
	--qemu-data-root) qemu_data_root=${2:?missing value}; shift 2 ;;
	--output-root) output_root=${2:?missing value}; shift 2 ;;
	--profile) profile=${2:?missing value}; shift 2 ;;
	--enable-template) enable_template=${2:?missing value}; shift 2 ;;
	--template-source) template_source=${2:?missing value}; shift 2 ;;
	--template-path) template_path=${2:?missing value}; shift 2 ;;
	--source-commit) source_commit=${2:?missing value}; shift 2 ;;
	-h | --help) usage; exit 0 ;;
	*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
done

[[ -n "${source_config}" ]] || { echo "--source-config is required" >&2; exit 2; }
[[ "${profile}" =~ ^[a-z0-9][a-z0-9-]*$ ]] || { echo "invalid profile name: ${profile}" >&2; exit 2; }
[[ "${enable_template}" == "true" || "${enable_template}" == "false" ]] || {
	echo "--enable-template must be true or false" >&2
	exit 2
}
[[ "${source_commit}" =~ ^([0-9a-f]{40}|[0-9a-f]{64})$ ]] || {
	echo "--source-commit must be a full lowercase Git object ID" >&2
	exit 2
}
[[ -d "${source_root}" ]] || { echo "missing Kata source root: ${source_root}" >&2; exit 1; }
[[ -r "${source_config}" ]] || { echo "missing Kata source config: ${source_config}" >&2; exit 1; }
[[ "${output_root}" == /* && "${template_path}" == /* ]] || {
	echo "output and template paths must be absolute" >&2
	exit 2
}

verify_bundle() {
	python3 - "$1" <<'PY'
import hashlib
import json
import os
import stat
import sys

root = os.path.realpath(sys.argv[1])
manifest_path = os.path.join(root, "bundle-manifest.json")
with open(manifest_path, encoding="utf-8") as f:
    manifest = json.load(f)
if manifest.get("version") != 1:
    raise SystemExit("unsupported Kata bundle manifest version")
declared = set()
for entry in manifest.get("files", []):
    rel = entry.get("path")
    if not isinstance(rel, str) or not rel or os.path.isabs(rel):
        raise SystemExit(f"invalid Kata asset path: {rel!r}")
    if rel in declared:
        raise SystemExit(f"duplicate Kata asset path: {rel!r}")
    declared.add(rel)
    path = os.path.join(root, rel)
    if os.path.commonpath((root, os.path.realpath(path))) != root:
        raise SystemExit(f"Kata asset escapes bundle: {rel!r}")
    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        raise SystemExit(f"missing Kata asset: {rel!r}")
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise SystemExit(f"Kata asset is not a regular file: {rel!r}")
    if entry.get("size") != os.path.getsize(path):
        raise SystemExit(f"Kata asset size mismatch: {rel!r}")
    digest = hashlib.sha256()
    with open(path, "rb") as asset:
        for block in iter(lambda: asset.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != entry.get("sha256"):
        raise SystemExit(f"Kata asset digest mismatch: {rel!r}")
actual = set()
for base, _, names in os.walk(root):
    for name in names:
        rel = os.path.relpath(os.path.join(base, name), root)
        if rel != "bundle-manifest.json":
            actual.add(rel)
if actual != declared:
    raise SystemExit("Kata bundle contains files not described by its manifest")
PY
}

install -d -m 0755 "${output_root}/bundles" "${output_root}/configs" "${output_root}/profiles"
tmp_bundle="$(mktemp -d "${output_root}/bundles/.build-XXXXXX")"
tmp_config=""
cleanup() {
	[[ -z "${tmp_config}" ]] || rm -f -- "${tmp_config}"
	[[ ! -d "${tmp_bundle}" ]] || rm -rf -- "${tmp_bundle}"
}
trap cleanup EXIT

declare -A source_paths
declare -A source_relative_paths
guest_key="$(python3 - "${source_config}" <<'PY'
import re, sys

section = ""
active = []
with open(sys.argv[1], encoding="utf-8") as f:
    for line in f:
        section_match = re.match(r"^\s*\[([^]]+)]\s*$", line)
        if section_match:
            section = section_match.group(1)
            continue
        if section != "hypervisor.qemu":
            continue
        setting = re.match(r'^\s*(initrd|image)\s*=\s*"([^"]*)"', line)
        if setting and setting.group(2):
            active.append("kata-" + setting.group(1))
if len(active) != 1:
    raise SystemExit("source config must select exactly one non-empty qemu initrd or image")
print(active[0])
PY
)"
ateom_manifest=false
if [[ -r "${source_root}/bundle-manifest.json" ]]; then
	verify_bundle "${source_root}"
	if python3 - "${source_root}/bundle-manifest.json" "${guest_key}" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    manifest = json.load(f)
names = {entry.get("name") for entry in manifest.get("files", [])}
required = {"kata-shim", "kata-qemu", "kata-kernel", sys.argv[2]}
has_ateom_format = manifest.get("format") == "ateom-kata-runtime-bundle"
has_support_closure = any((name or "").startswith("kata-qemu-support-") for name in names)
if manifest.get("version") != 1 or not required <= names or not (has_ateom_format or has_support_closure):
    raise SystemExit(1)
PY
	then
		ateom_manifest=true
	fi
fi
if [[ "${ateom_manifest}" == "true" ]]; then
	while IFS=$'\t' read -r name relative_path; do
		source_paths["${name}"]="${source_root}/${relative_path}"
		source_relative_paths["${name}"]="${relative_path}"
	done < <(python3 - "${source_root}/bundle-manifest.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as f:
    manifest = json.load(f)
if manifest.get("version") != 1:
    raise SystemExit("unsupported source bundle manifest version")
for entry in manifest.get("files", []):
    print(f"{entry['name']}\t{entry['path']}")
PY
)
else
	find_source() {
		local candidate
		for candidate in "$@"; do
			if [[ -f "${source_root}/${candidate}" ]]; then
				printf '%s' "${source_root}/${candidate}"
				return 0
			fi
		done
		return 1
	}
	source_paths[kata-shim]="$(find_source runtime-rs/bin/containerd-shim-kata-v2 bin/containerd-shim-kata-v2 bin/containerd-shim-kata-v2-rs)" || true
	source_paths[kata-qemu]="$(find_source bin/qemu-system-x86_64)" || true
	for candidate in share/kata-qemu/qemu share/qemu; do
		[[ -z "${qemu_data_root}" ]] || break
		if [[ -d "${source_root}/${candidate}" ]]; then
			qemu_data_root="${source_root}/${candidate}"
			break
		fi
	done
	source_paths[kata-kernel]="$(find_source share/vmlinux share/kata-vmlinux share/kata-containers/vmlinux.container)" || true
	source_paths[kata-initrd]="$(find_source share/kata-initrd.img share/kata.initrd share/kata-initrd share/kata-containers/kata-containers-initrd.img share/kata-containers/kata-alpine-3.22.initrd)" || true
	source_paths[kata-image]="$(find_source share/kata-image share/kata-image.img share/kata-containers/kata-containers.img)" || true
fi

for required in kata-shim kata-qemu kata-kernel; do
	[[ -n "${source_paths[${required}]:-}" ]] || { echo "source root is missing ${required}" >&2; exit 1; }
done

if [[ -n "${qemu_data_root}" ]]; then
	qemu_data_root="$(realpath -e "${qemu_data_root}")"
	[[ -d "${qemu_data_root}" ]] || { echo "invalid QEMU data directory: ${qemu_data_root}" >&2; exit 1; }
fi
[[ -n "${source_paths[${guest_key}]:-}" ]] || {
	echo "source root is missing ${guest_key} selected by ${source_config}" >&2
	exit 1
}

copy_asset() {
	local source_path=$1 destination=$2 real_source
	real_source="$(realpath -e "${source_path}")"
	case "${real_source}" in
	"$(realpath -e "${source_root}")"/*) ;;
	*) echo "source asset escapes ${source_root}: ${source_path}" >&2; exit 1 ;;
	esac
	install -d -m 0755 "$(dirname "${destination}")"
	cp --reflink=auto --preserve=mode "${real_source}" "${destination}"
}

if [[ -n "${shim_path}" ]]; then
	[[ -f "${shim_path}" ]] || { echo "missing shim override: ${shim_path}" >&2; exit 1; }
	install -d -m 0755 "${tmp_bundle}/bin"
	cp --reflink=auto --preserve=mode "$(realpath -e "${shim_path}")" "${tmp_bundle}/bin/containerd-shim-kata-v2"
else
	copy_asset "${source_paths[kata-shim]}" "${tmp_bundle}/bin/containerd-shim-kata-v2"
fi

if [[ -n "${qemu_data_root:-}" ]]; then
	copy_asset "${source_paths[kata-qemu]}" "${tmp_bundle}/libexec/qemu-system-x86_64"
	install -d -m 0755 "${tmp_bundle}/bin" "${tmp_bundle}/share/kata-qemu"
	cp -aL --reflink=auto "${qemu_data_root}" "${tmp_bundle}/share/kata-qemu/qemu"
	# The generated wrapper expands these variables when it runs.
	# shellcheck disable=SC2016
	printf '%s\n' \
		'#!/bin/sh' \
		'bundle_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)' \
		'exec "${bundle_dir}/libexec/qemu-system-x86_64" -L "${bundle_dir}/share/kata-qemu/qemu" "$@"' \
		>"${tmp_bundle}/bin/qemu-system-x86_64"
	chmod 0755 "${tmp_bundle}/bin/qemu-system-x86_64"
else
	copy_asset "${source_paths[kata-qemu]}" "${tmp_bundle}/bin/qemu-system-x86_64"
	for name in "${!source_paths[@]}"; do
		case "${name}" in kata-shim|kata-qemu|kata-kernel|kata-initrd|kata-image) continue ;; esac
		copy_asset "${source_paths[${name}]}" "${tmp_bundle}/${source_relative_paths[${name}]}"
	done
fi
copy_asset "${source_paths[kata-kernel]}" "${tmp_bundle}/share/kata-containers/vmlinux.container"
if [[ "${guest_key}" == "kata-initrd" ]]; then
	guest_path=share/kata-initrd.img
	copy_asset "${source_paths[kata-initrd]}" "${tmp_bundle}/${guest_path}"
else
	guest_path=share/kata-containers/kata-containers.img
	copy_asset "${source_paths[kata-image]}" "${tmp_bundle}/${guest_path}"
fi

"${root}/hack/generate-kata-runtime-manifest.sh" "${tmp_bundle}" >/dev/null
bundle_sha="$(sha256sum "${tmp_bundle}/bundle-manifest.json" | awk '{print $1}')"
bundle_dir="${output_root}/bundles/sha256-${bundle_sha}"
if [[ -d "${bundle_dir}" ]]; then
	verify_bundle "${bundle_dir}"
	rm -rf -- "${tmp_bundle}"
else
	mv "${tmp_bundle}" "${bundle_dir}"
fi

config_dir="${output_root}/configs/sha256-${bundle_sha}"
install -d -m 0755 "${config_dir}"
config_path="${config_dir}/configuration-${profile}.toml"
tmp_config="$(mktemp "${config_dir}/.configuration-${profile}.XXXXXX")"
python3 - "${source_config}" "${tmp_config}" "${bundle_dir}" "${guest_key}" "${guest_path}" "${enable_template}" "${template_path}" <<'PY'
import re, sys

source, target, bundle, guest_key, guest_rel, enable_template, template_path = sys.argv[1:]
values = {
    "path": f'{bundle}/bin/qemu-system-x86_64',
    "kernel": f'{bundle}/share/kata-containers/vmlinux.container',
    "initrd" if guest_key == "kata-initrd" else "image": f'{bundle}/{guest_rel}',
}
array_values = {
    "valid_hypervisor_paths": [f'{bundle}/bin/qemu-system-x86_64'],
}
section = ""
seen = set()
output = []
with open(source, encoding="utf-8") as f:
    for line in f:
        section_match = re.match(r"^\s*\[([^]]+)]\s*$", line)
        if section_match:
            section = section_match.group(1)
        setting = re.match(r"^(\s*)([a-z_]+)\s*=", line)
        if setting and section == "hypervisor.qemu" and setting.group(2) in values:
            key = setting.group(2)
            line = f'{setting.group(1)}{key} = "{values[key]}"\n'
            seen.add(key)
        elif setting and section == "hypervisor.qemu" and setting.group(2) in array_values:
            key = setting.group(2)
            quoted = ", ".join(f'"{value}"' for value in array_values[key])
            line = f'{setting.group(1)}{key} = [{quoted}]\n'
            seen.add(key)
        elif setting and section == "hypervisor.qemu.factory" and setting.group(2) == "enable_template":
            line = f'{setting.group(1)}enable_template = {enable_template}\n'
            seen.add("enable_template")
        elif setting and section == "hypervisor.qemu.factory" and setting.group(2) == "template_path":
            line = f'{setting.group(1)}template_path = "{template_path}"\n'
            seen.add("template_path")
        output.append(line)
required = set(values) | set(array_values) | {"enable_template", "template_path"}
missing = sorted(required - seen)
if missing:
    raise SystemExit("source config is missing active settings: " + ", ".join(missing))
with open(target, "w", encoding="utf-8") as f:
    f.writelines(output)
PY
chmod 0644 "${tmp_config}"
mv -f "${tmp_config}" "${config_path}"
tmp_config=""
config_sha="$(sha256sum "${config_path}" | awk '{print $1}')"

if [[ "${enable_template}" == "true" ]]; then
	if [[ -n "${template_source}" ]]; then
		[[ -d "${template_source}" ]] || { echo "missing template source: ${template_source}" >&2; exit 1; }
		if [[ -e "${template_path}" ]]; then
			echo "refusing to replace existing template path: ${template_path}" >&2
			exit 1
		fi
		install -d -m 0755 "$(dirname "${template_path}")"
		tmp_template="${template_path}.installing-$$"
		trap 'rm -rf -- "${tmp_template}"; cleanup' EXIT
		cp -a --reflink=auto "${template_source}" "${tmp_template}"
		mv "${tmp_template}" "${template_path}"
		trap cleanup EXIT
	elif [[ ! -e "${template_path}" ]]; then
		install -d -m 0755 "$(dirname "${template_path}")"
		KATA_CONF_FILE="${config_path}" "${bundle_dir}/bin/containerd-shim-kata-v2" factory init
	fi
	[[ -r "${template_path}/memory" && -r "${template_path}/state" ]] || {
		echo "template profile requires ${template_path}/{memory,state}" >&2
		exit 1
	}
	KATA_CONF_FILE="${config_path}" "${bundle_dir}/bin/containerd-shim-kata-v2" factory status >/dev/null
fi

profile_path="${output_root}/profiles/${profile}.env"
tmp_profile="$(mktemp "${output_root}/profiles/.${profile}.XXXXXX")"
{
	printf 'ATEOM_KATA_BUNDLE=%q\n' "${bundle_dir}"
	printf 'ATEOM_KATA_BUNDLE_SHA256=%q\n' "${bundle_sha}"
	printf 'ATEOM_KATA_CONFIG=%q\n' "${config_path}"
	printf 'ATEOM_KATA_CONFIG_SHA256=%q\n' "${config_sha}"
	printf 'ATEOM_KATA_ENABLE_TEMPLATE=%q\n' "${enable_template}"
	printf 'ATEOM_KATA_TEMPLATE_PATH=%q\n' "${template_path}"
	printf 'KATA_SOURCE_COMMIT=%q\n' "${source_commit}"
} >"${tmp_profile}"
chmod 0644 "${tmp_profile}"
mv -f "${tmp_profile}" "${profile_path}"

echo "ateom-kata profile: ${profile_path}"
echo "runtime bundle: ${bundle_dir}"
echo "runtime config: ${config_path}"
