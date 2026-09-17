{{/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Qualified resource name for a chart component.

Usage:
  {{ include "substrate.fullname" (list "ate-api-server" .) }}

When the release name is "substrate" (the canonical render in
hack/render-manifests.sh — `helm template substrate charts/substrate`), this
returns the bare component name, so the generated manifests/ate-install/
files keep their historical names ("ate-api-server", "ate-controller", ...).

Otherwise resources are prefixed with the release name in the standard Helm
style ("foo-ate-api-server", ...) so multiple releases coexist without
colliding.

The check is on the literal release name "substrate" rather than
$ctx.Chart.Name so this helper is context-safe: a parent chart can invoke it
with its own `.` (where .Chart.Name is the parent, not "substrate") and still
get the same prefixed name that this subchart's own templates render.
*/}}
{{- define "substrate.fullname" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- if eq $ctx.Release.Name "substrate" -}}
{{- $name -}}
{{- else -}}
{{- printf "%s-%s" $ctx.Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
ServiceAccount name of ate-api-server, as this chart creates it. Parent
charts that need to bind additional Roles to this SA (e.g. env-source
Secret/ConfigMap reads for ActorTemplate resolution) should reference this
helper instead of hardcoding "ate-api-server":

  {{ include "substrate.ateApiServer.serviceAccountName" . }}
*/}}
{{- define "substrate.ateApiServer.serviceAccountName" -}}
{{- include "substrate.fullname" (list "ate-api-server" .) -}}
{{- end -}}

{{/*
gRPC endpoint that clients dial to reach ate-api-server. dns:/// scheme +
release-prefixed Service name + release namespace + :443. Suitable for
consumption as ATE_API_ENDPOINT / --ateapi-address:

  {{ include "substrate.ateApi.endpoint" . }}
    -> dns:///<release>-api.<namespace>.svc:443
*/}}
{{- define "substrate.ateApi.endpoint" -}}
{{- printf "dns:///%s.%s.svc:443" (include "substrate.fullname" (list "api" .)) .Release.Namespace -}}
{{- end -}}

{{/*
Plaintext HTTP URL that clients use to reach atenet-router.

  {{ include "substrate.atenetRouter.url" . }}
    -> http://<release>-atenet-router.<namespace>.svc:80
*/}}
{{- define "substrate.atenetRouter.url" -}}
{{- printf "http://%s.%s.svc:80" (include "substrate.fullname" (list "atenet-router" .)) .Release.Namespace -}}
{{- end -}}

{{/*
OTLP endpoint a signal exports to, or empty when the signal is disabled or no
endpoint resolves. The per-signal endpoint wins over the generic one, matching
the precedence the OpenTelemetry SDK gives OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT
over OTEL_EXPORTER_OTLP_ENDPOINT.

Usage:
  {{ include "substrate.otel.signalEndpoint" (list "traces" .) }}
*/}}
{{- define "substrate.otel.signalEndpoint" -}}
{{- $signal := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- $cfg := index $ctx.Values.otel $signal -}}
{{- if $cfg.enabled -}}
{{- $cfg.endpoint | default $ctx.Values.otel.endpoint -}}
{{- end -}}
{{- end -}}

{{/*
OTEL_* env entries for a Go component, as a list of "- name/value" items.
Empty when nothing under .Values.otel is set, so callers can gate the env
key on the result.

Usage:
  {{- with include "substrate.otel.env" . }}
  {{- . | trim | nindent 8 }}
  {{- end }}
*/}}
{{- define "substrate.otel.env" -}}
{{- $otel := .Values.otel -}}
{{- if $otel.endpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $otel.endpoint | quote }}
{{- end }}
{{- range $signal := list "traces" "metrics" "logs" }}
{{- $cfg := index $otel $signal }}
{{- if not $cfg.enabled }}
{{- /* "none" is the SDK's own exporter name for "export nothing"; leaving the
       endpoint unset would fall back to the SDK default of localhost:4317. */}}
- name: OTEL_{{ upper $signal }}_EXPORTER
  value: none
{{- else if $cfg.endpoint }}
- name: OTEL_EXPORTER_OTLP_{{ upper $signal }}_ENDPOINT
  value: {{ $cfg.endpoint | quote }}
{{- end }}
{{- end }}
{{- if include "substrate.otel.signalEndpoint" (list "traces" .) }}
- name: OTEL_TRACES_SAMPLER
  value: parentbased_traceidratio
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ $otel.traces.samplingRatio | quote }}
{{- end }}
{{- end -}}

{{/*
Build an image reference for a substrate component binary.

Usage:
  {{ include "substrate.componentImage" (list "ateapi" .) }}

Produces  {image.registry}/{name}:{tag}  where tag is resolved as:
  1. image.tag value, if set and not the sentinel "<none>"
  2. .Chart.AppVersion, if image.tag is empty
  3. no tag (no colon) when image.tag is the sentinel "<none>"

The "<none>" sentinel is used by hack/render-manifests.sh so that ko:// refs
are emitted without a tag, letting `ko resolve` supply the digest at build time.
*/}}
{{- define "substrate.componentImage" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{/* image.registry used to carry the full prefix (ghcr.io/kagent-dev/substrate).
     It is now the registry host only, joined onto image.repository -- the same
     registry/repository split every kagent-family chart uses, so one
     global.imageRegistry value redirects them all. A values file still carrying
     a path in registry would render a doubled prefix that fails only at pod
     start, so it fails the render here instead and names the split. */}}
{{- /* A scheme'd registry (ko://...) is hack/render-manifests.sh passing an
     importpath prefix for `ko resolve` to substitute, same as the "<none>" tag
     sentinel below -- unambiguously not the old host+path shape, so the guard
     lets it through. */ -}}
{{- if and (contains "/" $ctx.Values.image.registry) (not (contains "://" $ctx.Values.image.registry)) -}}
{{- fail (printf "image.registry (%q) carries a path. It is now the registry host only: keep the path in image.repository, e.g. registry: ghcr.io, repository: kagent-dev/substrate." $ctx.Values.image.registry) -}}
{{- end -}}
{{- $registry := printf "%s/%s" (default $ctx.Values.image.registry (($ctx.Values.global).imageRegistry)) $ctx.Values.image.repository -}}
{{- $tag := $ctx.Values.image.tag | default $ctx.Chart.AppVersion -}}
{{- if ne $tag "<none>" -}}
{{- printf "%s/%s:%s" $registry $name $tag -}}
{{- else -}}
{{- printf "%s/%s" $registry $name -}}
{{- end -}}
{{- end -}}

{{/*
Rewrite a full image reference ({registry}/{path}:{tag}) onto global.imageRegistry.

The `images.*` values are single-string references, some digest-pinned, so the
mirror knob has to edit the string. The first path segment is a registry only when
it contains "." or ":" (the containerd rule); otherwise the reference is
docker.io-implied and the mirror is prefixed. The repository path is preserved
either way, so a mirror copies images under their existing paths.

Usage: {{ include "substrate.thirdPartyImage" (list .Values.images.postgres .) }}
*/}}
{{- define "substrate.thirdPartyImage" -}}
{{- $ref := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- $mirror := (($ctx.Values.global).imageRegistry) -}}
{{- if not $mirror -}}
{{- $ref -}}
{{- else -}}
{{- $parts := splitList "/" $ref -}}
{{- $first := first $parts -}}
{{- if and (gt (len $parts) 1) (or (contains "." $first) (contains ":" $first)) -}}
{{- printf "%s/%s" $mirror (join "/" (rest $parts)) -}}
{{- else -}}
{{- printf "%s/%s" $mirror $ref -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
imagePullSecrets for a pod spec: the chart's own list merged (union) with
global.imagePullSecrets. Renders nothing when both are empty.
*/}}
{{- define "substrate.imagePullSecrets" -}}
{{- $merged := concat (.Values.imagePullSecrets | default list) (((.Values.global).imagePullSecrets) | default list) | uniq -}}
{{- if $merged -}}
imagePullSecrets:
{{- toYaml $merged | nindent 0 }}
{{- end -}}
{{- end -}}

{{/*
imagePullPolicy: global.imagePullPolicy when set, IfNotPresent otherwise. One
definition so the fallback cannot drift between pods.
*/}}
{{- define "substrate.imagePullPolicy" -}}
{{- ((.Values.global).imagePullPolicy) | default "IfNotPresent" -}}
{{- end -}}
