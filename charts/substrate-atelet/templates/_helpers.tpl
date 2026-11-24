{{/*
Copyright 2026 The Agent Substrate Authors

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
Name of a substrate component object. Copied from the substrate chart, but
named after substrateReleaseName, so atelet's objects keep the names the
substrate release references.
*/}}
{{- define "substrate.fullname" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- $release := $ctx.Values.substrateReleaseName -}}
{{- if eq $release "substrate" -}}
{{- $name -}}
{{- else -}}
{{- printf "%s-%s" $release $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* OTLP endpoint a signal exports to. Copied from the substrate chart. */}}
{{- define "substrate.otel.signalEndpoint" -}}
{{- $signal := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- $cfg := index $ctx.Values.otel $signal -}}
{{- if $cfg.enabled -}}
{{- $cfg.endpoint | default $ctx.Values.otel.endpoint -}}
{{- end -}}
{{- end -}}

{{/* OTEL_* env entries. Copied from the substrate chart. */}}
{{- define "substrate.otel.env" -}}
{{- $otel := .Values.otel -}}
{{- if $otel.endpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ $otel.endpoint | quote }}
{{- end }}
{{- range $signal := list "traces" "metrics" "logs" }}
{{- $cfg := index $otel $signal }}
{{- if not (include "substrate.otel.signalEndpoint" (list $signal $)) }}
{{- /* Disabled, or enabled with no endpoint: request no OTLP export, since
       otlp without one retries localhost:4317. Enabled metrics keep the
       scraped endpoint with "prometheus"; otherwise the SDK name "none".
       Actor events still go to stdout. */}}
- name: OTEL_{{ upper $signal }}_EXPORTER
  value: {{ ternary "prometheus" "none" (and $cfg.enabled (eq $signal "metrics")) }}
{{- else if $cfg.endpoint }}
- name: OTEL_EXPORTER_OTLP_{{ upper $signal }}_ENDPOINT
  value: {{ $cfg.endpoint | quote }}
{{- end }}
{{- end }}
{{- if include "substrate.otel.signalEndpoint" (list "logs" .) }}
{{- /* Only logs need turning on: serverboot defaults actor events to stdout.
       Traces and metrics default to OTLP, so they need no such branch.
       Gated on an endpoint, since otlp without one retries localhost:4317. */}}
- name: OTEL_LOGS_EXPORTER
  value: otlp
{{- end }}
{{- if include "substrate.otel.signalEndpoint" (list "traces" .) }}
- name: OTEL_TRACES_SAMPLER
  value: parentbased_traceidratio
- name: OTEL_TRACES_SAMPLER_ARG
  value: {{ $otel.traces.samplingRatio | quote }}
{{- end }}
{{- end -}}

{{/* Image reference for a substrate component. Copied from the substrate chart. */}}
{{- define "substrate.componentImage" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{/* image.registry is the registry host only, joined onto image.repository,
     so one global.imageRegistry value redirects every image. A path in
     registry would render a doubled prefix that fails only at pod start, so
     it fails the render here instead and names the split. A ko:// registry is
     an importpath prefix for `ko resolve`, so the guard lets it through. */}}
{{- if and (contains "/" $ctx.Values.image.registry) (not (contains "://" $ctx.Values.image.registry)) -}}
{{- fail (printf "image.registry (%q) carries a path. It is now the registry host only: keep the path in image.repository, e.g. registry: ghcr.io, repository: agent-substrate/substrate." $ctx.Values.image.registry) -}}
{{- end -}}
{{- $registry := printf "%s/%s" (default $ctx.Values.image.registry (($ctx.Values.global).imageRegistry)) $ctx.Values.image.repository -}}
{{- $tag := $ctx.Values.image.tag | default $ctx.Chart.AppVersion -}}
{{- if ne $tag "<none>" -}}
{{- printf "%s/%s:%s" $registry $name $tag -}}
{{- else -}}
{{- printf "%s/%s" $registry $name -}}
{{- end -}}
{{- end -}}

{{/* A full image reference moved onto global.imageRegistry. Copied from the substrate chart. */}}
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

{{/* imagePullSecrets for a pod spec. Copied from the substrate chart. */}}
{{- define "substrate.imagePullSecrets" -}}
{{- $merged := concat (.Values.imagePullSecrets | default list) (((.Values.global).imagePullSecrets) | default list) | uniq -}}
{{- if $merged -}}
imagePullSecrets:
{{- toYaml $merged | nindent 0 }}
{{- end -}}
{{- end -}}

