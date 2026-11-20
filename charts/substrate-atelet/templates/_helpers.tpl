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
{{- $registry := default $ctx.Values.image.registry (($ctx.Values.global).imageRegistry) -}}
{{- printf "%s/%s/%s:%s" $registry $ctx.Values.image.repository $name ($ctx.Values.image.tag | default $ctx.Chart.AppVersion) -}}
{{- end -}}

{{/* imagePullSecrets for a pod spec. Copied from the substrate chart. */}}
{{- define "substrate.imagePullSecrets" -}}
{{- $merged := concat (.Values.imagePullSecrets | default list) (((.Values.global).imagePullSecrets) | default list) | uniq -}}
{{- if $merged -}}
imagePullSecrets:
{{- toYaml $merged | nindent 0 }}
{{- end -}}
{{- end -}}

{{/* Object storage settings for the node object-store plugin. */}}
{{- define "substrate.storage.env" -}}
- name: ATE_STORAGE_BACKEND
  value: {{ .Values.objectStore.backend | quote }}
{{- with .Values.objectStore.region }}
- name: AWS_REGION
  value: {{ . | quote }}
{{- end }}
{{- with .Values.objectStore.endpoint }}
- name: AWS_ENDPOINT_URL
  value: {{ . | quote }}
{{- end }}
{{- if .Values.objectStore.pathStyle }}
- name: AWS_S3_USE_PATH_STYLE
  value: "true"
{{- end }}
{{- with .Values.objectStore.credentialsSecret }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: AWS_ACCESS_KEY_ID
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: AWS_SECRET_ACCESS_KEY
{{- end }}
{{- with .Values.objectStorePlugin.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end }}
