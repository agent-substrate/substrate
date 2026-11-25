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

{{- define "podcert.fullname" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- if eq $ctx.Release.Name "substrate-podcert" -}}
{{- $name -}}
{{- else -}}
{{- printf "%s-%s" $ctx.Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "podcert.componentImage" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- if and (contains "/" $ctx.Values.image.registry) (not (contains "://" $ctx.Values.image.registry)) -}}
{{- fail "image.registry must contain only the registry host; put the path in image.repository" -}}
{{- end -}}
{{- $registry := default $ctx.Values.image.registry $ctx.Values.global.imageRegistry -}}
{{- $tag := default $ctx.Chart.AppVersion $ctx.Values.image.tag -}}
{{- if eq $tag "<none>" -}}
{{- printf "%s/%s/%s" $registry $ctx.Values.image.repository $name -}}
{{- else -}}
{{- printf "%s/%s/%s:%s" $registry $ctx.Values.image.repository $name $tag -}}
{{- end -}}
{{- end -}}

{{- define "podcert.imagePullSecrets" -}}
{{- $merged := concat .Values.imagePullSecrets .Values.global.imagePullSecrets | uniq -}}
{{- if $merged -}}
imagePullSecrets:
{{- toYaml $merged | nindent 0 }}
{{- end -}}
{{- end -}}
