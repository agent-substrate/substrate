{{/*
Qualified resource name for a chart component.

Usage:
  {{ include "substrate.fullname" (list "ate-api-server" .) }}

When the release name equals the chart name (the canonical render in
hack/render-manifests.sh — `helm template substrate charts/substrate`), this
returns the bare component name, so the generated manifests/ate-install/
files keep their historical names ("ate-api-server", "ate-controller", ...).

Otherwise resources are prefixed with the release name in the standard Helm
style ("foo-ate-api-server", ...) so multiple releases coexist without
colliding.
*/}}
{{- define "substrate.fullname" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- if eq $ctx.Release.Name $ctx.Chart.Name -}}
{{- $name -}}
{{- else -}}
{{- printf "%s-%s" $ctx.Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Validate auth.mode at template time.
*/}}
{{- define "substrate.validateAuthMode" -}}
{{- if not (or (eq .Values.auth.mode "mtls") (eq .Values.auth.mode "jwt")) -}}
{{- fail (printf "auth.mode must be 'mtls' or 'jwt', got %q" .Values.auth.mode) -}}
{{- end -}}
{{- if eq .Values.auth.mode "jwt" -}}
{{- if not .Values.auth.jwt.issuer -}}
{{- fail "auth.jwt.issuer is required when auth.mode=jwt" -}}
{{- end -}}
{{- end -}}
{{- end -}}
