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
atelet and its RBAC. The substrate chart renders it unless atelet.enabled is
false, and the substrate-atelet chart renders it as a separate release. Both
charts read the same values.
*/}}
{{- define "substrate.atelet" -}}
{{- if and .Values.atelet.imageCredentialProviderConfig .Values.atelet.imageCredentialProviderConfigYAML }}
{{- fail "set only one of atelet.imageCredentialProviderConfig and atelet.imageCredentialProviderConfigYAML" }}
{{- end }}
{{- $credentialProviderConfig := or .Values.atelet.imageCredentialProviderConfig .Values.atelet.imageCredentialProviderConfigYAML }}
{{- if ne (empty $credentialProviderConfig) (empty .Values.atelet.imageCredentialProviderBinDir) }}
{{- fail "atelet.imageCredentialProviderConfig (or atelet.imageCredentialProviderConfigYAML) and atelet.imageCredentialProviderBinDir must be set together" }}
{{- end }}

# atelet
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "substrate.fullname" (list "atelet" .) }}
  namespace: {{ .Release.Namespace }}
{{- if .Values.atelet.imageCredentialProviderConfigYAML }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "substrate.fullname" (list "atelet-image-credential-provider" .) }}
  namespace: {{ .Release.Namespace }}
data:
  config.yaml: |
    {{- .Values.atelet.imageCredentialProviderConfigYAML | nindent 4 }}
{{- end }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "substrate.fullname" (list "atelet-role" .) }}
rules:
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "watch", "list"]
- apiGroups: ["ate.dev"]
  resources: ["csidriverconfigs"]
  verbs: ["get", "watch", "list"]
# Sync failures are reported as Warning Events on the worker pod.
- apiGroups: ["events.k8s.io"]
  resources: ["events"]
  verbs: ["create", "patch"]
# ClusterTrustBundles referenced by SystemInfo trustBundle data sources are
# resolved on the node: atelet reads them through an informer and projects
# the sanitized PEM into actors (see cmd/atelet/trustbundle.go).
- apiGroups: ["certificates.k8s.io"]
  resources: ["clustertrustbundles"]
  verbs: ["get", "watch", "list"]
# WorkerPools and SandboxConfigs resolve and prewarm each local worker's runtimes.
- apiGroups: ["ate.dev"]
  resources: ["sandboxconfigs", "workerpools"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "substrate.fullname" (list "atelet-binding" .) }}
subjects:
- kind: ServiceAccount
  name: {{ include "substrate.fullname" (list "atelet" .) }}
  namespace: {{ .Release.Namespace }}
roleRef:
  kind: ClusterRole
  name: {{ include "substrate.fullname" (list "atelet-role" .) }}
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "substrate.fullname" (list "atelet-endpointslices" .) }}
  namespace: {{ .Release.Namespace }}
rules:
- apiGroups: ["discovery.k8s.io"]
  resources: ["endpointslices"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "substrate.fullname" (list "atelet-endpointslices" .) }}
  namespace: {{ .Release.Namespace }}
subjects:
- kind: ServiceAccount
  name: {{ include "substrate.fullname" (list "atelet" .) }}
  namespace: {{ .Release.Namespace }}
roleRef:
  kind: Role
  name: {{ include "substrate.fullname" (list "atelet-endpointslices" .) }}
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: {{ include "substrate.fullname" (list "ate-node-critical" .) }}
value: 1000000000
globalDefault: false
description: "Node-local ate components that every actor activation depends on."
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {{ include "substrate.fullname" (list "atelet" .) }}
  namespace: {{ .Release.Namespace }}
  labels:
    app: atelet
spec:
  selector:
    matchLabels:
      app: atelet
  template:
    metadata:
      labels:
        app: atelet
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      serviceAccountName: {{ include "substrate.fullname" (list "atelet" .) }}
      priorityClassName: {{ include "substrate.fullname" (list "ate-node-critical" .) }}
      # Run on nodes reserved for any WorkerPool sandbox class.
      tolerations:
      - key: ate.dev/sandboxClass
        operator: Exists
        effect: NoSchedule
      # GKE reserves ComputeClass nodes with this taint.
      - key: cloud.google.com/compute-class
        operator: Exists
        effect: NoSchedule
      {{- with include "substrate.imagePullSecrets" . }}{{- . | nindent 6 }}{{- end }}
      initContainers:
      - name: objectstore-plugin
        image: {{ include "substrate.componentImage" (list "objectstore-plugin" .) }}
        restartPolicy: Always
        args:
        - node
        - --socket=/run/objectstore-plugin/node.sock
        - --root=/var/lib/ate
        - --asset-staging-dir=/var/lib/objectstore-plugin/asset-staging
        - --asset-staging-capacity=8Gi
        - '--exchange-address={{ include "substrate.fullname" (list "api" .) }}.{{ .Release.Namespace }}.svc:8445'
        - '--exchange-server-name={{ include "substrate.fullname" (list "api" .) }}.{{ .Release.Namespace }}.svc'
        startupProbe:
          exec:
            command:
            - /ko-app/objectstore-plugin
            - healthcheck
            - --socket=/run/objectstore-plugin/node.sock
          periodSeconds: 2
          failureThreshold: 30
        securityContext:
          allowPrivilegeEscalation: false
          runAsUser: 0
          runAsGroup: 0
          capabilities:
            drop: [ALL]
        resources:
{{ toYaml .Values.objectStorePlugin.node.resources | indent 10 }}
        env:
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
{{- include "substrate.storage.env" . | nindent 8 }}
{{- with include "substrate.otel.env" . }}
{{- . | trim | nindent 8 }}
{{- end }}
        volumeMounts:
        - name: ate-base
          mountPath: /var/lib/ate
          mountPropagation: HostToContainer
        - name: objectstore-plugin
          mountPath: /run/objectstore-plugin
        - name: asset-staging
          mountPath: /var/lib/objectstore-plugin/asset-staging
        - name: servicedns-ca
          mountPath: /run/servicedns.podcert.ate.dev
          readOnly: true
      containers:
      - name: atelet
        image: {{ include "substrate.componentImage" (list "atelet" .) }}
        args:
        - {{ printf "--preview=%s" (join "," .Values.preview) | quote }}
        - --objectstore-plugin-socket=/run/objectstore-plugin/node.sock
{{- if $credentialProviderConfig }}
        - --image-credential-provider-config=/run/image-credential-provider/config.yaml
        - --image-credential-provider-bin-dir=/run/image-credential-provider/bin
{{- end }}
        - --grpc-server-cred-bundle=/run/podidentity.podcert.ate.dev/credential-bundle.pem
        - --client-ca-certs=/run/podidentity.podcert.ate.dev/trust-bundle.pem
        - '--herder-allowed-spiffe-id=spiffe://cluster.local/ns/{{ .Release.Namespace }}/sa/{{ include "substrate.fullname" (list "ate-api-server" .) }}'
        - --ateapi-ca-file=/run/servicedns.podcert.ate.dev/trust-bundle.pem
{{- with .Values.atelet.extraArgs }}
{{ toYaml . | indent 8 }}
{{- end }}
        securityContext:
          allowPrivilegeEscalation: false
          runAsUser: 0
          runAsGroup: 0
          capabilities:
            drop:
            - ALL
            add:
            - CHOWN
            - DAC_OVERRIDE
            - FOWNER
            - FSETID
        resources:
{{ toYaml .Values.atelet.resources | indent 10 }}
        env:
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: POD_UID
          valueFrom:
            fieldRef:
              fieldPath: metadata.uid
        - name: OTEL_RESOURCE_ATTRIBUTES
          value: k8s.namespace.name=$(POD_NAMESPACE),k8s.pod.name=$(POD_NAME),k8s.pod.uid=$(POD_UID),k8s.node.name=$(NODE_NAME),service.instance.id=$(POD_UID)
{{- with include "substrate.otel.env" . }}
{{- . | trim | nindent 8 }}
{{- end }}
{{- with .Values.atelet.extraEnv }}
{{ toYaml . | indent 8 }}
{{- end }}
        ports:
        - name: grpc
          containerPort: 21998
        - name: prometheus
          containerPort: 9090
          protocol: TCP
        - name: health
          containerPort: 9091
          protocol: TCP
        readinessProbe:
          httpGet:
            path: /readyz
            port: health
          initialDelaySeconds: 5
          periodSeconds: 2
          failureThreshold: 3
        livenessProbe:
          httpGet:
            path: /healthz
            port: health
          initialDelaySeconds: 10
          periodSeconds: 10
        volumeMounts:
        - name: objectstore-plugin
          mountPath: /run/objectstore-plugin
        - name: ate-base
          mountPath: /var/lib/ate
          mountPropagation: HostToContainer
        - name: podidentity
          mountPath: /run/podidentity.podcert.ate.dev
          readOnly: true
        - name: servicedns-ca
          mountPath: /run/servicedns.podcert.ate.dev
          readOnly: true
        - name: kubelet-plugins
          mountPath: /var/lib/kubelet/plugins
        - name: device-plugins
          mountPath: /var/lib/kubelet/device-plugins
        - name: host-dev
          mountPath: /host/dev
          readOnly: true
{{- if $credentialProviderConfig }}
        - name: image-credential-provider-bin
          mountPath: /run/image-credential-provider/bin
          readOnly: true
        - name: image-credential-provider-config
          mountPath: /run/image-credential-provider/config.yaml
{{- if .Values.atelet.imageCredentialProviderConfigYAML }}
          subPath: config.yaml
{{- end }}
          readOnly: true
{{- end }}
      volumes:
      - name: objectstore-plugin
        emptyDir: {}
      # Capacity plus 1Gi for filesystem overhead; only the plugin mounts it.
      - name: asset-staging
        emptyDir:
          sizeLimit: 9Gi
      - name: ate-base
        hostPath:
          path: /var/lib/ate
          type: DirectoryOrCreate
      - name: kubelet-plugins
        hostPath:
          path: /var/lib/kubelet/plugins
          type: DirectoryOrCreate
      - name: device-plugins
        hostPath:
          path: /var/lib/kubelet/device-plugins
          type: DirectoryOrCreate
      - name: host-dev
        hostPath:
          path: /dev
          type: Directory
{{- if $credentialProviderConfig }}
      - name: image-credential-provider-bin
        hostPath:
          path: {{ .Values.atelet.imageCredentialProviderBinDir | quote }}
          type: Directory
      - name: image-credential-provider-config
{{- if .Values.atelet.imageCredentialProviderConfigYAML }}
        configMap:
          name: {{ include "substrate.fullname" (list "atelet-image-credential-provider" .) }}
{{- else }}
        hostPath:
          path: {{ .Values.atelet.imageCredentialProviderConfig | quote }}
          type: File
{{- end }}
{{- end }}
      - name: podidentity
        projected:
          sources:
          - podCertificate:
              signerName: podidentity.podcert.ate.dev/identity
              keyType: ECDSAP256
              credentialBundlePath: credential-bundle.pem
          - clusterTrustBundle:
              signerName: podidentity.podcert.ate.dev/identity
              labelSelector:
                matchLabels:
                  podcert.ate.dev/canarying: live
              path: trust-bundle.pem
      - name: servicedns-ca
        projected:
          sources:
          - clusterTrustBundle:
              signerName: servicedns.podcert.ate.dev/identity
              labelSelector:
                matchLabels:
                  podcert.ate.dev/canarying: live
              path: trust-bundle.pem
{{- end -}}
