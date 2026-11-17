{{/*
Expand the name of the chart.
*/}}
{{- define "image-factory.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "image-factory.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "image-factory.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Expand the namespace of the release.
Allows overriding it for multi-namespace deployments in combined charts.
*/}}
{{- define "image-factory.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Common labels
*/}}
{{- define "image-factory.labels" -}}
helm.sh/chart: {{ include "image-factory.chart" . }}
{{ include "image-factory.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "image-factory.selectorLabels" -}}
app.kubernetes.io/name: {{ include "image-factory.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "image-factory.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "image-factory.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the name of the cache signing key secret to use.
If 'existingSecret' is set, use it.
Otherwise, use the chart's fullname with a '-cache-signing-key' suffix.
*/}}
{{- define "image-factory.cacheSigningKey" -}}
{{- if .Values.cacheSigningKey.existingSecret -}}
    {{- .Values.cacheSigningKey.existingSecret -}}
{{- else -}}
    {{- include "image-factory.fullname" . -}}-cache-signing-key
{{- end -}}
{{- end -}}

{{/*
Return the name of the htpasswd Secret to use.
If 'auth.existingSecret' is set, use it.
Otherwise, use the chart's fullname with a '-htpasswd' suffix.
*/}}
{{- define "image-factory.htpasswdSecret" -}}
{{- if .Values.auth.existingSecret -}}
    {{- .Values.auth.existingSecret -}}
{{- else -}}
    {{- include "image-factory.fullname" . -}}-htpasswd
{{- end -}}
{{- end -}}

{{/*
Return the name of the Grype DB PersistentVolumeClaim to use.
*/}}
{{- define "image-factory.grypeDBClaim" -}}
    {{- include "image-factory.fullname" . -}}-grype-db
{{- end -}}

{{/*
Worker labels. Workers carry a name of their own, so that the selectors of the
frontend Deployment and Services do not match them.
*/}}
{{- define "image-factory.worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "image-factory.name" . }}-worker
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "image-factory.worker.labels" -}}
helm.sh/chart: {{ include "image-factory.chart" . }}
{{ include "image-factory.worker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: worker
{{- end }}

{{/*
Return the name of the Secret with the build queue URL.
*/}}
{{- define "image-factory.queueSecret" -}}
{{- if .Values.queue.existingSecret -}}
    {{- .Values.queue.existingSecret -}}
{{- else -}}
    {{- include "image-factory.fullname" . -}}-queue
{{- end -}}
{{- end -}}

{{/*
Environment variables selecting the role of a replica. Takes a dict with "root" and "mode".
*/}}
{{- define "image-factory.queueEnv" -}}
- name: IF_QUEUE_MODE
  value: {{ .mode }}
- name: IF_QUEUE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "image-factory.queueSecret" .root }}
      key: url
{{- end -}}
