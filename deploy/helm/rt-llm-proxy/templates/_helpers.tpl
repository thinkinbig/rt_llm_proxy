{{- define "rt-llm-proxy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rt-llm-proxy.fullname" -}}
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

{{- define "rt-llm-proxy.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "rt-llm-proxy.labels" -}}
helm.sh/chart: {{ include "rt-llm-proxy.chart" . }}
{{ include "rt-llm-proxy.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "rt-llm-proxy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rt-llm-proxy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "rt-llm-proxy.redis.fullname" -}}
{{- printf "%s-redis" (include "rt-llm-proxy.fullname" .) }}
{{- end }}

{{- define "rt-llm-proxy.redis.addr" -}}
{{- printf "%s:%d" (include "rt-llm-proxy.redis.fullname" .) (int .Values.redis.port) }}
{{- end }}

{{- define "rt-llm-proxy.gemini.secretName" -}}
{{- if .Values.gemini.existingSecret }}
{{- .Values.gemini.existingSecret }}
{{- else }}
{{- include "rt-llm-proxy.fullname" . }}
{{- end }}
{{- end }}
