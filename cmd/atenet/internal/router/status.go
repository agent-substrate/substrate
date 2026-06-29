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

package router

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"go.opentelemetry.io/otel"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var BuildTag = "dev"

type RecordedQuery struct {
	Timestamp time.Time     `json:"timestamp"`
	Client    string        `json:"client"`
	Host      string        `json:"host"`
	Path      string        `json:"path"`
	Method    string        `json:"method"`
	Action    string        `json:"action"`
	Target    string        `json:"target"`
	Duration  time.Duration `json:"duration"`
}

type QueryRecorder struct {
	mu      sync.RWMutex
	queries []RecordedQuery
	size    int
	index   int
}

func NewQueryRecorder(size int) *QueryRecorder {
	return &QueryRecorder{
		queries: make([]RecordedQuery, 0, size),
		size:    size,
	}
}

func (qr *QueryRecorder) Add(q RecordedQuery) {
	if qr == nil {
		return
	}

	qr.mu.Lock()
	defer qr.mu.Unlock()

	if len(qr.queries) < qr.size {
		qr.queries = append(qr.queries, q)
	} else {
		qr.queries[qr.index] = q
		qr.index = (qr.index + 1) % qr.size
	}
}

func (qr *QueryRecorder) Get() []RecordedQuery {
	if qr == nil {
		return nil
	}

	qr.mu.RLock()
	defer qr.mu.RUnlock()

	n := len(qr.queries)
	if n == 0 {
		return nil
	}

	res := make([]RecordedQuery, n)
	if n < qr.size {
		for i := 0; i < n; i++ {
			res[i] = qr.queries[n-1-i]
		}
	} else {
		for i := 0; i < n; i++ {
			pos := (qr.index - 1 - i + n) % n
			res[i] = qr.queries[pos]
		}
	}

	return res
}

// redactPath drops the query string, which may carry credentials (CWE-598).
func redactPath(path string) string {
	p, _, _ := strings.Cut(path, "?")
	return p
}

func (qr *QueryRecorder) AddRouterRequest(
	start time.Time,
	duration time.Duration,
	action,
	target string,
	m *requestMetadata,
) {
	qr.Add(RecordedQuery{
		Timestamp: start,
		Client:    m.headers[":authority"],
		Host:      m.host,
		Path:      redactPath(m.path),
		Method:    m.headers[":method"],
		Action:    action,
		Target:    target,
		Duration:  duration,
	})
}

type TemplateInfo struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

type DashboardContext struct {
	BuildTag        string             `json:"build_tag"`
	RouterClusterIP string             `json:"router_cluster_ip"`
	Namespace       string             `json:"namespace"`
	HttpPort        int                `json:"port_http"`
	XdsPort         int                `json:"port_xds"`
	ExtprocPort     int                `json:"port_extproc"`
	StatusPort      int                `json:"status_port"`
	Args            string             `json:"args"`
	Flags           map[string]string  `json:"flags"`
	Queries         []FormattedQuery   `json:"queries"`
	Health          RouterHealthReport `json:"health"`
	Templates       []TemplateInfo     `json:"templates"`
}

type FormattedQuery struct {
	Timestamp string `json:"timestamp"`
	Client    string `json:"client"`
	Host      string `json:"host"`
	Path      string `json:"path"`
	Method    string `json:"method"`
	Action    string `json:"action"`
	Target    string `json:"target"`
	Duration  string `json:"duration"`
}

func (s *RouterServer) getRouterIP(ctx context.Context) string {
	if s.cfg.Standalone || s.clientset == nil {
		return "Standalone Mode (No Cluster IP)"
	}

	svc, err := s.clientset.CoreV1().Services(s.cfg.Namespace).Get(ctx, "atenet-router", metav1.GetOptions{})
	if err != nil {
		return fmt.Sprintf("Lookup Failed: %v", err)
	}

	if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == "None" {
		return "ClusterIP Unassigned"
	}

	return svc.Spec.ClusterIP
}

func (s *RouterServer) handleStatusz(w http.ResponseWriter, req *http.Request) {
	ctx, span := otel.Tracer(routerServiceName).Start(req.Context(), "handleStatusz")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	routerIP := s.getRouterIP(ctx)

	buildInfo := BuildTag
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				buildInfo += fmt.Sprintf(" (rev: %s)", setting.Value)
			}
		}
	}

	argsStr := strings.Join(os.Args, " ")

	flagsMap := make(map[string]string)
	if s.Cmd != nil {
		s.Cmd.Flags().VisitAll(func(f *pflag.Flag) {
			flagsMap[f.Name] = f.Value.String()
		})
	}

	var rawQueries []RecordedQuery
	if s.extprocSrv != nil && s.extprocSrv.recorder != nil {
		rawQueries = s.extprocSrv.recorder.Get()
	}

	formattedQueries := make([]FormattedQuery, len(rawQueries))
	for i, q := range rawQueries {
		formattedQueries[i] = FormattedQuery{
			Timestamp: q.Timestamp.Format(time.RFC3339),
			Client:    q.Client,
			Host:      q.Host,
			Path:      q.Path,
			Method:    q.Method,
			Action:    q.Action,
			Target:    q.Target,
			Duration:  fmt.Sprintf("%.2f ms", float64(q.Duration.Microseconds())/1000),
		}
	}

	var hr RouterHealthReport
	if s.health != nil {
		hr = s.health.Report()
	}

	var templateInfos []TemplateInfo
	if s.atStore != nil {
		templates, err := s.atStore.readyTemplates(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to get ActorTemplates for status", slog.String("err", err.Error()))
		} else {
			templateInfos = make([]TemplateInfo, len(templates))
			for i, t := range templates {
				templateInfos[i] = TemplateInfo{
					Name:      t.Name,
					Namespace: t.Namespace,
				}
			}
		}
	}

	data := DashboardContext{
		BuildTag:        buildInfo,
		RouterClusterIP: routerIP,
		Namespace:       s.cfg.Namespace,
		HttpPort:        s.cfg.HttpPort,
		XdsPort:         s.cfg.XdsPort,
		ExtprocPort:     s.cfg.ExtprocPort,
		StatusPort:      s.cfg.StatusPort,
		Args:            argsStr,
		Flags:           flagsMap,
		Queries:         formattedQueries,
		Health:          hr,
		Templates:       templateInfos,
	}

	accept := req.Header.Get("Accept")
	formatParam := req.URL.Query().Get("format")

	if strings.Contains(accept, "application/json") || formatParam == "json" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl, err := template.New("dashboard").Parse(dashboardHTML)
	if err != nil {
		http.Error(w, fmt.Sprintf("Template parsing failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = tmpl.Execute(w, data)
}

//go:embed dashboard.html
var dashboardHTML string
