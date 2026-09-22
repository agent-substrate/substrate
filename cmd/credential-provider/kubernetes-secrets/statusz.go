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

// The provider's /statusz page, mirroring the atenet router's: one handler
// serving an HTML dashboard by default and JSON under ?format=json (or
// Accept: application/json). It shows the provider's configuration as the
// running process holds it — notably the authorization policy loaded at
// startup, which the ConfigMap on disk may have since drifted from. Names
// and configuration only, never secret values.
package main

import (
	"encoding/json"
	"html/template"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/internal/version"
)

// statusContext is the /statusz payload; the JSON form is the contract tests
// and tooling read, the HTML form renders the same data.
type statusContext struct {
	Version      string              `json:"version"`
	ProviderName string              `json:"provider_name"`
	Args         string              `json:"args"`
	Flags        map[string]string   `json:"flags"`
	InjectorSAN  string              `json:"injector_san"`
	PolicyGrants map[string][]string `json:"policy_grants"`
}

// newStatuszHandler serves /statusz over the provider's state.
func newStatuszHandler(srv *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		flags := make(map[string]string)
		pflag.VisitAll(func(f *pflag.Flag) { flags[f.Name] = f.Value.String() })

		data := statusContext{
			Version:      version.String(),
			ProviderName: ProviderName,
			Args:         strings.Join(os.Args, " "),
			Flags:        flags,
			InjectorSAN:  *injectorIdentity,
			PolicyGrants: srv.Grants(),
		}

		if strings.Contains(req.Header.Get("Accept"), "application/json") || req.URL.Query().Get("format") == "json" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(data)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = statuszTemplate.Execute(w, data)
	}
}

var statuszTemplate = template.Must(template.New("statusz").Parse(`<!DOCTYPE html>
<html>
<head><title>k8s-credential-provider statusz</title>
<style>
body { font-family: monospace; margin: 2em; }
table { border-collapse: collapse; margin-bottom: 2em; }
th, td { border: 1px solid #999; padding: 4px 8px; text-align: left; }
th { background: #eee; }
</style>
</head>
<body>
<h1>k8s-credential-provider</h1>
<p>version: {{.Version}}<br>
provider: {{.ProviderName}}<br>
required caller SAN: {{.InjectorSAN}}<br>
args: {{.Args}}</p>

<h2>Namespace policy (as loaded at startup)</h2>
{{if .PolicyGrants}}
<table>
<tr><th>atespace</th><th>allowed namespaces</th></tr>
{{range $atespace, $namespaces := .PolicyGrants}}
<tr><td>{{$atespace}}</td><td>{{range $namespaces}}{{.}} {{end}}</td></tr>
{{end}}
</table>
{{else}}
<p><b>authorization disabled</b> — every namespace is allowed (dev only).</p>
{{end}}

<h2>Flags</h2>
<table>
<tr><th>flag</th><th>value</th></tr>
{{range $name, $value := .Flags}}
<tr><td>{{$name}}</td><td>{{$value}}</td></tr>
{{end}}
</table>
</body>
</html>
`))
