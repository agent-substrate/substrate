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

package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/client-go/tools/clientcmd"
)

// Where a run's record goes. Success overwrites one file per context, because
// the current configuration of a cluster is one thing. Failures accumulate,
// because each attempt is its own artifact and the operator chooses which to
// retry from.
const (
	installsDir = "installs"
	failedDir   = "failed"
)

// unkeyedContext names the record when no context was resolved. Two clusters
// then share one file, which is tolerable only because nothing reads these
// automatically; see docs/operator-install.md.
const unkeyedContext = "unkeyed"

// RecordDir is where records are written: the directory the caller was
// configured with, or the user cache directory.
//
// Not the repository root: an operator installing from a release has no
// checkout, and writing into someone's working tree to record their own run
// is a surprise. An operator who wants these under version control points the
// setting at the repository that holds them.
//
// The configured directory is a parameter rather than a setting this package
// reads, so nothing here depends on a particular key existing.
func RecordDir(configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("while resolving the cache directory for install records: %w", err)
	}
	return filepath.Join(base, "ate-setup"), nil
}

// RecordSuccess writes the configuration a completed run used, replacing the
// previous record for that context. It returns the path and the secret
// settings it left out.
func RecordSuccess(dir, context, substrateVersion string, r *Resolved) (string, []string, error) {
	d, omitted := NewDocument(r, DocumentMetadata{
		Context:          context,
		Outcome:          Succeeded,
		SubstrateVersion: substrateVersion,
	})
	path := filepath.Join(dir, installsDir, recordName(context)+".yaml")
	return path, omitted, write(path, d)
}

// RecordFailure writes the configuration an incomplete run used, under a name
// that does not collide with earlier attempts.
//
// Nothing reads it. A failed run must not change what a later run resolves --
// a botched value would otherwise become the default and reach the cluster on
// the next success -- so recovering it is an explicit act: pass the path to
// --config.
func RecordFailure(dir, context, substrateVersion string, r *Resolved, failedAt string) (string, []string, error) {
	d, omitted := NewDocument(r, DocumentMetadata{
		Context:          context,
		Outcome:          Failed,
		FailedAt:         failedAt,
		SubstrateVersion: substrateVersion,
	})
	stamp := time.Now().UTC().Format("20060102T150405Z")
	base := filepath.Join(dir, failedDir, recordName(context)+"-"+stamp)
	path, err := writeNew(base, d)
	return path, omitted, err
}

// ClusterKey identifies the cluster a run targeted, for naming its record.
//
// The context setting is empty for most installs: the documented deploys do
// not pass --context, and the cluster is then whichever one the kubeconfig
// already selects. Naming the record from the setting alone would put every
// such install in one file, where a second cluster silently replaces the
// first. An unset context is therefore resolved the same way the run itself
// resolves it.
func (r *Resolved) ClusterKey() string {
	if ctx := r.String("context"); ctx != "" {
		return ctx
	}
	// A Kind install derives its context from the cluster name, and does so
	// before there is a cluster in the kubeconfig to read it from.
	if r.Bool(kindEnabledKey) {
		return "kind-" + r.String("kindCluster.name")
	}
	return currentContext(r.String("kubeconfig"))
}

// currentContext is the context the kubeconfig selects, or "" when there is
// none to read. Failing to name the cluster is not an error here: it costs
// the record its name, which is better than failing a run that worked.
func currentContext(kubeconfig string) string {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	// $KUBECONFIG is a PATH-style list. A single entry is the explicit file;
	// several are a precedence chain, which is how client-go merges them.
	// An unset setting leaves the default rules, which find the kubeconfig
	// the same way kubectl does.
	switch paths := filepath.SplitList(kubeconfig); {
	case len(paths) == 1:
		rules.ExplicitPath = paths[0]
	case len(paths) > 1:
		rules.Precedence = paths
	}

	cfg, err := rules.Load()
	if err != nil {
		return ""
	}
	return cfg.CurrentContext
}

// recordName is the file name for a context, made safe for a path. Context
// names are not: EKS defaults to an ARN
// (arn:aws:eks:us-east-1:111122223333:cluster/prod) and GKE to
// gke_project_region_name.
func recordName(context string) string {
	if context == "" {
		return unkeyedContext
	}
	return strings.Map(func(ch rune) rune {
		switch {
		case ch == '.' || ch == '_' || ch == '-':
			return ch
		case ch >= '0' && ch <= '9', ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z':
			return ch
		}
		return '-'
	}, context)
}

// writeNew renders the document to the first free name starting at base,
// creating it exclusively.
//
// The timestamp is only second-granular, so two attempts inside one second
// would otherwise land on the same name and the later would erase the earlier
// -- which is the opposite of what accumulating failures is for. Creating
// exclusively rather than checking first also settles the race between two
// runs failing at once.
func writeNew(base string, d *Document) (string, error) {
	raw, err := d.Marshal()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		return "", err
	}
	for n := 1; ; n++ {
		path := base + ".yaml"
		if n > 1 {
			path = fmt.Sprintf("%s-%d.yaml", base, n)
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return "", err
		}
		if _, err := f.Write(raw); err != nil {
			f.Close()
			return "", err
		}
		return path, f.Close()
	}
}

// write renders the document to path through a temporary file and a rename,
// so a concurrent run against the same context cannot leave a half-written
// record that the next --config would fail to parse.
func write(path string, d *Document) error {
	raw, err := d.Marshal()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".record-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	// 0600: a record holds no secrets, but it does describe an installation.
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
