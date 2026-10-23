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

// Command site-content generates Hugo content stubs in site/content/ from the
// Markdown files under docs/ and demos/*/README.md so that site/content/ does
// not need to be checked into the repository.
package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

type sectionDef struct {
	Slug        string
	Title       string
	Weight      int
	Aliases     []string
	Description string
}

var sections = []sectionDef{
	{
		Slug:        "get-started",
		Title:       "Get Started",
		Weight:      1,
		Description: "Understand Agent Substrate's core value proposition and deploy your first stateful actor on Kind.",
	},
	{
		Slug:        "concepts",
		Title:       "Concepts",
		Weight:      2,
		Description: "High-level mental models and architectural explanations of how Agent Substrate works.",
	},
	{
		Slug:        "tutorials",
		Title:       "Tutorials",
		Weight:      3,
		Aliases:     []string{"/docs/examples/"},
		Description: "Step-by-step learning-oriented exercises demonstrating stateful actors, sandboxes, multiplexing, and networking on Agent Substrate.",
	},
	{
		Slug:        "guides",
		Title:       "How-to Guides",
		Weight:      4,
		Description: "Task-oriented recipes for installing, configuring, operating, and scaling Agent Substrate.",
	},
	{
		Slug:        "reference",
		Title:       "Reference",
		Weight:      5,
		Description: "Technical specifications for Agent Substrate APIs, CLI commands, terminology, and telemetry metrics.",
	},
	{
		Slug:        "security",
		Title:       "Security",
		Weight:      6,
		Description: "Security models, threat analysis, TLS MITM interception, and egress policies in Agent Substrate.",
	},
	{
		Slug:        "project",
		Title:       "Project",
		Weight:      7,
		Description: "Community channels, contribution guidelines, and repository conventions for Agent Substrate.",
	},
}

var (
	frontMatterRE = regexp.MustCompile(`\A---\n((?s:.*?)\n)?---\n+`)
	h1RE          = regexp.MustCompile(`(?m)^# +([^\n]+)$`)
)

func main() {
	repo := flag.String("repo", ".", "Path to the repository root.")
	out := flag.String("out", "", "Path to the Hugo content directory to generate (e.g. site/content).")
	flag.Parse()
	if *out == "" {
		fmt.Fprintln(os.Stderr, "site-content: --out is required")
		os.Exit(2)
	}
	if err := generate(*repo, *out); err != nil {
		fmt.Fprintf(os.Stderr, "site-content: %v\n", err)
		os.Exit(1)
	}
}

func generate(repoDir, outDir string) error {
	if err := os.RemoveAll(outDir); err != nil {
		return fmt.Errorf("cleaning %s: %w", outDir, err)
	}
	if err := writeIndexFiles(outDir); err != nil {
		return err
	}

	validSections := make(map[string]bool, len(sections))
	for _, s := range sections {
		validSections[s.Slug] = true
	}

	docsDir := filepath.Join(repoDir, "docs")
	err := filepath.WalkDir(docsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(docsDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel == "dev" || strings.HasPrefix(rel, "dev/") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".md") {
			return nil
		}

		repoRel := "docs/" + rel
		fm, body, err := readSourceDoc(path)
		if err != nil {
			return fmt.Errorf("%s: %w", repoRel, err)
		}
		if excluded(fm) {
			return nil
		}

		parts := strings.Split(rel, "/")
		if len(parts) != 2 || !validSections[parts[0]] {
			return fmt.Errorf("%s: must either live in a docsite section directory under docs/ or set 'docsite: false' in YAML front matter", repoRel)
		}
		section, filename := parts[0], parts[1]
		dst := filepath.Join(outDir, "docs", section, filename)
		return writeStub(dst, repoRel, strings.TrimSuffix(filename, ".md"), fm, body)
	})
	if err != nil {
		return err
	}

	demosPattern := filepath.Join(repoDir, "demos", "*", "README.md")
	demoFiles, err := filepath.Glob(demosPattern)
	if err != nil {
		return err
	}
	slices.Sort(demoFiles)
	for _, path := range demoFiles {
		rel, err := filepath.Rel(repoDir, path)
		if err != nil {
			return err
		}
		repoRel := filepath.ToSlash(rel)
		fm, body, err := readSourceDoc(path)
		if err != nil {
			return fmt.Errorf("%s: %w", repoRel, err)
		}
		if excluded(fm) {
			continue
		}
		slug := filepath.Base(filepath.Dir(path))
		dst := filepath.Join(outDir, "docs", "tutorials", slug+".md")
		if err := writeStub(dst, repoRel, slug, fm, body); err != nil {
			return err
		}
	}

	return nil
}

func writeIndexFiles(outDir string) error {
	homeIndex := `---
title: "Agent Substrate"
linkTitle: "Agent Substrate"
description: >
  Every agent runs in its own microVM or gVisor sandbox with an identity, no
  credentials, and no open network. Idle agents suspend, resume in under 500ms
  on any ready worker, and keep their memory and filesystem state.
---

{{< home-redesign >}}
`
	if err := writeFile(filepath.Join(outDir, "_index.md"), homeIndex); err != nil {
		return err
	}

	docsIndex := `---
title: "Documentation"
linkTitle: "Documentation"
weight: 20
menu:
  main:
    weight: 20
---
`
	if err := writeFile(filepath.Join(outDir, "docs", "_index.md"), docsIndex); err != nil {
		return err
	}

	for _, s := range sections {
		fm := map[string]any{
			"title":       s.Title,
			"linkTitle":   s.Title,
			"weight":      s.Weight,
			"description": s.Description,
		}
		if len(s.Aliases) > 0 {
			fm["aliases"] = s.Aliases
		}
		b, err := yaml.Marshal(fm)
		if err != nil {
			return err
		}
		content := fmt.Sprintf("---\n%s---\n", string(b))
		if err := writeFile(filepath.Join(outDir, "docs", s.Slug, "_index.md"), content); err != nil {
			return err
		}
	}
	return nil
}

func readSourceDoc(path string) (map[string]any, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	text := string(raw)
	fm := make(map[string]any)
	body := text
	if m := frontMatterRE.FindStringSubmatch(text); m != nil {
		if strings.TrimSpace(m[1]) != "" {
			if err := yaml.Unmarshal([]byte(m[1]), &fm); err != nil {
				return nil, "", fmt.Errorf("parsing YAML front matter: %w", err)
			}
		}
		body = text[len(m[0]):]
	}
	return fm, body, nil
}

func excluded(fm map[string]any) bool {
	v, ok := fm["docsite"]
	if !ok {
		return false
	}
	b, isBool := v.(bool)
	return isBool && !b
}

func writeStub(dstPath, repoSource, fallbackSlug string, fm map[string]any, body string) error {
	outFM := make(map[string]any, len(fm)+4)
	for k, v := range fm {
		if k == "docsite" {
			continue
		}
		outFM[k] = v
	}

	title, _ := outFM["title"].(string)
	if strings.TrimSpace(title) == "" {
		title = inferTitle(body, fallbackSlug)
		outFM["title"] = title
	}
	if lt, _ := outFM["linkTitle"].(string); strings.TrimSpace(lt) == "" {
		outFM["linkTitle"] = title
	}
	if desc, _ := outFM["description"].(string); strings.TrimSpace(desc) == "" {
		if inferred := inferDescription(body); inferred != "" {
			outFM["description"] = inferred
		}
	}
	outFM["repo_source"] = repoSource

	b, err := yaml.Marshal(outFM)
	if err != nil {
		return err
	}
	stub := fmt.Sprintf("---\n%s---\n\n{{%% include-file %%}}\n", string(b))
	return writeFile(dstPath, stub)
}

func inferTitle(body, fallback string) string {
	if m := h1RE.FindStringSubmatch(body); m != nil {
		t := strings.TrimSpace(m[1])
		t = strings.ReplaceAll(t, "`", "")
		if t != "" {
			return t
		}
	}
	return fallback
}

func inferDescription(body string) string {
	lines := strings.Split(body, "\n")
	var para []string
	inCode := false
	inComment := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "```") {
			inCode = !inCode
			continue
		}
		if inCode {
			continue
		}
		if strings.HasPrefix(trimmed, "<!--") {
			if !strings.Contains(trimmed, "-->") {
				inComment = true
			}
			continue
		}
		if inComment {
			if strings.Contains(trimmed, "-->") {
				inComment = false
			}
			continue
		}
		if trimmed == "" {
			if len(para) > 0 {
				break
			}
			continue
		}
		if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "|") || strings.HasPrefix(trimmed, ">") || strings.HasPrefix(trimmed, "<") || strings.HasPrefix(trimmed, "* ") || strings.HasPrefix(trimmed, "- ") {
			if len(para) > 0 {
				break
			}
			continue
		}
		para = append(para, trimmed)
	}
	return strings.Join(para, " ")
}

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}
