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

// envdoc collects env.Var declarations without loading or executing packages.
package main

import (
	"bytes"
	"cmp"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const envPackage = "github.com/agent-substrate/substrate/internal/env"

func main() {
	root := flag.String("root", "../..", "Repository root")
	check := flag.Bool("check", false, "Check the reference without writing it")
	flag.Parse()
	if err := run(*root, *check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(root string, check bool) error {
	entries, err := collect(root)
	if err != nil {
		return err
	}
	output := render(entries)
	path := filepath.Join(root, "docs/environment-variables.md")
	if check {
		current, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, output) {
			return errors.New("environment reference is stale; run hack/update/environment-variables.sh")
		}
		return nil
	}
	return os.WriteFile(path, output, 0o644)
}

type entry struct {
	fields      map[string]string
	typ, source string
	line        int
}

func collect(root string) ([]entry, error) {
	var entries []entry
	for _, dir := range []string{"cmd", "internal", "pkg"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if !bytes.Contains(source, []byte(envPackage)) {
				return nil
			}
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			found, err := declarations(filepath.ToSlash(relative), source)
			if err != nil {
				return err
			}
			entries = append(entries, found...)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(entries, func(a, b entry) int {
		return cmp.Or(cmp.Compare(a.fields["SystemProvided"], b.fields["SystemProvided"]), cmp.Compare(a.fields["Name"], b.fields["Name"]), cmp.Compare(a.fields["Component"], b.fields["Component"]))
	})
	seen := map[[2]string]bool{}
	for _, e := range entries {
		key := [2]string{e.fields["Name"], e.fields["Component"]}
		if seen[key] {
			return nil, fmt.Errorf("duplicate declaration for %s in %s", key[0], key[1])
		}
		seen[key] = true
	}

	if len(entries) == 0 {
		return nil, errors.New("no environment declarations found")
	}
	return entries, nil
}

func declarations(path string, source []byte) ([]entry, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, source, 0)
	if err != nil {
		return nil, err
	}
	alias := ""
	for _, imp := range file.Imports {
		if imp.Path.Value == strconv.Quote(envPackage) {
			alias = "env"
			if imp.Name != nil {
				alias = imp.Name.Name
			}
		}
	}
	if alias == "" {
		return nil, nil
	}
	if alias == "." {
		return nil, fmt.Errorf("%s: env imports must be named", path)
	}
	constants := map[string]ast.Expr{}
	for _, declaration := range file.Decls {
		gen, ok := declaration.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i < len(value.Values) {
					constants[name.Name] = value.Values[i]
				}
			}
		}
	}
	var entries []entry
	ast.Inspect(file, func(node ast.Node) bool {
		if err != nil {
			return false
		}
		lit, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		indexed, ok := lit.Type.(*ast.IndexExpr)
		if !ok {
			return true
		}
		selector, ok := indexed.X.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Var" {
			return true
		}
		ident, ok := selector.X.(*ast.Ident)
		if !ok || ident.Name != alias {
			return true
		}
		e := entry{fields: map[string]string{"SystemProvided": "false"}, source: path, line: fset.Position(lit.Pos()).Line}
		typ, ok := indexed.Index.(*ast.Ident)
		if !ok || (typ.Name != "string" && typ.Name != "bool") {
			err = fmt.Errorf("%s:%d: unsupported env.Var type", path, e.line)
			return false
		}
		e.typ = typ.Name
		for _, element := range lit.Elts {
			kv, ok := element.(*ast.KeyValueExpr)
			if !ok {
				err = fmt.Errorf("%s:%d: env.Var fields must be named", path, e.line)
				return false
			}
			key := kv.Key.(*ast.Ident).Name
			if key == "Parse" {
				continue
			} // Never evaluate custom parsers.
			value, valueErr := literal(kv.Value, constants, map[string]bool{})
			if valueErr != nil {
				err = fmt.Errorf("%s:%d: %s: %w", path, e.line, key, valueErr)
				return false
			}
			e.fields[key] = value
		}
		for _, key := range []string{"Name", "Default", "Component", "Description", "AcceptedValues", "Precedence"} {
			value, exists := e.fields[key]
			if !exists || (key != "Default" && value == "") {
				err = fmt.Errorf("%s:%d: missing %s metadata", path, e.line, key)
				return false
			}
		}
		entries = append(entries, e)
		return false
	})
	return entries, err
}

// Only literals and same-file constants are permitted. In particular, defaults
// and metadata cannot come from environment reads or arbitrary function calls.
func literal(expr ast.Expr, constants map[string]ast.Expr, visiting map[string]bool) (string, error) {
	switch value := expr.(type) {
	case *ast.BasicLit:
		if value.Kind == token.STRING {
			return strconv.Unquote(value.Value)
		}
	case *ast.Ident:
		if value.Name == "true" || value.Name == "false" {
			return value.Name, nil
		}
		if constant, ok := constants[value.Name]; ok && !visiting[value.Name] {
			visiting[value.Name] = true
			return literal(constant, constants, visiting)
		}
	}
	return "", errors.New("must be a string/boolean literal or a same-file constant")
}

func render(entries []entry) []byte {
	var out strings.Builder
	out.WriteString(`# Go environment variables

<!-- Generated by tools/envdoc. Do not edit; run hack/update/environment-variables.sh. -->

This reference covers settings read directly by production Go components and
shared runtime packages. Setup tools, demos, test-only settings, and variables
read only inside dependencies (such as AWS credentials and OTel SDK settings)
are outside this initial registry. Shared settings can have different defaults
and precedence in each consumer; each declaration is listed separately below.

Types describe the value returned to the consumer. Some strings are parsed or
forwarded later; their accepted values and effective defaults are documented
separately. An explicitly empty value is distinct from an unset variable.
Documentation is generated from declared metadata, never the live environment.

To add a setting, see [the declaration guide](dev/environment-variables.md).

`)
	escape := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\n", " ", "\r", " ", "*", "\\*", "[", "\\[", "]", "\\]", "`", "\\`")
	category := ""
	for _, e := range entries {
		f := e.fields
		next := "Operator configuration"
		if f["SystemProvided"] == "true" {
			next = "System-provided values"
		}
		if next != category {
			fmt.Fprintf(&out, "## %s\n\n", next)
			category = next
		}
		fmt.Fprintf(&out, "### %s (%s)\n\n%s\n\n", escape.Replace(f["Name"]), escape.Replace(f["Component"]), escape.Replace(f["Description"]))
		value := f["Default"]
		if e.typ == "string" {
			value = strconv.Quote(value)
		}
		fmt.Fprintf(&out, "- Type: %s\n- Declared default: %s\n", e.typ, escape.Replace(value))
		if f["DefaultDescription"] != "" {
			fmt.Fprintf(&out, "- Effective default: %s\n", escape.Replace(f["DefaultDescription"]))
		}
		fmt.Fprintf(&out, "- Accepted values: %s\n- Precedence: %s\n- Source: [%s](../%s#L%d)\n\n", escape.Replace(f["AcceptedValues"]), escape.Replace(f["Precedence"]), e.source, e.source, e.line)
	}
	return []byte(strings.TrimSuffix(out.String(), "\n"))
}
