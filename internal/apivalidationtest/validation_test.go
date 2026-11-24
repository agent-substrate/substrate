// Copyright 2026 The Agent Substrate Authors
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

package apivalidationtest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

type formatCase struct {
	name  string
	value string
	valid bool
}

type formatTest struct {
	format string
	field  string
	set    func(*ChildWithoutTags, string)
	cases  []formatCase
}

// sweep returns a case for each rune in [lo, hi], embedded as wrap(r). Each
// rune is valid if allowed reports true.
func sweep(lo, hi rune, wrap func(string) string, allowed func(rune) bool) []formatCase {
	var cases []formatCase
	for r := lo; r <= hi; r++ {
		cases = append(cases, formatCase{
			name:  fmt.Sprintf("sweep U+%04X", r),
			value: wrap(string(r)),
			valid: allowed(r),
		})
	}
	return cases
}

func concat(lists ...[]formatCase) []formatCase {
	var all []formatCase
	for _, l := range lists {
		all = append(all, l...)
	}
	return all
}

func between(s string) func(string) string {
	return func(c string) string { return s + c + s }
}

func inSet(set string) func(rune) bool {
	return func(r rune) bool { return strings.ContainsRune(set, r) }
}

const (
	alnum = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	// RFC 3986 pchar, less pct-encoded, plus '/'.
	pathChars = alnum + "-._~!$&'()*+,;=:@/"
	// RFC 9110 tchar.
	tchars = alnum + "!#$%&'*+-.^_`|~"
	// DNS-1123 label characters.
	labelChars = "abcdefghijklmnopqrstuvwxyz0123456789-"
)

var formatTests = []formatTest{{
	format: "ate-singleton-name",
	field:  "singletonName",
	set:    func(c *ChildWithoutTags, v string) { c.SingletonName = v },
	cases: []formatCase{
		{"empty", "", true},
		{"default", "default", true},
		{"capitalized", "Default", false},
		{"upper", "DEFAULT", false},
		{"leading space", " default", false},
		{"trailing space", "default ", false},
		{"trailing newline", "default\n", false},
		{"prefix", "defaults", false},
		{"suffix", "xdefault", false},
		{"truncated", "defaul", false},
		{"other", "other", false},
		{"repeated", "defaultdefault", false},
	},
}, {
	format: "ate-http-probe-path",
	field:  "httpProbePath",
	set:    func(c *ChildWithoutTags, v string) { c.HTTPProbePath = v },
	cases: concat([]formatCase{
		{"empty", "", true},
		{"root", "/", true},
		{"simple", "/healthz", true},
		{"nested", "/a/b/c", true},
		{"trailing slash", "/healthz/", true},
		{"double slash", "//a", true},
		{"dot segments", "/./../a", true},
		{"every path character", "/" + pathChars, true},
		{"percent escape", "/a%20b", true},
		{"percent escape lower hex", "/%7e", true},
		{"percent escape mixed hex", "/%aF", true},
		{"percent escape at end", "/a%2F", true},
		{"adjacent percent escapes", "/%25%25", true},
		{"no leading slash", "healthz", false},
		{"relative", "a/b", false},
		{"absolute URL", "http://host/healthz", false},
		{"query", "/healthz?x=1", false},
		{"empty query", "/healthz?", false},
		{"fragment", "/healthz#x", false},
		{"empty fragment", "/healthz#", false},
		{"truncated percent escape", "/a%2", false},
		{"bare percent", "/a%", false},
		{"percent non-hex", "/a%zz", false},
		{"percent half hex", "/a%g0", false},
		{"space", "/a b", false},
		{"non-ASCII", "/é", false},
		{"leading newline", "\n/a", false},
		{"trailing newline", "/a\n", false},
	},
		// '%' only appears in percent-escapes, tested above.
		sweep(0, 0x7f, func(c string) string {
			if c == "%" {
				return "/a%25b"
			}
			return "/a" + c + "b"
		}, func(r rune) bool { return r == '%' || inSet(pathChars)(r) }),
		sweep(0x80, 0xff, between("/"), func(rune) bool { return false }),
	),
}, {
	format: "ate-env-var-name",
	field:  "envVarName",
	set:    func(c *ChildWithoutTags, v string) { c.EnvVarName = v },
	cases: concat([]formatCase{
		{"empty", "", true},
		{"conventional", "HOME_DIR", true},
		{"lower", "home_dir", true},
		{"leading digit", "1ABC", true},
		{"printable punctuation", "a.b-c", true},
		{"space", "A B", true},
		{"only space", " ", true},
		{"equals sign", "A=B", false},
		{"only equals sign", "=", false},
		{"trailing equals sign", "A=", false},
		{"tab", "A\tB", false},
		{"trailing newline", "A\n", false},
		{"non-ASCII", "é", false},
		{"emoji", "\U0001F600", false},
	},
		sweep(0, 0x7f, between("A"), func(r rune) bool { return r >= ' ' && r <= '~' && r != '=' }),
		sweep(0x80, 0xff, between("A"), func(rune) bool { return false }),
	),
}, {
	format: "ate-volume-type-name",
	field:  "volumeTypeName",
	set:    func(c *ChildWithoutTags, v string) { c.VolumeTypeName = v },
	cases: concat([]formatCase{
		{"empty", "", true},
		{"single letter", "a", true},
		{"single digit", "0", true},
		{"dns subdomain", "pd.csi.storage.gke.io", true},
		{"hyphen", "a-b", true},
		{"double hyphen", "a--b", true},
		{"numeric labels", "1.2.3", true},
		// maxLength bounds the length; the format does not.
		{"long label", strings.Repeat("a", 300), true},
		{"substrate prefix", "substrate.io/gcs", true},
		{"substrate prefix with subdomain", "substrate.io/a.b-c", true},
		{"upper", "PD", false},
		{"mixed case", "Pd.io", false},
		{"leading hyphen", "-a", false},
		{"trailing hyphen", "a-", false},
		{"label leading hyphen", "a.-b", false},
		{"label trailing hyphen", "a-.b", false},
		{"leading dot", ".a", false},
		{"trailing dot", "a.", false},
		{"empty label", "a..b", false},
		{"other prefix", "example.io/gcs", false},
		{"prefix only", "substrate.io/", false},
		{"prefix with upper", "substrate.io/PD", false},
		{"prefix twice", "substrate.io/substrate.io/a", false},
		{"prefix double slash", "substrate.io//a", false},
		{"prefix dot is literal", "substrateXio/gcs", false},
		{"bare slash", "/gcs", false},
		{"trailing slash", "gcs/", false},
		{"trailing newline", "a\n", false},
	},
		sweep(0, 0x7f, between("a"), func(r rune) bool { return r == '.' || inSet(labelChars)(r) }),
		sweep(0x80, 0xff, between("a"), func(rune) bool { return false }),
	),
}, {
	format: "ate-volume-storage-id",
	field:  "volumeStorageID",
	set:    func(c *ChildWithoutTags, v string) { c.VolumeStorageID = v },
	cases: concat([]formatCase{
		{"empty", "", true},
		{"plain", "projects/p/disks/d-1", true},
		{"tab", "a\tb", true},
		{"newline", "a\nb", true},
		{"carriage return", "a\rb", true},
		{"non-breaking space", "a\u00a0b", true},
		{"CJK", "日本", true},
		{"emoji", "\U0001F600", true},
		{"NUL", "a\x00b", false},
		{"DEL", "a\x7fb", false},
		{"C1 control", "a\u0085b", false},
		{"leading control", "\x01a", false},
		{"trailing control", "a\x1f", false},
		// The regexp decodes an invalid UTF-8 byte as U+FFFD.
		{"invalid UTF-8", "a\x85b", true},
	},
		sweep(0, 0xff, between("a"), func(r rune) bool {
			return !(r <= 0x08 || r == 0x0b || r == 0x0c || (r >= 0x0e && r <= 0x1f) || (r >= 0x7f && r <= 0x9f))
		}),
	),
}, {
	format: "ate-http-header-name",
	field:  "httpHeaderName",
	set:    func(c *ChildWithoutTags, v string) { c.HTTPHeaderName = v },
	cases: concat([]formatCase{
		{"empty", "", true},
		{"conventional", "Authorization", true},
		{"lower with punctuation", "x-custom_header", true},
		{"every token character", tchars, true},
		{"single character", "X", true},
		{"space", "bad header", false},
		{"colon", "bad:header", false},
		{"non-ASCII", "héader", false},
		{"leading space", " X", false},
		{"trailing newline", "X\n", false},
	},
		sweep(0, 0x7f, between("X"), inSet(tchars)),
		sweep(0x80, 0xff, between("X"), func(rune) bool { return false }),
	),
}, {
	format: "ate-http-header-value",
	field:  "httpHeaderValue",
	set:    func(c *ChildWithoutTags, v string) { c.HTTPHeaderValue = v },
	cases: concat([]formatCase{
		{"empty", "", true},
		{"bearer", "Bearer token", true},
		{"trailing space", "Bearer ", true},
		{"tab", "value\tvalue", true},
		{"obs-text", "\u0080\u0081", true},
		{"non-ASCII", "é", true},
		{"invalid UTF-8", "a\xffb", true},
		{"carriage return", "a\rb", false},
		{"newline", "a\nb", false},
		{"CRLF injection", "a\r\nX-Evil: 1", false},
		{"NUL", "a\x00b", false},
		{"unit separator", "a\x1fb", false},
		{"DEL", "a\x7fb", false},
	},
		sweep(0, 0xff, between("a"), func(r rune) bool { return r == '\t' || (r >= ' ' && r != 0x7f) }),
	),
}}

func TestFormats(t *testing.T) {
	ctx := context.Background()
	create := operation.Operation{Type: operation.Create}
	update := operation.Operation{Type: operation.Update}
	matcher := field.ErrorMatcher{}.ByType().ByField().ByValue().ByOrigin()

	for _, ft := range formatTests {
		for _, tc := range ft.cases {
			t.Run(ft.format+"/"+tc.name, func(t *testing.T) {
				var obj ChildWithoutTags
				ft.set(&obj, tc.value)

				want := func(p *field.Path) field.ErrorList {
					if tc.valid {
						return nil
					}
					return field.ErrorList{
						field.Invalid(p.Child(ft.field), tc.value, "").WithOrigin("format=" + ft.format),
					}
				}

				child := Child(obj)
				matcher.Test(t, want(nil), Validate_Child(ctx, create, nil, &child, nil))
				// Unchanged values are not revalidated.
				oldChild := child
				matcher.Test(t, nil, Validate_Child(ctx, update, nil, &child, &oldChild))

				parent := Parent{Child: obj}
				matcher.Test(t, want(field.NewPath("child")), Validate_Parent(ctx, create, nil, &parent, nil))
				oldParent := parent
				matcher.Test(t, nil, Validate_Parent(ctx, update, nil, &parent, &oldParent))
			})
		}
	}
}
