package oss

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

type XmlDecoderLite struct {
	reader          io.Reader
	attributePrefix string
	useRawToken     bool
}

func NewXmlDecoderLite(r io.Reader) *XmlDecoderLite {
	return &XmlDecoderLite{
		reader:          r,
		attributePrefix: "+@",
		useRawToken:     true,
	}
}

func (dec *XmlDecoderLite) Decode(root *XmlNode) error {
	return dec.decodeXML(root)
}

type XmlNode struct {
	Children []*XmlChildren
	Data     []string
}

type XmlChildren struct {
	K string
	V []*XmlNode
}

func (n *XmlNode) addChild(s string, c *XmlNode) {
	if n.Children == nil {
		n.Children = make([]*XmlChildren, 0)
	}
	for _, childEntry := range n.Children {
		if childEntry.K == s {
			childEntry.V = append(childEntry.V, c)
			return
		}
	}
	n.Children = append(n.Children, &XmlChildren{K: s, V: []*XmlNode{c}})
}

func (n *XmlNode) value() any {
	if len(n.Children) > 0 {
		return n.GetMap()
	}
	if n.Data != nil {
		return n.Data[0]
	}
	return nil
}

func (n *XmlNode) GetMap() map[string]any {
	node := map[string]any{}
	for _, kv := range n.Children {
		label := kv.K
		children := kv.V
		if len(children) > 1 {
			vals := make([]any, 0)
			for _, child := range children {
				vals = append(vals, child.value())
			}
			node[label] = vals
		} else {
			node[label] = children[0].value()
		}
	}
	return node
}

type element struct {
	parent *element
	n      *XmlNode
	label  string
}

func (dec *XmlDecoderLite) decodeXML(root *XmlNode) error {
	xmlDec := xml.NewDecoder(dec.reader)

	started := false

	// Create first element from the root node
	elem := &element{
		parent: nil,
		n:      root,
	}

	getToken := func() (xml.Token, error) {
		if dec.useRawToken {
			return xmlDec.RawToken()
		}
		return xmlDec.Token()
	}

	for {
		t, e := getToken()
		if e != nil && !errors.Is(e, io.EOF) {
			return e
		}
		if t == nil {
			break
		}

		switch se := t.(type) {
		case xml.StartElement:
			elem = &element{
				parent: elem,
				n:      &XmlNode{},
				label:  se.Name.Local,
			}

			for _, a := range se.Attr {
				elem.n.addChild(dec.attributePrefix+a.Name.Local, &XmlNode{Data: []string{a.Value}})
			}
		case xml.CharData:
			newBit := trimNonGraphic(string(se))
			if !started && len(newBit) > 0 {
				return fmt.Errorf("invalid XML: Encountered chardata [%v] outside of XML node", newBit)
			}

			if len(newBit) > 0 {
				elem.n.Data = append(elem.n.Data, newBit)
			}
		case xml.EndElement:
			if elem.parent != nil {
				elem.parent.n.addChild(elem.label, elem.n)
			}
			elem = elem.parent
		}
		started = true
	}

	return nil
}

func trimNonGraphic(s string) string {
	if s == "" {
		return s
	}

	var first *int
	var last int
	for i, r := range []rune(s) {
		if !unicode.IsGraphic(r) || unicode.IsSpace(r) {
			continue
		}

		if first == nil {
			f := i
			first = &f
			last = i
		} else {
			last = i
		}
	}

	if first == nil {
		return ""
	}

	return string([]rune(s)[*first : last+1])
}

var (
	escQuot = []byte("&#34;") // shorter than "&quot;"
	escApos = []byte("&#39;") // shorter than "&apos;"
	escAmp  = []byte("&amp;")
	escLT   = []byte("&lt;")
	escGT   = []byte("&gt;")
	escTab  = []byte("&#x9;")
	escNL   = []byte("&#xA;")
	escCR   = []byte("&#xD;")
	escFFFD = []byte("\uFFFD") // Unicode replacement character
)

// escapeXml EscapeString writes to p the properly escaped XML equivalent
// of the plain text data s.
func escapeXml(s string) string {
	var p strings.Builder
	var esc []byte
	hextable := "0123456789ABCDEF"
	escPattern := []byte("&#x00;")
	last := 0
	for i := 0; i < len(s); {
		r, width := utf8.DecodeRuneInString(s[i:])
		i += width
		switch r {
		case '"':
			esc = escQuot
		case '\'':
			esc = escApos
		case '&':
			esc = escAmp
		case '<':
			esc = escLT
		case '>':
			esc = escGT
		case '\t':
			esc = escTab
		case '\n':
			esc = escNL
		case '\r':
			esc = escCR
		default:
			if !isInCharacterRange(r) || (r == 0xFFFD && width == 1) {
				if r >= 0x00 && r < 0x20 {
					escPattern[3] = hextable[r>>4]
					escPattern[4] = hextable[r&0x0f]
					esc = escPattern
				} else {
					esc = escFFFD
				}
				break
			}
			continue
		}
		p.WriteString(s[last : i-width])
		p.Write(esc)
		last = i
	}
	p.WriteString(s[last:])
	return p.String()
}

// Decide whether the given rune is in the XML Character Range, per
// the Char production of https://www.xml.com/axml/testaxml.htm,
// Section 2.2 Characters.
func isInCharacterRange(r rune) (inrange bool) {
	return r == 0x09 ||
		r == 0x0A ||
		r == 0x0D ||
		r >= 0x20 && r <= 0xD7FF ||
		r >= 0xE000 && r <= 0xFFFD ||
		r >= 0x10000 && r <= 0x10FFFF
}
