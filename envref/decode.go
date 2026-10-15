package envref

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

const nullTag = "!!null"

// DecodeYAML decodes the first YAML document in data into out, expanding
// references in scalar values on the way, the same order docker compose uses:
// parse into a node tree, interpolate values on the tree, then decode into the
// typed struct.
//
// Only values are expanded; mapping keys are left alone. Because expansion
// works on parsed nodes, a value containing quotes, '#', ':' or newlines can
// never change the document structure.
//
// Typing follows how the value was written:
//
//	port: ${DB_PORT}      plain: re-typed after expansion, so 5432 decodes into an int field
//	password: "${DB_PW}"  quoted: always a string
//
// Quote references to secrets. A plain reference is re-typed by YAML rules, so
// when the target is an interface (any, map[string]any) a secret such as
// "0123" or "1e3" would be decoded as a number.
//
// Inside flow collections ({...} or [...]) the reference must be quoted:
// "{" and "}" are flow indicators, so {port: ${P}} is not valid YAML.
//
// A plain value that expands to something YAML reads as null ("null", "~") is
// kept as a string: whoever wrote ${X} meant a value, and silently zeroing a
// password because the secret happens to be "null" is worse than surprising.
// A plain value that expands to nothing (only possible through an empty
// default such as ${X:-}) stays null, so the field keeps its default; write
// "${X:-}" with quotes to get an empty string instead.
//
// A key written with no value follows yaml.v3: string, number, bool and struct
// fields keep what out already held (usually a default), while pointer, map,
// slice and interface fields are reset. yaml.v2 reset all of them, so configs
// that relied on "key:" meaning "empty" should write key: "" instead.
//
// Every missing variable is reported in one *MissingError, and out is not
// touched unless all references resolve. If decoding itself fails, out may be
// partly filled, as with yaml.Unmarshal. Decoding is not strict: unknown
// fields are ignored.
//
// Decoding uses go.yaml.in/yaml/v3. Custom unmarshalers written against
// gopkg.in/yaml.v3 (UnmarshalYAML(*yaml.Node) with that module's Node type)
// are a different type and are not called; implement them with
// go.yaml.in/yaml/v3, or use the UnmarshalYAML(func(any) error) form.
//
// Decode errors never carry expanded values. In a *yaml.TypeError, each entry
// on a line that holds an expanded value is replaced by one naming the config
// path instead of the value; other entries are kept. Any other decode error
// (for example from a TextUnmarshaler, which may quote its input in any form)
// is replaced by a *DecodeError when the document contains expanded values.
// A custom unmarshaler that builds its own *yaml.TypeError must report the
// line of the node whose value it echoes; an entry attributed to a parent
// node's line is kept as written.
//
// DecodeYAML is meant for trusted configuration files. Error messages list
// config paths, which for deeply nested documents with many missing
// references can be long; paths per variable and per line are capped.
func DecodeYAML(data []byte, out any, opts ...Option) (*Result, error) {
	o := buildOptions(opts)
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if doc.Kind == 0 {
		// Empty input: nothing to decode, same as yaml.Unmarshal.
		return newState().result(), nil
	}
	w := &walker{st: newState(), src: o.source, expandedAt: map[int][]string{}, anchored: map[*yaml.Node]bool{}}
	if err := w.node(&doc); err != nil {
		return nil, err
	}
	if err := w.st.err(); err != nil {
		return nil, err
	}
	if err := doc.Decode(out); err != nil {
		return nil, suppressExpanded(err, w.expandedAt)
	}
	return w.st.result(), nil
}

// DecodeError replaces a decode error whose message could not be checked for
// expanded values. The original error is deliberately not wrapped.
type DecodeError struct {
	// Paths lists config paths whose values were expanded, sorted and capped
	// at 20.
	Paths []string
}

func (e *DecodeError) Error() string {
	return "envref: decoding failed; the message is suppressed because it may contain an expanded value " +
		"(expanded at: " + strings.Join(e.Paths, ", ") + ")"
}

var errLine = regexp.MustCompile(`^line (\d+): `)

// suppressExpanded removes expanded values from a decode error by position
// rather than by searching the text: yaml quotes, truncates and escapes values
// in ways that string matching cannot reliably undo.
func suppressExpanded(err error, expandedAt map[int][]string) error {
	if len(expandedAt) == 0 {
		return err
	}
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		return &DecodeError{Paths: allPaths(expandedAt)}
	}
	entries := make([]string, len(te.Errors))
	for i, e := range te.Errors {
		m := errLine.FindStringSubmatch(e)
		if m == nil {
			entries[i] = "cannot decode a value; message suppressed because it may contain an expanded value"
			continue
		}
		line, _ := strconv.Atoi(m[1])
		paths, hit := expandedAt[line]
		if !hit {
			entries[i] = e
			continue
		}
		where := strings.Join(paths, ", ")
		if where == "" {
			where = "<root>"
		}
		entries[i] = fmt.Sprintf("line %d: cannot decode the expanded value at %s into the target type", line, where)
	}
	return &yaml.TypeError{Errors: entries}
}

// maxDecodeErrorPaths bounds DecodeError.Paths.
const maxDecodeErrorPaths = 20

func allPaths(expandedAt map[int][]string) []string {
	var paths []string
	for _, ps := range expandedAt {
		paths = append(paths, ps...)
	}
	sort.Strings(paths)
	if len(paths) > maxDecodeErrorPaths {
		paths = paths[:maxDecodeErrorPaths]
	}
	return paths
}

// maxPathsPerLine bounds what is kept per source line for error messages; a
// line rarely holds more than a few values, and the bound keeps a hostile
// one-line flow collection from growing the error without limit.
const maxPathsPerLine = 3

// walker expands scalar values in place. The config path is kept as a stack of
// segments and only joined when a value actually contains a reference, so
// large documents without references cost no more than the parse.
type walker struct {
	st  *state
	src Source
	seg []segment
	// expandedAt maps a source line to the paths of values expanded on it.
	expandedAt map[int][]string
	// anchored records anchored scalars visited in value position.
	anchored map[*yaml.Node]bool
}

type segment struct {
	key     string
	index   int
	isIndex bool
}

func (w *walker) path() string {
	var b strings.Builder
	for _, s := range w.seg {
		if s.isIndex {
			b.WriteByte('[')
			b.WriteString(strconv.Itoa(s.index))
			b.WriteByte(']')
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('.')
		}
		if s.key == "" {
			b.WriteString(`""`)
			continue
		}
		b.WriteString(s.key)
	}
	return b.String()
}

func (w *walker) node(n *yaml.Node) error {
	switch n.Kind {
	case yaml.DocumentNode:
		for _, c := range n.Content {
			if err := w.node(c); err != nil {
				return err
			}
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			w.seg = append(w.seg, segment{key: n.Content[i].Value})
			err := w.node(n.Content[i+1])
			w.seg = w.seg[:len(w.seg)-1]
			if err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for i, c := range n.Content {
			w.seg = append(w.seg, segment{index: i, isIndex: true})
			err := w.node(c)
			w.seg = w.seg[:len(w.seg)-1]
			if err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		return w.scalar(n)
	case yaml.AliasNode:
		// The anchored node is expanded where it is defined and every alias
		// sees the result. Keys are not expanded, so an alias to an anchored
		// key would carry a literal reference into a value: reject it. An
		// escaped "$${" in such a key is carried over unchanged, as keys are.
		if a := n.Alias; a != nil && a.Kind == yaml.ScalarNode && !w.anchored[a] && hasReference(a.Value) {
			return &SyntaxError{Path: w.path(), Reason: "alias to a mapping key containing a reference is not supported"}
		}
	}
	return nil
}

func (w *walker) scalar(n *yaml.Node) error {
	if n.Anchor != "" {
		w.anchored[n] = true
	}
	if !strings.Contains(n.Value, refOpen) {
		return nil
	}
	var cached string
	path := func() string {
		if cached == "" {
			cached = w.path()
		}
		return cached
	}
	out, substituted, err := w.st.expand(n.Value, path, w.src)
	if err != nil {
		return err
	}
	if !substituted {
		// Only escapes ($${) or references to missing variables: the text
		// holds no expanded value, but keep the unescaped form.
		n.Value = out
		return nil
	}
	n.Value = out
	if ps := w.expandedAt[n.Line]; len(ps) < maxPathsPerLine {
		w.expandedAt[n.Line] = append(ps, path())
	}
	if isPlain(n) {
		// Drop the tag the parser inferred from the "${...}" text so the
		// decoder re-infers it from the expanded value. An empty result can
		// only come from an empty default and stays null; any other value
		// that reads as null is a real value and stays a string.
		n.Tag = ""
		if out != "" && n.ShortTag() == nullTag {
			n.Tag = "!!str"
		}
	}
	return nil
}

// hasReference reports whether s contains a ${ that is not part of a $${ escape.
func hasReference(s string) bool {
	for i := 0; i < len(s); {
		switch rest := s[i:]; {
		case strings.HasPrefix(rest, refEscape):
			i += len(refEscape)
		case strings.HasPrefix(rest, refOpen):
			return true
		default:
			i++
		}
	}
	return false
}

// isPlain reports whether n was written without quotes, block style or an
// explicit tag, i.e. whether YAML would infer its type from its text.
func isPlain(n *yaml.Node) bool {
	const typed = yaml.TaggedStyle | yaml.DoubleQuotedStyle | yaml.SingleQuotedStyle |
		yaml.LiteralStyle | yaml.FoldedStyle
	return n.Style&typed == 0
}
