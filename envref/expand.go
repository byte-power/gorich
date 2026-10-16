// Package envref expands ${VAR} references in configuration values, so secrets
// can live outside config files (for example injected by Kubernetes envFrom
// from a Secret synced out of AWS Secrets Manager) while the config file keeps
// describing the structure. DecodeYAML is the entry point for YAML files.
//
// Syntax, a strict subset of docker compose interpolation:
//
//	${NAME}          required; unset or empty is an error
//	${NAME:-value}   optional; unset or empty falls back to value (may be
//	                 empty, may not contain '}' or another reference)
//	$${              literal "${"
//
// Any other "$" is left untouched, so existing values such as "a$b" or "$$"
// keep their meaning. NAME must match [A-Za-z_][A-Za-z0-9_]*.
//
// Differences from compose are deliberate: a missing required variable fails
// instead of silently becoming "", and substituted values are never expanded
// again, so a secret that happens to contain "${...}" is used verbatim.
//
// Expansion runs on parsed values, never on raw file text, so values
// containing quotes, '#', ':' or newlines cannot alter the document structure.
package envref

import (
	"fmt"
	"strings"
)

const (
	refOpen    = "${"
	refEscape  = "$${"
	defaultSep = ":-"
)

type options struct {
	source Source
}

// Option configures expansion.
type Option func(*options)

// WithSource replaces the default Env() source. A nil source provides no
// variables at all (every reference is missing); it never falls back to the
// process environment, so a caller that meant a restricted source cannot leak
// the environment by accident.
func WithSource(s Source) Option {
	return func(o *options) {
		if s == nil {
			s = Map(nil)
		}
		o.source = s
	}
}

func buildOptions(opts []Option) options {
	o := options{source: Env()}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// ExpandString expands the references in s.
//
// On a missing required variable it returns a *MissingError; on a malformed
// reference it returns a *SyntaxError. Neither error ever contains a resolved
// value.
func ExpandString(s string, opts ...Option) (string, *Result, error) {
	o := buildOptions(opts)
	st := newState()
	out, _, err := st.expand(s, func() string { return "" }, o.source)
	if err != nil {
		return "", nil, err
	}
	if err := st.err(); err != nil {
		return "", nil, err
	}
	return out, st.result(), nil
}

// expand expands one string value. path returns where the value came from; it
// is only called when a path is actually recorded, because building it is
// proportional to the nesting depth. substituted reports whether any reference
// was replaced, which is not the same as the text changing: a secret may equal
// the reference text itself.
func (st *state) expand(s string, path func() string, src Source) (out string, substituted bool, err error) {
	if !strings.Contains(s, refOpen) {
		return s, false, nil
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		rest := s[i:]
		switch {
		case strings.HasPrefix(rest, refEscape):
			b.WriteString(refOpen)
			i += len(refEscape)
		case strings.HasPrefix(rest, refOpen):
			end := strings.IndexByte(rest, '}')
			if end < 0 {
				return "", false, &SyntaxError{Path: path(), Reason: "unterminated ${ reference"}
			}
			body := rest[len(refOpen):end]
			i += end + 1
			name, def, hasDefault := strings.Cut(body, defaultSep)
			if !validName(name) {
				return "", false, &SyntaxError{Path: path(), Reason: fmt.Sprintf("invalid variable name %q", name)}
			}
			// The default ends at the first '}', so "${A:-${B}}" would silently yield
			// the literal "${B}" followed by "}". Nesting is not supported: reject it.
			if strings.Contains(def, refOpen) {
				return "", false, &SyntaxError{Path: path(), Reason: fmt.Sprintf("nested reference in default value of %q is not supported", name)}
			}
			st.reference(name)
			value, ok := src.Lookup(name)
			if !ok || value == "" {
				if !hasDefault {
					st.missing(name, path)
					continue
				}
				value = def
			}
			b.WriteString(value)
			substituted = true
		default:
			b.WriteByte(s[i])
			i++
		}
	}
	return b.String(), substituted, nil
}

func validName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_', c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
