package envref

import (
	"fmt"
	"sort"
	"strings"
)

// Result reports what an expansion touched.
type Result struct {
	// Referenced lists every variable name that appeared in a reference,
	// sorted and de-duplicated. Callers use it to keep those variables out of
	// places that must not see secrets, such as a script sandbox or a debug
	// dump of the environment.
	Referenced []string
}

// Contains reports whether name was referenced.
func (r *Result) Contains(name string) bool {
	if r == nil {
		return false
	}
	i := sort.SearchStrings(r.Referenced, name)
	return i < len(r.Referenced) && r.Referenced[i] == name
}

// MissingError lists required variables that were unset or empty.
type MissingError struct {
	// Names is sorted and de-duplicated.
	Names []string
	// Paths maps each name to the config paths that reference it, sorted and
	// capped at 10 per name.
	Paths map[string][]string
}

func (e *MissingError) Error() string {
	parts := make([]string, 0, len(e.Names))
	for _, name := range e.Names {
		paths := e.Paths[name]
		if len(paths) == 0 {
			parts = append(parts, name)
			continue
		}
		parts = append(parts, fmt.Sprintf("%s (at %s)", name, strings.Join(paths, ", ")))
	}
	return "envref: required variables not set or empty: " + strings.Join(parts, "; ")
}

// SyntaxError reports a malformed reference. It carries the config path, not
// the offending value.
type SyntaxError struct {
	Path   string
	Reason string
}

func (e *SyntaxError) Error() string {
	if e.Path == "" {
		return "envref: " + e.Reason
	}
	return fmt.Sprintf("envref: %s at %s", e.Reason, e.Path)
}

// maxPathsPerName bounds how many config paths are kept for one missing
// variable, so a variable referenced thousands of times cannot blow up the
// error message.
const maxPathsPerName = 10

type state struct {
	referenced map[string]struct{}
	missingAt  map[string]map[string]struct{}
}

func newState() *state {
	return &state{referenced: map[string]struct{}{}, missingAt: map[string]map[string]struct{}{}}
}

func (st *state) reference(name string) { st.referenced[name] = struct{}{} }

func (st *state) missing(name string, path func() string) {
	paths, ok := st.missingAt[name]
	if !ok {
		paths = map[string]struct{}{}
		st.missingAt[name] = paths
	}
	if len(paths) < maxPathsPerName {
		if p := path(); p != "" {
			paths[p] = struct{}{}
		}
	}
}

func (st *state) err() error {
	if len(st.missingAt) == 0 {
		return nil
	}
	e := &MissingError{Names: sortedKeys(st.missingAt), Paths: make(map[string][]string, len(st.missingAt))}
	for name, paths := range st.missingAt {
		if len(paths) > 0 {
			e.Paths[name] = sortedKeys(paths)
		}
	}
	return e
}

func (st *state) result() *Result {
	return &Result{Referenced: sortedKeys(st.referenced)}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
