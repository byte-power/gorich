package envref

import "os"

// Source supplies the value of a referenced variable.
//
// A value that is not found, or found but empty, is treated as missing by the
// expander; sources do not need to special-case empty values.
type Source interface {
	Lookup(name string) (string, bool)
}

// SourceFunc adapts a plain function to Source.
type SourceFunc func(name string) (string, bool)

// Lookup calls f(name).
func (f SourceFunc) Lookup(name string) (string, bool) { return f(name) }

// Env returns a Source backed by the process environment.
func Env() Source { return SourceFunc(os.LookupEnv) }

// Map returns a Source backed by a fixed map, mainly for tests.
func Map(values map[string]string) Source {
	return SourceFunc(func(name string) (string, bool) {
		v, ok := values[name]
		return v, ok
	})
}

// Chain returns a Source that asks each source in order and returns the first
// non-empty value. It lets a later migration add, say, a mounted-secret
// directory behind the environment without touching config files.
func Chain(sources ...Source) Source {
	return SourceFunc(func(name string) (string, bool) {
		for _, s := range sources {
			if v, ok := s.Lookup(name); ok && v != "" {
				return v, true
			}
		}
		return "", false
	})
}
