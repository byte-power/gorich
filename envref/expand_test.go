package envref

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpandString(t *testing.T) {
	src := Map(map[string]string{
		"DB_PASSWORD": `p@ss:w/rd#1"x'=`,
		"USER":        "app_rw",
		"EMPTY":       "",
		"NESTED":      "${USER}",
	})
	cases := []struct {
		name, in, want string
	}{
		{"no reference", "postgres://h:5432/db", "postgres://h:5432/db"},
		{"lone dollar kept", "a$b", "a$b"},
		{"double dollar kept", "a$$b", "a$$b"},
		{"dollar name without braces kept", "$USER", "$USER"},
		{"escape", "$${USER}", "${USER}"},
		{"whole value", "${DB_PASSWORD}", `p@ss:w/rd#1"x'=`},
		{"inside string", "postgres://${USER}@h/db", "postgres://app_rw@h/db"},
		{"several", "${USER}:${USER}", "app_rw:app_rw"},
		{"default used when unset", "${NOPE:-fallback}", "fallback"},
		{"default used when empty", "${EMPTY:-fallback}", "fallback"},
		{"empty default", "x${NOPE:-}y", "xy"},
		{"default ignored when set", "${USER:-other}", "app_rw"},
		{"substituted value not re-expanded", "${NESTED}", "${USER}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := ExpandString(tc.in, WithSource(src))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestExpandStringMissing(t *testing.T) {
	src := Map(map[string]string{"EMPTY": ""})
	_, _, err := ExpandString("${A}/${EMPTY}/${A}/${B:-ok}", WithSource(src))
	var missing *MissingError
	require.True(t, errors.As(err, &missing), "err = %v", err)
	assert.Equal(t, []string{"A", "EMPTY"}, missing.Names)
}

func TestExpandStringSyntaxErrors(t *testing.T) {
	for _, in := range []string{"${}", "${1A}", "${A-B}", "${A.B}", "${A", "x${A:-b", "${A:-${B}}", "${A:-x${B}y}"} {
		t.Run(in, func(t *testing.T) {
			_, _, err := ExpandString(in, WithSource(Map(nil)))
			var syn *SyntaxError
			assert.True(t, errors.As(err, &syn), "in=%q err=%v", in, err)
		})
	}
}

func TestErrorsNeverContainValues(t *testing.T) {
	const secret = "s3cr3t-value"
	var c struct {
		A string `yaml:"a"`
		B string `yaml:"b"`
	}
	_, err := DecodeYAML([]byte("a: ${SET}\nb: ${UNSET}\n"), &c, WithSource(Map(map[string]string{"SET": secret})))
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secret)
	assert.Contains(t, err.Error(), "UNSET (at b)")
}

func TestResultReferenced(t *testing.T) {
	_, res, err := ExpandString("${B}${A}${B}${C:-x}", WithSource(Map(map[string]string{"A": "1", "B": "2"})))
	require.NoError(t, err)
	assert.Equal(t, []string{"A", "B", "C"}, res.Referenced)
	assert.True(t, res.Contains("A"))
	assert.False(t, res.Contains("D"))
}

func TestChain(t *testing.T) {
	src := Chain(Map(map[string]string{"A": "", "B": "first"}), Map(map[string]string{"A": "second", "B": "x"}))
	got, _, err := ExpandString("${A}-${B}", WithSource(src))
	require.NoError(t, err)
	assert.Equal(t, "second-first", got)
}

func TestEnvSource(t *testing.T) {
	t.Setenv("ENVREF_TEST_VALUE", "a=b=c")
	got, _, err := ExpandString("${ENVREF_TEST_VALUE}")
	require.NoError(t, err)
	assert.Equal(t, "a=b=c", got)
}
