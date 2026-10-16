package envref

import (
	"errors"
	"math/big"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// assertNoLeak fails if any 5-byte piece of a secret shows up in msg (shorter
// pieces would collide with ordinary words and config paths in the message).
// Secrets shorter than 5 bytes cannot be checked this way and are rejected so
// the assertion never passes vacuously.
func assertNoLeak(t *testing.T, msg string, secrets ...string) {
	t.Helper()
	for _, s := range secrets {
		if len(s) < 5 {
			t.Fatalf("assertNoLeak needs secrets of at least 5 bytes, got %d", len(s))
		}
		for i := 0; i+5 <= len(s); i++ {
			if piece := s[i : i+5]; strings.Contains(msg, piece) {
				t.Fatalf("error leaks part %q of a secret: %s", piece, msg)
			}
		}
	}
}

func decodeErr(t *testing.T, doc string, env map[string]string, out any) error {
	t.Helper()
	_, err := DecodeYAML([]byte(doc), out, WithSource(Map(env)))
	require.Error(t, err)
	return err
}

func TestDecodeErrorBacktickInSecret(t *testing.T) {
	var c struct {
		A int `yaml:"a"`
	}
	for _, s := range []string{"ab`cdefgh", "abc`defGHIJKLMNOPQRS", "`", "x`"} {
		err := decodeErr(t, "a: ${S}\n", map[string]string{"S": s}, &c)
		if parts := strings.ReplaceAll(s, "`", ""); len(parts) >= 5 {
			assertNoLeak(t, err.Error(), parts)
		}
		// short values are checked structurally: the entry must be the replaced one
		assert.Contains(t, err.Error(), "line 1: cannot decode the expanded value at a into")
		assert.Contains(t, err.Error(), "line 1")
		assert.Contains(t, err.Error(), " a ")
	}
}

func TestDecodeErrorBacktickInEarlierMessage(t *testing.T) {
	var c struct {
		A int `yaml:"a"`
		B int `yaml:"b"`
	}
	const secret = "TOPSECRET-123456"
	err := decodeErr(t, "a: x`y\nb: ${S}\n", map[string]string{"S": secret}, &c)
	assertNoLeak(t, err.Error(), secret)
	// the unrelated plain-text error stays readable
	assert.Contains(t, err.Error(), "line 1")
	var te *yaml.TypeError
	require.True(t, errors.As(err, &te), "type errors stay *yaml.TypeError: %T", err)
	assert.Len(t, te.Errors, 2)
}

func TestDecodeErrorTextUnmarshalerEchoesValue(t *testing.T) {
	cases := []struct {
		name   string
		secret string
		out    any
	}{
		{"netip", `hunter"22\x`, &struct {
			V netip.Addr `yaml:"v"`
		}{}},
		{"big.Int", `hunter"22`, &struct {
			V *big.Int `yaml:"v"`
		}{}},
		{"time", "pässwörd-1", &struct {
			V time.Time `yaml:"v"`
		}{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := decodeErr(t, "v: \"${S}\"\n", map[string]string{"S": tc.secret}, tc.out)
			assertNoLeak(t, err.Error(), tc.secret, `hunter\"22`, `p\xc3\xa4ssw`)
		})
	}
}

func TestDecodeErrorSubstringSecrets(t *testing.T) {
	var c struct {
		A  string     `yaml:"a"`
		IP netip.Addr `yaml:"ip"`
	}
	env := map[string]string{"PW": "hunter22", "PEPPER": "pepperXYZ"}
	for i := 0; i < 50; i++ { // the old masking depended on map order
		err := decodeErr(t, "a: \"${PW}\"\nip: \"${PW}${PEPPER}\"\n", env, &c)
		assertNoLeak(t, err.Error(), "hunter22", "pepperXYZ")
	}
}

// Errors that do not involve an expanded value keep their original message.
func TestDecodeErrorWithoutExpansionUnchanged(t *testing.T) {
	var c struct {
		A int `yaml:"a"`
	}
	_, want := DecodeYAML([]byte("a: notanint\n"), &c, WithSource(Map(nil)))
	require.Error(t, want)
	assert.Contains(t, want.Error(), "notanint")
}

// An error on an alias is reported at the anchored node's line, which is where
// the expansion happened, so it must be suppressed too.
func TestDecodeErrorThroughAlias(t *testing.T) {
	var c struct {
		Base string `yaml:"base"`
		N    int    `yaml:"n"`
	}
	const secret = "Qz7-aliased-s3cr3t-Wk9"
	err := decodeErr(t, "base: &s ${S}\nn: *s\n", map[string]string{"S": secret}, &c)
	assertNoLeak(t, err.Error(), secret)
}

type dsnPart string

// UnmarshalText echoes only a piece of its input, the way custom parsers often
// do, so masking the whole expanded value would not catch it.
func (d *dsnPart) UnmarshalText(b []byte) error {
	user, pass, _ := strings.Cut(string(b), ":")
	if len(pass) > 0 {
		return errors.New(`bad password part "` + pass + `" for user ` + user)
	}
	*d = dsnPart(b)
	return nil
}

func TestDecodeErrorCustomUnmarshalerEchoesPiece(t *testing.T) {
	var c struct {
		D dsnPart `yaml:"dsn"`
	}
	err := decodeErr(t, "dsn: \"${U}:${P}\"\n", map[string]string{"U": "admin", "P": "S3cretPw99"}, &c)
	assertNoLeak(t, err.Error(), "S3cretPw99")
	var de *DecodeError
	require.True(t, errors.As(err, &de), "err = %T %v", err, err)
	assert.Equal(t, []string{"dsn"}, de.Paths)
}

func TestEmptyDefault(t *testing.T) {
	c := struct {
		N int    `yaml:"n"`
		S string `yaml:"s"`
		Q string `yaml:"q"`
	}{N: 7, S: "keep", Q: "replaced"}
	_, err := DecodeYAML([]byte("n: ${N:-}\ns: ${S:-}\nq: \"${Q:-}\"\n"), &c, WithSource(Map(nil)))
	require.NoError(t, err)
	assert.Equal(t, 7, c.N, "plain empty default is null: int keeps its default")
	assert.Equal(t, "keep", c.S, "plain empty default is null: string keeps its default")
	assert.Equal(t, "", c.Q, "quoted empty default is an empty string")
}

// A secret whose text equals the reference itself still counts as expanded.
func TestDecodeErrorSecretEqualsReferenceText(t *testing.T) {
	var c struct {
		A int `yaml:"a"`
	}
	err := decodeErr(t, "a: ${S}\n", map[string]string{"S": "${S}"}, &c)
	assert.NotContains(t, err.Error(), "`${S}`")
	assert.Contains(t, err.Error(), "expanded value at a ")
}

func TestMissingPathsCappedAndEmptyKey(t *testing.T) {
	var b strings.Builder
	b.WriteString("\"\": \"${GONE}\"\nl:\n")
	for i := 0; i < 50; i++ {
		b.WriteString("  - \"${GONE}\"\n")
	}
	var c map[string]any
	_, err := DecodeYAML([]byte(b.String()), &c, WithSource(Map(nil)))
	var missing *MissingError
	require.True(t, errors.As(err, &missing), "err = %v", err)
	assert.Len(t, missing.Paths["GONE"], 10)
	assert.Contains(t, missing.Paths["GONE"], `""`, "empty key is shown as \"\", not as an index")
}

func TestDeepManyMissingStaysFast(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 2000; i++ {
		b.WriteString("{abcdefghij: ")
	}
	b.WriteString("[")
	for i := 0; i < 20000; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"${MISS}"`)
	}
	b.WriteString("]")
	for i := 0; i < 2000; i++ {
		b.WriteString("}")
	}
	var c any
	start := time.Now()
	_, err := DecodeYAML([]byte(b.String()), &c, WithSource(Map(nil)))
	var missing *MissingError
	require.True(t, errors.As(err, &missing))
	assert.Less(t, len(err.Error()), 300_000, "error size must stay bounded")
	// coarse guard against quadratic blowup (it took minutes before); generous for -race/-cover
	assert.Less(t, time.Since(start), 20*time.Second)
}

func TestAliasToAnchoredKeyWithEscapeAllowed(t *testing.T) {
	var c map[string]string
	_, err := DecodeYAML([]byte("? &k \"$${LITERAL}\"\n: v\na: *k\n"), &c, WithSource(Map(nil)))
	require.NoError(t, err)
	assert.Equal(t, "$${LITERAL}", c["a"], "keys are not expanded, so the alias carries the key text as is")
}

// A nil source must fail closed, not fall back to the process environment.
func TestWithSourceNil(t *testing.T) {
	t.Setenv("ENVREF_NIL_SOURCE", "from-env")
	_, _, err := ExpandString("${ENVREF_NIL_SOURCE}", WithSource(nil))
	var missing *MissingError
	require.True(t, errors.As(err, &missing), "err = %v", err)
	assert.Equal(t, []string{"ENVREF_NIL_SOURCE"}, missing.Names)
}

func TestDecodeErrorRootScalar(t *testing.T) {
	var n int
	err := decodeErr(t, "${S}\n", map[string]string{"S": "not-a-number-xyz"}, &n)
	assert.Contains(t, err.Error(), "expanded value at <root> into")
	assertNoLeak(t, err.Error(), "not-a-number-xyz")
}

// A reference inside an anchored mapping key, reused as a value through an
// alias, must not slip through as the literal "${X}".
func TestAliasToAnchoredKeyRejected(t *testing.T) {
	var c map[string]string
	_, err := DecodeYAML([]byte("? &k \"${X}\"\n: v\na: *k\n"), &c, WithSource(Map(map[string]string{"X": "x"})))
	var syn *SyntaxError
	require.True(t, errors.As(err, &syn), "err = %v", err)
}
