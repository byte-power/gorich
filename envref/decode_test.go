package envref

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func decode(t *testing.T, doc string, env map[string]string, out any) (*Result, error) {
	t.Helper()
	return DecodeYAML([]byte(doc), out, WithSource(Map(env)))
}

func TestDecodeYAMLRequiredDefaultEscape(t *testing.T) {
	var got struct {
		Required string `yaml:"required"`
		Default  string `yaml:"default"`
		Override string `yaml:"override"`
		Empty    string `yaml:"empty"`
		Escaped  string `yaml:"escaped"`
		Dollar   string `yaml:"dollar"`
		Mixed    string `yaml:"mixed"`
	}
	doc := `
required: "${A}"
default: "${UNSET:-fallback}"
override: "${A:-fallback}"
empty: "${UNSET:-}"
escaped: "$${A}"
dollar: "a$b$$c"
mixed: "pre-${A}-${B:-x}-post"
`
	if _, err := decode(t, doc, map[string]string{"A": "va"}, &got); err != nil {
		t.Fatal(err)
	}
	want := []string{"va", "fallback", "va", "", "${A}", "a$b$$c", "pre-va-x-post"}
	have := []string{got.Required, got.Default, got.Override, got.Empty, got.Escaped, got.Dollar, got.Mixed}
	if !reflect.DeepEqual(have, want) {
		t.Fatalf("got %q, want %q", have, want)
	}
}

func TestDecodeYAMLNoRecursiveExpansion(t *testing.T) {
	var got struct {
		Secret string `yaml:"secret"`
	}
	if _, err := decode(t, `secret: ${S}`, map[string]string{"S": "x${OTHER}y", "OTHER": "leak"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Secret != "x${OTHER}y" {
		t.Fatalf("substituted value was expanded again: %q", got.Secret)
	}
}

func TestDecodeYAMLPlainScalarRetyped(t *testing.T) {
	var got struct {
		Port    int               `yaml:"port"`
		Enabled bool              `yaml:"enabled"`
		Ratio   float64           `yaml:"ratio"`
		Name    string            `yaml:"name"`
		NullLik string            `yaml:"null_like"`
		Any     map[string]any    `yaml:"any"`
		Ptr     *int              `yaml:"ptr"`
		Strs    map[string]string `yaml:"strs"`
	}
	doc := `
port: ${PORT}
enabled: ${ENABLED}
ratio: ${RATIO}
name: ${PORT}
null_like: ${NULLISH}
any:
  n: ${PORT}
  b: ${ENABLED}
ptr: ${PORT}
strs:
  k: ${ENABLED}
`
	env := map[string]string{"PORT": "5432", "ENABLED": "true", "RATIO": "0.25", "NULLISH": "null"}
	if _, err := decode(t, doc, env, &got); err != nil {
		t.Fatal(err)
	}
	if got.Port != 5432 || !got.Enabled || got.Ratio != 0.25 {
		t.Fatalf("plain scalars not re-typed: %+v", got)
	}
	if got.Name != "5432" {
		t.Fatalf("int-looking value into string field: %q", got.Name)
	}
	if got.NullLik != "null" {
		t.Fatalf("null-looking secret must stay a string, got %q", got.NullLik)
	}
	if got.Any["n"] != 5432 || got.Any["b"] != true {
		t.Fatalf("interface values not re-typed: %#v", got.Any)
	}
	if got.Ptr == nil || *got.Ptr != 5432 {
		t.Fatalf("pointer field: %v", got.Ptr)
	}
	if got.Strs["k"] != "true" {
		t.Fatalf("bool-looking value into string map: %q", got.Strs["k"])
	}
}

func TestDecodeYAMLQuotedStaysString(t *testing.T) {
	var got struct {
		Double map[string]any `yaml:"double"`
		Single any            `yaml:"single"`
		Tagged any            `yaml:"tagged"`
		Block  any            `yaml:"block"`
	}
	doc := `
double: {v: "${PORT}"}
single: '${PORT}'
tagged: !!str ${PORT}
block: |-
  ${PORT}
`
	if _, err := decode(t, doc, map[string]string{"PORT": "5432"}, &got); err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]any{"double": got.Double["v"], "single": got.Single, "tagged": got.Tagged, "block": got.Block} {
		if v != "5432" {
			t.Errorf("%s: want string \"5432\", got %#v", name, v)
		}
	}

	var typed struct {
		Port int `yaml:"port"`
	}
	if _, err := decode(t, `port: "${PORT}"`, map[string]string{"PORT": "5432"}, &typed); err == nil {
		t.Fatal("quoted value must not silently become an int")
	}
}

func TestDecodeYAMLValueCannotChangeStructure(t *testing.T) {
	type cfg struct {
		A string `yaml:"a"`
		B string `yaml:"b"`
	}
	hostile := []string{
		"x\nb: injected",
		`x" ` + "\n" + `b: "injected`,
		"x # not a comment",
		"x: y",
		"{b: injected}",
		"- item",
		"&anchor *alias",
	}
	for _, v := range hostile {
		for _, doc := range []string{"a: ${V}\nb: keep\n", "a: \"${V}\"\nb: keep\n"} {
			var got cfg
			if _, err := decode(t, doc, map[string]string{"V": v}, &got); err != nil {
				t.Fatalf("value %q doc %q: %v", v, doc, err)
			}
			if got.A != v || got.B != "keep" {
				t.Fatalf("value %q changed structure: %+v", v, got)
			}
		}
	}
}

func TestDecodeYAMLKeysNotExpanded(t *testing.T) {
	var got map[string]string
	if _, err := decode(t, `"${K}": "${V}"`, map[string]string{"K": "key", "V": "val"}, &got); err != nil {
		t.Fatal(err)
	}
	if got["${K}"] != "val" || len(got) != 1 {
		t.Fatalf("keys must stay literal: %#v", got)
	}
}

func TestDecodeYAMLMissingVarsAllReportedOutUntouched(t *testing.T) {
	type cfg struct {
		Name string `yaml:"name"`
		DB   struct {
			User     string `yaml:"user"`
			Password string `yaml:"password"`
		} `yaml:"db"`
		Hosts []string `yaml:"hosts"`
	}
	got := cfg{Name: "before"}
	doc := `
name: decoded
db:
  user: ${DB_USER}
  password: "${DB_PASSWORD}"
hosts: ["${DB_USER}", "ok"]
`
	_, err := decode(t, doc, map[string]string{"DB_USER": ""}, &got)
	var me *MissingError
	if !errors.As(err, &me) {
		t.Fatalf("want *MissingError, got %v", err)
	}
	if !reflect.DeepEqual(me.Names, []string{"DB_PASSWORD", "DB_USER"}) {
		t.Fatalf("names: %v", me.Names)
	}
	if !reflect.DeepEqual(me.Paths["DB_USER"], []string{"db.user", "hosts[0]"}) {
		t.Fatalf("paths: %v", me.Paths)
	}
	if got.Name != "before" {
		t.Fatalf("out was written despite missing vars: %+v", got)
	}

	var syn struct {
		A string `yaml:"a"`
	}
	_, err = decode(t, "a: \"${BAD-NAME}\"", nil, &syn)
	var se *SyntaxError
	if !errors.As(err, &se) || se.Path != "a" {
		t.Fatalf("want *SyntaxError at a, got %v", err)
	}
}

func TestDecodeYAMLSequencesAndNested(t *testing.T) {
	type upstream struct {
		Name     string `yaml:"name"`
		Endpoint string `yaml:"endpoint"`
		Port     int    `yaml:"port"`
	}
	var got struct {
		Upstreams []upstream                `yaml:"upstreams"`
		Matrix    [][]string                `yaml:"matrix"`
		Named     map[string]upstream       `yaml:"named"`
		Deep      map[string]map[string]any `yaml:"deep"`
	}
	doc := `
upstreams:
  - name: user
    endpoint: "${U}"
    port: ${P}
  - name: pay
    endpoint: "${V}"
    port: 81
matrix: [["${U}"], ["${V}", x]]
named:
  a: {endpoint: "${U}"}
deep:
  l1: {l2: "${V}"}
`
	env := map[string]string{"U": "u.svc", "V": "v.svc", "P": "80"}
	if _, err := decode(t, doc, env, &got); err != nil {
		t.Fatal(err)
	}
	if got.Upstreams[0].Endpoint != "u.svc" || got.Upstreams[0].Port != 80 || got.Upstreams[1].Endpoint != "v.svc" {
		t.Fatalf("upstreams: %+v", got.Upstreams)
	}
	if got.Matrix[0][0] != "u.svc" || got.Matrix[1][0] != "v.svc" || got.Named["a"].Endpoint != "u.svc" || got.Deep["l1"]["l2"] != "v.svc" {
		t.Fatalf("nested: %+v", got)
	}
}

func TestDecodeYAMLAnchorsAliases(t *testing.T) {
	type db struct {
		Host     string `yaml:"host"`
		Password string `yaml:"password"`
	}
	var got struct {
		Base    db `yaml:"base"`
		Primary db `yaml:"primary"`
		Replica db `yaml:"replica"`
	}
	doc := `
base: &base
  host: h
  password: "${PW}"
primary: *base
replica:
  <<: *base
  host: r
`
	res, err := decode(t, doc, map[string]string{"PW": "s3cret"}, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got.Primary.Password != "s3cret" || got.Replica.Password != "s3cret" || got.Replica.Host != "r" {
		t.Fatalf("aliases did not see the expansion: %+v", got)
	}
	if !reflect.DeepEqual(res.Referenced, []string{"PW"}) {
		t.Fatalf("referenced: %v", res.Referenced)
	}
}

func TestDecodeYAMLReferencedNames(t *testing.T) {
	var got map[string]any
	res, err := decode(t, "a: ${B}\nc: ${A:-x}\nd: ${B}\ne: plain\n", map[string]string{"B": "1"}, &got)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.Referenced, []string{"A", "B"}) || !res.Contains("A") || res.Contains("e") {
		t.Fatalf("referenced: %v", res.Referenced)
	}

	res, err = decode(t, "a: 1\n", nil, &got)
	if err != nil || len(res.Referenced) != 0 {
		t.Fatalf("no references: %v %v", res, err)
	}
}

func TestDecodeYAMLUnknownFieldsIgnored(t *testing.T) {
	var got struct {
		Known string `yaml:"known"`
	}
	if _, err := decode(t, "known: ${K}\nunknown: ${U:-x}\n", map[string]string{"K": "k"}, &got); err != nil {
		t.Fatalf("unknown fields must be ignored: %v", err)
	}
	if got.Known != "k" {
		t.Fatalf("known: %q", got.Known)
	}
}

func TestDecodeYAMLFlowStyleNeedsQuotes(t *testing.T) {
	var got map[string]any
	if _, err := decode(t, "a: {p: ${P}}", map[string]string{"P": "1"}, &got); err == nil {
		t.Fatal("unquoted ${} inside a flow mapping is invalid YAML and must fail to parse")
	}
	if _, err := decode(t, `a: {p: "${P}"}`, map[string]string{"P": "1"}, &got); err != nil {
		t.Fatalf("quoted form must work: %v", err)
	}
}

// Locks down yaml.v3 null semantics: a key with no value keeps existing scalars and
// structs, but zeroes pointers, maps and slices. Clearing a string requires "".
func TestDecodeYAMLNullSemantics(t *testing.T) {
	type sub struct {
		X string `yaml:"x"`
	}
	got := struct {
		S     string         `yaml:"s"`
		I     int            `yaml:"i"`
		St    sub            `yaml:"st"`
		P     *sub           `yaml:"p"`
		M     map[string]int `yaml:"m"`
		Empty string         `yaml:"empty"`
	}{S: ":8089", I: 5, St: sub{X: "5m"}, P: &sub{X: "p"}, M: map[string]int{"a": 1}, Empty: "default"}
	doc := "s:\ni: ~\nst:\np:\nm: null\nempty: \"\"\n"
	if _, err := decode(t, doc, nil, &got); err != nil {
		t.Fatal(err)
	}
	if got.S != ":8089" || got.I != 5 || got.St.X != "5m" {
		t.Fatalf("scalars/structs must keep existing values on null: %+v", got)
	}
	if got.P != nil || got.M != nil {
		t.Fatalf("pointers/maps are reset on null: %+v", got)
	}
	if got.Empty != "" {
		t.Fatalf(`explicit "" must clear: %q`, got.Empty)
	}
}

func TestDecodeYAMLEdgeInputs(t *testing.T) {
	var got map[string]any
	if _, err := decode(t, "", nil, &got); err != nil || got != nil {
		t.Fatalf("empty input: %v %v", got, err)
	}
	if _, err := decode(t, "a: [unclosed", nil, &got); err == nil {
		t.Fatal("parse error expected")
	}
	if _, err := decode(t, "a: ${A}\n---\nb: ${B}\n", map[string]string{"A": "1"}, &got); err != nil || got["a"] != 1 {
		t.Fatalf("only the first document is decoded: %v %v", got, err)
	}
	var wrong struct {
		A int `yaml:"a"`
	}
	_, err := decode(t, "a: ${A}", map[string]string{"A": "s3cret-not-int"}, &wrong)
	if err == nil {
		t.Fatal("type mismatch after expansion must fail decoding")
	}
	if strings.Contains(err.Error(), "s3cret") || !strings.Contains(err.Error(), "expanded value at a ") {
		t.Fatalf("decode error must not echo expanded values: %v", err)
	}
}
