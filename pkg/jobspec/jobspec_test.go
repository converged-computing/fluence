package jobspec

import (
	"reflect"
	"strings"
	"testing"
)

const sampleYAML = `version: 9999
resources:
  - type: slot
    count: 1
    label: default
    with:
      - type: qpu
        count: 1
attributes:
tasks:
  - command: ["sampler"]
    slot: default
    count:
      per_slot: 1
`

func TestLoadYAMLFields(t *testing.T) {
	js, err := Load(sampleYAML)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if js.Version != 9999 {
		t.Errorf("version = %d, want 9999", js.Version)
	}
	if len(js.Resources) != 1 || js.Resources[0].Type != "slot" {
		t.Fatalf("resources = %+v", js.Resources)
	}
	with := js.Resources[0].With
	if len(with) != 1 || with[0].Type != "qpu" || with[0].Count != 1 {
		t.Fatalf("nested resource = %+v", with)
	}
	if len(js.Tasks) != 1 || js.Tasks[0].Count["per_slot"] != 1 {
		t.Fatalf("tasks = %+v", js.Tasks)
	}
}

func TestLoadJSON(t *testing.T) {
	const j = `{"version":9999,"resources":[{"type":"qpu","count":1}],"attributes":null,"tasks":[{"command":["sampler"]}]}`
	js, err := Load(j)
	if err != nil {
		t.Fatalf("Load JSON: %v", err)
	}
	if js.Version != 9999 || js.Resources[0].Type != "qpu" {
		t.Fatalf("parsed = %+v", js)
	}
}

func TestRoundTripYAML(t *testing.T) {
	js, err := Load(sampleYAML)
	if err != nil {
		t.Fatal(err)
	}
	out, err := js.YAML()
	if err != nil {
		t.Fatal(err)
	}
	reparsed, err := Load(out)
	if err != nil {
		t.Fatalf("reparse produced YAML: %v", err)
	}
	if !reflect.DeepEqual(js, reparsed) {
		t.Fatalf("round trip changed the jobspec:\n%+v\nvs\n%+v", js, reparsed)
	}
}

func TestYAMLToJSONToYAMLPreservesData(t *testing.T) {
	fromYAML, err := Load(sampleYAML)
	if err != nil {
		t.Fatal(err)
	}
	asJSON, err := fromYAML.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(asJSON, "\"version\": 9999") {
		t.Fatalf("json output missing version:\n%s", asJSON)
	}
	fromJSON, err := Load(asJSON)
	if err != nil {
		t.Fatalf("reparse JSON: %v", err)
	}
	if !reflect.DeepEqual(fromYAML, fromJSON) {
		t.Fatalf("yaml->json->struct differs from yaml->struct:\n%+v\nvs\n%+v", fromYAML, fromJSON)
	}
}
