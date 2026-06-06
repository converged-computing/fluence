// Package jobspec loads a Flux jobspec from a YAML or JSON string into a typed
// value and renders it back out as either format. It exists so callers can
// accept a spec in whichever form they have and hand Fluxion (or anything else)
// the form it wants.
//
//	js, err := jobspec.Load(someString)   // YAML or JSON, auto-detected
//	y, err  := js.YAML()                  // render as YAML
//	j, err  := js.JSON()                  // render as JSON
//
// One set of `json` struct tags drives both formats: sigs.k8s.io/yaml maps YAML
// onto those tags (and JSON is a subset of YAML), and encoding/json uses them
// directly.
package jobspec

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"sigs.k8s.io/yaml"
)

// Jobspec is a Flux jobspec (version 1). Counts are modeled as integers, which
// covers the common case; resource "count" ranges ({min,max,...}) are not
// modeled. Attributes is kept generic so nothing is dropped on round-trip.
type Jobspec struct {
	Version    int                    `json:"version"`
	Resources  []Resource             `json:"resources"`
	Attributes map[string]interface{} `json:"attributes"`
	Tasks      []Task                 `json:"tasks"`
}

// Resource is one entry in the (possibly nested) resource request.
type Resource struct {
	Type      string     `json:"type"`
	Count     int        `json:"count,omitempty"`
	Label     string     `json:"label,omitempty"`
	Exclusive *bool      `json:"exclusive,omitempty"`
	With      []Resource `json:"with,omitempty"`
}

// Task is one entry in the tasks list. Count holds keys like "per_slot" or
// "total".
type Task struct {
	Command []string       `json:"command"`
	Slot    string         `json:"slot,omitempty"`
	Count   map[string]int `json:"count,omitempty"`
}

// Load parses a jobspec from a YAML or JSON string. JSON is a subset of YAML,
// so the YAML decoder accepts both; detection is only used to label errors.
func Load(data string) (*Jobspec, error) {
	var js Jobspec
	if err := yaml.Unmarshal([]byte(data), &js); err != nil {
		return nil, fmt.Errorf("parse %s jobspec: %w", detectFormat(data), err)
	}
	return &js, nil
}

// LoadFile reads and parses a jobspec file (YAML or JSON).
func LoadFile(path string) (*Jobspec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read jobspec %s: %w", path, err)
	}
	return Load(string(b))
}

// YAML renders the jobspec as a YAML string.
func (j *Jobspec) YAML() (string, error) {
	b, err := yaml.Marshal(j)
	if err != nil {
		return "", fmt.Errorf("render jobspec as yaml: %w", err)
	}
	return string(b), nil
}

// JSON renders the jobspec as an indented JSON string.
func (j *Jobspec) JSON() (string, error) {
	b, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return "", fmt.Errorf("render jobspec as json: %w", err)
	}
	return string(b), nil
}

// detectFormat is a best-effort label ("json" or "yaml") for error messages.
func detectFormat(s string) string {
	t := strings.TrimSpace(s)
	if strings.HasPrefix(t, "{") || strings.HasPrefix(t, "[") {
		return "json"
	}
	return "yaml"
}
