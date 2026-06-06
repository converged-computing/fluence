// Package jgf builds a Fluxion JSON Graph Format (JGF) resource graph
// programmatically. The fluxion-quantum POC used hand-written JGF files; a
// scheduler has to generate the graph at runtime from the live cluster, so this
// package provides a builder that assigns vertex ids, uniq ids, and containment
// paths automatically.
package jgf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

const containment = "containment"

// Vertex is a handle returned by the builder. Callers use it to attach children
// and edges; they do not set ids or paths themselves.
type Vertex struct {
	key       string // graph node id (stringified uniq id)
	path      string // containment path, e.g. /cluster0/node0/core
	typ       string
	basename  string
	name      string
	id        int64 // per-basename index
	uniqID    int64 // global index
	size      int64
	unit      string
	exclusive bool
	props     map[string]any
}

// Name is the vertex name (basename + per-type index, or an explicit name).
func (v *Vertex) Name() string { return v.name }

// Builder accumulates vertices and containment edges.
type Builder struct {
	vertices []*Vertex
	edges    [][2]string // source key, target key (containment)
	nextUniq int64
	typeIdx  map[string]int64
}

// NewBuilder returns an empty builder.
func NewBuilder() *Builder {
	return &Builder{typeIdx: map[string]int64{}}
}

// Options controls how a single vertex is created.
type Options struct {
	// Name overrides the default basename+index name (used for quantum
	// backends, where the name is the QRMI backend id).
	Name string
	// Size is the resource count at this vertex (e.g. cores per node).
	Size int64
	// Unit labels the size (e.g. "MB" for memory). Empty for countables.
	Unit string
	// Exclusive marks the vertex as exclusively allocated.
	Exclusive bool
	// Properties are extra metadata fields merged into the vertex (e.g.
	// num_qubits, vendor). They are emitted alongside the standard fields.
	Properties map[string]any
}

// AddRoot creates a top-level vertex (typically the cluster).
func (b *Builder) AddRoot(typ, basename string, opts Options) *Vertex {
	return b.add(nil, typ, basename, opts)
}

// AddChild creates a vertex contained by parent (adds a containment edge).
func (b *Builder) AddChild(parent *Vertex, typ, basename string, opts Options) *Vertex {
	v := b.add(parent, typ, basename, opts)
	b.edges = append(b.edges, [2]string{parent.key, v.key})
	return v
}

func (b *Builder) add(parent *Vertex, typ, basename string, opts Options) *Vertex {
	idx := b.typeIdx[basename]
	b.typeIdx[basename] = idx + 1

	name := opts.Name
	if name == "" {
		name = fmt.Sprintf("%s%d", basename, idx)
	}
	size := opts.Size
	if size == 0 {
		size = 1
	}
	v := &Vertex{
		key:       fmt.Sprintf("%d", b.nextUniq),
		typ:       typ,
		basename:  basename,
		name:      name,
		id:        idx,
		uniqID:    b.nextUniq,
		size:      size,
		unit:      opts.Unit,
		exclusive: opts.Exclusive,
		props:     opts.Properties,
	}
	if parent == nil {
		v.path = "/" + name
	} else {
		v.path = parent.path + "/" + name
	}
	b.nextUniq++
	b.vertices = append(b.vertices, v)
	return v
}

// node is the JGF wire form of a vertex.
type node struct {
	ID       string   `json:"id"`
	Metadata metadata `json:"metadata"`
}

type metadata struct {
	v *Vertex
}

// MarshalJSON emits the standard Fluxion metadata fields and flattens any extra
// properties into the same object.
func (m metadata) MarshalJSON() ([]byte, error) {
	out := map[string]any{
		"type":      m.v.typ,
		"basename":  m.v.basename,
		"name":      m.v.name,
		"id":        m.v.id,
		"uniq_id":   m.v.uniqID,
		"rank":      -1,
		"exclusive": m.v.exclusive,
		"unit":      m.v.unit,
		"size":      m.v.size,
		"paths":     map[string]string{containment: m.v.path},
	}
	for k, val := range m.v.props {
		// Do not allow properties to clobber required fields.
		if _, reserved := out[k]; !reserved {
			out[k] = val
		}
	}
	// Stable key order for deterministic output.
	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		vb, err := json.Marshal(out[k])
		if err != nil {
			return nil, err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

type edge struct {
	Source   string       `json:"source"`
	Target   string       `json:"target"`
	Metadata edgeMetadata `json:"metadata"`
}

type edgeMetadata struct {
	Subsystem string `json:"subsystem"`
}

type graph struct {
	Nodes []node `json:"nodes"`
	Edges []edge `json:"edges"`
}

// Doc is a complete JGF document.
type Doc struct {
	Graph graph `json:"graph"`
}

// Doc assembles the accumulated vertices and edges into a JGF document.
func (b *Builder) Doc() Doc {
	d := Doc{}
	for _, v := range b.vertices {
		d.Graph.Nodes = append(d.Graph.Nodes, node{ID: v.key, Metadata: metadata{v: v}})
	}
	for _, e := range b.edges {
		d.Graph.Edges = append(d.Graph.Edges, edge{
			Source:   e[0],
			Target:   e[1],
			Metadata: edgeMetadata{Subsystem: containment},
		})
	}
	return d
}

// JSON renders the document as indented JGF.
func (b *Builder) JSON() ([]byte, error) {
	return json.MarshalIndent(b.Doc(), "", "  ")
}
