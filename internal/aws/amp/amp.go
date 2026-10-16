// Package amp is the data plane of Amazon Managed Service for Prometheus: it stores the samples that
// arrive by remote write (snappy-compressed protobuf) and answers the Prometheus query API for them.
// The control plane (workspaces, rule groups, alert manager definitions) is the generic engine's.
//
// ponytail: samples are kept in memory, per workspace, without a retention limit, and are not saved
// with the state. PromQL is a subset: an instant or range vector selector with =, !=, =~ and !~
// matchers, rate/irate/increase over a range (without Prometheus's extrapolation to the window's
// edges), and sum/avg/min/max/count with by or without. Binary operators, functions
// beyond those, subqueries and histograms are rejected with a bad_data error.
package amp

import (
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/klauspost/compress/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

// Sample is one value at a time in milliseconds.
type Sample struct {
	T int64
	V float64
}

// Series is a metric and its samples in time order; the metric name is the label __name__.
type Series struct {
	Labels  map[string]string
	Samples []Sample
}

// Store holds the series of every workspace.
type Store struct {
	mu sync.Mutex
	ws map[string]map[string]*Series // workspace → labels key → series
}

// New makes an empty store.
func New() *Store { return &Store{ws: map[string]map[string]*Series{}} }

// Delete forgets a workspace's samples.
func (s *Store) Delete(ws string) {
	s.mu.Lock()
	delete(s.ws, ws)
	s.mu.Unlock()
}

// snapshot returns copies of the workspace's series.
func (s *Store) snapshot(ws string) []*Series {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Series
	for _, sr := range s.ws[ws] {
		out = append(out, &Series{Labels: sr.Labels, Samples: slices.Clone(sr.Samples)})
	}
	return out
}

func labelsKey(l map[string]string) string {
	var b strings.Builder
	for _, k := range sortedKeys(l) {
		fmt.Fprintf(&b, "%s=%q,", k, l[k])
	}
	return b.String()
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *Store) add(ws string, labels map[string]string, smp Sample) {
	m := s.ws[ws]
	if m == nil {
		m = map[string]*Series{}
		s.ws[ws] = m
	}
	k := labelsKey(labels)
	sr := m[k]
	if sr == nil {
		sr = &Series{Labels: labels}
		m[k] = sr
	}
	i := sort.Search(len(sr.Samples), func(i int) bool { return sr.Samples[i].T >= smp.T })
	if i < len(sr.Samples) && sr.Samples[i].T == smp.T {
		sr.Samples[i] = smp
		return
	}
	sr.Samples = slices.Insert(sr.Samples, i, smp)
}

// maxWrite caps one remote write request after unpacking (64 MiB, well above what AMP allows).
const maxWrite = 64 << 20

// Write stores the samples of a remote write request body (snappy-compressed protobuf WriteRequest)
// and returns how many it took.
func (s *Store) Write(ws string, body []byte) (int, error) {
	// The first bytes of a snappy body say how big it unpacks. Check that before
	// decoding, so a tiny body cannot make us allocate gigabytes.
	if n, err := snappy.DecodedLen(body); err != nil || n > maxWrite {
		return 0, fmt.Errorf("snappy: body unpacks to more than %d bytes", maxWrite)
	}
	raw, err := snappy.Decode(nil, body)
	if err != nil {
		return 0, fmt.Errorf("snappy: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for len(raw) > 0 {
		num, typ, l := protowire.ConsumeTag(raw)
		if l < 0 {
			return n, protowire.ParseError(l)
		}
		raw = raw[l:]
		if num != 1 || typ != protowire.BytesType { // metadata and the rest are not kept
			if l = protowire.ConsumeFieldValue(num, typ, raw); l < 0 {
				return n, protowire.ParseError(l)
			}
			raw = raw[l:]
			continue
		}
		ts, l := protowire.ConsumeBytes(raw)
		if l < 0 {
			return n, protowire.ParseError(l)
		}
		raw = raw[l:]
		labels, samples, err := decodeSeries(ts)
		if err != nil {
			return n, err
		}
		if labels["__name__"] == "" {
			return n, fmt.Errorf("a series has no __name__")
		}
		for _, smp := range samples {
			s.add(ws, labels, smp)
			n++
		}
	}
	return n, nil
}

// decodeSeries reads a TimeSeries message: labels (1) and samples (2).
func decodeSeries(b []byte) (map[string]string, []Sample, error) {
	labels := map[string]string{}
	var samples []Sample
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return nil, nil, protowire.ParseError(l)
		}
		b = b[l:]
		if typ != protowire.BytesType || num != 1 && num != 2 {
			if l = protowire.ConsumeFieldValue(num, typ, b); l < 0 {
				return nil, nil, protowire.ParseError(l)
			}
			b = b[l:]
			continue
		}
		msg, l := protowire.ConsumeBytes(b)
		if l < 0 {
			return nil, nil, protowire.ParseError(l)
		}
		b = b[l:]
		if num == 1 {
			k, v, err := decodeLabel(msg)
			if err != nil {
				return nil, nil, err
			}
			labels[k] = v
		} else {
			smp, err := decodeSample(msg)
			if err != nil {
				return nil, nil, err
			}
			samples = append(samples, smp)
		}
	}
	return labels, samples, nil
}

func decodeLabel(b []byte) (name, value string, err error) {
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return "", "", protowire.ParseError(l)
		}
		b = b[l:]
		if typ == protowire.BytesType && (num == 1 || num == 2) {
			v, l := protowire.ConsumeBytes(b)
			if l < 0 {
				return "", "", protowire.ParseError(l)
			}
			b = b[l:]
			if num == 1 {
				name = string(v)
			} else {
				value = string(v)
			}
			continue
		}
		if l = protowire.ConsumeFieldValue(num, typ, b); l < 0 {
			return "", "", protowire.ParseError(l)
		}
		b = b[l:]
	}
	return
}

func decodeSample(b []byte) (smp Sample, err error) {
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return smp, protowire.ParseError(l)
		}
		b = b[l:]
		switch {
		case num == 1 && typ == protowire.Fixed64Type:
			v, l := protowire.ConsumeFixed64(b)
			if l < 0 {
				return smp, protowire.ParseError(l)
			}
			smp.V, b = math.Float64frombits(v), b[l:]
		case num == 2 && typ == protowire.VarintType:
			v, l := protowire.ConsumeVarint(b)
			if l < 0 {
				return smp, protowire.ParseError(l)
			}
			smp.T, b = int64(v), b[l:]
		default:
			if l = protowire.ConsumeFieldValue(num, typ, b); l < 0 {
				return smp, protowire.ParseError(l)
			}
			b = b[l:]
		}
	}
	return smp, nil
}

// EncodeWrite builds a remote write body from series (for tests and clients of this package).
func EncodeWrite(series ...*Series) []byte {
	var out []byte
	for _, sr := range series {
		var ts []byte
		for _, k := range sortedKeys(sr.Labels) {
			var l []byte
			l = protowire.AppendTag(l, 1, protowire.BytesType)
			l = protowire.AppendString(l, k)
			l = protowire.AppendTag(l, 2, protowire.BytesType)
			l = protowire.AppendString(l, sr.Labels[k])
			ts = protowire.AppendTag(ts, 1, protowire.BytesType)
			ts = protowire.AppendBytes(ts, l)
		}
		for _, smp := range sr.Samples {
			var m []byte
			m = protowire.AppendTag(m, 1, protowire.Fixed64Type)
			m = binary.LittleEndian.AppendUint64(m, math.Float64bits(smp.V))
			m = protowire.AppendTag(m, 2, protowire.VarintType)
			m = protowire.AppendVarint(m, uint64(smp.T))
			ts = protowire.AppendTag(ts, 2, protowire.BytesType)
			ts = protowire.AppendBytes(ts, m)
		}
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendBytes(out, ts)
	}
	return snappy.Encode(nil, out)
}

// Labels lists the label names of a workspace, or the values of one label when name is set.
func (s *Store) Labels(ws, name string) []string {
	set := map[string]bool{}
	for _, sr := range s.snapshot(ws) {
		if name == "" {
			for k := range sr.Labels {
				set[k] = true
			}
		} else if v, ok := sr.Labels[name]; ok {
			set[v] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
