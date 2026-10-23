// Package cost prices Terraform resources from the clouds' official on-demand price lists, which
// are embedded in pogi. It never fetches prices over the network.
package cost

import (
	"bytes"
	"compress/gzip"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
)

// The price lists, one file per cloud, written by scripts/prices.
//
//go:embed prices/*.json.gz
var priceFiles embed.FS

// DB is a price list. Keys start with the cloud: "aws/ec2", "gcp/compute/n2/vcpu".
type DB struct {
	Published map[string]string  `json:"published"` // cloud → the date the prices were fetched (2026-10-20)
	Source    map[string]string  `json:"source"`    // cloud → where they came from, in plain words
	Tables    map[string][]Size  `json:"tables"`    // "aws/ec2" → every size of that kind
	Columns   map[string]*Column `json:"columns"`   // "aws/ec2/linux" → a price for each size of a table
	Rates     map[string]*Rate   `json:"rates"`     // "aws/ebs/gp3/storage" → one unit price per region

	index map[string]map[string]int // table → size name → position
}

// Size is one machine, database or cache size.
type Size struct {
	Name     string  `json:"n"`
	Family   string  `json:"f,omitempty"`
	VCPU     float64 `json:"c,omitempty"`
	MemGB    float64 `json:"m,omitempty"`
	GPU      int     `json:"g,omitempty"`
	GPUModel string  `json:"gm,omitempty"`
	Arch     string  `json:"a,omitempty"` // "arm64"; "" is x86_64
	Old      bool    `json:"o,omitempty"` // a previous generation
}

// Column prices each size of a table, per region.
type Column struct {
	Table  string               `json:"table"`
	Unit   string               `json:"unit"`   // "hour"
	Prices map[string][]float64 `json:"prices"` // region → price per Unit, in the table's order; 0 where the size isn't sold
}

// Rate is a single unit price, per region.
type Rate struct {
	Unit   string             `json:"unit"` // "GB-month", "hour", "1M requests"
	Prices map[string]float64 `json:"prices"`
}

// SizePrice is a size with its price in one region.
type SizePrice struct {
	Size
	Price float64 // per the column's Unit
}

var (
	defaultOnce sync.Once
	defaultDB   *DB
	defaultErr  error
)

// Default returns the embedded price list. It is read once, on first use.
func Default() (*DB, error) {
	defaultOnce.Do(func() {
		defaultDB = &DB{}
		files, _ := priceFiles.ReadDir("prices")
		for _, f := range files {
			b, err := priceFiles.ReadFile("prices/" + f.Name())
			if err != nil {
				defaultErr = err
				return
			}
			part, err := Decode(b)
			if err != nil {
				defaultErr = fmt.Errorf("price list %s: %w", f.Name(), err)
				return
			}
			defaultDB.merge(part)
		}
		defaultDB.reindex()
	})
	return defaultDB, defaultErr
}

// Decode reads a gzip-compressed JSON price list.
func Decode(b []byte) (*DB, error) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	raw, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	var db DB
	if err := json.Unmarshal(raw, &db); err != nil {
		return nil, err
	}
	for k, c := range db.Columns {
		n := len(db.Tables[c.Table])
		if n == 0 {
			return nil, fmt.Errorf("column %s: table %q has no sizes", k, c.Table)
		}
		for r, p := range c.Prices {
			if len(p) != n {
				return nil, fmt.Errorf("column %s, region %s: %d prices for %d sizes", k, r, len(p), n)
			}
		}
	}
	db.reindex()
	return &db, nil
}

// Encode writes db as gzip-compressed JSON.
func Encode(db *DB) ([]byte, error) {
	raw, err := json.Marshal(db)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)
	if _, err := zw.Write(raw); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (db *DB) merge(o *DB) {
	db.Published = mergeMap(db.Published, o.Published)
	db.Source = mergeMap(db.Source, o.Source)
	db.Tables = mergeMap(db.Tables, o.Tables)
	db.Columns = mergeMap(db.Columns, o.Columns)
	db.Rates = mergeMap(db.Rates, o.Rates)
}

func mergeMap[V any](a, b map[string]V) map[string]V {
	if a == nil {
		a = map[string]V{}
	}
	for k, v := range b {
		a[k] = v
	}
	return a
}

func (db *DB) reindex() {
	db.index = map[string]map[string]int{}
	for t, sizes := range db.Tables {
		m := make(map[string]int, len(sizes))
		for i, s := range sizes {
			m[s.Name] = i
		}
		db.index[t] = m
	}
}

// Rate returns a unit price and its unit.
func (db *DB) Rate(key, region string) (price float64, unit string, ok bool) {
	r := db.Rates[key]
	if r == nil {
		return 0, "", false
	}
	p, ok := r.Prices[region]
	return p, r.Unit, ok
}

// Hourly returns the price per unit of one size in a region. ok is false when the size is not
// sold there.
func (db *DB) Hourly(column, region, size string) (float64, bool) {
	c := db.Columns[column]
	if c == nil {
		return 0, false
	}
	i, ok := db.index[c.Table][size]
	if !ok {
		return 0, false
	}
	p := c.Prices[region]
	if p == nil || p[i] == 0 {
		return 0, false
	}
	return p[i], true
}

// Size returns a size's specs.
func (db *DB) Size(table, name string) (Size, bool) {
	i, ok := db.index[table][name]
	if !ok {
		return Size{}, false
	}
	return db.Tables[table][i], true
}

// Sizes lists the sizes of a column sold in a region, cheapest first.
func (db *DB) Sizes(column, region string) []SizePrice {
	c := db.Columns[column]
	if c == nil {
		return nil
	}
	var out []SizePrice
	for i, p := range c.Prices[region] {
		if p > 0 {
			out = append(out, SizePrice{db.Tables[c.Table][i], p})
		}
	}
	slices.SortStableFunc(out, func(a, b SizePrice) int {
		if a.Price != b.Price {
			if a.Price < b.Price {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

// Regions lists the regions of a cloud that have any price, sorted.
func (db *DB) Regions(cloud string) []string {
	seen := map[string]bool{}
	for k, c := range db.Columns {
		if strings.HasPrefix(k, cloud+"/") {
			for r := range c.Prices {
				seen[r] = true
			}
		}
	}
	for k, r := range db.Rates {
		if strings.HasPrefix(k, cloud+"/") {
			for reg := range r.Prices {
				seen[reg] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for r := range seen {
		out = append(out, r)
	}
	slices.Sort(out)
	return out
}
