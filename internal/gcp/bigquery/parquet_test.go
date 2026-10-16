package bigquery_test

import (
	"os"
	"os/exec"
	"testing"

	"cloud.google.com/go/bigquery"
)

// Parquet loads are read by DuckDB; the file is made with it, in the engine image.
func TestLoadParquet(t *testing.T) {
	e := setup(t)
	e.needDocker(t)
	dir := t.TempDir()
	py := `import duckdb; duckdb.sql("COPY (SELECT 1::BIGINT AS id, 'ada' AS name, [1.5::DOUBLE, 2.5::DOUBLE] AS xs, {'a': 7} AS s, TIMESTAMP '2026-01-02 03:04:05' AS ts) TO '/data/x.parquet' (FORMAT parquet)")`
	if out, err := exec.Command("docker", "run", "--rm", "-v", dir+":/data", "pogi/athena-duckdb:1.5.6", "python", "-c", py).CombinedOutput(); err != nil {
		t.Skipf("cannot make a Parquet file with the engine image: %v %s", err, out)
	}
	b := must(os.ReadFile(dir + "/x.parquet"))
	put(t, e, "lake", "x.parquet", string(b))
	must(0, e.client.Dataset("ds").Create(ctx, nil))
	ref := bigquery.NewGCSReference("gs://lake/x.parquet")
	ref.SourceFormat = bigquery.Parquet
	if st := must(must(e.client.Dataset("ds").Table("pq").LoaderFrom(ref).Run(ctx)).Wait(ctx)); st.Err() != nil {
		t.Fatal(st.Err())
	}
	m := must(e.client.Dataset("ds").Table("pq").Metadata(ctx))
	if len(m.Schema) != 5 || m.Schema[2].Type != bigquery.FloatFieldType || !m.Schema[2].Repeated || m.Schema[3].Type != bigquery.RecordFieldType {
		t.Fatalf("schema = %v", m.Schema)
	}
	if r := rows(t, e.client.Dataset("ds").Table("pq").Read(ctx)); show(r) != "[[1 ada [1.5 2.5] [7] 2026-01-02T03:04:05]]" {
		t.Fatalf("rows = %v", show(r))
	}
}
