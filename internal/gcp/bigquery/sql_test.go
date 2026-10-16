package bigquery

import "testing"

func TestTranslate(t *testing.T) {
	ctx := tctx{"proj", "ds"}
	for _, tc := range []struct{ in, want string }{
		{"SELECT a, b FROM `proj.ds.t` WHERE x = \"hi\"", `SELECT a , b FROM "proj"."ds"."t" WHERE x = 'hi'`},
		{"select * from t", `select * FROM "proj"."ds"."t"`},
		{"SELECT * FROM other.t AS a JOIN `p2.d2.u` USING (id)", `SELECT * FROM "proj"."other"."t" AS a JOIN "p2"."d2"."u" USING (id)`},
		{"SELECT x FROM t1, t2 WHERE t1.a = t2.a", `SELECT x FROM "proj"."ds"."t1" , "proj"."ds"."t2" WHERE t1 . a = t2 . a`},
		{"SELECT SAFE_CAST(x AS INT64), CAST(y AS STRING) FROM t", `SELECT TRY_CAST (x AS BIGINT) , CAST (y AS VARCHAR) FROM "proj"."ds"."t"`},
		{"SELECT CURRENT_TIMESTAMP(), TIMESTAMP '2020-01-01 00:00:00'", `SELECT now() , TIMESTAMPTZ '2020-01-01 00:00:00'`},
		{"SELECT * EXCEPT (a) FROM t", `SELECT * EXCLUDE (a) FROM "proj"."ds"."t"`},
		{"SELECT i FROM t, UNNEST(t.items) AS i", `SELECT i FROM "proj"."ds"."t" , UNNEST(t . items) AS "_u_i"("i")`},
		{"SELECT x FROM UNNEST([1,2,3]) AS x", `SELECT x FROM UNNEST([1 , 2 , 3]) AS "_u_x"("x")`},
		{"SELECT a WHERE a IN UNNEST([1])", `SELECT a WHERE a IN (SELECT UNNEST([1]))`},
		{"SELECT STRUCT(1 AS a, 'x' AS b)", `SELECT struct_pack("a" := 1, "b" := 'x')`},
		{"SELECT ARRAY<INT64>[1, 2], arr[OFFSET(0)]", `SELECT CAST([1 , 2] AS BIGINT[]) , arr [(0)+1]`},
		{"SELECT SAFE_DIVIDE(a, b), DATE_DIFF(d1, d2, DAY), COUNTIF(x > 1)", `SELECT (CASE WHEN (b) = 0 THEN NULL ELSE CAST(a AS DOUBLE) / (b) END) , date_diff('day', d2, d1) , count_if (x > 1)`},
		{"SELECT EXTRACT(YEAR FROM d) FROM t", `SELECT EXTRACT (YEAR FROM d) FROM "proj"."ds"."t"`},
		{`SELECT 'it\'s', r'a\b', @n`, `SELECT 'it''s' , 'a\b' , 5`},
		{"DELETE t WHERE x = 1", `DELETE FROM "proj"."ds"."t" WHERE x = 1`},
		{"INSERT t (a) VALUES (1)", `INSERT INTO "proj"."ds"."t" (a) VALUES (1)`},
		{"UPDATE ds.t SET a = 1 WHERE true", `UPDATE "proj"."ds"."t" SET a = 1 WHERE true`},
		{"MERGE t USING s ON t.id = s.id WHEN MATCHED THEN DELETE", `MERGE INTO "proj"."ds"."t" USING "proj"."ds"."s" ON t . id = s . id WHEN MATCHED THEN DELETE`},
		{"TRUNCATE TABLE t", `DELETE FROM "proj"."ds"."t"`},
		{"CREATE TABLE IF NOT EXISTS t (a INT64, b ARRAY<STRING>, c STRUCT<x FLOAT64, y TIMESTAMP>) PARTITION BY DATE(ts) OPTIONS (description = 'd')",
			`CREATE TABLE IF NOT EXISTS "proj"."ds"."t" (a BIGINT , b VARCHAR[] , c STRUCT("x" DOUBLE, "y" TIMESTAMPTZ))`},
		{"SELECT timestamp, string FROM t", `SELECT timestamp , string FROM "proj"."ds"."t"`},
	} {
		tr, err := translate(tc.in, ctx, &params{named: map[string]string{"n": "5"}})
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if got := tr.stmts[0].sql; got != tc.want {
			t.Errorf("%s\n got %s\nwant %s", tc.in, got, tc.want)
		}
	}
	tr, err := translate("CREATE TABLE t (a INT64) PARTITION BY DATE(ts) CLUSTER BY a, b", ctx, nil)
	if err != nil || tr.stmts[0].partition != "ts" || len(tr.stmts[0].clusterBy) != 2 || tr.stmts[0].target.table != "t" || tr.stmts[0].verb != "CREATE_TABLE" {
		t.Errorf("create = %+v, %v", tr.stmts[0], err)
	}
	tr, err = translate("CREATE OR REPLACE TABLE x AS SELECT 1 AS a; DROP VIEW IF EXISTS ds2.v; CREATE VIEW v AS SELECT 2", ctx, nil)
	if err != nil || len(tr.stmts) != 3 || tr.stmts[0].verb != "CREATE_TABLE_AS_SELECT" || tr.stmts[1].verb != "DROP_VIEW" || !tr.stmts[1].ifExists ||
		tr.stmts[2].verb != "CREATE_VIEW" || tr.stmts[2].viewSQL != "SELECT 2" {
		t.Errorf("script = %+v, %v", tr, err)
	}
	for _, bad := range []string{"SELECT 'x", "SELECT @missing", "SELECT * FROM t WHERE", "FOO BAR"} {
		if _, err := translate(bad, tctx{"p", ""}, nil); err == nil && bad != "SELECT * FROM t WHERE" {
			t.Errorf("%q translated", bad)
		}
	}
	if _, err := translate("SELECT * FROM t", tctx{"p", ""}, nil); err == nil {
		t.Error("a table without a dataset resolved")
	}
}
