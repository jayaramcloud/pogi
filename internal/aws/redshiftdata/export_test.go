package redshiftdata

// Bind is bind for the tests: it rewrites SQL for PostgreSQL and binds :name parameters.
func Bind(sqls []string, params map[string]string) ([]string, [][]any, error) {
	var ps []param
	for k, v := range params {
		ps = append(ps, param{k, v})
	}
	return bind(sqls, ps)
}
