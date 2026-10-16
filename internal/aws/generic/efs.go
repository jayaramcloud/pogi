package generic

// EFS: a file system has a lifecycle configuration even before one is put.
func init() {
	Override("efs", "DescribeLifecycleConfiguration", func(c *Call) (map[string]any, error) {
		if out, err := c.Default(); err == nil {
			return out, nil
		}
		if c.Get("FileSystem", str(c.In["FileSystemId"])) == nil {
			return nil, c.NotFound("File system '%s' does not exist", str(c.In["FileSystemId"]))
		}
		return map[string]any{"LifecyclePolicies": []any{}}, nil
	})
}
