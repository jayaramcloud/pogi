package generic

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3io"
	"github.com/nrndev/pogi/internal/core"
)

// S3 Tables: table buckets hold namespaces, which hold Iceberg tables. A table's warehouse is a real
// bucket in pogi's S3 (<uuid>--table-s3), and its first metadata file is a real object in it, so a
// metadataLocation can be read back with the S3 API. UpdateTableMetadataLocation honours the version token.
//
// ponytail: maintenance, replication and metrics configurations are not stored;
// the Iceberg REST catalog endpoint is not served.
func init() {
	Override("s3tables", "CreateTableBucket", func(c *Call) (map[string]any, error) {
		name := str(c.In["name"])
		if !regexp.MustCompile(`^[0-9a-z][0-9a-z-]{1,61}[0-9a-z]$`).MatchString(name) {
			return nil, c.Invalid("Table bucket name %q must be 3 to 63 lowercase letters, digits and hyphens", name)
		}
		if c.Get("TableBucket", name) != nil {
			return nil, c.Exists("A table bucket named %s already exists", name)
		}
		arn := c.arnOf("bucket/"+name, nil)
		data := map[string]any{"arn": arn, "name": name, "createdAt": s2Time(time.Now()), "ownerAccountId": c.Account,
			"tableBucketId": randHex(16), "type": "customer",
			"encryptionConfiguration":   map[string]any{"sseAlgorithm": "AES256"},
			"storageClassConfiguration": map[string]any{"storageClass": "STANDARD"}}
		for _, k := range []string{"encryptionConfiguration", "storageClassConfiguration"} {
			if v := c.In[k]; v != nil {
				data[k] = v
			}
		}
		c.Res = s2Store(c, "TableBucket", name, name, arn, data)
		s2Tags(c, c.Res)
		return map[string]any{"arn": arn}, nil
	})
	Override("s3tables", "GetTableBucket", func(c *Call) (map[string]any, error) {
		b, err := s3tBucket(c, str(c.In["tableBucketARN"]))
		if err != nil {
			return nil, err
		}
		return b.Data, nil
	})
	Override("s3tables", "ListTableBuckets", func(c *Call) (map[string]any, error) {
		var items []any
		for _, b := range c.List("TableBucket") {
			if strings.HasPrefix(b.Name, str(c.In["prefix"])) {
				items = append(items, b.Data)
			}
		}
		return s3tPage(c, "tableBuckets", items, "maxBuckets"), nil
	})
	Override("s3tables", "DeleteTableBucket", func(c *Call) (map[string]any, error) {
		b, err := s3tBucket(c, str(c.In["tableBucketARN"]))
		if err != nil {
			return nil, err
		}
		if len(s3tIn(c, "Namespace", b.Name)) > 0 {
			return nil, c.Exists("The table bucket %s is not empty: delete its namespaces first", b.Name)
		}
		c.Svc.Delete(c.Account, c.Region, "TableBucket", b.Name)
		return map[string]any{}, nil
	})
	// attributes of a bucket and of a table that are stored as they come
	for _, a := range []struct{ bucket, table, member string }{
		{"TableBucketPolicy", "TablePolicy", "resourcePolicy"}, {"TableBucketEncryption", "TableEncryption", "encryptionConfiguration"},
		{"TableBucketStorageClass", "TableStorageClass", "storageClassConfiguration"}} {
		for _, on := range []string{a.bucket, a.table} {
			isTable := on == a.table
			attr := "_" + a.member
			Override("s3tables", "Put"+on, func(c *Call) (map[string]any, error) {
				r, err := s3tTarget(c, isTable)
				if err != nil {
					return nil, err
				}
				r.Data[attr] = c.In[a.member]
				c.Put(r)
				return map[string]any{}, nil
			})
			Override("s3tables", "Get"+on, func(c *Call) (map[string]any, error) {
				r, err := s3tTarget(c, isTable)
				if err != nil {
					return nil, err
				}
				v := r.Data[attr]
				if v == nil && a.member != "resourcePolicy" {
					v = r.Data[a.member]
					if b, err := s3tBucket(c, str(c.In["tableBucketARN"])); v == nil && err == nil { // a table has its bucket's
						v = b.Data[a.member]
					}
				}
				if v == nil {
					return nil, c.NotFound("The %s of %s does not exist", on, r.Name)
				}
				return map[string]any{a.member: v}, nil
			})
			Override("s3tables", "Delete"+on, func(c *Call) (map[string]any, error) {
				r, err := s3tTarget(c, isTable)
				if err != nil {
					return nil, err
				}
				delete(r.Data, attr)
				c.Put(r)
				return map[string]any{}, nil
			})
		}
	}

	// maintenance: what is enabled by default, and what was put
	for _, m := range []struct {
		op, kind string
		table    bool
		defaults map[string]any
	}{
		{"TableBucket", "TableBucket", false, map[string]any{"icebergUnreferencedFileRemoval": map[string]any{"status": "enabled",
			"settings": map[string]any{"icebergUnreferencedFileRemoval": map[string]any{"unreferencedDays": 3.0, "nonCurrentDays": 10.0}}}}},
		{"Table", "Table", true, map[string]any{
			"icebergCompaction":         map[string]any{"status": "enabled", "settings": map[string]any{"icebergCompaction": map[string]any{"targetFileSizeMB": 512.0, "strategy": "auto"}}},
			"icebergSnapshotManagement": map[string]any{"status": "enabled", "settings": map[string]any{"icebergSnapshotManagement": map[string]any{"minSnapshotsToKeep": 1.0, "maxSnapshotAgeHours": 120.0}}}}},
	} {
		Override("s3tables", "Get"+m.op+"MaintenanceConfiguration", func(c *Call) (map[string]any, error) {
			r, err := s3tTarget(c, m.table)
			if err != nil {
				return nil, err
			}
			conf := s2Clone(m.defaults)
			for k, v := range s2Map(r.Data["_maintenance"]) {
				conf[k] = v
			}
			if m.table {
				return map[string]any{"configuration": conf, "tableARN": r.ARN}, nil
			}
			return map[string]any{"configuration": conf, "tableBucketARN": r.ARN}, nil
		})
		Override("s3tables", "Put"+m.op+"MaintenanceConfiguration", func(c *Call) (map[string]any, error) {
			r, err := s3tTarget(c, m.table)
			if err != nil {
				return nil, err
			}
			conf := s2Map(r.Data["_maintenance"])
			conf[str(c.In["type"])] = c.In["value"]
			r.Data["_maintenance"] = conf
			c.Put(r)
			return map[string]any{}, nil
		})
	}

	Override("s3tables", "CreateNamespace", func(c *Call) (map[string]any, error) {
		b, err := s3tBucket(c, str(c.In["tableBucketARN"]))
		if err != nil {
			return nil, err
		}
		ns := s2Names(c.In["namespace"])
		if len(ns) != 1 || !s3tName.MatchString(ns[0]) {
			return nil, c.Invalid("A namespace is one name of 1 to 255 lowercase letters, digits and underscores")
		}
		key := b.Name + "/" + ns[0]
		if c.Get("Namespace", key) != nil {
			return nil, c.Exists("The namespace %s already exists", ns[0])
		}
		s2Store(c, "Namespace", key, ns[0], "", map[string]any{"namespace": []any{ns[0]}, "createdAt": s2Time(time.Now()), "createdBy": c.Account,
			"ownerAccountId": c.Account, "namespaceId": core.UUID(), "tableBucketId": b.Data["tableBucketId"]})
		return map[string]any{"namespace": []any{ns[0]}, "tableBucketARN": b.ARN}, nil
	})
	Override("s3tables", "GetNamespace", func(c *Call) (map[string]any, error) {
		_, r, err := s3tNamespace(c)
		if err != nil {
			return nil, err
		}
		return r.Data, nil
	})
	Override("s3tables", "ListNamespaces", func(c *Call) (map[string]any, error) {
		b, err := s3tBucket(c, str(c.In["tableBucketARN"]))
		if err != nil {
			return nil, err
		}
		var items []any
		for _, r := range s3tIn(c, "Namespace", b.Name) {
			if strings.HasPrefix(r.Name, str(c.In["prefix"])) {
				items = append(items, r.Data)
			}
		}
		return s3tPage(c, "namespaces", items, "maxNamespaces"), nil
	})
	Override("s3tables", "DeleteNamespace", func(c *Call) (map[string]any, error) {
		b, r, err := s3tNamespace(c)
		if err != nil {
			return nil, err
		}
		if len(s3tTables(c, b.Name, r.Name)) > 0 {
			return nil, c.Exists("The namespace %s is not empty: delete its tables first", r.Name)
		}
		c.Svc.Delete(c.Account, c.Region, "Namespace", r.Key)
		return map[string]any{}, nil
	})

	Override("s3tables", "CreateTable", func(c *Call) (map[string]any, error) {
		b, ns, err := s3tNamespace(c)
		if err != nil {
			return nil, err
		}
		name := str(c.In["name"])
		if !s3tName.MatchString(name) {
			return nil, c.Invalid("A table name is 1 to 255 lowercase letters, digits and underscores")
		}
		key := b.Name + "/" + ns.Name + "/" + name
		if c.Get("Table", key) != nil {
			return nil, c.Exists("The table %s already exists in namespace %s", name, ns.Name)
		}
		id := core.UUID()
		arn := b.ARN + "/table/" + id
		warehouse := "s3://" + id + "--table-s3"
		token := randHex(8)
		meta := warehouse + "/metadata/00000-" + core.UUID() + ".metadata.json"
		now := s2Time(time.Now())
		s3tWriteMetadata(c, meta, id, warehouse, c.In["metadata"])
		r := s2Store(c, "Table", key, name, arn, map[string]any{"tableARN": arn, "name": name, "namespace": []any{ns.Name}, "format": str(c.In["format"]),
			"createdAt": now, "modifiedAt": now, "createdBy": c.Account, "modifiedBy": c.Account, "ownerAccountId": c.Account, "type": "customer",
			"namespaceId": ns.Data["namespaceId"], "tableBucketId": b.Data["tableBucketId"], "warehouseLocation": warehouse,
			"metadataLocation": meta, "versionToken": token, "_id": id})
		c.Res = r
		s2Tags(c, r)
		return map[string]any{"tableARN": arn, "versionToken": token}, nil
	})
	Override("s3tables", "GetTable", func(c *Call) (map[string]any, error) {
		t, err := s3tFind(c)
		if err != nil {
			return nil, err
		}
		return t.Data, nil
	})
	Override("s3tables", "ListTables", func(c *Call) (map[string]any, error) {
		b, err := s3tBucket(c, str(c.In["tableBucketARN"]))
		if err != nil {
			return nil, err
		}
		var items []any
		for _, r := range s3tIn(c, "Table", b.Name) {
			if ns := str(c.In["namespace"]); (ns == "" || slices.Contains(s2Names(r.Data["namespace"]), ns)) && strings.HasPrefix(r.Name, str(c.In["prefix"])) {
				items = append(items, r.Data)
			}
		}
		return s3tPage(c, "tables", items, "maxTables"), nil
	})
	Override("s3tables", "GetTableMetadataLocation", func(c *Call) (map[string]any, error) {
		t, err := s3tFind(c)
		if err != nil {
			return nil, err
		}
		return t.Data, nil
	})
	Override("s3tables", "UpdateTableMetadataLocation", func(c *Call) (map[string]any, error) {
		t, err := s3tFind(c)
		if err != nil {
			return nil, err
		}
		if str(c.In["versionToken"]) != str(t.Data["versionToken"]) {
			return nil, c.Exists("The version token does not match the table's current one: it was changed by another call")
		}
		t.Data["metadataLocation"], t.Data["versionToken"], t.Data["modifiedAt"] = c.In["metadataLocation"], randHex(8), s2Time(time.Now())
		t.Updated = time.Now().UTC()
		c.Put(t)
		return map[string]any{"metadataLocation": t.Data["metadataLocation"], "name": t.Name, "namespace": t.Data["namespace"], "tableARN": t.ARN, "versionToken": t.Data["versionToken"]}, nil
	})
	Override("s3tables", "RenameTable", func(c *Call) (map[string]any, error) {
		t, err := s3tFind(c)
		if err != nil {
			return nil, err
		}
		if v := str(c.In["versionToken"]); v != "" && v != str(t.Data["versionToken"]) {
			return nil, c.Exists("The version token does not match the table's current one")
		}
		b, _ := s3tBucket(c, str(c.In["tableBucketARN"]))
		ns := s2Names(t.Data["namespace"])[0]
		if n := str(c.In["newNamespaceName"]); n != "" {
			if c.Get("Namespace", b.Name+"/"+n) == nil {
				return nil, c.NotFound("The namespace %s does not exist", n)
			}
			ns = n
		}
		name := t.Name
		if n := str(c.In["newName"]); n != "" {
			name = n
		}
		key := b.Name + "/" + ns + "/" + name
		if key != t.Key && c.Get("Table", key) != nil {
			return nil, c.Exists("The table %s already exists in namespace %s", name, ns)
		}
		c.Svc.Delete(c.Account, c.Region, "Table", t.Key)
		t.Key, t.Name, t.IDs = key, name, map[string]string{"Table": key}
		t.Data["name"], t.Data["namespace"], t.Data["versionToken"], t.Data["modifiedAt"] = name, []any{ns}, randHex(8), s2Time(time.Now())
		c.Put(t)
		return map[string]any{}, nil
	})
	Override("s3tables", "DeleteTable", func(c *Call) (map[string]any, error) {
		t, err := s3tFind(c)
		if err != nil {
			return nil, err
		}
		if v := str(c.In["versionToken"]); v != "" && v != str(t.Data["versionToken"]) {
			return nil, c.Exists("The version token does not match the table's current one")
		}
		c.Svc.Delete(c.Account, c.Region, "Table", t.Key)
		if bucket, _, ok := s3io.Split(str(t.Data["warehouseLocation"])); ok { // the warehouse goes with the table
			s3 := s3io.Client{Hub: c.Hub(), Account: c.Account, Region: c.Region}
			objs, _, _ := s3.List(bucket, "", "")
			for _, o := range objs {
				s3.Delete(bucket, o.Key)
			}
			s3.DeleteBucket(bucket)
		}
		return map[string]any{}, nil
	})
}

var s3tName = regexp.MustCompile(`^[0-9a-z_]{1,255}$`)

// s3tBucket finds the table bucket an ARN names.
func s3tBucket(c *Call, arn string) (*Resource, error) {
	_, name, _ := strings.Cut(arn, ":bucket/")
	name, _, _ = strings.Cut(name, "/")
	if b := c.Get("TableBucket", name); b != nil {
		return b, nil
	}
	return nil, c.NotFound("The table bucket %s does not exist", arn)
}

// s3tNamespace finds the table bucket and the namespace a call names.
func s3tNamespace(c *Call) (b, ns *Resource, err error) {
	if b, err = s3tBucket(c, str(c.In["tableBucketARN"])); err != nil {
		return
	}
	n := str(c.In["namespace"])
	if l := s2Names(c.In["namespace"]); len(l) == 1 { // CreateNamespace takes a list
		n = l[0]
	}
	if ns = c.Get("Namespace", b.Name+"/"+n); ns == nil {
		err = c.NotFound("The namespace %s does not exist", n)
	}
	return
}

// s3tIn lists the namespaces or tables of a bucket.
func s3tIn(c *Call, typ, bucket string) []*Resource {
	var out []*Resource
	for _, r := range c.List(typ) {
		if strings.HasPrefix(r.Key, bucket+"/") {
			out = append(out, r)
		}
	}
	return out
}

func s3tTables(c *Call, bucket, ns string) []*Resource {
	var out []*Resource
	for _, r := range s3tIn(c, "Table", bucket) {
		if s2Names(r.Data["namespace"])[0] == ns {
			out = append(out, r)
		}
	}
	return out
}

// s3tFind finds a table by ARN, or by bucket, namespace and name.
func s3tFind(c *Call) (*Resource, error) {
	if arn := str(c.In["tableArn"]); arn != "" {
		for _, r := range c.List("Table") {
			if r.ARN == arn {
				return r, nil
			}
		}
		return nil, c.NotFound("The table %s does not exist", arn)
	}
	b, err := s3tBucket(c, str(c.In["tableBucketARN"]))
	if err != nil {
		return nil, err
	}
	if t := c.Get("Table", b.Name+"/"+str(c.In["namespace"])+"/"+str(c.In["name"])); t != nil {
		return t, nil
	}
	return nil, c.NotFound("The table %s does not exist in namespace %s", str(c.In["name"]), str(c.In["namespace"]))
}

// s3tTarget is the table bucket or the table a policy-like call is about.
func s3tTarget(c *Call, table bool) (*Resource, error) {
	if table {
		return s3tFind(c)
	}
	return s3tBucket(c, str(c.In["tableBucketARN"]))
}

// s3tPage lists items under a name with this API's paging: a max and a continuation token.
func s3tPage(c *Call, member string, items []any, limit string) map[string]any {
	page, next := s2Page(items, c.In[limit], str(c.In["continuationToken"]))
	out := map[string]any{member: orEmpty(page)}
	if next != "" {
		out["continuationToken"] = next
	}
	return out
}

// s3tWriteMetadata writes the first Iceberg metadata file of a table into its warehouse, when S3 is there.
func s3tWriteMetadata(c *Call, loc, id, warehouse string, md any) {
	bucket, key, _ := s3io.Split(loc)
	var fields []any
	for i, f := range asList(s2Map(s2Map(s2Map(md)["iceberg"])["schema"])["fields"]) {
		fm := s2Map(f)
		fields = append(fields, map[string]any{"id": i + 1, "name": fm["name"], "required": fm["required"] == true, "type": fm["type"]})
	}
	if fields == nil {
		fields = []any{}
	}
	b, _ := json.Marshal(map[string]any{"format-version": 2, "table-uuid": id, "location": warehouse, "last-sequence-number": 0,
		"last-updated-ms": time.Now().UnixMilli(), "last-column-id": len(fields),
		"current-schema-id": 0, "schemas": []any{map[string]any{"type": "struct", "schema-id": 0, "fields": fields}},
		"default-spec-id": 0, "partition-specs": []any{map[string]any{"spec-id": 0, "fields": []any{}}}, "last-partition-id": 999,
		"default-sort-order-id": 0, "sort-orders": []any{map[string]any{"order-id": 0, "fields": []any{}}},
		"properties": map[string]any{}, "current-snapshot-id": -1, "snapshots": []any{}, "snapshot-log": []any{}, "metadata-log": []any{}})
	s3 := s3io.Client{Hub: c.Hub(), Account: c.Account, Region: c.Region}
	if s3.MakeBucket(bucket) == nil {
		s3.Put(bucket, key, b)
	}
}
