package generic_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestLakeFormation(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := lakeformation.NewFromConfig(awstest.Config(t, s.URL))

	// data lake settings: defaults first, then what was put
	d, err := c.GetDataLakeSettings(ctx, &lakeformation.GetDataLakeSettingsInput{})
	if err != nil || len(d.DataLakeSettings.DataLakeAdmins) != 0 || len(d.DataLakeSettings.CreateDatabaseDefaultPermissions) != 1 {
		t.Fatalf("default settings: %v %+v", err, d)
	}
	admin := "arn:aws:iam::000000000000:role/lf-admin"
	if _, err := c.PutDataLakeSettings(ctx, &lakeformation.PutDataLakeSettingsInput{DataLakeSettings: &types.DataLakeSettings{
		DataLakeAdmins: []types.DataLakePrincipal{{DataLakePrincipalIdentifier: aws.String(admin)}}, TrustedResourceOwners: []string{"111111111111"}}}); err != nil {
		t.Fatal(err)
	}
	d, _ = c.GetDataLakeSettings(ctx, &lakeformation.GetDataLakeSettingsInput{})
	if len(d.DataLakeSettings.DataLakeAdmins) != 1 || aws.ToString(d.DataLakeSettings.DataLakeAdmins[0].DataLakePrincipalIdentifier) != admin || len(d.DataLakeSettings.CreateDatabaseDefaultPermissions) != 0 ||
		len(d.DataLakeSettings.TrustedResourceOwners) != 1 {
		t.Fatalf("settings: %+v", d.DataLakeSettings)
	}

	// registered resources
	loc := "arn:aws:s3:::lake-bucket/data"
	if _, err := c.RegisterResource(ctx, &lakeformation.RegisterResourceInput{ResourceArn: aws.String(loc), UseServiceLinkedRole: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RegisterResource(ctx, &lakeformation.RegisterResourceInput{ResourceArn: aws.String(loc), UseServiceLinkedRole: aws.Bool(true)}); err == nil {
		t.Fatal("registered twice")
	}
	if g, err := c.DescribeResource(ctx, &lakeformation.DescribeResourceInput{ResourceArn: aws.String(loc)}); err != nil || aws.ToString(g.ResourceInfo.ResourceArn) != loc {
		t.Fatalf("describe resource: %v %+v", err, g)
	}
	if _, err := c.UpdateResource(ctx, &lakeformation.UpdateResourceInput{ResourceArn: aws.String(loc), RoleArn: aws.String("arn:aws:iam::000000000000:role/lf")}); err != nil {
		t.Fatal(err)
	}
	if g, _ := c.DescribeResource(ctx, &lakeformation.DescribeResourceInput{ResourceArn: aws.String(loc)}); aws.ToString(g.ResourceInfo.RoleArn) != "arn:aws:iam::000000000000:role/lf" {
		t.Fatalf("updated resource: %+v", g.ResourceInfo)
	}
	if l, err := c.ListResources(ctx, &lakeformation.ListResourcesInput{}); err != nil || len(l.ResourceInfoList) != 1 {
		t.Fatalf("list resources: %v %+v", err, l)
	}

	// permissions: grant, merge, list by principal, revoke
	analyst := &types.DataLakePrincipal{DataLakePrincipalIdentifier: aws.String("arn:aws:iam::000000000000:role/analyst")}
	other := &types.DataLakePrincipal{DataLakePrincipalIdentifier: aws.String("arn:aws:iam::000000000000:role/other")}
	db := &types.Resource{Database: &types.DatabaseResource{Name: aws.String("sales")}}
	table := &types.Resource{Table: &types.TableResource{DatabaseName: aws.String("sales"), Name: aws.String("orders")}}
	grant := func(p *types.DataLakePrincipal, r *types.Resource, perms ...types.Permission) {
		if _, err := c.GrantPermissions(ctx, &lakeformation.GrantPermissionsInput{Principal: p, Resource: r, Permissions: perms}); err != nil {
			t.Fatal(err)
		}
	}
	grant(analyst, db, types.PermissionDescribe)
	grant(analyst, db, types.PermissionAlter, types.PermissionDescribe)
	grant(analyst, table, types.PermissionSelect)
	grant(other, db, types.PermissionDrop)
	list := func(in *lakeformation.ListPermissionsInput) []types.PrincipalResourcePermissions {
		out, err := c.ListPermissions(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		return out.PrincipalResourcePermissions
	}
	if got := list(&lakeformation.ListPermissionsInput{}); len(got) != 3 {
		t.Fatalf("all permissions: %+v", got)
	}
	got := list(&lakeformation.ListPermissionsInput{Principal: analyst, Resource: db})
	if len(got) != 1 || len(got[0].Permissions) != 2 || !slices.Contains(got[0].Permissions, types.PermissionAlter) || got[0].Resource.Database == nil {
		t.Fatalf("analyst on db: %+v", got)
	}
	if got := list(&lakeformation.ListPermissionsInput{Principal: analyst, ResourceType: types.DataLakeResourceTypeTable}); len(got) != 1 || got[0].Resource.Table == nil {
		t.Fatalf("analyst tables: %+v", got)
	}
	if _, err := c.RevokePermissions(ctx, &lakeformation.RevokePermissionsInput{Principal: analyst, Resource: db, Permissions: []types.Permission{types.PermissionAlter}}); err != nil {
		t.Fatal(err)
	}
	if got := list(&lakeformation.ListPermissionsInput{Principal: analyst, Resource: db}); len(got) != 1 || len(got[0].Permissions) != 1 || got[0].Permissions[0] != types.PermissionDescribe {
		t.Fatalf("after revoke: %+v", got)
	}
	if _, err := c.RevokePermissions(ctx, &lakeformation.RevokePermissionsInput{Principal: analyst, Resource: db, Permissions: []types.Permission{types.PermissionDescribe}}); err != nil {
		t.Fatal(err)
	}
	if got := list(&lakeformation.ListPermissionsInput{Principal: analyst, Resource: db}); len(got) != 0 {
		t.Fatalf("revoked all: %+v", got)
	}
	if got := list(&lakeformation.ListPermissionsInput{}); len(got) != 2 {
		t.Fatalf("remaining: %+v", got)
	}
	b, err := c.BatchGrantPermissions(ctx, &lakeformation.BatchGrantPermissionsInput{Entries: []types.BatchPermissionsRequestEntry{{Id: aws.String("1"), Principal: other, Resource: table, Permissions: []types.Permission{types.PermissionSelect}}}})
	if err != nil || len(b.Failures) != 0 || len(list(&lakeformation.ListPermissionsInput{Principal: other})) != 2 {
		t.Fatalf("batch grant: %v %+v", err, b)
	}

	// LF-tags: create, update, list, tag a table
	if _, err := c.CreateLFTag(ctx, &lakeformation.CreateLFTagInput{TagKey: aws.String("env"), TagValues: []string{"dev", "prod"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateLFTag(ctx, &lakeformation.CreateLFTagInput{TagKey: aws.String("env"), TagValues: []string{"x"}}); err == nil {
		t.Fatal("duplicate tag")
	}
	if _, err := c.UpdateLFTag(ctx, &lakeformation.UpdateLFTagInput{TagKey: aws.String("env"), TagValuesToAdd: []string{"test"}, TagValuesToDelete: []string{"dev"}}); err != nil {
		t.Fatal(err)
	}
	g, err := c.GetLFTag(ctx, &lakeformation.GetLFTagInput{TagKey: aws.String("env")})
	if err != nil || !slices.Equal(g.TagValues, []string{"prod", "test"}) || aws.ToString(g.CatalogId) != "000000000000" {
		t.Fatalf("get tag: %v %+v", err, g)
	}
	if l, err := c.ListLFTags(ctx, &lakeformation.ListLFTagsInput{}); err != nil || len(l.LFTags) != 1 {
		t.Fatalf("list tags: %v %+v", err, l)
	}
	if o, err := c.AddLFTagsToResource(ctx, &lakeformation.AddLFTagsToResourceInput{Resource: table, LFTags: []types.LFTagPair{{TagKey: aws.String("env"), TagValues: []string{"prod"}}}}); err != nil || len(o.Failures) != 0 {
		t.Fatalf("tag table: %v %+v", err, o)
	}
	if o, _ := c.AddLFTagsToResource(ctx, &lakeformation.AddLFTagsToResourceInput{Resource: table, LFTags: []types.LFTagPair{{TagKey: aws.String("env"), TagValues: []string{"dev"}}}}); len(o.Failures) != 1 {
		t.Fatalf("a value that was deleted: %+v", o)
	}
	if o, err := c.GetResourceLFTags(ctx, &lakeformation.GetResourceLFTagsInput{Resource: table}); err != nil || len(o.LFTagsOnTable) != 1 || aws.ToString(o.LFTagsOnTable[0].TagKey) != "env" {
		t.Fatalf("table tags: %v %+v", err, o)
	}
	if _, err := c.RemoveLFTagsFromResource(ctx, &lakeformation.RemoveLFTagsFromResourceInput{Resource: table, LFTags: []types.LFTagPair{{TagKey: aws.String("env"), TagValues: []string{"prod"}}}}); err != nil {
		t.Fatal(err)
	}

	// deletes and the typed not-found errors
	if _, err := c.DeleteLFTag(ctx, &lakeformation.DeleteLFTagInput{TagKey: aws.String("env")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.EntityNotFoundException
	if _, err := c.GetLFTag(ctx, &lakeformation.GetLFTagInput{TagKey: aws.String("env")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted tag: %v", err)
	}
	if _, err := c.DeregisterResource(ctx, &lakeformation.DeregisterResourceInput{ResourceArn: aws.String(loc)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeResource(ctx, &lakeformation.DescribeResourceInput{ResourceArn: aws.String(loc)}); !errors.As(err, &nf) {
		t.Fatalf("describe deregistered: %v", err)
	}
}
