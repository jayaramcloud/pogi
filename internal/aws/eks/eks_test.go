package eks_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/aws/smithy-go"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	pogieks "github.com/nrndev/pogi/internal/aws/eks"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func start(t *testing.T) (*core.Hub, *pogieks.Service, string, *eks.Client) {
	var svc *pogieks.Service
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = pogieks.New(h); return svc })
	return h, svc, url, eks.NewFromConfig(awstest.Config(t, url))
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func create(t *testing.T, c *eks.Client, name string) *types.Cluster {
	t.Helper()
	out, err := c.CreateCluster(ctx, &eks.CreateClusterInput{Name: aws.String(name), RoleArn: aws.String("arn:aws:iam::000000000000:role/eks"),
		ResourcesVpcConfig: &types.VpcConfigRequest{SubnetIds: []string{"subnet-1", "subnet-2"}}, Tags: map[string]string{"team": "a"}})
	if err != nil || out.Cluster.Status != types.ClusterStatusCreating {
		t.Fatalf("create %+v, %v", out, err)
	}
	return out.Cluster
}

func TestClusters(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // metadata only
	_, _, _, c := start(t)
	name := aws.String("demo")
	create(t, c, "demo")
	d, err := c.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	cl := d.Cluster
	if cl.Status != types.ClusterStatusActive || *cl.Version != "1.35" || *cl.Arn != "arn:aws:eks:us-east-1:000000000000:cluster/demo" ||
		!strings.HasSuffix(*cl.Endpoint, ".eks.amazonaws.com") || cl.AccessConfig.AuthenticationMode != types.AuthenticationModeConfigMap ||
		!*cl.AccessConfig.BootstrapClusterCreatorAdminPermissions || !cl.ResourcesVpcConfig.EndpointPublicAccess || *cl.ResourcesVpcConfig.VpcId == "" ||
		*cl.KubernetesNetworkConfig.ServiceIpv4Cidr != "10.100.0.0/16" || cl.Identity.Oidc.Issuer == nil || cl.Tags["team"] != "a" ||
		len(cl.ResourcesVpcConfig.SubnetIds) != 2 || cl.CreatedAt == nil {
		t.Fatalf("describe %+v", cl)
	}
	l, err := c.ListClusters(ctx, &eks.ListClustersInput{})
	if err != nil || !slices.Equal(l.Clusters, []string{"demo"}) {
		t.Fatalf("list %v, %v", l, err)
	}

	_, err = c.CreateCluster(ctx, &eks.CreateClusterInput{Name: name, RoleArn: cl.RoleArn, ResourcesVpcConfig: &types.VpcConfigRequest{}})
	wantCode(t, err, "ResourceInUseException")
	_, err = c.CreateCluster(ctx, &eks.CreateClusterInput{Name: aws.String("old"), Version: aws.String("1.20"), RoleArn: cl.RoleArn, ResourcesVpcConfig: &types.VpcConfigRequest{}})
	wantCode(t, err, "InvalidParameterException")
	_, err = c.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: aws.String("nope")})
	wantCode(t, err, "ResourceNotFoundException")

	u, err := c.UpdateClusterConfig(ctx, &eks.UpdateClusterConfigInput{Name: name,
		Logging: &types.Logging{ClusterLogging: []types.LogSetup{{Types: []types.LogType{types.LogTypeApi}, Enabled: aws.Bool(true)}}}})
	if err != nil || u.Update.Status != types.UpdateStatusInProgress || u.Update.Type != types.UpdateTypeLoggingUpdate {
		t.Fatalf("update config %+v, %v", u, err)
	}
	if du, err := c.DescribeUpdate(ctx, &eks.DescribeUpdateInput{Name: name, UpdateId: u.Update.Id}); err != nil || du.Update.Status != types.UpdateStatusSuccessful {
		t.Fatalf("describe update %+v, %v", du, err)
	}
	if _, err := c.UpdateClusterVersion(ctx, &eks.UpdateClusterVersionInput{Name: name, Version: aws.String("1.36")}); err != nil {
		t.Fatal(err)
	}
	if lu, err := c.ListUpdates(ctx, &eks.ListUpdatesInput{Name: name}); err != nil || len(lu.UpdateIds) != 2 {
		t.Fatalf("list updates %+v, %v", lu, err)
	}
	d, _ = c.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: name})
	if *d.Cluster.Version != "1.36" || !*d.Cluster.Logging.ClusterLogging[0].Enabled {
		t.Fatalf("after updates %+v", d.Cluster)
	}

	if _, err := c.TagResource(ctx, &eks.TagResourceInput{ResourceArn: cl.Arn, Tags: map[string]string{"env": "dev"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &eks.UntagResourceInput{ResourceArn: cl.Arn, TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &eks.ListTagsForResourceInput{ResourceArn: cl.Arn}); err != nil || len(tags.Tags) != 1 || tags.Tags["env"] != "dev" {
		t.Fatalf("tags %+v, %v", tags, err)
	}
	_, err = c.ListTagsForResource(ctx, &eks.ListTagsForResourceInput{ResourceArn: aws.String("arn:aws:eks:us-east-1:000000000000:cluster/nope")})
	wantCode(t, err, "NotFoundException")

	del, err := c.DeleteCluster(ctx, &eks.DeleteClusterInput{Name: name})
	if err != nil || del.Cluster.Status != types.ClusterStatusDeleting {
		t.Fatalf("delete %+v, %v", del, err)
	}
	_, err = c.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: name})
	wantCode(t, err, "ResourceNotFoundException")
}

func TestParts(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, _, c := start(t)
	name := aws.String("parts")
	create(t, c, "parts")

	ng := &eks.CreateNodegroupInput{ClusterName: name, NodegroupName: aws.String("ng"), NodeRole: aws.String("arn:aws:iam::000000000000:role/node"),
		Subnets: []string{"subnet-1"}, Labels: map[string]string{"a": "1"},
		ScalingConfig: &types.NodegroupScalingConfig{MinSize: aws.Int32(1), MaxSize: aws.Int32(3), DesiredSize: aws.Int32(2)}}
	if out, err := c.CreateNodegroup(ctx, ng); err != nil || out.Nodegroup.Status != types.NodegroupStatusCreating {
		t.Fatalf("create nodegroup %+v, %v", out, err)
	}
	_, err := c.CreateNodegroup(ctx, ng)
	wantCode(t, err, "ResourceInUseException")
	get := func() *types.Nodegroup {
		out, err := c.DescribeNodegroup(ctx, &eks.DescribeNodegroupInput{ClusterName: name, NodegroupName: aws.String("ng")})
		if err != nil {
			t.Fatal(err)
		}
		return out.Nodegroup
	}
	if n := get(); n.Status != types.NodegroupStatusActive || *n.Version != "1.35" || n.AmiType != types.AMITypesAl2023X8664Standard ||
		*n.DiskSize != 20 || n.InstanceTypes[0] != "t3.medium" || *n.ScalingConfig.DesiredSize != 2 || len(n.Resources.AutoScalingGroups) != 1 ||
		!strings.HasPrefix(*n.NodegroupArn, "arn:aws:eks:us-east-1:000000000000:nodegroup/parts/ng/") {
		t.Fatalf("nodegroup %+v", n)
	}
	u, err := c.UpdateNodegroupConfig(ctx, &eks.UpdateNodegroupConfigInput{ClusterName: name, NodegroupName: aws.String("ng"),
		ScalingConfig: &types.NodegroupScalingConfig{DesiredSize: aws.Int32(3)},
		Labels:        &types.UpdateLabelsPayload{AddOrUpdateLabels: map[string]string{"b": "2"}, RemoveLabels: []string{"a"}},
		Taints:        &types.UpdateTaintsPayload{AddOrUpdateTaints: []types.Taint{{Key: aws.String("k"), Value: aws.String("v"), Effect: types.TaintEffectNoSchedule}}}})
	if err != nil || u.Update.Status != types.UpdateStatusInProgress {
		t.Fatalf("update nodegroup %+v, %v", u, err)
	}
	if du, err := c.DescribeUpdate(ctx, &eks.DescribeUpdateInput{Name: name, UpdateId: u.Update.Id, NodegroupName: aws.String("ng")}); err != nil || du.Update.Status != types.UpdateStatusSuccessful {
		t.Fatalf("describe nodegroup update %+v, %v", du, err)
	}
	_, err = c.DescribeUpdate(ctx, &eks.DescribeUpdateInput{Name: name, UpdateId: u.Update.Id})
	wantCode(t, err, "ResourceNotFoundException")
	if _, err := c.UpdateNodegroupVersion(ctx, &eks.UpdateNodegroupVersionInput{ClusterName: name, NodegroupName: aws.String("ng"), Version: aws.String("1.36")}); err != nil {
		t.Fatal(err)
	}
	if n := get(); *n.ScalingConfig.DesiredSize != 3 || *n.ScalingConfig.MaxSize != 3 || len(n.Labels) != 1 || n.Labels["b"] != "2" ||
		len(n.Taints) != 1 || *n.Version != "1.36" || !strings.HasPrefix(*n.ReleaseVersion, "1.36.") {
		t.Fatalf("updated nodegroup %+v", n)
	}
	if l, err := c.ListNodegroups(ctx, &eks.ListNodegroupsInput{ClusterName: name}); err != nil || !slices.Equal(l.Nodegroups, []string{"ng"}) {
		t.Fatalf("list nodegroups %+v, %v", l, err)
	}
	_, err = c.DeleteCluster(ctx, &eks.DeleteClusterInput{Name: name})
	wantCode(t, err, "ResourceInUseException")
	if out, err := c.DeleteNodegroup(ctx, &eks.DeleteNodegroupInput{ClusterName: name, NodegroupName: aws.String("ng")}); err != nil || out.Nodegroup.Status != types.NodegroupStatusDeleting {
		t.Fatalf("delete nodegroup %+v, %v", out, err)
	}
	_, err = c.DescribeNodegroup(ctx, &eks.DescribeNodegroupInput{ClusterName: name, NodegroupName: aws.String("ng")})
	wantCode(t, err, "ResourceNotFoundException")

	// access entries need the API authentication mode
	role := aws.String("arn:aws:iam::000000000000:role/dev")
	admin := aws.String("arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy")
	_, err = c.CreateAccessEntry(ctx, &eks.CreateAccessEntryInput{ClusterName: name, PrincipalArn: role})
	wantCode(t, err, "InvalidRequestException")
	if _, err := c.UpdateClusterConfig(ctx, &eks.UpdateClusterConfigInput{Name: name, AccessConfig: &types.UpdateAccessConfigRequest{AuthenticationMode: types.AuthenticationModeApiAndConfigMap}}); err != nil {
		t.Fatal(err)
	}
	e, err := c.CreateAccessEntry(ctx, &eks.CreateAccessEntryInput{ClusterName: name, PrincipalArn: role, KubernetesGroups: []string{"devs"}})
	if err != nil || *e.AccessEntry.Type != "STANDARD" || *e.AccessEntry.Username != *role ||
		!strings.HasPrefix(*e.AccessEntry.AccessEntryArn, "arn:aws:eks:us-east-1:000000000000:access-entry/parts/role/000000000000/dev/") {
		t.Fatalf("create access entry %+v, %v", e, err)
	}
	if _, err := c.AssociateAccessPolicy(ctx, &eks.AssociateAccessPolicyInput{ClusterName: name, PrincipalArn: role, PolicyArn: admin,
		AccessScope: &types.AccessScope{Type: types.AccessScopeTypeCluster}}); err != nil {
		t.Fatal(err)
	}
	if p, err := c.ListAssociatedAccessPolicies(ctx, &eks.ListAssociatedAccessPoliciesInput{ClusterName: name, PrincipalArn: role}); err != nil ||
		len(p.AssociatedAccessPolicies) != 1 || *p.AssociatedAccessPolicies[0].PolicyArn != *admin {
		t.Fatalf("associated policies %+v, %v", p, err)
	}
	if l, err := c.ListAccessEntries(ctx, &eks.ListAccessEntriesInput{ClusterName: name, AssociatedPolicyArn: admin}); err != nil || !slices.Equal(l.AccessEntries, []string{*role}) {
		t.Fatalf("list access entries %+v, %v", l, err)
	}
	if de, err := c.DescribeAccessEntry(ctx, &eks.DescribeAccessEntryInput{ClusterName: name, PrincipalArn: role}); err != nil || de.AccessEntry.KubernetesGroups[0] != "devs" {
		t.Fatalf("describe access entry %+v, %v", de, err)
	}
	if _, err := c.DisassociateAccessPolicy(ctx, &eks.DisassociateAccessPolicyInput{ClusterName: name, PrincipalArn: role, PolicyArn: admin}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DisassociateAccessPolicy(ctx, &eks.DisassociateAccessPolicyInput{ClusterName: name, PrincipalArn: role, PolicyArn: admin})
	wantCode(t, err, "ResourceNotFoundException")
	if _, err := c.DeleteAccessEntry(ctx, &eks.DeleteAccessEntryInput{ClusterName: name, PrincipalArn: role}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeAccessEntry(ctx, &eks.DescribeAccessEntryInput{ClusterName: name, PrincipalArn: role})
	wantCode(t, err, "ResourceNotFoundException")
	if p, err := c.ListAccessPolicies(ctx, &eks.ListAccessPoliciesInput{}); err != nil || len(p.AccessPolicies) != 4 {
		t.Fatalf("access policies %+v, %v", p, err)
	}

	if _, err := c.CreateFargateProfile(ctx, &eks.CreateFargateProfileInput{ClusterName: name, FargateProfileName: aws.String("fp"),
		PodExecutionRoleArn: role, Selectors: []types.FargateProfileSelector{{Namespace: aws.String("default")}}}); err != nil {
		t.Fatal(err)
	}
	if fp, err := c.DescribeFargateProfile(ctx, &eks.DescribeFargateProfileInput{ClusterName: name, FargateProfileName: aws.String("fp")}); err != nil ||
		fp.FargateProfile.Status != types.FargateProfileStatusActive || *fp.FargateProfile.Selectors[0].Namespace != "default" {
		t.Fatalf("fargate profile %+v, %v", fp, err)
	}
	if _, err := c.DeleteFargateProfile(ctx, &eks.DeleteFargateProfileInput{ClusterName: name, FargateProfileName: aws.String("fp")}); err != nil {
		t.Fatal(err)
	}

	a, err := c.CreateAddon(ctx, &eks.CreateAddonInput{ClusterName: name, AddonName: aws.String("vpc-cni"), Tags: map[string]string{"x": "y"}})
	if err != nil || a.Addon.Status != types.AddonStatusCreating || *a.Addon.AddonVersion == "" {
		t.Fatalf("create addon %+v, %v", a, err)
	}
	ua, err := c.UpdateAddon(ctx, &eks.UpdateAddonInput{ClusterName: name, AddonName: aws.String("vpc-cni"), ConfigurationValues: aws.String(`{"env":{}}`)})
	if err != nil {
		t.Fatal(err)
	}
	if du, err := c.DescribeUpdate(ctx, &eks.DescribeUpdateInput{Name: name, UpdateId: ua.Update.Id, AddonName: aws.String("vpc-cni")}); err != nil || du.Update.Status != types.UpdateStatusSuccessful {
		t.Fatalf("addon update %+v, %v", du, err)
	}
	if da, err := c.DescribeAddon(ctx, &eks.DescribeAddonInput{ClusterName: name, AddonName: aws.String("vpc-cni")}); err != nil ||
		da.Addon.Status != types.AddonStatusActive || *da.Addon.ConfigurationValues != `{"env":{}}` {
		t.Fatalf("addon %+v, %v", da, err)
	}
	if tags, err := c.ListTagsForResource(ctx, &eks.ListTagsForResourceInput{ResourceArn: a.Addon.AddonArn}); err != nil || tags.Tags["x"] != "y" {
		t.Fatalf("addon tags %+v, %v", tags, err)
	}
	if v, err := c.DescribeAddonVersions(ctx, &eks.DescribeAddonVersionsInput{AddonName: aws.String("coredns")}); err != nil || len(v.Addons) != 1 {
		t.Fatalf("addon versions %+v, %v", v, err)
	}

	pia, err := c.CreatePodIdentityAssociation(ctx, &eks.CreatePodIdentityAssociationInput{ClusterName: name, Namespace: aws.String("default"),
		ServiceAccount: aws.String("app"), RoleArn: role})
	if err != nil || !strings.HasPrefix(*pia.Association.AssociationId, "a-") {
		t.Fatalf("pod identity %+v, %v", pia, err)
	}
	for ns, want := range map[string]int{"default": 1, "other": 0} {
		if l, err := c.ListPodIdentityAssociations(ctx, &eks.ListPodIdentityAssociationsInput{ClusterName: name, Namespace: aws.String(ns)}); err != nil || len(l.Associations) != want {
			t.Fatalf("pod identities in %s: %+v, %v", ns, l, err)
		}
	}
	if _, err := c.DeletePodIdentityAssociation(ctx, &eks.DeletePodIdentityAssociationInput{ClusterName: name, AssociationId: pia.Association.AssociationId}); err != nil {
		t.Fatal(err)
	}

	idp := &types.IdentityProviderConfig{Type: aws.String("oidc"), Name: aws.String("okta")}
	if _, err := c.AssociateIdentityProviderConfig(ctx, &eks.AssociateIdentityProviderConfigInput{ClusterName: name,
		Oidc: &types.OidcIdentityProviderConfigRequest{IdentityProviderConfigName: idp.Name, IssuerUrl: aws.String("https://okta.example.com"), ClientId: aws.String("k8s")}}); err != nil {
		t.Fatal(err)
	}
	if di, err := c.DescribeIdentityProviderConfig(ctx, &eks.DescribeIdentityProviderConfigInput{ClusterName: name, IdentityProviderConfig: idp}); err != nil ||
		di.IdentityProviderConfig.Oidc.Status != types.ConfigStatusActive || *di.IdentityProviderConfig.Oidc.ClientId != "k8s" {
		t.Fatalf("identity provider %+v, %v", di, err)
	}
	if _, err := c.DisassociateIdentityProviderConfig(ctx, &eks.DisassociateIdentityProviderConfigInput{ClusterName: name, IdentityProviderConfig: idp}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeIdentityProviderConfig(ctx, &eks.DescribeIdentityProviderConfigInput{ClusterName: name, IdentityProviderConfig: idp})
	wantCode(t, err, "ResourceNotFoundException")
}

// token makes the token `aws eks get-token` makes: a presigned STS
// GetCallerIdentity URL that signs the header x-k8s-aws-id.
func token(t *testing.T, url, cluster, key, secret string, at time.Time) string {
	req, _ := http.NewRequest("GET", url+"/?Action=GetCallerIdentity&Version=2011-06-15&X-Amz-Expires=60", nil)
	req.Header.Set("x-k8s-aws-id", cluster)
	u, _, err := v4.NewSigner().PresignHTTP(ctx, aws.Credentials{AccessKeyID: key, SecretAccessKey: secret}, req,
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "sts", "us-east-1", at)
	if err != nil {
		t.Fatal(err)
	}
	return "k8s-aws-v1." + base64.RawURLEncoding.EncodeToString([]byte(u))
}

func TestTokenReview(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	h, _, url, c := start(t)
	create(t, c, "authn")
	review := func(token string) (bool, string, []string) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"apiVersion": "authentication.k8s.io/v1beta1", "kind": "TokenReview", "spec": map[string]any{"token": token}})
		resp, err := http.Post(url+"/_aws/eks/authn/000000000000/us-east-1/authn?timeout=30s", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var tr struct {
			APIVersion string
			Status     struct {
				Authenticated bool
				User          struct {
					Username string
					Groups   []string
				}
			}
		}
		if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil || tr.APIVersion != "authentication.k8s.io/v1beta1" {
			t.Fatalf("review %+v, %v", tr, err)
		}
		return tr.Status.Authenticated, tr.Status.User.Username, tr.Status.User.Groups
	}
	now := time.Now()
	if ok, user, groups := review(token(t, url, "authn", "test", "test", now)); !ok || user != "arn:aws:iam::000000000000:root" || !slices.Equal(groups, []string{"system:masters"}) {
		t.Fatalf("root: %v %s %v", ok, user, groups)
	}
	for why, tok := range map[string]string{
		"another cluster": token(t, url, "other", "test", "test", now), "wrong secret": token(t, url, "authn", "test", "nope", now),
		"expired": token(t, url, "authn", "test", "test", now.Add(-16*time.Minute)), "unknown key": token(t, url, "authn", "AKIANOBODY", "x", now),
		"another account's root": token(t, url, "authn", "111111111111", "test", now), "not a token": "k8s-aws-v1.bm9wZQ",
	} {
		if ok, _, _ := review(tok); ok {
			t.Errorf("%s: authenticated", why)
		}
	}

	// an IAM user gets in through an access entry
	h.AddIdentity("AKIADEV", core.Identity{Account: "000000000000", ARN: "arn:aws:iam::000000000000:user/dev", UserID: "AIDADEV", Secret: "devsecret"})
	dev := token(t, url, "authn", "AKIADEV", "devsecret", now)
	if ok, _, _ := review(dev); ok {
		t.Fatal("a user without an access entry is let in")
	}
	name, arn := aws.String("authn"), aws.String("arn:aws:iam::000000000000:user/dev")
	if _, err := c.UpdateClusterConfig(ctx, &eks.UpdateClusterConfigInput{Name: name, AccessConfig: &types.UpdateAccessConfigRequest{AuthenticationMode: types.AuthenticationModeApi}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateAccessEntry(ctx, &eks.CreateAccessEntryInput{ClusterName: name, PrincipalArn: arn, KubernetesGroups: []string{"devs"}}); err != nil {
		t.Fatal(err)
	}
	if ok, user, groups := review(dev); !ok || user != *arn || !slices.Equal(groups, []string{"devs"}) {
		t.Fatalf("access entry: %v %s %v", ok, user, groups)
	}
	if _, err := c.AssociateAccessPolicy(ctx, &eks.AssociateAccessPolicyInput{ClusterName: name, PrincipalArn: arn, AccessScope: &types.AccessScope{Type: types.AccessScopeTypeCluster},
		PolicyArn: aws.String("arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy")}); err != nil {
		t.Fatal(err)
	}
	if _, _, groups := review(dev); !slices.Equal(groups, []string{"devs", "system:masters"}) {
		t.Fatalf("cluster admin policy: %v", groups)
	}
}

// TestLoad: a persisted cluster whose engine is gone turns FAILED with a note.
func TestLoad(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, svc, _, c := start(t)
	state := `{"clusters":{"000000000000/us-east-1/old":{"doc":{"name":"old","status":"ACTIVE"},"container":"pogi-eks-old","token":"t"},
		"000000000000/us-east-1/meta":{"doc":{"name":"meta","status":"ACTIVE"}}}}`
	if err := svc.Load([]byte(state)); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]types.ClusterStatus{"old": types.ClusterStatusFailed, "meta": types.ClusterStatusActive} {
		if d, err := c.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: aws.String(name)}); err != nil || d.Cluster.Status != want {
			t.Fatalf("%s: %+v, %v", name, d, err)
		}
	}
	if b, _ := json.Marshal(svc.Inspect("Clusters", "old")); !strings.Contains(string(b), "Docker is not available") || strings.Contains(string(b), `"token"`) {
		t.Fatalf("inspect %s", b)
	}
}

// TestKubectl runs a cluster on k3s and reaches it the EKS way:
// `aws eks update-kubeconfig`, then kubectl authenticated by `aws eks get-token`.
func TestKubectl(t *testing.T) {
	for _, bin := range []string{"kubectl", "aws"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("needs %s", bin)
		}
	}
	h, svc, url, c := start(t)
	if testing.Short() || !h.Docker().Available() {
		t.Skip("needs a running container engine")
	}
	name := "e2e-" + core.RandHex(3)
	create(t, c, name)
	active := func() *types.Cluster {
		for end := time.Now().Add(4 * time.Minute); ; time.Sleep(time.Second) {
			d, err := c.DescribeCluster(ctx, &eks.DescribeClusterInput{Name: aws.String(name)})
			if err != nil {
				t.Fatal(err)
			}
			if d.Cluster.Status == types.ClusterStatusActive {
				return d.Cluster
			}
			if d.Cluster.Status == types.ClusterStatusFailed || time.Now().After(end) {
				b, _ := json.Marshal(svc.Inspect("Clusters", name))
				t.Fatalf("cluster %s: %s", d.Cluster.Status, b)
			}
		}
	}
	cl := active()
	dir := t.TempDir()
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "AWS_") })
	env = append(env, "AWS_ENDPOINT_URL="+url, "AWS_ACCESS_KEY_ID=test", "AWS_SECRET_ACCESS_KEY=test", "AWS_REGION=us-east-1",
		"AWS_CONFIG_FILE="+filepath.Join(dir, "none"), "AWS_SHARED_CREDENTIALS_FILE="+filepath.Join(dir, "none"))
	run := func(name string, args ...string) (string, error) {
		cmd := exec.Command(name, args...)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	kubeconfig := filepath.Join(dir, "kubeconfig")
	nodes := func() {
		t.Helper()
		if out, err := run("aws", "eks", "update-kubeconfig", "--name", name, "--kubeconfig", kubeconfig); err != nil {
			t.Fatalf("update-kubeconfig: %v\n%s", err, out)
		}
		out, err := run("kubectl", "--kubeconfig", kubeconfig, "get", "nodes", "--no-headers")
		if lines := strings.Split(strings.TrimSpace(out), "\n"); err != nil || len(lines) != 1 || !strings.Contains(lines[0], " Ready ") {
			t.Fatalf("kubectl get nodes: %v\n%s", err, out)
		}
	}
	nodes()

	// a token for another cluster is refused
	tok, err := run("aws", "eks", "get-token", "--cluster-name", "wrong", "--query", "status.token", "--output", "text")
	if err != nil {
		t.Fatalf("get-token: %v\n%s", err, tok)
	}
	ca, _ := base64.StdEncoding.DecodeString(*cl.CertificateAuthority.Data)
	os.WriteFile(filepath.Join(dir, "ca.crt"), ca, 0o600)
	os.WriteFile(filepath.Join(dir, "empty"), nil, 0o600)
	out, err := run("kubectl", "--kubeconfig", filepath.Join(dir, "empty"), "--server", *cl.Endpoint, "--certificate-authority", filepath.Join(dir, "ca.crt"),
		"--token", strings.TrimSpace(tok), "get", "nodes")
	if err == nil || !strings.Contains(out, "Unauthorized") {
		t.Fatalf("a token for another cluster: %v\n%s", err, out)
	}

	// persisted state restarts the container
	b, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	active()
	nodes()

	if _, err := c.DeleteCluster(ctx, &eks.DeleteClusterInput{Name: aws.String(name)}); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if _, err := h.Docker().Inspect(ctx, "pogi-eks-"+name); err != nil {
			break
		}
		if time.Now().After(end) {
			t.Fatal("DeleteCluster leaves its container")
		}
	}

	// a persisted cluster whose container is gone turns FAILED
	if err := svc.Load([]byte(`{"clusters":{"000000000000/us-east-1/` + name + `":{"doc":{"name":"` + name + `","version":"1.35"},"container":"pogi-eks-` + name + `"}}}`)); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if b, _ := json.Marshal(svc.Inspect("Clusters", name)); strings.Contains(string(b), `"FAILED"`) {
			break
		}
		if time.Now().After(end) {
			t.Fatal("a cluster without its container stays")
		}
	}
}

// floci #1942: with EC2 enabled, a cluster needs real subnets of one VPC, and shows that VPC.
func TestClusterSubnets(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // metadata only
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return pogieks.New(h) }, func(h *core.Hub) core.Service { return ec2svc.New(h) })
	c, e := eks.NewFromConfig(awstest.Config(t, url)), ec2.NewFromConfig(awstest.Config(t, url))
	role := aws.String("arn:aws:iam::000000000000:role/eks")
	_, err := c.CreateCluster(ctx, &eks.CreateClusterInput{Name: aws.String("bad"), RoleArn: role, ResourcesVpcConfig: &types.VpcConfigRequest{SubnetIds: []string{"subnet-1", "subnet-2"}}})
	wantCode(t, err, "InvalidParameterException")
	vpc := func(cidr string) *ec2.CreateVpcOutput {
		o, err := e.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String(cidr)})
		if err != nil {
			t.Fatal(err)
		}
		return o
	}
	subnet := func(v *ec2.CreateVpcOutput, cidr string) string {
		o, err := e.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: v.Vpc.VpcId, CidrBlock: aws.String(cidr)})
		if err != nil {
			t.Fatal(err)
		}
		return *o.Subnet.SubnetId
	}
	v1, v2 := vpc("10.70.0.0/16"), vpc("10.71.0.0/16")
	a, b, other := subnet(v1, "10.70.1.0/24"), subnet(v1, "10.70.2.0/24"), subnet(v2, "10.71.1.0/24")
	_, err = c.CreateCluster(ctx, &eks.CreateClusterInput{Name: aws.String("mixed"), RoleArn: role, ResourcesVpcConfig: &types.VpcConfigRequest{SubnetIds: []string{a, other}}})
	wantCode(t, err, "InvalidParameterException")
	o, err := c.CreateCluster(ctx, &eks.CreateClusterInput{Name: aws.String("good"), RoleArn: role, ResourcesVpcConfig: &types.VpcConfigRequest{SubnetIds: []string{a, b}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(o.Cluster.ResourcesVpcConfig.VpcId); got != *v1.Vpc.VpcId {
		t.Fatalf("vpcId = %q, want %q", got, *v1.Vpc.VpcId)
	}
}
