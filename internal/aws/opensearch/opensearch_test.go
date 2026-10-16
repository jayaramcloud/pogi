package opensearch_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	es "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	estypes "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	aos "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"

	"github.com/nrndev/pogi/internal/aws/opensearch"
	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/docker/dockertest"
)

var ctx = context.Background()

func setup(t *testing.T) (*opensearch.Service, *aos.Client, *es.Client, string) {
	var svc *opensearch.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = opensearch.New(h); return svc })
	cfg := awstest.Config(t, url)
	return svc, aos.NewFromConfig(cfg), es.NewFromConfig(cfg), url
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(timeout); !cond(); time.Sleep(200 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// must fails the test on err: must(client.Op(…))(t).
func must[T any](v T, err error) func(*testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func isErr[E error](t *testing.T, err error) {
	t.Helper()
	var e E
	if !errors.As(err, &e) {
		t.Fatalf("want %T, got %v", e, err)
	}
}

// endpointClient sends requests for any host to pogi, the way *.localhost
// resolves to it.
func endpointClient(url string) *http.Client {
	pogi := core.Hostport(url)
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, pogi)
	}}}
}

// call sends a request to a domain endpoint, signed for es when sign is set.
func call(t *testing.T, hc *http.Client, method, url, body string, sign bool) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if sign {
		sum := sha256.Sum256([]byte(body))
		req.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
		awstest.Sign(t, req, "es")
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	b, _ := io.ReadAll(resp.Body)
	json.Unmarshal(b, &out)
	return resp.StatusCode, out
}

func describe(t *testing.T, client *aos.Client, name string) *types.DomainStatus {
	t.Helper()
	return must(client.DescribeDomain(ctx, &aos.DescribeDomainInput{DomainName: aws.String(name)}))(t).DomainStatus
}

func TestDomainLifecycle(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	svc, client, _, url := setup(t)
	out := must(client.CreateDomain(ctx, &aos.CreateDomainInput{
		DomainName: aws.String("logs"), EngineVersion: aws.String("OpenSearch_2.19"),
		ClusterConfig:  &types.ClusterConfig{InstanceType: types.OpenSearchPartitionInstanceTypeT3SmallSearch, InstanceCount: aws.Int32(2)},
		EBSOptions:     &types.EBSOptions{EBSEnabled: aws.Bool(true), VolumeSize: aws.Int32(20)},
		AccessPolicies: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
		AdvancedSecurityOptions: &types.AdvancedSecurityOptionsInput{Enabled: aws.Bool(false),
			MasterUserOptions: &types.MasterUserOptions{MasterUserName: aws.String("admin"), MasterUserPassword: aws.String("Sup3r-secret")}},
		TagList: []types.Tag{{Key: aws.String("team"), Value: aws.String("search")}},
	}))(t)
	arn := core.ARN("es", "us-east-1", core.DefaultAccount, "domain/logs")
	if st := out.DomainStatus; !aws.ToBool(st.Processing) || !aws.ToBool(st.Created) || aws.ToBool(st.Deleted) || st.Endpoint != nil ||
		aws.ToString(st.ARN) != arn || st.DomainProcessingStatus != types.DomainProcessingStatusTypeCreating {
		t.Fatalf("created %+v", st)
	}

	st := describe(t, client, "logs")
	_, port, _ := net.SplitHostPort(core.Hostport(url))
	ep := aws.ToString(st.Endpoint)
	if aws.ToBool(st.Processing) || !strings.HasPrefix(ep, "logs-") || !strings.HasSuffix(ep, ".us-east-1.opensearch.localhost:"+port) ||
		aws.ToString(st.DomainId) != core.DefaultAccount+"/logs" || aws.ToString(st.EngineVersion) != "OpenSearch_2.19" ||
		st.DomainProcessingStatus != types.DomainProcessingStatusTypeActive {
		t.Fatalf("described %+v", st)
	}
	if c := st.ClusterConfig; c.InstanceType != types.OpenSearchPartitionInstanceTypeT3SmallSearch || aws.ToInt32(c.InstanceCount) != 2 ||
		c.DedicatedMasterEnabled == nil || st.EBSOptions.VolumeType != types.VolumeTypeGp2 || aws.ToInt32(st.EBSOptions.VolumeSize) != 20 ||
		st.CognitoOptions == nil || st.AdvancedOptions["rest.action.multi.allow_explicit_index"] != "true" || st.AutoTuneOptions.State != types.AutoTuneStateDisabled {
		t.Fatalf("options %+v %+v", c, st)
	}
	if b, _ := svc.Save(); strings.Contains(string(b), "Sup3r-secret") {
		t.Fatal("the master password is kept")
	}

	dc := must(client.DescribeDomainConfig(ctx, &aos.DescribeDomainConfigInput{DomainName: aws.String("logs")}))(t).DomainConfig
	if aws.ToString(dc.EngineVersion.Options) != "OpenSearch_2.19" || dc.EngineVersion.Status.State != types.OptionStateActive ||
		dc.EngineVersion.Status.UpdateVersion != 1 || dc.EngineVersion.Status.CreationDate == nil ||
		aws.ToInt32(dc.ClusterConfig.Options.InstanceCount) != 2 || aws.ToString(dc.AccessPolicies.Options) == "" ||
		dc.AutoTuneOptions.Options.DesiredState != types.AutoTuneDesiredStateDisabled || dc.AutoTuneOptions.Status.State != types.AutoTuneStateDisabled {
		t.Fatalf("config %+v", dc)
	}

	up := must(client.UpdateDomainConfig(ctx, &aos.UpdateDomainConfigInput{DomainName: aws.String("logs"),
		ClusterConfig:   &types.ClusterConfig{InstanceCount: aws.Int32(3)},
		AdvancedOptions: map[string]string{"indices.query.bool.max_clause_count": "2048"}}))(t).DomainConfig
	if c := up.ClusterConfig.Options; aws.ToInt32(c.InstanceCount) != 3 || c.InstanceType != types.OpenSearchPartitionInstanceTypeT3SmallSearch ||
		up.ClusterConfig.Status.UpdateVersion != 2 || up.AdvancedOptions.Options["indices.query.bool.max_clause_count"] != "2048" {
		t.Fatalf("updated %+v", up.ClusterConfig)
	}
	progress := must(client.DescribeDomainChangeProgress(ctx, &aos.DescribeDomainChangeProgressInput{DomainName: aws.String("logs")}))(t).ChangeProgressStatus
	if aws.ToString(progress.ChangeId) != aws.ToString(describe(t, client, "logs").ChangeProgressDetails.ChangeId) ||
		progress.Status != types.OverallChangeStatusCompleted || !slices.Equal(progress.CompletedProperties, []string{"AdvancedOptions", "ClusterConfig"}) {
		t.Fatalf("progress %+v", progress)
	}

	names := must(client.ListDomainNames(ctx, &aos.ListDomainNamesInput{}))(t).DomainNames
	if len(names) != 1 || aws.ToString(names[0].DomainName) != "logs" || names[0].EngineType != types.EngineTypeOpenSearch {
		t.Fatalf("names %+v", names)
	}
	if n := must(client.ListDomainNames(ctx, &aos.ListDomainNamesInput{EngineType: types.EngineTypeElasticsearch}))(t).DomainNames; len(n) != 0 {
		t.Fatalf("elasticsearch names %+v", n)
	}
	if l := must(client.DescribeDomains(ctx, &aos.DescribeDomainsInput{DomainNames: []string{"logs", "nope"}}))(t).DomainStatusList; len(l) != 1 {
		t.Fatalf("describe domains %+v", l)
	}

	must(client.AddTags(ctx, &aos.AddTagsInput{ARN: aws.String(arn), TagList: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))(t)
	must(client.RemoveTags(ctx, &aos.RemoveTagsInput{ARN: aws.String(arn), TagKeys: []string{"team"}}))(t)
	if tags := must(client.ListTags(ctx, &aos.ListTagsInput{ARN: aws.String(arn)}))(t).TagList; len(tags) != 1 || aws.ToString(tags[0].Key) != "env" {
		t.Fatalf("tags %+v", tags)
	}

	// without Docker the endpoint answers, with a clear error
	hc := endpointClient(url)
	if code, body := call(t, hc, "GET", "http://"+ep+"/", "", true); code != 503 || !strings.Contains(fmt.Sprint(body["error"]), "Docker is not available") {
		t.Fatalf("endpoint without Docker: %d %v", code, body)
	}
	if code, _ := call(t, hc, "GET", "http://other-1.us-east-1.opensearch.localhost/", "", false); code != 404 {
		t.Fatalf("unknown endpoint: %d", code)
	}
	if v := svc.Inspect("Domains", "logs").(map[string]any); v["pogi"].(rds.Server).Note != rds.NoDocker || len(svc.Resources()[0].Rows) != 1 {
		t.Fatalf("inspect %+v", v)
	}

	b := must(svc.Save())(t)
	svc.Reset()
	_, err := client.DescribeDomain(ctx, &aos.DescribeDomainInput{DomainName: aws.String("logs")})
	isErr[*types.ResourceNotFoundException](t, err)
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(describe(t, client, "logs").Endpoint); got != ep {
		t.Fatalf("endpoint after load %q, want %q", got, ep)
	}

	del := must(client.DeleteDomain(ctx, &aos.DeleteDomainInput{DomainName: aws.String("logs")}))(t).DomainStatus
	if !aws.ToBool(del.Deleted) || !aws.ToBool(del.Processing) {
		t.Fatalf("deleted %+v", del)
	}
	_, err = client.DescribeDomainConfig(ctx, &aos.DescribeDomainConfigInput{DomainName: aws.String("logs")})
	isErr[*types.ResourceNotFoundException](t, err)
}

func TestLegacyAPI(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, client, legacy, url := setup(t)
	st := must(legacy.CreateElasticsearchDomain(ctx, &es.CreateElasticsearchDomainInput{DomainName: aws.String("books"),
		ElasticsearchVersion:       aws.String("7.10"),
		ElasticsearchClusterConfig: &estypes.ElasticsearchClusterConfig{InstanceType: estypes.ESPartitionInstanceTypeM5LargeElasticsearch},
		TagList:                    []estypes.Tag{{Key: aws.String("app"), Value: aws.String("shop")}},
	}))(t).DomainStatus
	if aws.ToString(st.ElasticsearchVersion) != "7.10" || !aws.ToBool(st.Processing) {
		t.Fatalf("created %+v", st)
	}
	_, port, _ := net.SplitHostPort(core.Hostport(url))
	st = must(legacy.DescribeElasticsearchDomain(ctx, &es.DescribeElasticsearchDomainInput{DomainName: aws.String("books")}))(t).DomainStatus
	if ep := aws.ToString(st.Endpoint); !strings.HasSuffix(ep, ".us-east-1.es.localhost:"+port) ||
		st.ElasticsearchClusterConfig.InstanceType != estypes.ESPartitionInstanceTypeM5LargeElasticsearch || aws.ToInt32(st.ElasticsearchClusterConfig.InstanceCount) != 1 {
		t.Fatalf("described %+v", st)
	}
	if o := describe(t, client, "books"); aws.ToString(o.EngineVersion) != "Elasticsearch_7.10" || o.ClusterConfig.InstanceType != "m5.large.elasticsearch" {
		t.Fatalf("through the OpenSearch API %+v", o)
	}

	cfg := must(legacy.UpdateElasticsearchDomainConfig(ctx, &es.UpdateElasticsearchDomainConfigInput{DomainName: aws.String("books"),
		ElasticsearchClusterConfig: &estypes.ElasticsearchClusterConfig{InstanceCount: aws.Int32(2)}}))(t).DomainConfig
	if aws.ToInt32(cfg.ElasticsearchClusterConfig.Options.InstanceCount) != 2 || aws.ToString(cfg.ElasticsearchVersion.Options) != "7.10" {
		t.Fatalf("updated %+v", cfg)
	}
	cfg = must(legacy.DescribeElasticsearchDomainConfig(ctx, &es.DescribeElasticsearchDomainConfigInput{DomainName: aws.String("books")}))(t).DomainConfig
	if cfg.ElasticsearchClusterConfig.Status.State != estypes.OptionStateActive || cfg.ElasticsearchClusterConfig.Status.UpdateVersion != 2 {
		t.Fatalf("config %+v", cfg.ElasticsearchClusterConfig)
	}

	vs := must(legacy.ListElasticsearchVersions(ctx, &es.ListElasticsearchVersionsInput{}))(t).ElasticsearchVersions
	if !slices.Contains(vs, "7.10") || !slices.Contains(vs, "OpenSearch_2.19") {
		t.Fatalf("versions %v", vs)
	}
	compat := must(legacy.GetCompatibleElasticsearchVersions(ctx, &es.GetCompatibleElasticsearchVersionsInput{DomainName: aws.String("books")}))(t).CompatibleElasticsearchVersions
	if len(compat) != 1 || aws.ToString(compat[0].SourceVersion) != "7.10" || !slices.Contains(compat[0].TargetVersions, "OpenSearch_1.3") {
		t.Fatalf("compatible %+v", compat)
	}
	names := must(legacy.ListDomainNames(ctx, &es.ListDomainNamesInput{}))(t).DomainNames
	if len(names) != 1 || names[0].EngineType != estypes.EngineTypeElasticsearch {
		t.Fatalf("names %+v", names)
	}
	if l := must(legacy.DescribeElasticsearchDomains(ctx, &es.DescribeElasticsearchDomainsInput{DomainNames: []string{"books"}}))(t).DomainStatusList; len(l) != 1 {
		t.Fatalf("describe domains %+v", l)
	}
	arn := aws.ToString(st.ARN)
	if tags := must(legacy.ListTags(ctx, &es.ListTagsInput{ARN: aws.String(arn)}))(t).TagList; len(tags) != 1 || aws.ToString(tags[0].Value) != "shop" {
		t.Fatalf("tags %+v", tags)
	}
	if del := must(legacy.DeleteElasticsearchDomain(ctx, &es.DeleteElasticsearchDomainInput{DomainName: aws.String("books")}))(t).DomainStatus; !aws.ToBool(del.Deleted) {
		t.Fatalf("deleted %+v", del)
	}
	_, err := legacy.DescribeElasticsearchDomain(ctx, &es.DescribeElasticsearchDomainInput{DomainName: aws.String("books")})
	isErr[*estypes.ResourceNotFoundException](t, err)
}

func TestVersionsAndErrors(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, client, _, _ := setup(t)
	vs := must(client.ListVersions(ctx, &aos.ListVersionsInput{}))(t).Versions
	if !slices.Contains(vs, "OpenSearch_2.19") || !slices.Contains(vs, "OpenSearch_1.3") || !slices.Contains(vs, "Elasticsearch_7.10") {
		t.Fatalf("versions %v", vs)
	}
	if all := must(client.GetCompatibleVersions(ctx, &aos.GetCompatibleVersionsInput{}))(t).CompatibleVersions; len(all) != len(vs)-1 {
		t.Fatalf("compatible versions %+v", all)
	}
	st := must(client.CreateDomain(ctx, &aos.CreateDomainInput{DomainName: aws.String("plain")}))(t).DomainStatus
	if aws.ToString(st.EngineVersion) != vs[0] || !strings.HasPrefix(vs[0], "OpenSearch_") {
		t.Fatalf("default version %q of %v", aws.ToString(st.EngineVersion), vs)
	}
	compat := must(client.GetCompatibleVersions(ctx, &aos.GetCompatibleVersionsInput{DomainName: aws.String("plain")}))(t).CompatibleVersions
	if len(compat) != 1 || aws.ToString(compat[0].SourceVersion) != vs[0] || len(compat[0].TargetVersions) != 0 {
		t.Fatalf("compatible %+v", compat)
	}
	if l := must(client.ListDomainsForPackage(ctx, &aos.ListDomainsForPackageInput{PackageID: aws.String("F123456789")}))(t).DomainPackageDetailsList; len(l) != 0 {
		t.Fatalf("package domains %+v", l)
	}

	_, err := client.DescribeDomain(ctx, &aos.DescribeDomainInput{DomainName: aws.String("missing")})
	isErr[*types.ResourceNotFoundException](t, err)
	_, err = client.UpdateDomainConfig(ctx, &aos.UpdateDomainConfigInput{DomainName: aws.String("missing")})
	isErr[*types.ResourceNotFoundException](t, err)
	_, err = client.DeleteDomain(ctx, &aos.DeleteDomainInput{DomainName: aws.String("missing")})
	isErr[*types.ResourceNotFoundException](t, err)
	_, err = client.GetCompatibleVersions(ctx, &aos.GetCompatibleVersionsInput{DomainName: aws.String("missing")})
	isErr[*types.ResourceNotFoundException](t, err)
	_, err = client.CreateDomain(ctx, &aos.CreateDomainInput{DomainName: aws.String("plain")})
	isErr[*types.ResourceAlreadyExistsException](t, err)
	_, err = client.CreateDomain(ctx, &aos.CreateDomainInput{DomainName: aws.String("Bad_Name")})
	isErr[*types.ValidationException](t, err)
	_, err = client.CreateDomain(ctx, &aos.CreateDomainInput{DomainName: aws.String("future"), EngineVersion: aws.String("OpenSearch_9.9")})
	isErr[*types.ValidationException](t, err)
	_, err = client.ListTags(ctx, &aos.ListTagsInput{ARN: aws.String(core.ARN("es", "us-east-1", core.DefaultAccount, "domain/missing"))})
	isErr[*types.ResourceNotFoundException](t, err)
	_, err = client.AddTags(ctx, &aos.AddTagsInput{ARN: aws.String("not-an-arn"), TagList: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}})
	isErr[*types.ValidationException](t, err)
}

// TestEngineContainer runs a domain on a fake engine whose container is a
// test server, and checks what reaches it through the endpoint.
func TestEngineContainer(t *testing.T) {
	var mu sync.Mutex
	var seen []*http.Request
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_cluster/health" {
			io.WriteString(w, `{"status":"yellow"}`)
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen = append(seen, r)
		mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"body": string(b), "path": r.URL.Path})
	}))
	t.Cleanup(backend.Close)
	f := dockertest.Start(t, backend.Listener.Addr().(*net.TCPAddr).Port, false)
	_, client, _, url := setup(t)
	if st := must(client.CreateDomain(ctx, &aos.CreateDomainInput{DomainName: aws.String("search"), EngineVersion: aws.String("OpenSearch_2.19")}))(t).DomainStatus; !aws.ToBool(st.Processing) {
		t.Fatalf("created %+v", st)
	}
	var st *types.DomainStatus
	eventually(t, "the domain to be ready", 10*time.Second, func() bool { st = describe(t, client, "search"); return !aws.ToBool(st.Processing) })
	run := f.Called("create")
	if len(run) != 1 || !strings.Contains(run[0], "--name pogi-opensearch-search -p 127.0.0.1:") ||
		!strings.Contains(run[0], ":9200 -e discovery.type=single-node -e OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m -e DISABLE_SECURITY_PLUGIN=true") ||
		!strings.HasSuffix(run[0], " opensearchproject/opensearch:2.19.6") {
		t.Fatalf("docker create = %q", run)
	}

	code, out := call(t, endpointClient(url), "PUT", "http://"+aws.ToString(st.Endpoint)+"/movies/_doc/1", `{"title":"Alien"}`, true)
	if code != 200 || out["path"] != "/movies/_doc/1" || out["body"] != `{"title":"Alien"}` {
		t.Fatalf("proxied %d %v", code, out)
	}
	mu.Lock()
	for _, h := range []string{"Authorization", "X-Amz-Date", "X-Amz-Content-Sha256"} {
		if v := seen[0].Header.Get(h); v != "" {
			t.Errorf("%s reached the engine: %q", h, v)
		}
	}
	mu.Unlock()

	must(client.DeleteDomain(ctx, &aos.DeleteDomainInput{DomainName: aws.String("search")}))(t)
	eventually(t, "docker rm", 10*time.Second, func() bool { return len(f.Called("rm -f -v pogi-opensearch-search")) == 2 })
}

// TestOpenSearchInDocker indexes and searches through a domain endpoint
// backed by a real OpenSearch container.
func TestOpenSearchInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	_, client, _, url := setup(t)
	name := "it-" + strconv.Itoa(os.Getpid())
	must(client.CreateDomain(ctx, &aos.CreateDomainInput{DomainName: aws.String(name), EngineVersion: aws.String("OpenSearch_2.19")}))(t)
	var st *types.DomainStatus
	eventually(t, "the cluster (the first run pulls the image)", 15*time.Minute, func() bool {
		if st = describe(t, client, name); st.DomainProcessingStatus == types.DomainProcessingStatusTypeIsolated {
			t.Fatalf("the engine failed: %+v", st)
		}
		return !aws.ToBool(st.Processing)
	})
	base, hc := "http://"+aws.ToString(st.Endpoint), endpointClient(url)
	if code, out := call(t, hc, "PUT", base+"/movies", `{"settings":{"number_of_replicas":0}}`, false); code != 200 {
		t.Fatalf("create index: %d %v", code, out)
	}
	for i, title := range []string{"The Matrix", "The Matrix Reloaded", "Inception"} {
		if code, out := call(t, hc, "PUT", fmt.Sprintf("%s/movies/_doc/%d", base, i+1), `{"title":"`+title+`"}`, false); code != 201 {
			t.Fatalf("index %q: %d %v", title, code, out)
		}
	}
	call(t, hc, "POST", base+"/movies/_refresh", "", false)
	for _, sign := range []bool{false, true} {
		code, out := call(t, hc, "POST", base+"/movies/_search", `{"query":{"match":{"title":"matrix"}}}`, sign)
		var ids []string
		if hits, ok := out["hits"].(map[string]any); ok {
			for _, h := range hits["hits"].([]any) {
				ids = append(ids, h.(map[string]any)["_id"].(string))
			}
		}
		if slices.Sort(ids); code != 200 || !slices.Equal(ids, []string{"1", "2"}) {
			t.Fatalf("search (signed %v): %d %v", sign, code, out)
		}
	}

	must(client.DeleteDomain(ctx, &aos.DeleteDomainInput{DomainName: aws.String(name)}))(t)
	eventually(t, "container removal", time.Minute, func() bool {
		list, err := d.List(ctx, docker.LabelService+"=opensearch", docker.LabelResource+"="+name)
		return err == nil && len(list) == 0
	})
}
