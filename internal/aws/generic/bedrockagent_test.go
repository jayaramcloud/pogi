package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentruntime"
	rttypes "github.com/aws/aws-sdk-go-v2/service/bedrockagentruntime/types"
	"github.com/nrndev/pogi"
)

const agentRole = "arn:aws:iam::000000000000:role/agent"

func TestBedrockAgent(t *testing.T) {
	ctx := context.Background()
	c := bedrockagent.NewFromConfig(aiStart(t, pogi.Config{}))

	a, err := c.CreateAgent(ctx, &bedrockagent.CreateAgentInput{AgentName: aws.String("helper"), FoundationModel: aws.String("anthropic.claude-3-haiku-20240307-v1:0"),
		AgentResourceRoleArn: aws.String(agentRole), Instruction: aws.String("You help people with their orders, and you are polite."), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	id, arn := aws.ToString(a.Agent.AgentId), aws.ToString(a.Agent.AgentArn)
	if len(id) != 10 || arn == "" || a.Agent.AgentStatus != types.AgentStatusNotPrepared || aws.ToString(a.Agent.AgentName) != "helper" {
		t.Fatalf("create: %+v", a.Agent)
	}
	g, err := c.GetAgent(ctx, &bedrockagent.GetAgentInput{AgentId: aws.String(id)})
	if err != nil || aws.ToString(g.Agent.Instruction) != "You help people with their orders, and you are polite." || aws.ToString(g.Agent.FoundationModel) != "anthropic.claude-3-haiku-20240307-v1:0" {
		t.Fatalf("get: %v %+v", err, g)
	}
	l, err := c.ListAgents(ctx, &bedrockagent.ListAgentsInput{})
	if err != nil || len(l.AgentSummaries) != 1 || aws.ToString(l.AgentSummaries[0].AgentName) != "helper" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdateAgent(ctx, &bedrockagent.UpdateAgentInput{AgentId: aws.String(id), AgentName: aws.String("helper2"), FoundationModel: aws.String("amazon.nova-lite-v1:0"),
		AgentResourceRoleArn: aws.String(agentRole), Instruction: aws.String("changed: you now answer only questions about orders")}); err != nil {
		t.Fatal(err)
	}
	if g, _ = c.GetAgent(ctx, &bedrockagent.GetAgentInput{AgentId: aws.String(id)}); aws.ToString(g.Agent.AgentName) != "helper2" || aws.ToString(g.Agent.Instruction) != "changed: you now answer only questions about orders" {
		t.Fatalf("after update: %+v", g.Agent)
	}
	p, err := c.PrepareAgent(ctx, &bedrockagent.PrepareAgentInput{AgentId: aws.String(id)})
	if err != nil || p.AgentStatus != types.AgentStatusPrepared {
		t.Fatalf("prepare: %v %+v", err, p)
	}
	if g, _ = c.GetAgent(ctx, &bedrockagent.GetAgentInput{AgentId: aws.String(id)}); g.Agent.AgentStatus != types.AgentStatusPrepared {
		t.Fatalf("status after prepare: %v", g.Agent.AgentStatus)
	}

	// an action group on the draft version
	ag, err := c.CreateAgentActionGroup(ctx, &bedrockagent.CreateAgentActionGroupInput{AgentId: aws.String(id), AgentVersion: aws.String("DRAFT"), ActionGroupName: aws.String("orders"),
		ActionGroupExecutor: &types.ActionGroupExecutorMemberCustomControl{Value: types.CustomControlMethodReturnControl}})
	if err != nil || aws.ToString(ag.AgentActionGroup.ActionGroupId) == "" {
		t.Fatalf("action group: %v %+v", err, ag)
	}
	lag, err := c.ListAgentActionGroups(ctx, &bedrockagent.ListAgentActionGroupsInput{AgentId: aws.String(id), AgentVersion: aws.String("DRAFT")})
	if err != nil || len(lag.ActionGroupSummaries) != 1 || aws.ToString(lag.ActionGroupSummaries[0].ActionGroupName) != "orders" {
		t.Fatalf("list action groups: %v %+v", err, lag)
	}
	gag, err := c.GetAgentActionGroup(ctx, &bedrockagent.GetAgentActionGroupInput{AgentId: aws.String(id), AgentVersion: aws.String("DRAFT"), ActionGroupId: ag.AgentActionGroup.ActionGroupId})
	if err != nil || aws.ToString(gag.AgentActionGroup.ActionGroupName) != "orders" {
		t.Fatalf("get action group: %v %+v", err, gag)
	}
	if _, err := c.DeleteAgentActionGroup(ctx, &bedrockagent.DeleteAgentActionGroupInput{AgentId: aws.String(id), AgentVersion: aws.String("DRAFT"), ActionGroupId: ag.AgentActionGroup.ActionGroupId}); err != nil {
		t.Fatal(err)
	}

	// an alias
	al, err := c.CreateAgentAlias(ctx, &bedrockagent.CreateAgentAliasInput{AgentId: aws.String(id), AgentAliasName: aws.String("live")})
	if err != nil || al.AgentAlias.AgentAliasStatus != types.AgentAliasStatusPrepared || aws.ToString(al.AgentAlias.AgentAliasId) == "" {
		t.Fatalf("alias: %v %+v", err, al)
	}
	gal, err := c.GetAgentAlias(ctx, &bedrockagent.GetAgentAliasInput{AgentId: aws.String(id), AgentAliasId: al.AgentAlias.AgentAliasId})
	if err != nil || aws.ToString(gal.AgentAlias.AgentAliasName) != "live" {
		t.Fatalf("get alias: %v %+v", err, gal)
	}
	if lal, err := c.ListAgentAliases(ctx, &bedrockagent.ListAgentAliasesInput{AgentId: aws.String(id)}); err != nil || len(lal.AgentAliasSummaries) != 1 {
		t.Fatalf("list aliases: %v %+v", err, lal)
	}
	if _, err := c.TagResource(ctx, &bedrockagent.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"team": "ai"}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &bedrockagent.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &bedrockagent.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ := c.ListTagsForResource(ctx, &bedrockagent.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); len(tags.Tags) != 1 {
		t.Fatalf("tags after untag: %+v", tags)
	}

	if _, err := c.DeleteAgentAlias(ctx, &bedrockagent.DeleteAgentAliasInput{AgentId: aws.String(id), AgentAliasId: al.AgentAlias.AgentAliasId}); err != nil {
		t.Fatal(err)
	}
	d, err := c.DeleteAgent(ctx, &bedrockagent.DeleteAgentInput{AgentId: aws.String(id)})
	if err != nil || d.AgentStatus != types.AgentStatusDeleting {
		t.Fatalf("delete: %v %+v", err, d)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetAgent(ctx, &bedrockagent.GetAgentInput{AgentId: aws.String(id)}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %T %v", err, err)
	}
}

func TestBedrockKnowledgeBase(t *testing.T) {
	ctx := context.Background()
	c := bedrockagent.NewFromConfig(aiStart(t, pogi.Config{}))

	kb, err := c.CreateKnowledgeBase(ctx, &bedrockagent.CreateKnowledgeBaseInput{Name: aws.String("docs"), RoleArn: aws.String(agentRole),
		KnowledgeBaseConfiguration: &types.KnowledgeBaseConfiguration{Type: types.KnowledgeBaseTypeVector, VectorKnowledgeBaseConfiguration: &types.VectorKnowledgeBaseConfiguration{
			EmbeddingModelArn: aws.String("arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0")}},
		StorageConfiguration: &types.StorageConfiguration{Type: types.KnowledgeBaseStorageTypeOpensearchServerless, OpensearchServerlessConfiguration: &types.OpenSearchServerlessConfiguration{
			CollectionArn: aws.String("arn:aws:aoss:us-east-1:000000000000:collection/abc"), VectorIndexName: aws.String("idx"),
			FieldMapping: &types.OpenSearchServerlessFieldMapping{MetadataField: aws.String("m"), TextField: aws.String("t"), VectorField: aws.String("v")}}},
		Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	k := kb.KnowledgeBase
	if aws.ToString(k.KnowledgeBaseId) == "" || k.Status != types.KnowledgeBaseStatusActive || aws.ToString(k.Name) != "docs" {
		t.Fatalf("create: %+v", k)
	}
	g, err := c.GetKnowledgeBase(ctx, &bedrockagent.GetKnowledgeBaseInput{KnowledgeBaseId: k.KnowledgeBaseId})
	if err != nil || g.KnowledgeBase.KnowledgeBaseConfiguration.Type != types.KnowledgeBaseTypeVector {
		t.Fatalf("get: %v %+v", err, g)
	}
	if l, err := c.ListKnowledgeBases(ctx, &bedrockagent.ListKnowledgeBasesInput{}); err != nil || len(l.KnowledgeBaseSummaries) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	ds, err := c.CreateDataSource(ctx, &bedrockagent.CreateDataSourceInput{KnowledgeBaseId: k.KnowledgeBaseId, Name: aws.String("bucket"),
		DataSourceConfiguration: &types.DataSourceConfiguration{Type: types.DataSourceTypeS3, S3Configuration: &types.S3DataSourceConfiguration{BucketArn: aws.String("arn:aws:s3:::docs")}}})
	if err != nil || ds.DataSource.Status != types.DataSourceStatusAvailable {
		t.Fatalf("data source: %v %+v", err, ds)
	}
	if l, err := c.ListDataSources(ctx, &bedrockagent.ListDataSourcesInput{KnowledgeBaseId: k.KnowledgeBaseId}); err != nil || len(l.DataSourceSummaries) != 1 {
		t.Fatalf("list data sources: %v %+v", err, l)
	}
	ij, err := c.StartIngestionJob(ctx, &bedrockagent.StartIngestionJobInput{KnowledgeBaseId: k.KnowledgeBaseId, DataSourceId: ds.DataSource.DataSourceId})
	if err != nil || ij.IngestionJob.Status != types.IngestionJobStatusComplete {
		t.Fatalf("ingestion: %v %+v", err, ij)
	}
	if _, err := c.UpdateKnowledgeBase(ctx, &bedrockagent.UpdateKnowledgeBaseInput{KnowledgeBaseId: k.KnowledgeBaseId, Name: aws.String("docs2"), RoleArn: aws.String(agentRole),
		KnowledgeBaseConfiguration: k.KnowledgeBaseConfiguration, StorageConfiguration: k.StorageConfiguration}); err != nil {
		t.Fatal(err)
	}
	if g, _ = c.GetKnowledgeBase(ctx, &bedrockagent.GetKnowledgeBaseInput{KnowledgeBaseId: k.KnowledgeBaseId}); aws.ToString(g.KnowledgeBase.Name) != "docs2" {
		t.Fatalf("after update: %+v", g.KnowledgeBase)
	}
	if _, err := c.TagResource(ctx, &bedrockagent.TagResourceInput{ResourceArn: k.KnowledgeBaseArn, Tags: map[string]string{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &bedrockagent.ListTagsForResourceInput{ResourceArn: k.KnowledgeBaseArn}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteDataSource(ctx, &bedrockagent.DeleteDataSourceInput{KnowledgeBaseId: k.KnowledgeBaseId, DataSourceId: ds.DataSource.DataSourceId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteKnowledgeBase(ctx, &bedrockagent.DeleteKnowledgeBaseInput{KnowledgeBaseId: k.KnowledgeBaseId}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetKnowledgeBase(ctx, &bedrockagent.GetKnowledgeBaseInput{KnowledgeBaseId: k.KnowledgeBaseId}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %T %v", err, err)
	}
}

// InvokeAgent answers through the gateway as an event stream; Retrieve is deterministic.
func TestBedrockAgentRuntime(t *testing.T) {
	ctx := context.Background()
	fm := aiNewFakeModel(t)
	cfg := aiStart(t, pogi.Config{AIEndpoint: fm.URL, AIModels: map[string]string{"*": "m"}})
	ctl, rt := bedrockagent.NewFromConfig(cfg), bedrockagentruntime.NewFromConfig(cfg)

	a, err := ctl.CreateAgent(ctx, &bedrockagent.CreateAgentInput{AgentName: aws.String("helper"), FoundationModel: aws.String("anthropic.claude-3-haiku-20240307-v1:0"),
		AgentResourceRoleArn: aws.String(agentRole), Instruction: aws.String("You help people with their orders, and you are polite.")})
	if err != nil {
		t.Fatal(err)
	}
	al, err := ctl.CreateAgentAlias(ctx, &bedrockagent.CreateAgentAliasInput{AgentId: a.Agent.AgentId, AgentAliasName: aws.String("live")})
	if err != nil {
		t.Fatal(err)
	}
	out, err := rt.InvokeAgent(ctx, &bedrockagentruntime.InvokeAgentInput{AgentId: a.Agent.AgentId, AgentAliasId: al.AgentAlias.AgentAliasId, SessionId: aws.String("s1"), InputText: aws.String("where is my order")})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(out.SessionId) != "s1" {
		t.Fatalf("session: %v", out.SessionId)
	}
	var text strings.Builder
	for ev := range out.GetStream().Events() {
		if ch, ok := ev.(*rttypes.ResponseStreamMemberChunk); ok {
			text.Write(ch.Value.Bytes)
		}
	}
	if err := out.GetStream().Err(); err != nil || text.String() != "local says: where is my order" {
		t.Fatalf("invoke agent: %q %v", text.String(), err)
	}
	if sys := fm.reqs[0]["messages"].([]any)[0].(map[string]any); sys["role"] != "system" || sys["content"] != "You help people with their orders, and you are polite." || fm.reqs[0]["model"] != "m" {
		t.Fatalf("agent instruction not sent: %v", fm.reqs[0])
	}
	// the session remembers the conversation: system, first question, first aiAnswer, second question
	out, err = rt.InvokeAgent(ctx, &bedrockagentruntime.InvokeAgentInput{AgentId: a.Agent.AgentId, AgentAliasId: al.AgentAlias.AgentAliasId, SessionId: aws.String("s1"), InputText: aws.String("and the second one")})
	if err != nil {
		t.Fatal(err)
	}
	for range out.GetStream().Events() {
	}
	if msgs := fm.reqs[len(fm.reqs)-1]["messages"].([]any); len(msgs) != 4 || msgs[2].(map[string]any)["content"] != "local says: where is my order" {
		t.Fatalf("session history: %v", msgs)
	}
	if _, err := rt.InvokeAgent(ctx, &bedrockagentruntime.InvokeAgentInput{AgentId: aws.String("NOSUCHAGENT"), AgentAliasId: aws.String("TSTALIASID"), SessionId: aws.String("s"), InputText: aws.String("x")}); err == nil {
		t.Fatal("invoked an unknown agent")
	}

	kb, err := ctl.CreateKnowledgeBase(ctx, &bedrockagent.CreateKnowledgeBaseInput{Name: aws.String("docs"), RoleArn: aws.String(agentRole),
		KnowledgeBaseConfiguration: &types.KnowledgeBaseConfiguration{Type: types.KnowledgeBaseTypeVector},
		StorageConfiguration:       &types.StorageConfiguration{Type: types.KnowledgeBaseStorageTypeOpensearchServerless}})
	if err != nil {
		t.Fatal(err)
	}
	q := &bedrockagentruntime.RetrieveInput{KnowledgeBaseId: kb.KnowledgeBase.KnowledgeBaseId, RetrievalQuery: &rttypes.KnowledgeBaseQuery{Text: aws.String("refund policy")},
		RetrievalConfiguration: &rttypes.KnowledgeBaseRetrievalConfiguration{VectorSearchConfiguration: &rttypes.KnowledgeBaseVectorSearchConfiguration{NumberOfResults: aws.Int32(3)}}}
	r1, err := rt.Retrieve(ctx, q)
	if err != nil || len(r1.RetrievalResults) != 3 {
		t.Fatalf("retrieve: %v %+v", err, r1)
	}
	r2, _ := rt.Retrieve(ctx, q)
	for i, r := range r1.RetrievalResults {
		if aws.ToString(r.Content.Text) == "" || aws.ToString(r.Content.Text) != aws.ToString(r2.RetrievalResults[i].Content.Text) || aws.ToFloat64(r.Score) != aws.ToFloat64(r2.RetrievalResults[i].Score) {
			t.Fatalf("retrieve result %d is not stable or empty: %+v", i, r)
		}
	}
	if r1.RetrievalResults[0].Score == nil || aws.ToFloat64(r1.RetrievalResults[0].Score) < aws.ToFloat64(r1.RetrievalResults[2].Score) {
		t.Fatalf("results not ordered by score: %+v", r1.RetrievalResults)
	}
	var nf *rttypes.ResourceNotFoundException
	q.KnowledgeBaseId = aws.String("NOSUCHKB01")
	if _, err := rt.Retrieve(ctx, q); !errors.As(err, &nf) {
		t.Fatalf("retrieve from unknown kb: %T %v", err, err)
	}
}
