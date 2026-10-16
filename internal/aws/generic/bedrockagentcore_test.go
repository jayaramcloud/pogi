package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/nrndev/pogi"
)

func TestBedrockAgentCoreRuntime(t *testing.T) {
	ctx := context.Background()
	c := bedrockagentcorecontrol.NewFromConfig(aiStart(t, pogi.Config{}))
	const role = "arn:aws:iam::000000000000:role/agentcore"

	art := &types.AgentRuntimeArtifactMemberContainerConfiguration{Value: types.ContainerConfiguration{ContainerUri: aws.String("000000000000.dkr.ecr.us-east-1.amazonaws.com/agent:latest")}}
	net := &types.NetworkConfiguration{NetworkMode: types.NetworkModePublic}
	r, err := c.CreateAgentRuntime(ctx, &bedrockagentcorecontrol.CreateAgentRuntimeInput{AgentRuntimeName: aws.String("my_agent"), AgentRuntimeArtifact: art, RoleArn: aws.String(role),
		NetworkConfiguration: net, Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	id, arn := aws.ToString(r.AgentRuntimeId), aws.ToString(r.AgentRuntimeArn)
	if id == "" || arn == "" || r.Status != types.AgentRuntimeStatusReady || aws.ToString(r.AgentRuntimeVersion) == "" {
		t.Fatalf("create: %+v", r)
	}
	g, err := c.GetAgentRuntime(ctx, &bedrockagentcorecontrol.GetAgentRuntimeInput{AgentRuntimeId: aws.String(id)})
	if err != nil || aws.ToString(g.AgentRuntimeName) != "my_agent" || g.Status != types.AgentRuntimeStatusReady || aws.ToString(g.RoleArn) != role {
		t.Fatalf("get: %v %+v", err, g)
	}
	l, err := c.ListAgentRuntimes(ctx, &bedrockagentcorecontrol.ListAgentRuntimesInput{})
	if err != nil || len(l.AgentRuntimes) != 1 || aws.ToString(l.AgentRuntimes[0].AgentRuntimeName) != "my_agent" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdateAgentRuntime(ctx, &bedrockagentcorecontrol.UpdateAgentRuntimeInput{AgentRuntimeId: aws.String(id), AgentRuntimeArtifact: art, RoleArn: aws.String(role),
		NetworkConfiguration: net, Description: aws.String("updated")}); err != nil {
		t.Fatal(err)
	}
	if g, _ = c.GetAgentRuntime(ctx, &bedrockagentcorecontrol.GetAgentRuntimeInput{AgentRuntimeId: aws.String(id)}); aws.ToString(g.Description) != "updated" {
		t.Fatalf("after update: %+v", g)
	}
	if _, err := c.TagResource(ctx, &bedrockagentcorecontrol.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"team": "ai"}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTagsForResource(ctx, &bedrockagentcorecontrol.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &bedrockagentcorecontrol.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ := c.ListTagsForResource(ctx, &bedrockagentcorecontrol.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); len(tags.Tags) != 1 {
		t.Fatalf("tags after untag: %+v", tags)
	}
	ep, err := c.CreateAgentRuntimeEndpoint(ctx, &bedrockagentcorecontrol.CreateAgentRuntimeEndpointInput{AgentRuntimeId: aws.String(id), Name: aws.String("prod")})
	if err != nil || ep.Status != types.AgentRuntimeEndpointStatusReady {
		t.Fatalf("endpoint: %v %+v", err, ep)
	}
	if le, err := c.ListAgentRuntimeEndpoints(ctx, &bedrockagentcorecontrol.ListAgentRuntimeEndpointsInput{AgentRuntimeId: aws.String(id)}); err != nil || len(le.RuntimeEndpoints) != 1 {
		t.Fatalf("list endpoints: %v %+v", err, le)
	}
	if _, err := c.DeleteAgentRuntimeEndpoint(ctx, &bedrockagentcorecontrol.DeleteAgentRuntimeEndpointInput{AgentRuntimeId: aws.String(id), EndpointName: aws.String("prod")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteAgentRuntime(ctx, &bedrockagentcorecontrol.DeleteAgentRuntimeInput{AgentRuntimeId: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetAgentRuntime(ctx, &bedrockagentcorecontrol.GetAgentRuntimeInput{AgentRuntimeId: aws.String(id)}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %T %v", err, err)
	}
}

func TestBedrockAgentCoreMemoryAndGateway(t *testing.T) {
	ctx := context.Background()
	c := bedrockagentcorecontrol.NewFromConfig(aiStart(t, pogi.Config{}))

	m, err := c.CreateMemory(ctx, &bedrockagentcorecontrol.CreateMemoryInput{Name: aws.String("chat_memory"), EventExpiryDuration: aws.Int32(30)})
	if err != nil || m.Memory.Status != types.MemoryStatusActive || aws.ToString(m.Memory.Id) == "" {
		t.Fatalf("memory: %v %+v", err, m)
	}
	g, err := c.GetMemory(ctx, &bedrockagentcorecontrol.GetMemoryInput{MemoryId: m.Memory.Id})
	if err != nil || aws.ToString(g.Memory.Name) != "chat_memory" || aws.ToInt32(g.Memory.EventExpiryDuration) != 30 {
		t.Fatalf("get memory: %v %+v", err, g)
	}
	if l, err := c.ListMemories(ctx, &bedrockagentcorecontrol.ListMemoriesInput{}); err != nil || len(l.Memories) != 1 {
		t.Fatalf("list memories: %v %+v", err, l)
	}
	if _, err := c.DeleteMemory(ctx, &bedrockagentcorecontrol.DeleteMemoryInput{MemoryId: m.Memory.Id}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetMemory(ctx, &bedrockagentcorecontrol.GetMemoryInput{MemoryId: m.Memory.Id}); !errors.As(err, &nf) {
		t.Fatalf("get deleted memory: %T %v", err, err)
	}

	gw, err := c.CreateGateway(ctx, &bedrockagentcorecontrol.CreateGatewayInput{Name: aws.String("tools"), RoleArn: aws.String("arn:aws:iam::000000000000:role/gw"), AuthorizerType: types.AuthorizerTypeNone})
	if err != nil || gw.Status != types.GatewayStatusReady || aws.ToString(gw.GatewayId) == "" {
		t.Fatalf("gateway: %v %+v", err, gw)
	}
	if got, err := c.GetGateway(ctx, &bedrockagentcorecontrol.GetGatewayInput{GatewayIdentifier: gw.GatewayId}); err != nil || aws.ToString(got.Name) != "tools" {
		t.Fatalf("get gateway: %v %+v", err, got)
	}
	if _, err := c.DeleteGateway(ctx, &bedrockagentcorecontrol.DeleteGatewayInput{GatewayIdentifier: gw.GatewayId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetGateway(ctx, &bedrockagentcorecontrol.GetGatewayInput{GatewayIdentifier: gw.GatewayId}); !errors.As(err, &nf) {
		t.Fatalf("get deleted gateway: %T %v", err, err)
	}
}
