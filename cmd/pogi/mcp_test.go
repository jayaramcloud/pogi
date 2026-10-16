package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nrndev/pogi"
)

// bridged connects an MCP client to `pogi mcp` (the bridge) over pipes, the
// way an agent runs it, speaking the legacy initialize handshake.
func bridged(t *testing.T, endpoint string) (*mcp.ClientSession, error) {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	go bridge(context.Background(), endpoint, inR, outW)
	t.Cleanup(func() { inW.Close(); outR.Close() })
	return mcp.NewClient(&mcp.Implementation{Name: "agent", Version: "1"}, nil).Connect(context.Background(),
		&mcp.IOTransport{Reader: outR, Writer: inW}, &mcp.ClientSessionOptions{ProtocolVersion: "2025-06-18"})
}

func TestMCPBridge(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t)
	cs, err := bridged(t, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	if cs.InitializeResult().ServerInfo.Name != "pogi" {
		t.Fatalf("initialize: %+v", cs.InitializeResult())
	}
	tools, err := cs.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) == 0 {
		t.Fatalf("tools/list: %v %v", tools, err)
	}
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "status"})
	if err != nil || res.IsError || !strings.Contains(res.Content[0].(*mcp.TextContent).Text, `"name":"sqs"`) {
		t.Fatalf("status: %+v %v", res, err)
	}

	// Without a server, initialize fails with how to start one.
	if _, err := bridged(t, "http://127.0.0.1:1"); err == nil || !strings.Contains(err.Error(), "pogi start") {
		t.Fatalf("want a hint to start pogi, got %v", err)
	}
}

func TestAgentsInit(t *testing.T) {
	file := filepath.Join(t.TempDir(), "AGENTS.md")
	os.WriteFile(file, []byte("# Notes\nkeep me"), 0o644)
	brief := agentsBrief("http://localhost:7644")
	read := func() string { b, _ := os.ReadFile(file); return string(b) }

	if _, err := agentsInit(file, brief, false); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != "# Notes\nkeep me\n\n"+brief+"\n" {
		t.Fatalf("append:\n%s", got)
	}
	if msg, err := agentsInit(file, brief, false); err != nil || !strings.Contains(msg, "up to date") {
		t.Fatalf("rerun: %q %v", msg, err)
	}
	newer := strings.Replace(brief, "pogi start", "pogi up", 1)
	if _, err := agentsInit(file, newer, false); err == nil {
		t.Fatal("a differing section was replaced without --force")
	}
	if _, err := agentsInit(file, newer, true); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != "# Notes\nkeep me\n\n"+newer+"\n" {
		t.Fatalf("force:\n%s", got)
	}
}
