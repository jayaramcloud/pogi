// Package mcp is pogi's MCP server: tools that let coding agents inspect and
// control a running pogi, served over Streamable HTTP at /_pogi/mcp.
package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nrndev/pogi/internal/core"
)

const instructions = `pogi emulates AWS, Google Cloud and Azure on one local endpoint (connection_info shows it and the SDK environment).
Make cloud calls with the usual SDKs and CLIs pointed at pogi; there is no cloud-API passthrough here.
Use these tools to look inside and control it: request_log and get_request explain failed calls, list_resources and inspect_resource show state, peek_queue and inbox show messages,
and reset, snapshots, chaos, deploy_stack, invoke_function and seed change it. All state is local and disposable.`

// Handler returns the MCP endpoint for hub. It is stateless (every POST
// stands alone) and answers in plain JSON. The tool schemas are built on the
// first request, not at start-up.
func Handler(hub *core.Hub) http.Handler {
	h := sync.OnceValue(func() http.Handler {
		s := sdk.NewServer(&sdk.Implementation{Name: "pogi", Version: hub.Cfg.Version}, &sdk.ServerOptions{Instructions: instructions})
		register(s, hub)
		return sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return s }, &sdk.StreamableHTTPOptions{
			Stateless: true, JSONResponse: true,
			DisableLocalhostProtection: true, // done below, so that *.localhost passes too
		})
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" {
			if u, err := url.Parse(o); err != nil || !core.Loopback(u.Hostname()) {
				http.Error(w, "pogi: MCP refuses requests from origin "+o, http.StatusForbidden)
				return
			}
		}
		// DNS rebinding: a page on evil.example resolving to 127.0.0.1 still sends Host: evil.example.
		if !hub.Cfg.AllowExternal && !core.Loopback((&url.URL{Host: r.Host}).Hostname()) {
			http.Error(w, "pogi: MCP refuses host "+r.Host+" (use localhost)", http.StatusForbidden)
			return
		}
		h().ServeHTTP(w, r)
	})
}

type obj = map[string]any

// add registers a tool. f's result, always a JSON object, becomes the tool's
// compact JSON text and its structured content; an error becomes a tool error.
func add[In any](s *sdk.Server, name, desc string, readOnly bool, f func(In) (any, error)) {
	sdk.AddTool(s, &sdk.Tool{Name: name, Description: desc, Annotations: &sdk.ToolAnnotations{ReadOnlyHint: readOnly}},
		func(_ context.Context, _ *sdk.CallToolRequest, in In) (*sdk.CallToolResult, any, error) {
			out, err := f(in)
			if err != nil {
				return nil, nil, err
			}
			// Our own text: json.Marshal would escape the angle brackets of XML and HTML bodies.
			var b strings.Builder
			enc := json.NewEncoder(&b)
			enc.SetEscapeHTML(false)
			if err := enc.Encode(out); err != nil {
				return nil, nil, err
			}
			return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: strings.TrimSuffix(b.String(), "\n")}}}, out, nil
		})
}
