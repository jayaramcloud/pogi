package appservice_test

import (
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

// A resource put through the resource manager is a row of the console's table and can be inspected.
func TestConsoleListsARMResource(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azappservice", "azfunctions")
	h := e.Srv.Hub
	id, _ := arm.ParseID("/subscriptions/" + core.AzureSubscription + "/resourceGroups/rg1/providers/Microsoft.Web/serverfarms/plan1")
	body := map[string]any{"location": "eastus", "sku": map[string]any{"name": "B1"}, "properties": map[string]any{}}
	if _, err := arm.Lookup(h, "Microsoft.Web/serverfarms").Put(&core.Ctx{Hub: h, Internal: true}, id, body); err != nil {
		t.Fatal(err)
	}
	b := h.Service("azappservice").(core.Browsable)
	var rows [][]string
	var cols []string
	for _, tb := range b.Resources() {
		if tb.Name == "App Service plans" {
			rows, cols = tb.Rows, tb.Columns
		}
	}
	if len(rows) != 1 || rows[0][0] != "plan1" || cols[1] != "Resource group" || cols[2] != "Location" || rows[0][1] != "rg1" || rows[0][2] != "eastus" {
		t.Fatalf("columns %v rows %v", cols, rows)
	}
	if h.Service("azappservice").(core.Inspectable).Inspect("App Service plans", "plan1") == nil || h.Service("azappservice").(core.Inspectable).Inspect("App Service plans", "nope") != nil {
		t.Fatal("inspect finds the resource and only it")
	}
}
