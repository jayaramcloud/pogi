package cosmos_test

import (
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

// A resource put through the resource manager is a row of the console's table and can be inspected.
func TestConsoleListsARMResource(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "cosmos")
	h := e.Srv.Hub
	id, _ := arm.ParseID("/subscriptions/" + core.AzureSubscription + "/resourceGroups/rg1/providers/Microsoft.DocumentDB/databaseAccounts/acct1")
	body := map[string]any{"location": "eastus", "properties": map[string]any{}}
	if _, err := arm.Lookup(h, "Microsoft.DocumentDB/databaseAccounts").Put(&core.Ctx{Hub: h, Internal: true}, id, body); err != nil {
		t.Fatal(err)
	}
	b := h.Service("cosmos").(core.Browsable)
	var rows [][]string
	var cols []string
	for _, tb := range b.Resources() {
		if tb.Name == "Accounts" {
			rows, cols = tb.Rows, tb.Columns
		}
	}
	if len(rows) != 1 || rows[0][0] != "acct1" || cols[1] != "Resource group" || cols[2] != "Location" || rows[0][1] != "rg1" || rows[0][2] != "eastus" {
		t.Fatalf("columns %v rows %v", cols, rows)
	}
	if h.Service("cosmos").(core.Inspectable).Inspect("Accounts", "acct1") == nil || h.Service("cosmos").(core.Inspectable).Inspect("Accounts", "nope") != nil {
		t.Fatal("inspect finds the resource and only it")
	}
}
