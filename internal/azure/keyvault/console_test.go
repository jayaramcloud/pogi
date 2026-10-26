package keyvault_test

import (
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

// A resource put through the resource manager is a row of the console's table and can be inspected.
func TestConsoleListsARMResource(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "keyvault")
	h := e.Srv.Hub
	id, _ := arm.ParseID("/subscriptions/" + core.AzureSubscription + "/resourceGroups/rg1/providers/Microsoft.KeyVault/vaults/v1")
	body := map[string]any{"location": "eastus", "properties": map[string]any{}}
	if _, err := arm.Lookup(h, "Microsoft.KeyVault/vaults").Put(&core.Ctx{Hub: h, Internal: true}, id, body); err != nil {
		t.Fatal(err)
	}
	b := h.Service("keyvault").(core.Browsable)
	var rows [][]string
	var cols []string
	for _, tb := range b.Resources() {
		if tb.Name == "Vaults" {
			rows, cols = tb.Rows, tb.Columns
		}
	}
	if len(rows) != 1 || rows[0][0] != "v1" || cols[1] != "Resource group" || cols[2] != "Location" || rows[0][1] != "rg1" || rows[0][2] != "eastus" {
		t.Fatalf("columns %v rows %v", cols, rows)
	}
	if h.Service("keyvault").(core.Inspectable).Inspect("Vaults", "v1") == nil || h.Service("keyvault").(core.Inspectable).Inspect("Vaults", "nope") != nil {
		t.Fatal("inspect finds the resource and only it")
	}
}
