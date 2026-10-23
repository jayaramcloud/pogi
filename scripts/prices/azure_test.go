package main

import (
	"testing"

	"github.com/nrndev/pogi/internal/cost"
)

func TestPickerTiersAndGlobal(t *testing.T) {
	pk := newPicker([]string{"eastus", "westus"})
	// A free first tier, then two paid tiers: the first paid tier is the price.
	pk.add("a", item{ArmRegionName: "eastus", Price: 0, Tier: 0}, 1)
	pk.add("a", item{ArmRegionName: "eastus", Price: 5, Tier: 100}, 1)
	pk.add("a", item{ArmRegionName: "eastus", Price: 3, Tier: 50}, 1)
	pk.add("a", item{ArmRegionName: "eastus", Price: 1, Tier: 500}, 1)
	// A global price is used where a region has none of its own; other regions' rows are ignored.
	pk.add("g", item{ArmRegionName: "Global", Price: 2}, 10)
	pk.add("g", item{ArmRegionName: "westus", Price: 4}, 10)
	pk.add("g", item{ArmRegionName: "attdallas1", Price: 99}, 1)
	if got := pk.price("a", "eastus"); got != 3 {
		t.Errorf("first paid tier: %v, want 3", got)
	}
	if got := pk.price("g", "eastus"); got != 20 {
		t.Errorf("global price: %v, want 20", got)
	}
	if got := pk.price("g", "westus"); got != 40 {
		t.Errorf("regional price: %v, want 40", got)
	}
	// Two meters for one price: the higher is kept and the tie is reported.
	pk.add("a", item{ArmRegionName: "eastus", Price: 4, Tier: 50}, 1)
	if got := pk.price("a", "eastus"); got != 4 || len(pk.conflicts) != 1 {
		t.Errorf("tie: price %v, %d conflicts", got, len(pk.conflicts))
	}
}

func TestTooManyConflictsFail(t *testing.T) {
	pk := newPicker([]string{"eastus"})
	for i := 0; i < 25; i++ {
		key := string(rune('a' + i))
		pk.add(key, item{ArmRegionName: "eastus", Price: 1}, 1)
		pk.add(key, item{ArmRegionName: "eastus", Price: 2}, 1)
	}
	if pk.check("test") == nil {
		t.Error("25 disagreeing prices were accepted; a loose filter should fail the run")
	}
}

func TestRound6(t *testing.T) {
	for in, want := range map[float64]float64{0: 0, 0.123456789: 0.123457, 1234.56789: 1234.57, 0.0000123456789: 0.0000123457} {
		if got := round6(in); got != want {
			t.Errorf("round6(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestVMNames(t *testing.T) {
	if got := vmFamily("Standard_D2s_v5"); got != "Ds_v5" {
		t.Errorf("family: %q", got)
	}
	if got := vmInfo(nil, "Standard_E16-4as_v6"); got.VCPU != 4 || got.Family != "Eas_v6" {
		t.Errorf("a constrained size not in the SKU list: %+v", got)
	}
	sizes := map[string]cost.Size{"Standard_D2s_v3": {}, "Standard_D2s_v4": {}, "Standard_D2s_v5": {}, "Standard_F2": {}}
	markOld(sizes)
	if !sizes["Standard_D2s_v3"].Old || sizes["Standard_D2s_v4"].Old || sizes["Standard_D2s_v5"].Old || sizes["Standard_F2"].Old {
		t.Errorf("previous generation flags: %+v", sizes)
	}
	if gpuModel("Standard_NC24ads_A100_v4") != "A100" || gpuModel("Standard_NC6s_v3") != "V100" || gpuModel("Standard_D2s_v5") != "" {
		t.Error("gpu models")
	}
}
