package generic

import (
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Recovery Services: a vault starts with the backup and storage settings Terraform reads, and holds backup
// policies. Nothing is backed up and nothing is replicated: there are no protected items, jobs or recovery points.

const vaults = "Microsoft.RecoveryServices/vaults"

func init() {
	// the clients never send a location for these
	for _, t := range []string{"/backupconfig", "/backupstorageconfig"} {
		Override(vaults+t, Hook{Untracked: true, Created: http.StatusOK})
	}
	Override(vaults+"/backupPolicies", Hook{Untracked: true, Created: http.StatusOK, Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
		dateTimes(armres.Props(r))
		return nil
	}})
	Override(vaults, Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Standard"}})
			armres.Defaults(armres.Props(r), map[string]any{"publicNetworkAccess": "Enabled", "privateEndpointStateForBackup": "None",
				"privateEndpointStateForSiteRecovery": "None"})
			for _, ch := range []struct {
				typ, name string
				props     map[string]any
			}{
				{"backupconfig", "vaultconfig", map[string]any{"enhancedSecurityState": "Enabled", "softDeleteFeatureState": "Enabled",
					"softDeleteRetentionPeriodInDays": 14, "isSoftDeleteFeatureStateEditable": true}},
				{"backupstorageconfig", "vaultstorageconfig", map[string]any{"storageModelType": "GeoRedundant", "storageType": "GeoRedundant",
					"storageTypeState": "Unlocked", "crossRegionRestoreFlag": false}},
			} {
				cid := id.Child(ch.typ, ch.name)
				if s.Has(cid) {
					continue
				}
				if err := s.Create(c, cid, map[string]any{"properties": ch.props}); err != nil {
					return err
				}
			}
			return nil
		},
		// the Site Recovery settings of a vault: nothing is set
		Extra: func(s *Service) []arm.Provider {
			return []arm.Provider{{Type: vaults + "/replicationVaultSettings", Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
				if !s.Has(id.Parent()) {
					return nil, arm.NotFound(id)
				}
				return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "properties": map[string]any{}}, nil
			}}}
		},
	})
}

// dateTimes turns the times of day in a policy's schedules ("23:00:00Z") into the date-times Azure keeps and
// returns ("2018-01-01T23:00:00Z"); Terraform reads the time back from them.
func dateTimes(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, c := range x {
			if list, ok := c.([]any); ok && (k == "scheduleRunTimes" || k == "retentionTimes") {
				for i, t := range list {
					if str, ok := t.(string); ok && len(str) > 0 && !strings.Contains(str, "T") {
						list[i] = "2018-01-01T" + str
					}
				}
				continue
			}
			dateTimes(c)
		}
	case []any:
		for _, c := range x {
			dateTimes(c)
		}
	}
}
