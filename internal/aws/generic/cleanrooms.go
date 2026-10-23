package generic

import "github.com/nrndev/pogi/internal/core"

// Clean Rooms: collaborations are stored and read back; nothing queries any data. Like AWS, a collaboration
// gets a UUID for its identifier, not its name.
func init() {
	Override("cleanrooms", "CreateCollaboration", func(c *Call) (map[string]any, error) {
		c.In["CollaborationIdentifier"] = core.UUID()
		return c.Default()
	})
	// A collaboration's members are the creator (ACTIVE) and the members named in the create call (INVITED).
	Override("cleanrooms", "ListMembers", func(c *Call) (map[string]any, error) {
		r := c.resolve(c.m.types["collaboration"])
		if r == nil {
			return nil, c.NotFound("Collaboration %s not found.", str(c.In["collaborationIdentifier"]))
		}
		d := normIndex(r.Data)
		at := r.Created.Format(timeFmt)
		member := func(account, name string, abilities any, status string) map[string]any {
			if abilities == nil {
				abilities = []any{}
			}
			return map[string]any{"accountId": account, "displayName": name, "abilities": abilities, "status": status,
				"createTime": at, "updateTime": at, "paymentConfiguration": map[string]any{"queryCompute": map[string]any{"isResponsible": true}}}
		}
		out := []any{member(r.Account, str(d["creatordisplayname"]), d["creatormemberabilities"], "ACTIVE")}
		others, _ := d["members"].([]any)
		for _, m := range others {
			if mm, ok := m.(map[string]any); ok {
				out = append(out, member(str(mm["accountId"]), str(mm["displayName"]), mm["memberAbilities"], "INVITED"))
			}
		}
		return map[string]any{"memberSummaries": out}, nil
	})
}
