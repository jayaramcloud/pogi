package ssm

import (
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// setting is a service setting the caller changed; a setting that is not here has its default.
type setting struct {
	Value    string    `json:"value"`
	Modified time.Time `json:"modified"`
	User     string    `json:"user"`
}

// settingSpec is a setting AWS defines: its default and the values it accepts (empty: any).
type settingSpec struct {
	def    string
	values []string
}

var settingSpecs = map[string]settingSpec{
	"/ssm/managed-instance/default-ec2-instance-management-role": {def: "$None"},
	"/ssm/automation/customer-script-log-destination":            {def: "CloudWatch", values: []string{"CloudWatch", "S3"}},
	"/ssm/automation/customer-script-log-group-name":             {def: "/aws/ssm/automation/customer-script-execution"},
	"/ssm/documents/console/public-sharing-permission":           {def: "Enable", values: []string{"Enable", "Disable"}},
	"/ssm/managed-instance/activation-tier":                      {def: "standard", values: []string{"standard", "advanced"}},
	"/ssm/opsinsights/opscenter":                                 {def: "Disabled", values: []string{"Enabled", "Disabled"}},
	"/ssm/parameter-store/default-parameter-tier":                {def: "Standard", values: []string{"Standard", "Advanced", "Intelligent-Tiering"}},
	"/ssm/parameter-store/high-throughput-enabled":               {def: "false", values: []string{"true", "false"}},
}

func errNoSetting(id string) error {
	return core.Errorf(400, "ServiceSettingNotFound", "The specified service setting was not found. Either the service name or the setting has not been provisioned by the AWS service team.")
}

// settingOut is a ServiceSetting; a setting nobody changed is Default and was last modified by AWS.
func (s *Service) settingOut(c *core.Ctx, id string) any {
	spec, st := settingSpecs[id], s.st.Settings[key(c.Account, c.Region, id)]
	out := map[string]any{"SettingId": id, "SettingValue": spec.def, "Status": "Default", "LastModifiedDate": core.Time(publicDate),
		"LastModifiedUser": "System", "ARN": c.ARN("ssm", "servicesetting"+id)}
	if st != nil {
		out["SettingValue"], out["Status"] = st.Value, "Customized"
		out["LastModifiedDate"], out["LastModifiedUser"] = core.Time(st.Modified), st.User
	}
	return map[string]any{"ServiceSetting": out}
}

func (s *Service) getServiceSetting(c *core.Ctx, in *struct{ SettingId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := settingSpecs[in.SettingId]; !ok {
		return nil, errNoSetting(in.SettingId)
	}
	return s.settingOut(c, in.SettingId), nil
}

func (s *Service) updateServiceSetting(c *core.Ctx, in *struct{ SettingId, SettingValue string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := settingSpecs[in.SettingId]
	switch {
	case !ok:
		return nil, errNoSetting(in.SettingId)
	case in.SettingValue == "" || len(spec.values) > 0 && !slices.Contains(spec.values, in.SettingValue):
		return nil, validation("The value %q is not valid for %s. It must be one of: %s.", in.SettingValue, in.SettingId, strings.Join(spec.values, ", "))
	}
	user := c.Principal
	if user == "" {
		user = c.GlobalARN("iam", "root")
	}
	s.st.Settings[key(c.Account, c.Region, in.SettingId)] = &setting{in.SettingValue, time.Now().UTC(), user}
	return map[string]any{}, nil
}

func (s *Service) resetServiceSetting(c *core.Ctx, in *struct{ SettingId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := settingSpecs[in.SettingId]; !ok {
		return nil, errNoSetting(in.SettingId)
	}
	delete(s.st.Settings, key(c.Account, c.Region, in.SettingId))
	return s.settingOut(c, in.SettingId), nil
}
