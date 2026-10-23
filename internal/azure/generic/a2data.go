package generic

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Storage, data and health services (NetApp Files, Managed Lustre, Elastic SAN, Storage Mover, File Sync, Data
// Share, Database Migration Service, Analysis Services, HDInsight, Power BI Embedded, Fabric, Managed Redis,
// Oracle Database@Azure, Health Data Services, Digital Twins, IoT Hub Device Provisioning) store what is sent
// and fill what Terraform reads back: states, host names, endpoints, keys. Nothing runs behind them.

const (
	redisEnt   = "Microsoft.Cache/redisEnterprise"
	redisEntDB = redisEnt + "/databases"
)

func init() {
	Override(redisEnt, Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			setProps(r, map[string]any{"resourceState": "Running", "hostName": fmt.Sprintf("%s.%s.redis.azure.net", host(id), loc(r)), "redisVersion": "7.4"})
		},
	})
	Override(redisEntDB, Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"clientProtocol": "Encrypted", "port": 10000, "clusteringPolicy": "OSSCluster",
				"evictionPolicy": "VolatileLRU", "accessKeysAuthentication": "Enabled"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) { setProps(r, map[string]any{"resourceState": "Running"}) },
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			"listKeys": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				return map[string]any{"primaryKey": armres.Key("redisent/primary/" + key(id)), "secondaryKey": armres.Key("redisent/secondary/" + key(id))}, nil
			},
		},
	})
}

const hdi = "Microsoft.HDInsight/clusters"

// HDInsight: a cluster reports its state and its two public endpoints. Terraform also reads the cluster's
// configurations (the gateway login and the storage account) and its monitoring extensions; those come from what
// was sent, and the passwords and keys stay out of the cluster itself, as in Azure.
func init() {
	cfg := func(s *Service, id arm.ID) (map[string]any, error) {
		cl, ok := s.Raw(id.Parent())
		if !ok {
			return nil, core.Errorf(http.StatusNotFound, "ResourceNotFound", "The Resource '%s' under resource group '%s' was not found.", id.Parent().Name(), id.ResourceGroup)
		}
		p := armres.Props(cl)
		out := map[string]map[string]any{}
		if def, _ := p["clusterDefinition"].(map[string]any); def != nil {
			if cs, _ := def["configurations"].(map[string]any); cs != nil {
				for k, v := range cs {
					m, _ := v.(map[string]any)
					out[k] = map[string]any{}
					for kk, vv := range m {
						out[k][kk] = fmt.Sprint(vv) // Azure answers every value as a string
					}
				}
			}
		}
		// core-site holds the default storage account, as in Azure.
		core := map[string]any{}
		if sp, _ := p["storageProfile"].(map[string]any); sp != nil {
			accts, _ := sp["storageaccounts"].([]any)
			for _, a := range accts {
				m, _ := a.(map[string]any)
				if m == nil || m["isDefault"] != true {
					continue
				}
				name := armres.Str(m, "name")
				core["fs.defaultFS"] = "wasbs://" + armres.Str(m, "container") + "@" + name
				core["fs.azure.account.key."+name] = armres.Str(m, "key")
			}
		}
		out["core-site"] = core
		res := map[string]any{}
		for k, v := range out {
			res[k] = v
		}
		return res, nil
	}
	Override(hdi, Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"tier": "Standard", "osType": "Linux"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			if def, _ := p["clusterDefinition"].(map[string]any); def != nil {
				delete(def, "configurations")
			}
			if sp, _ := p["storageProfile"].(map[string]any); sp != nil {
				accts, _ := sp["storageaccounts"].([]any)
				for _, a := range accts {
					if m, _ := a.(map[string]any); m != nil {
						delete(m, "key")
					}
				}
			}
			n := host(id)
			setProps(r, map[string]any{"clusterState": "Running", "clusterId": a2dID(id),
				"connectivityEndpoints": []any{
					map[string]any{"name": "HTTPS", "protocol": "HTTPS", "location": n + ".azurehdinsight.net", "port": 443},
					map[string]any{"name": "SSH", "protocol": "SSH", "location": n + "-ssh.azurehdinsight.net", "port": 22},
				}})
		},
		// POST .../clusters/<name>/configurations lists every configuration (the sensitive ones too).
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			"configurations": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				all, err := cfg(s, id.Child("configurations", "x"))
				return map[string]any{"configurations": all}, err
			},
		},
		Extra: func(s *Service) []arm.Provider {
			return []arm.Provider{
				{Type: hdi + "/configurations",
					Doc: func(c *core.Ctx, parent arm.ID, _ url.Values) (any, error) {
						all, err := cfg(s, parent.Child("configurations", "x"))
						return map[string]any{"configurations": all}, err
					},
					Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
						all, err := cfg(s, id)
						if err != nil {
							return nil, err
						}
						one, ok := all[id.Name()]
						if !ok {
							return map[string]any{}, nil
						}
						return one.(map[string]any), nil
					}},
				{Type: hdi + "/extensions", Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
					if !s.Has(id.Parent()) {
						return nil, core.Errorf(http.StatusNotFound, "ResourceNotFound", "The Resource '%s' was not found.", id.Parent().Name())
					}
					if id.Name() == "azureMonitor" {
						return map[string]any{"clusterMonitoringEnabled": false}, nil
					}
					return map[string]any{"clusterMonitoringEnabled": false, "workspaceId": ""}, nil
				}},
			}
		},
	})
}

// a2dID is a GUID made from a resource's id, the id a service reports as its own: the same on every read.
func a2dID(id arm.ID) string { return guid("a2d/" + key(id)) }

func init() {
	view := func(typ string, f func(id arm.ID, r map[string]any) map[string]any) {
		h := hooks[strings.ToLower(typ)]
		h.View = func(s *Service, id arm.ID, r map[string]any) { setProps(r, f(id, r)) }
		Override(typ, h)
	}

	// NetApp Files
	Override("Microsoft.NetApp/netAppAccounts/capacityPools", Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"qosType": "Auto", "coolAccess": false, "encryptionType": "Single"})
			return nil
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			setProps(r, map[string]any{"poolId": a2dID(id), "totalThroughputMibps": 0, "utilizedThroughputMibps": 0})
		},
	})
	view("Microsoft.NetApp/netAppAccounts/capacityPools/volumes", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"fileSystemId": a2dID(id), "mountTargets": []any{map[string]any{"mountTargetId": a2dID(id.Child("mt", "1")), "fileSystemId": a2dID(id), "ipAddress": "10.0.0.4"}}}
	})

	// Managed Lustre
	view("Microsoft.StorageCache/amlFilesystems", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"clientInfo": map[string]any{"mgsAddress": "10.0.0.4", "containerStorageInterface": map[string]any{},
			"lustreVersion": "2.15.0", "mountCommand": "mount -t lustre 10.0.0.4@tcp:/lustrefs /mnt/lustre"},
			"health": map[string]any{"state": "Available", "statusDescription": "amlfilesystem is ready for use."}}
	})

	// Elastic SAN
	view("Microsoft.ElasticSan/elasticSans", func(id arm.ID, r map[string]any) map[string]any {
		tib, _ := armres.Props(r)["baseSizeTiB"].(float64)
		return map[string]any{"totalSizeTiB": tib, "totalIops": int(tib) * 5000, "totalMBps": int(tib) * 200}
	})
	view("Microsoft.ElasticSan/elasticSans/volumegroups/volumes", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"volumeId": a2dID(id), "storageTarget": map[string]any{"targetIqn": "iqn.2022-05.net.windows.core.blob.ElasticSan." + host(id),
			"targetPortalHostname": host(id) + ".z1.blob.storage.azure.net", "targetPortalPort": 3260, "provisioningState": "Succeeded", "status": "Running"}}
	})

	// File Sync
	view("Microsoft.StorageSync/storageSyncServices", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"storageSyncServiceStatus": 0, "storageSyncServiceUid": a2dID(id)}
	})
	view("Microsoft.StorageSync/storageSyncServices/syncGroups", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"syncGroupStatus": "0", "uniqueId": a2dID(id)}
	})

	// Analysis Services, Power BI Embedded, Fabric
	view("Microsoft.AnalysisServices/servers", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"state": "Succeeded", "serverFullName": "asazure://" + loc(r) + ".asazure.windows.net/" + host(id)}
	})
	view("Microsoft.PowerBIDedicated/capacities", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"state": "Succeeded", "mode": "Gen2"}
	})
	view("Microsoft.Fabric/capacities", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"state": "Active"}
	})

	// Oracle Database@Azure
	view("Oracle.Database/cloudExadataInfrastructures", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"lifecycleState": "Available"}
	})

	// Health Data Services
	view("Microsoft.HealthcareApis/workspaces/dicomservices", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"serviceUrl": "https://" + host(id.Parent()) + "-" + host(id) + ".dicom.azurehealthcareapis.com"}
	})

	// Digital Twins
	view("Microsoft.DigitalTwins/digitalTwinsInstances", func(id arm.ID, r map[string]any) map[string]any {
		return map[string]any{"hostName": host(id) + ".api." + loc(r) + ".digitaltwins.azure.net"}
	})

	// IoT Hub Device Provisioning: the service owner's keys come from listkeys
	Override("Microsoft.Devices/provisioningServices", Hook{
		View: func(s *Service, id arm.ID, r map[string]any) {
			setProps(r, map[string]any{"state": "Active", "serviceOperationsHostName": host(id) + ".azure-devices-provisioning.net",
				"deviceProvisioningHostName": loc(r) + ".azure-devices-provisioning.net", "idScope": "0ne" + a2dID(id)[:8]})
		},
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			"listkeys": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				return map[string]any{"value": []any{map[string]any{"keyName": "provisioningserviceowner", "rights": "ServiceConfig, EnrollmentWrite, EnrollmentRead, RegistrationStatusRead, RegistrationStatusWrite",
					"primaryKey": armres.Key("dps/primary/" + key(id)), "secondaryKey": armres.Key("dps/secondary/" + key(id))}}}, nil
			},
		},
	})
}
