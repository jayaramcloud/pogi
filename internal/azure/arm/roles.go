package arm

// Role is a built-in Azure role: its real ID and permissions.
type Role struct {
	GUID, Name, Desc                   string
	Actions, NotActions, Data, NotData []string
}

// std is what the "contributor of one service" roles share; their real definitions repeat it.
var std = []string{"Microsoft.Authorization/*/read", "Microsoft.Insights/alertRules/*", "Microsoft.Resources/deployments/*",
	"Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Support/*"}

func role(guid, name, desc string, actions ...string) Role {
	return Role{GUID: guid, Name: name, Desc: desc, Actions: actions}
}

// svc is a role of one service: its actions plus std.
func svc(guid, name, desc string, actions ...string) Role {
	return role(guid, name, desc, append(append([]string{}, actions...), std...)...)
}

// data is a role of data actions only, with the reads every data role has.
func data(guid, name, desc string, actions, dataActions []string) Role {
	r := svc(guid, name, desc, actions...)
	r.Data = dataActions
	return r
}

func (r Role) not(a ...string) Role { r.NotActions = a; return r }

func ss(a ...string) []string { return a }

const (
	st = "Microsoft.Storage/storageAccounts/"
	kv = "Microsoft.KeyVault/vaults/"
)

// Roles are the built-in roles, with the IDs and permissions Azure publishes. Roles that span many
// providers in Azure keep the actions of their own service, not every line of the original.
var Roles = []Role{
	role("8e3af657-a8ff-443c-a75c-2fe8c4bcb635", "Owner", "Grants full access to manage all resources, including the ability to assign roles in Azure RBAC.", "*"),
	role("b24988ac-6180-42a0-ab88-20f7382dd24c", "Contributor", "Grants full access to manage all resources, but does not allow you to assign roles in Azure RBAC.", "*").
		not("Microsoft.Authorization/*/Delete", "Microsoft.Authorization/*/Write", "Microsoft.Authorization/elevateAccess/Action",
			"Microsoft.Blueprint/blueprintAssignments/write", "Microsoft.Blueprint/blueprintAssignments/delete",
			"Microsoft.Compute/galleries/share/action", "Microsoft.Purview/consents/write", "Microsoft.Purview/consents/delete"),
	role("acdd72a7-3385-48ef-bd42-f606fba81ae7", "Reader", "View all resources, but does not allow you to make any changes.", "*/read"),
	role("18d7d88d-d35e-4fb5-a5c3-7773c20a72d9", "User Access Administrator", "Lets you manage user access to Azure resources.",
		"*/read", "Microsoft.Authorization/*", "Microsoft.Support/*"),
	role("f58310d9-a9f6-439a-9e8d-f62e7b41a168", "Role Based Access Control Administrator", "Manage access to Azure resources by assigning roles using Azure RBAC.",
		"Microsoft.Authorization/roleAssignments/write", "Microsoft.Authorization/roleAssignments/delete", "Microsoft.Authorization/*/read",
		"Microsoft.Resources/subscriptions/read", "Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Management/managementGroups/read", "Microsoft.Support/*"),
	role("36243c78-bf99-498c-9df9-86d9f8d28608", "Resource Policy Contributor", "Users with rights to create/modify resource policy, create support ticket and read resources/hierarchy.",
		"*/read", "Microsoft.Authorization/policyassignments/*", "Microsoft.Authorization/policydefinitions/*", "Microsoft.Authorization/policysetdefinitions/*", "Microsoft.PolicyInsights/*", "Microsoft.Support/*"),
	role("4a9ae827-6dc8-4573-8ac7-8239d42aa03f", "Tag Contributor", "Lets you manage tags on entities, without providing access to the entities themselves.",
		"Microsoft.Authorization/*/read", "Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Resources/tags/*", "Microsoft.Support/*"),
	role("72fafb9e-0641-4937-9268-a91bfd8191a3", "Cost Management Reader", "Can view cost data and configuration (e.g. budgets, exports).",
		"Microsoft.Consumption/*/read", "Microsoft.CostManagement/*/read", "Microsoft.Billing/billingPeriods/read", "Microsoft.Resources/subscriptions/read",
		"Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Advisor/configurations/read", "Microsoft.Advisor/recommendations/read", "Microsoft.Management/managementGroups/read"),
	role("434105ed-43f6-45c7-a02f-909b2ba83430", "Cost Management Contributor", "Can view costs and manage cost configuration (e.g. budgets, exports).",
		"Microsoft.Consumption/*", "Microsoft.CostManagement/*", "Microsoft.Billing/billingPeriods/read", "Microsoft.Resources/subscriptions/read",
		"Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Advisor/configurations/read", "Microsoft.Advisor/recommendations/read", "Microsoft.Management/managementGroups/read"),
	role("fa23ad8b-c56e-40d8-ac0c-ce449e1d2c64", "Billing Reader", "Allows read access to billing data.",
		"Microsoft.Authorization/*/read", "Microsoft.Billing/*/read", "Microsoft.Commerce/*/read", "Microsoft.Consumption/*/read", "Microsoft.Management/managementGroups/read",
		"Microsoft.CostManagement/*/read", "Microsoft.Support/*"),
	role("39bc4728-0917-49c7-9d2c-d95423bc2eb4", "Security Reader", "View permissions for Microsoft Defender for Cloud. Can view recommendations, alerts, a security policy, and security states, but cannot make changes.",
		"Microsoft.Authorization/*/read", "Microsoft.Insights/alertRules/read", "Microsoft.operationalInsights/workspaces/*/read", "Microsoft.Resources/deployments/*/read",
		"Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Security/*/read", "Microsoft.IoTSecurity/*/read", "Microsoft.Support/*/read"),
	role("fb1c8493-542b-48eb-b624-b4c8fea62acd", "Security Admin", "View and update permissions for Microsoft Defender for Cloud. Same permissions as the Security Reader role and can also update the security policy and dismiss alerts and recommendations.",
		"Microsoft.Authorization/policyAssignments/*", "Microsoft.Authorization/*/read", "Microsoft.Insights/alertRules/*", "Microsoft.Resources/deployments/*",
		"Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Security/*", "Microsoft.Support/*"),

	// Storage
	role("17d1049b-9a84-46fb-8f53-869881c3d3ab", "Storage Account Contributor", "Permits management of storage accounts. Provides access to the account key, which can be used to access data via Shared Key authorization.",
		"Microsoft.Authorization/*/read", "Microsoft.Insights/alertRules/*", "Microsoft.Insights/diagnosticSettings/*", "Microsoft.Network/virtualNetworks/subnets/joinViaServiceEndpoint/action",
		"Microsoft.ResourceHealth/availabilityStatuses/read", "Microsoft.Resources/deployments/*", "Microsoft.Resources/subscriptions/resourceGroups/read", st+"*", "Microsoft.Support/*"),
	role("81a9662b-bebf-436f-a333-f67b29880f12", "Storage Account Key Operator Service Role", "Permits listing and regenerating storage account access keys.",
		"Microsoft.Authorization/*/read", "Microsoft.Resources/deployments/*", "Microsoft.Resources/subscriptions/resourceGroups/read", st+"listkeys/action", st+"regeneratekey/action"),
	data("b7e6dc6d-f1e8-4753-8033-0f276bb0955b", "Storage Blob Data Owner", "Provides full access to Azure Storage blob containers and data, including assigning POSIX access control.",
		ss(st+"blobServices/containers/*"), ss(st+"blobServices/containers/blobs/*")),
	data("ba92f5b4-2d11-453d-a403-e96b0029c9fe", "Storage Blob Data Contributor", "Allows for read, write and delete access to Azure Storage blob containers and data.",
		ss(st+"blobServices/containers/delete", st+"blobServices/containers/read", st+"blobServices/containers/write", st+"blobServices/generateUserDelegationKey/action"),
		ss(st+"blobServices/containers/blobs/delete", st+"blobServices/containers/blobs/read", st+"blobServices/containers/blobs/write",
			st+"blobServices/containers/blobs/move/action", st+"blobServices/containers/blobs/add/action")),
	data("2a2b9908-6ea1-4ae2-8e65-a410df84e7d1", "Storage Blob Data Reader", "Allows for read access to Azure Storage blob containers and data.",
		ss(st+"blobServices/containers/read", st+"blobServices/generateUserDelegationKey/action"), ss(st+"blobServices/containers/blobs/read")),
	data("db58b8e5-c6ad-4a2a-8342-4190687cbf4a", "Storage Blob Delegator", "Get a user delegation key, which can then be used to create a shared access signature for a container or blob that is signed with Azure AD credentials.",
		ss(st+"blobServices/generateUserDelegationKey/action"), nil),
	data("974c5e8b-45b9-4653-ba55-5f855dd0fb88", "Storage Queue Data Contributor", "Allows for read, write, and delete access to Azure Storage queues and queue messages.",
		ss(st+"queueServices/queues/delete", st+"queueServices/queues/read", st+"queueServices/queues/write"),
		ss(st+"queueServices/queues/messages/delete", st+"queueServices/queues/messages/read", st+"queueServices/queues/messages/write", st+"queueServices/queues/messages/process/action")),
	data("19e7f393-937e-4f77-808e-94535e297925", "Storage Queue Data Reader", "Allows for read access to Azure Storage queues and queue messages.",
		ss(st+"queueServices/queues/read"), ss(st+"queueServices/queues/messages/read")),
	data("8a0f0c08-91a1-4084-bc3d-661d67233fed", "Storage Queue Data Message Processor", "Allows for peek, receive, and delete access to Azure Storage queue messages.",
		nil, ss(st+"queueServices/queues/messages/read", st+"queueServices/queues/messages/process/action")),
	data("c6a89b2d-59bc-44d0-9896-0f6e12d7b80a", "Storage Queue Data Message Sender", "Allows for sending of Azure Storage queue messages.",
		nil, ss(st+"queueServices/queues/messages/add/action")),
	data("0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3", "Storage Table Data Contributor", "Allows for read, write and delete access to Azure Storage tables and entities.",
		ss(st+"tableServices/tables/read", st+"tableServices/tables/write", st+"tableServices/tables/delete"),
		ss(st+"tableServices/tables/entities/read", st+"tableServices/tables/entities/write", st+"tableServices/tables/entities/delete",
			st+"tableServices/tables/entities/add/action", st+"tableServices/tables/entities/update/action")),
	data("76199698-9eea-4c19-bc75-cec21354c6b6", "Storage Table Data Reader", "Allows for read access to Azure Storage tables and entities.",
		ss(st+"tableServices/tables/read"), ss(st+"tableServices/tables/entities/read")),

	// Key Vault
	data("00482a5a-887f-4fb3-b363-3b7fe8e74483", "Key Vault Administrator", "Perform all data plane operations on a key vault and all objects in it, including certificates, keys, and secrets. Cannot manage key vault resources or manage role assignments.",
		nil, ss("Microsoft.KeyVault/*")),
	data("b86a8fe4-44ce-4948-aee5-eccb2c155cd7", "Key Vault Secrets Officer", "Perform any action on the secrets of a key vault, except manage permissions.",
		nil, ss(kv+"secrets/*")),
	data("4633458b-17de-408a-b874-0445c86b69e6", "Key Vault Secrets User", "Read secret contents. Only works for key vaults that use the 'Azure role-based access control' permission model.",
		nil, ss(kv+"secrets/getSecret/action", kv+"secrets/readMetadata/action")),
	data("21090545-7ca7-4776-b22c-e363652d74d2", "Key Vault Reader", "Read metadata of key vaults and its certificates, keys, and secrets. Cannot read sensitive values such as secret contents or key material.",
		ss("Microsoft.KeyVault/checkNameAvailability/read", kv+"read", "Microsoft.KeyVault/deletedVaults/read", "Microsoft.KeyVault/locations/deletedVaults/read", "Microsoft.KeyVault/locations/operationResults/read"),
		ss(kv+"*/read", kv+"secrets/readMetadata/action")),
	data("14b46e9e-c2b7-41b4-b07b-48a6ebf60603", "Key Vault Crypto Officer", "Perform any action on the keys of a key vault, except manage permissions.",
		nil, ss(kv+"keys/*")),
	data("12338af0-0e69-4776-bea7-57ae8d297424", "Key Vault Crypto User", "Perform cryptographic operations using keys.",
		nil, ss(kv+"keys/read", kv+"keys/update/action", kv+"keys/backup/action", kv+"keys/encrypt/action", kv+"keys/decrypt/action",
			kv+"keys/wrap/action", kv+"keys/unwrap/action", kv+"keys/sign/action", kv+"keys/verify/action")),
	data("e147488a-f6f5-4113-8e2d-b22465e65bf6", "Key Vault Crypto Service Encryption User", "Read metadata of keys and perform wrap/unwrap operations.",
		nil, ss(kv+"keys/read", kv+"keys/wrap/action", kv+"keys/unwrap/action")),
	data("a4417e6f-fecd-4de8-b567-7b0420556985", "Key Vault Certificates Officer", "Perform any action on the certificates of a key vault, except manage permissions.",
		nil, ss(kv+"certificates/*", kv+"certificatecas/*", kv+"certificatecontacts/write")),
	data("db79e9a7-68ee-4b58-9aeb-b90e7c24fcba", "Key Vault Certificate User", "Read entire certificate contents including secret and key portion.",
		nil, ss(kv+"certificates/read", kv+"secrets/getSecret/action", kv+"secrets/readMetadata/action")),
	svc("f25e0fa2-a7c8-4377-a976-54943a77a395", "Key Vault Contributor", "Manage key vaults, but does not allow you to assign roles in Azure RBAC, and does not allow you to access secrets, keys, or certificates.",
		"Microsoft.KeyVault/*").not("Microsoft.KeyVault/locations/deletedVaults/purge/action"),

	// Messaging and events
	role("090c5cfd-751d-490a-894a-3ce6f1109419", "Azure Service Bus Data Owner", "Allows for full access to Azure Service Bus resources.", "Microsoft.ServiceBus/*").withData("Microsoft.ServiceBus/*"),
	role("69a216fc-b8fb-44d8-bc22-1f3c2cd27a39", "Azure Service Bus Data Sender", "Allows for send access to Azure Service Bus resources.").withData("Microsoft.ServiceBus/*/send/action"),
	role("4f6d3b9b-027b-4f4c-9142-0e5a2a2247e0", "Azure Service Bus Data Receiver", "Allows for receive access to Azure Service Bus resources.").withData("Microsoft.ServiceBus/*/receive/action"),
	role("f526a384-b230-433a-b45c-95f59c4a2dec", "Azure Event Hubs Data Owner", "Allows for full access to Azure Event Hubs resources.", "Microsoft.EventHub/*").withData("Microsoft.EventHub/*"),
	role("2b629674-e913-4c01-ae53-ef4638d8f975", "Azure Event Hubs Data Sender", "Allows send access to Azure Event Hubs resources.").withData("Microsoft.EventHub/*/send/action"),
	role("a638d3c7-ab3a-418d-83e6-5f17a39d4fde", "Azure Event Hubs Data Receiver", "Allows receive access to Azure Event Hubs resources.").withData("Microsoft.EventHub/*/receive/action"),
	role("d5a91429-5739-47e2-a06b-3470a27159e7", "EventGrid Data Sender", "Allows send access to event grid events.").withData("Microsoft.EventGrid/events/send/action"),
	role("1d8c3fe3-8864-474b-8749-01e3783e8157", "EventGrid Data Contributor", "Allows send and receive access to event grid events.").withData("Microsoft.EventGrid/events/send/action", "Microsoft.EventGrid/events/receive/action"),
	role("78cbd9e7-9798-4e2e-9b5a-547d9ebb31fb", "EventGrid Data Receiver", "Allows receive access to Event Grid events.").withData("Microsoft.EventGrid/events/receive/action"),
	svc("1e241071-0855-49ea-94dc-649edcd759de", "EventGrid Contributor", "Lets you manage EventGrid operations.", "Microsoft.EventGrid/*"),
	svc("428e0ff0-5e57-4d9c-a221-2c70d0e0a443", "EventGrid EventSubscription Contributor", "Lets you manage EventGrid event subscription operations.", "Microsoft.EventGrid/eventSubscriptions/*"),
	svc("7e4f1700-ea5a-4f59-8f37-079cfe29dce3", "SignalR/Web PubSub Contributor", "Create, Read, Update, and Delete SignalR service resources.", "Microsoft.SignalRService/*"),
	svc("12cf5a90-567b-43ae-8102-96cf46c7d9b4", "Web PubSub Service Owner", "Full access to Azure Web PubSub Service REST APIs.", "Microsoft.SignalRService/*"),

	// Databases and caches
	role("fbdf93bf-df7d-467e-a4d2-9458aa1360c8", "Cosmos DB Account Reader Role", "Can read Azure Cosmos DB account data. See DocumentDB Account Contributor for managing Azure Cosmos DB accounts.",
		"Microsoft.Authorization/*/read", "Microsoft.Insights/alertRules/read", "Microsoft.Insights/metricDefinitions/read", "Microsoft.Insights/metrics/read",
		"Microsoft.DocumentDB/*/read", "Microsoft.DocumentDB/databaseAccounts/readonlykeys/action", "Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Support/*"),
	svc("5bd9cd88-fe45-4216-938b-f97437e15450", "DocumentDB Account Contributor", "Can manage Azure Cosmos DB accounts. Azure Cosmos DB is a globally distributed multi-model database service.",
		"Microsoft.DocumentDb/databaseAccounts/*", "Microsoft.Network/virtualNetworks/subnets/joinViaServiceEndpoint/action"),
	svc("230815da-be43-4aae-9cb4-875f7bd000aa", "Cosmos DB Operator", "Lets you manage Azure Cosmos DB accounts, but not access data in them. Prevents access to account keys and connection strings.",
		"Microsoft.DocumentDb/databaseAccounts/*").not("Microsoft.DocumentDB/databaseAccounts/listKeys/*", "Microsoft.DocumentDB/databaseAccounts/readonlykeys/*",
		"Microsoft.DocumentDB/databaseAccounts/regenerateKey/*", "Microsoft.DocumentDB/databaseAccounts/listConnectionStrings/*", "Microsoft.DocumentDB/databaseAccounts/listKeys/action"),
	svc("e0f68234-74aa-48ed-b826-c38b57376e17", "Redis Cache Contributor", "Lets you manage Redis caches, but not access to them.", "Microsoft.Cache/register/action", "Microsoft.Cache/redis/*"),
	svc("9b7fa17d-e63e-47b0-bb0a-15c516ac86ec", "SQL DB Contributor", "Lets you manage SQL databases, but not access to them. Also, you can't manage their security-related policies or their parent SQL servers.",
		"Microsoft.Sql/locations/*/read", "Microsoft.Sql/servers/databases/*").not("Microsoft.Sql/servers/databases/ledgerDigestUploads/enable/action", "Microsoft.Sql/servers/databases/auditingSettings/*"),
	svc("6d8ee4ec-f05a-4a1d-8b00-a9b17e38b437", "SQL Server Contributor", "Lets you manage SQL servers and databases, but not access to them, and not their security-related policies.",
		"Microsoft.Sql/servers/*").not("Microsoft.Sql/servers/auditingSettings/*", "Microsoft.Sql/servers/databases/auditingSettings/*", "Microsoft.Sql/servers/securityAlertPolicies/*"),
	role("5ae67dd6-50cb-40e7-96ff-dc2bfa4b606b", "App Configuration Data Owner", "Allows full access to App Configuration data.").withData("Microsoft.AppConfiguration/configurationStores/*/read",
		"Microsoft.AppConfiguration/configurationStores/*/write", "Microsoft.AppConfiguration/configurationStores/*/delete", "Microsoft.AppConfiguration/configurationStores/*/action",
		"Microsoft.AppConfiguration/configurationStores/*/snapshot/*"),
	role("516239f1-63e1-4d78-a4de-a74fb236a071", "App Configuration Data Reader", "Allows read access to App Configuration data.").withData("Microsoft.AppConfiguration/configurationStores/*/read"),

	// Compute, web, containers, network
	svc("9980e02c-c2be-4d73-94e8-173b1dc7cf3c", "Virtual Machine Contributor", "Create and manage virtual machines, manage disks, install and run software, reset password of the root user of the virtual machine using VM extensions, and manage local user accounts using VM extensions. This role does not grant you management access to the virtual network or storage account the virtual machines are connected to.",
		"Microsoft.Compute/availabilitySets/*", "Microsoft.Compute/locations/*", "Microsoft.Compute/virtualMachines/*", "Microsoft.Compute/virtualMachineScaleSets/*",
		"Microsoft.Compute/disks/write", "Microsoft.Compute/disks/read", "Microsoft.Compute/disks/delete", "Microsoft.DevTestLab/schedules/*",
		"Microsoft.Network/applicationGateways/backendAddressPools/join/action", "Microsoft.Network/loadBalancers/backendAddressPools/join/action",
		"Microsoft.Network/loadBalancers/inboundNatPools/join/action", "Microsoft.Network/loadBalancers/inboundNatRules/join/action", "Microsoft.Network/loadBalancers/probes/join/action",
		"Microsoft.Network/loadBalancers/read", "Microsoft.Network/locations/*/read", "Microsoft.Network/networkInterfaces/*", "Microsoft.Network/networkSecurityGroups/join/action",
		"Microsoft.Network/networkSecurityGroups/read", "Microsoft.Network/publicIPAddresses/join/action", "Microsoft.Network/publicIPAddresses/read",
		"Microsoft.Network/virtualNetworks/read", "Microsoft.Network/virtualNetworks/subnets/join/action", "Microsoft.RecoveryServices/locations/*",
		"Microsoft.ResourceHealth/availabilityStatuses/read", "Microsoft.Storage/storageAccounts/listKeys/action", "Microsoft.Storage/storageAccounts/read"),
	role("1c0163c0-47e6-4577-8991-ea5c82e286e4", "Virtual Machine Administrator Login", "View Virtual Machines in the portal and login as administrator.",
		"Microsoft.Network/publicIPAddresses/read", "Microsoft.Network/virtualNetworks/read", "Microsoft.Network/loadBalancers/read", "Microsoft.Network/networkInterfaces/read",
		"Microsoft.Compute/virtualMachines/*/read", "Microsoft.HybridCompute/machines/*/read", "Microsoft.Authorization/*/read").
		withData("Microsoft.Compute/virtualMachines/login/action", "Microsoft.Compute/virtualMachines/loginAsAdmin/action"),
	role("fb879df8-f326-4884-b1cf-06f3ad86be52", "Virtual Machine User Login", "View Virtual Machines in the portal and login as a regular user.",
		"Microsoft.Network/publicIPAddresses/read", "Microsoft.Network/virtualNetworks/read", "Microsoft.Network/loadBalancers/read", "Microsoft.Network/networkInterfaces/read",
		"Microsoft.Compute/virtualMachines/*/read", "Microsoft.HybridCompute/machines/*/read", "Microsoft.Authorization/*/read").
		withData("Microsoft.Compute/virtualMachines/login/action"),
	svc("4d97b98b-1d4f-4787-a291-c67834d212e7", "Network Contributor", "Lets you manage networks, but not access to them.", "Microsoft.Network/*", "Microsoft.ResourceHealth/availabilityStatuses/read"),
	svc("befefa01-2a29-4197-83a8-272ff33ce314", "DNS Zone Contributor", "Lets you manage DNS zones and record sets in Azure DNS, but does not let you control who has access to them.", "Microsoft.Network/dnsZones/*", "Microsoft.Network/dnsOperationResults/*", "Microsoft.Network/dnsOperationStatuses/*"),
	svc("b12aa53e-6015-4669-85d0-8515ebb3ae7f", "Private DNS Zone Contributor", "Lets you manage private DNS zone resources, but not the virtual networks they are linked to.", "Microsoft.Network/privateDnsZones/*", "Microsoft.Network/privateDnsOperationResults/*", "Microsoft.Network/privateDnsOperationStatuses/*", "Microsoft.Network/virtualNetworks/join/action"),
	svc("de139f84-1756-47ae-9be6-808fbbe84772", "Website Contributor", "Lets you manage websites (not web plans), but not access to them.", "Microsoft.Web/sites/*", "Microsoft.Web/certificates/*", "Microsoft.Web/listSitesAssignedToHostName/read"),
	svc("2cc479cb-7b4d-49a8-b449-8c00fd0f0a4b", "Web Plan Contributor", "Manage the web plans for websites. Does not allow you to assign roles in Azure RBAC.", "Microsoft.Web/serverFarms/*"),
	svc("ed7f3fbd-7b88-4dd4-9017-9adb7ce333f8", "Azure Kubernetes Service Contributor Role", "Grants access to read and write Azure Kubernetes Service clusters", "Microsoft.ContainerService/managedClusters/read", "Microsoft.ContainerService/managedClusters/write"),
	role("4abbcc35-e782-43d8-92c5-2d3f1bd2253f", "Azure Kubernetes Service Cluster User Role", "List cluster user credential action.",
		"Microsoft.ContainerService/managedClusters/listClusterUserCredential/action", "Microsoft.ContainerService/managedClusters/runcommand/action"),
	role("7f951dda-4ed3-4680-a7ca-43fe172d538d", "AcrPull", "Pull artifacts from a container registry.").withData("Microsoft.ContainerRegistry/registries/pull/read"),
	role("8311e382-0749-4cb8-b61a-304f252e45ec", "AcrPush", "Push artifacts to or pull artifacts from a container registry.").withData("Microsoft.ContainerRegistry/registries/pull/read", "Microsoft.ContainerRegistry/registries/push/write"),
	role("c2f4ef07-c644-48eb-af81-4b1b4947fb11", "AcrDelete", "Delete artifacts from a container registry.").withData("Microsoft.ContainerRegistry/registries/artifacts/delete"),
	svc("312a565d-c81f-4fd8-895a-4e21e48d571c", "API Management Service Contributor", "Can manage service and the APIs", "Microsoft.ApiManagement/service/*"),
	svc("71522526-b88f-4d52-b57f-d31fc3546d0d", "API Management Service Reader Role", "Read-only access to service and APIs", "Microsoft.ApiManagement/service/*/read"),

	// Identity, monitoring, integration
	svc("e40ec5ca-96e0-45a2-b4ff-59039f2c2b59", "Managed Identity Contributor", "Create, Read, Update, and Delete User Assigned Identity", "Microsoft.ManagedIdentity/userAssignedIdentities/read",
		"Microsoft.ManagedIdentity/userAssignedIdentities/write", "Microsoft.ManagedIdentity/userAssignedIdentities/delete"),
	role("f1a07417-d97a-45cb-824c-7a7467783830", "Managed Identity Operator", "Read and Assign User Assigned Identity", "Microsoft.ManagedIdentity/userAssignedIdentities/*/read",
		"Microsoft.ManagedIdentity/userAssignedIdentities/*/assign/action", "Microsoft.Authorization/*/read", "Microsoft.Insights/alertRules/*", "Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Support/*"),
	role("749f88d5-cbae-40b8-bcfc-e573ddc772fa", "Monitoring Contributor", "Can read all monitoring data and edit monitoring settings.", "*/read", "Microsoft.AlertsManagement/alerts/*",
		"Microsoft.AlertsManagement/alertsSummary/*", "Microsoft.Insights/actiongroups/*", "Microsoft.Insights/activityLogAlerts/*", "Microsoft.Insights/alertRules/*",
		"Microsoft.Insights/diagnosticSettings/*", "Microsoft.Insights/metricAlerts/*", "Microsoft.Insights/scheduledQueryRules/*", "Microsoft.Insights/components/*",
		"Microsoft.OperationalInsights/workspaces/*", "Microsoft.Resources/deployments/*", "Microsoft.Support/*"),
	role("43d0d8ad-25c7-4714-9337-8ba259a9fe05", "Monitoring Reader", "Can read all monitoring data.", "*/read"),
	role("3913510d-42f4-4e42-8a64-420c390055eb", "Monitoring Metrics Publisher", "Enables publishing metrics against Azure resources.", "Microsoft.Insights/Register/Action", "Microsoft.Support/*", "Microsoft.Resources/subscriptions/resourceGroups/read").
		withData("Microsoft.Insights/Metrics/Write"),
	svc("92aaf0da-9dab-42b6-94a3-d43ce8d16293", "Log Analytics Contributor", "Log Analytics Contributor can read all monitoring data and edit monitoring settings.", "*/read", "Microsoft.ClassicCompute/virtualMachines/extensions/*",
		"Microsoft.OperationalInsights/*", "Microsoft.OperationsManagement/*", "Microsoft.Automation/automationAccounts/*", "Microsoft.Storage/storageAccounts/listKeys/action"),
	role("73c42c96-874c-492b-b04d-ab87d138a893", "Log Analytics Reader", "Log Analytics Reader can view and search all monitoring data as well as and view monitoring settings.", "*/read",
		"Microsoft.OperationalInsights/workspaces/analytics/query/action", "Microsoft.OperationalInsights/workspaces/search/action", "Microsoft.Support/*").not("Microsoft.OperationalInsights/workspaces/sharedKeys/read"),
	svc("673868aa-7521-48a0-acc6-0f60742d39f5", "Data Factory Contributor", "Create and manage data factories, as well as child resources within them.", "Microsoft.DataFactory/dataFactories/*", "Microsoft.DataFactory/factories/*", "Microsoft.EventGrid/eventSubscriptions/*"),
	svc("87a39d53-fc1b-424a-814c-f7e04687dc9e", "Logic App Contributor", "Lets you manage logic apps, but not change access to them.", "Microsoft.Logic/*", "Microsoft.Web/connectionGateways/*", "Microsoft.Web/connections/*", "Microsoft.Web/customApis/*", "Microsoft.Web/serverFarms/join/action"),
	svc("f353d9bd-d4a6-484e-a77a-8050b599b867", "Automation Contributor", "Manage Azure Automation resources and other resources using Azure Automation.", "Microsoft.Automation/automationAccounts/*"),
	svc("5e467623-bb1f-42f4-a55d-6e525e11384b", "Backup Contributor", "Lets you manage backup service, but can't create vaults and give access to others", "Microsoft.RecoveryServices/locations/*", "Microsoft.RecoveryServices/Vaults/backupFabrics/*"),
	svc("7ca78c08-252a-4471-8644-bb5ff32d4ba0", "Search Service Contributor", "Lets you manage Search services, but not access to them.", "Microsoft.Search/searchServices/*"),
	role("8ebe5a00-799e-43f5-93ac-243d3dce84a7", "Search Index Data Contributor", "Grants full access to Azure Cognitive Search index data.").withData("Microsoft.Search/searchServices/indexes/documents/*"),
	role("1407120a-92aa-4202-b7e9-c0e197c71c8f", "Search Index Data Reader", "Grants read access to Azure Cognitive Search index data.").withData("Microsoft.Search/searchServices/indexes/documents/read"),
	role("a97b65f3-24c7-4388-baec-2e87135dc908", "Cognitive Services User", "Lets you read and list keys of Cognitive Services.", "Microsoft.CognitiveServices/*/read", "Microsoft.CognitiveServices/accounts/listkeys/action",
		"Microsoft.Insights/alertRules/read", "Microsoft.Insights/diagnosticSettings/read", "Microsoft.Insights/logDefinitions/read", "Microsoft.Insights/metricdefinitions/read", "Microsoft.Insights/metrics/read",
		"Microsoft.Resources/deployments/read", "Microsoft.Resources/subscriptions/operationresults/read", "Microsoft.Resources/subscriptions/read", "Microsoft.Resources/subscriptions/resourceGroups/read", "Microsoft.Support/*"),
	svc("25fbc0a9-bd7c-42a3-aa1a-3b75d497ee68", "Cognitive Services Contributor", "Lets you create, read, update, delete and manage keys of Cognitive Services.", "Microsoft.CognitiveServices/*"),
}

func (r Role) withData(a ...string) Role { r.Data = a; return r }
