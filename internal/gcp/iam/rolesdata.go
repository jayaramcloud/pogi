package iam

import (
	"slices"
	"strings"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
)

// Google's predefined roles for the services pogi models, one per line as
// "id|title|permissions". In permissions, "x.{a,b}" expands to x.a x.b, "@id" includes another
// role, and "rm" is resourcemanager.projects.{get,list}, which most roles carry. The basic roles
// (owner, editor, viewer) are made from every permission below and in perms.go.
const roleDefs = `
pubsub.viewer|Pub/Sub Viewer|rm pubsub.{schemas,snapshots,subscriptions,topics}.{get,list}
pubsub.publisher|Pub/Sub Publisher|rm pubsub.topics.publish
pubsub.subscriber|Pub/Sub Subscriber|rm pubsub.snapshots.{get,list} pubsub.subscriptions.{consume,get,list} pubsub.topics.attachSubscription
pubsub.editor|Pub/Sub Editor|@pubsub.viewer pubsub.schemas.{attach,create,delete,validate} pubsub.snapshots.{create,delete,seek,update} pubsub.subscriptions.{consume,create,delete,update} pubsub.topics.{attachSubscription,create,delete,detachSubscription,publish,update}
pubsub.admin|Pub/Sub Admin|@pubsub.editor pubsub.{schemas,snapshots,subscriptions,topics}.{getIamPolicy,setIamPolicy}
storage.legacyBucketReader|Storage Legacy Bucket Reader|storage.buckets.get storage.objects.list
storage.legacyBucketWriter|Storage Legacy Bucket Writer|@storage.legacyBucketReader storage.objects.{create,delete}
storage.legacyBucketOwner|Storage Legacy Bucket Owner|@storage.legacyBucketWriter storage.buckets.{getIamPolicy,setIamPolicy,update}
storage.legacyObjectReader|Storage Legacy Object Reader|storage.objects.{get,getIamPolicy}
storage.legacyObjectOwner|Storage Legacy Object Owner|@storage.legacyObjectReader storage.objects.{setIamPolicy,update}
storage.objectViewer|Storage Object Viewer|rm storage.objects.{get,list} storage.managedFolders.{get,list}
storage.objectCreator|Storage Object Creator|rm storage.objects.create
storage.objectUser|Storage Object User|rm storage.objects.{create,delete,get,list,restore,update} storage.managedFolders.{create,delete,get,list}
storage.objectAdmin|Storage Object Admin|@storage.objectUser storage.objects.{getIamPolicy,setIamPolicy} storage.managedFolders.{getIamPolicy,setIamPolicy}
storage.admin|Storage Admin|@storage.objectAdmin storage.buckets.{create,delete,get,getIamPolicy,list,setIamPolicy,update} storage.hmacKeys.{create,delete,get,list,update} storage.multipartUploads.{abort,create,list,listParts}
secretmanager.secretAccessor|Secret Manager Secret Accessor|secretmanager.versions.access
secretmanager.secretVersionAdder|Secret Manager Secret Version Adder|secretmanager.versions.add
secretmanager.secretVersionManager|Secret Manager Secret Version Manager|secretmanager.versions.{add,destroy,disable,enable,get,list}
secretmanager.viewer|Secret Manager Viewer|rm secretmanager.locations.{get,list} secretmanager.secrets.{get,list} secretmanager.versions.{get,list}
secretmanager.admin|Secret Manager Admin|@secretmanager.viewer secretmanager.secrets.{create,delete,getIamPolicy,setIamPolicy,update} secretmanager.versions.{access,add,destroy,disable,enable}
datastore.viewer|Cloud Datastore Viewer|rm datastore.databases.{get,getMetadata,list} datastore.entities.{get,list} datastore.indexes.{get,list} datastore.namespaces.{get,list} datastore.statistics.{get,list}
datastore.user|Cloud Datastore User|@datastore.viewer datastore.entities.{create,delete,update}
datastore.owner|Cloud Datastore Owner|@datastore.user datastore.indexes.{create,delete,update}
run.invoker|Cloud Run Invoker|run.routes.invoke
run.viewer|Cloud Run Viewer|rm run.{configurations,executions,jobs,revisions,routes,services,tasks}.{get,list} run.locations.list run.operations.{get,list}
run.developer|Cloud Run Developer|@run.viewer run.executions.{cancel,delete} run.jobs.{create,delete,run,update} run.revisions.delete run.services.{create,delete,update}
run.admin|Cloud Run Admin|@run.developer run.{jobs,services}.{getIamPolicy,setIamPolicy} run.routes.invoke
cloudfunctions.invoker|Cloud Functions Invoker|cloudfunctions.functions.invoke
cloudfunctions.viewer|Cloud Functions Viewer|rm cloudfunctions.functions.{get,list} cloudfunctions.locations.list cloudfunctions.operations.{get,list} cloudfunctions.runtimes.list
cloudfunctions.developer|Cloud Functions Developer|@cloudfunctions.viewer cloudfunctions.functions.{call,create,delete,sourceCodeGet,sourceCodeSet,update}
cloudfunctions.admin|Cloud Functions Admin|@cloudfunctions.developer cloudfunctions.functions.{getIamPolicy,setIamPolicy}
cloudkms.viewer|Cloud KMS Viewer|rm cloudkms.{cryptoKeyVersions,cryptoKeys,importJobs,keyRings,locations}.{get,list} cloudkms.cryptoKeyVersions.viewPublicKey
cloudkms.publicKeyViewer|Cloud KMS Public Key Viewer|cloudkms.cryptoKeyVersions.viewPublicKey cloudkms.locations.{get,list} resourcemanager.projects.get
cloudkms.cryptoKeyEncrypter|Cloud KMS CryptoKey Encrypter|cloudkms.cryptoKeyVersions.useToEncrypt cloudkms.locations.{get,list} resourcemanager.projects.get
cloudkms.cryptoKeyDecrypter|Cloud KMS CryptoKey Decrypter|cloudkms.cryptoKeyVersions.useToDecrypt cloudkms.locations.{get,list} resourcemanager.projects.get
cloudkms.cryptoKeyEncrypterDecrypter|Cloud KMS CryptoKey Encrypter/Decrypter|cloudkms.cryptoKeyVersions.{useToDecrypt,useToEncrypt} cloudkms.locations.{get,list} resourcemanager.projects.get
cloudkms.signer|Cloud KMS CryptoKey Signer|cloudkms.cryptoKeyVersions.{useToSign,viewPublicKey} cloudkms.locations.{get,list} resourcemanager.projects.get
cloudkms.signerVerifier|Cloud KMS CryptoKey Signer/Verifier|cloudkms.cryptoKeyVersions.{useToSign,useToVerify,viewPublicKey} cloudkms.locations.{get,list} resourcemanager.projects.get
cloudkms.admin|Cloud KMS Admin|rm cloudkms.cryptoKeys.{create,get,getIamPolicy,list,setIamPolicy,update} cloudkms.cryptoKeyVersions.{create,destroy,get,list,restore,update,viewPublicKey} cloudkms.importJobs.{create,get,getIamPolicy,list,setIamPolicy,useToImport} cloudkms.keyRings.{create,get,getIamPolicy,list,setIamPolicy} cloudkms.locations.{get,list}
cloudtasks.enqueuer|Cloud Tasks Enqueuer|cloudtasks.tasks.create
cloudtasks.viewer|Cloud Tasks Viewer|rm cloudtasks.{locations,queues,tasks}.{get,list}
cloudtasks.queueAdmin|Cloud Tasks Queue Admin|rm cloudtasks.queues.{create,delete,get,getIamPolicy,list,pause,purge,resume,setIamPolicy,update} cloudtasks.locations.{get,list}
cloudtasks.admin|Cloud Tasks Admin|@cloudtasks.queueAdmin @cloudtasks.viewer cloudtasks.tasks.{create,delete,fail,run}
cloudscheduler.viewer|Cloud Scheduler Viewer|rm cloudscheduler.jobs.{get,list} cloudscheduler.locations.list
cloudscheduler.admin|Cloud Scheduler Admin|@cloudscheduler.viewer cloudscheduler.jobs.{create,delete,enable,fullView,pause,run,update}
iam.serviceAccountViewer|Service Account Viewer|iam.serviceAccounts.{get,getIamPolicy,list}
iam.serviceAccountUser|Service Account User|rm iam.serviceAccounts.{actAs,get,list}
iam.serviceAccountTokenCreator|Service Account Token Creator|iam.serviceAccounts.{getAccessToken,getOpenIdToken,implicitDelegation,signBlob,signJwt}
iam.serviceAccountAdmin|Service Account Admin|rm iam.serviceAccounts.{create,delete,disable,enable,get,getIamPolicy,list,setIamPolicy,undelete,update}
iam.serviceAccountKeyAdmin|Service Account Key Admin|rm iam.serviceAccountKeys.{create,delete,disable,enable,get,list}
iam.workloadIdentityUser|Workload Identity User|iam.serviceAccounts.{get,getAccessToken,getOpenIdToken,list}
iam.roleViewer|Role Viewer|rm iam.roles.{get,list}
iam.roleAdmin|Role Administrator|@iam.roleViewer iam.roles.{create,delete,undelete,update}
iam.securityAdmin|Security Admin|@iam.roleViewer resourcemanager.projects.{getIamPolicy,setIamPolicy}
resourcemanager.projectIamAdmin|Project IAM Admin|resourcemanager.projects.{get,getIamPolicy,setIamPolicy}
browser|Browser|resourcemanager.projects.{get,getIamPolicy,list} resourcemanager.folders.{get,list} resourcemanager.organizations.get
serviceusage.serviceUsageViewer|Service Usage Viewer|serviceusage.services.{get,list}
serviceusage.serviceUsageConsumer|Service Usage Consumer|serviceusage.services.{get,list,use}
serviceusage.serviceUsageAdmin|Service Usage Admin|@serviceusage.serviceUsageConsumer serviceusage.services.{disable,enable}
logging.viewer|Logs Viewer|rm logging.{logEntries,logs}.list logging.{buckets,sinks,views}.{get,list}
logging.logWriter|Logs Writer|logging.logEntries.{create,route}
logging.admin|Logging Admin|@logging.viewer @logging.logWriter logging.{buckets,sinks,views}.{create,delete,update}
monitoring.viewer|Monitoring Viewer|rm monitoring.{alertPolicies,dashboards,metricDescriptors,monitoredResourceDescriptors}.{get,list} monitoring.timeSeries.list
monitoring.metricWriter|Monitoring Metric Writer|monitoring.metricDescriptors.{create,get,list} monitoring.monitoredResourceDescriptors.{get,list} monitoring.timeSeries.create
cloudsql.viewer|Cloud SQL Viewer|rm cloudsql.{databases,instances}.{get,list}
cloudsql.client|Cloud SQL Client|cloudsql.instances.{connect,get}
cloudsql.editor|Cloud SQL Editor|@cloudsql.viewer cloudsql.instances.{connect,create,update} cloudsql.databases.{create,update}
cloudsql.admin|Cloud SQL Admin|@cloudsql.editor cloudsql.{databases,instances}.delete cloudsql.users.{create,delete,list,update}
bigquery.jobUser|BigQuery Job User|rm bigquery.jobs.create
bigquery.dataViewer|BigQuery Data Viewer|bigquery.datasets.get bigquery.tables.{get,getData,list}
bigquery.dataEditor|BigQuery Data Editor|@bigquery.dataViewer bigquery.datasets.create bigquery.tables.{create,delete,update,updateData}
bigquery.admin|BigQuery Admin|@bigquery.dataEditor bigquery.datasets.{delete,update} bigquery.jobs.{create,list}
firebaseauth.viewer|Firebase Authentication Viewer|firebaseauth.users.{get,list}
firebaseauth.admin|Firebase Authentication Admin|@firebaseauth.viewer firebaseauth.users.{create,delete,update}
`

// expand turns "a.{b,c}.{d,e}" into its combinations.
func expand(p string) []string {
	i := strings.IndexByte(p, '{')
	if i < 0 {
		return []string{p}
	}
	j := strings.IndexByte(p[i:], '}') + i
	var out []string
	for _, alt := range strings.Split(p[i+1:j], ",") {
		out = append(out, expand(p[:i]+alt+p[j+1:])...)
	}
	return out
}

// loadRoles builds the predefined roles from roleDefs. The basic roles take every other role's
// permissions plus every permission of the modelled calls (perms.go): owner all of them, editor
// all but IAM administration and deleting the project, viewer the get and list ones.
func loadRoles() []*role {
	byID := map[string]*role{}
	var out []*role
	all := map[string]bool{}
	for p := range callPerms() {
		all[p] = true
	}
	for _, line := range strings.Split(strings.TrimSpace(roleDefs), "\n") {
		f := strings.SplitN(line, "|", 3)
		var perms []string
		for _, tok := range strings.Fields(f[2]) {
			switch {
			case tok == "rm":
				perms = append(perms, "resourcemanager.projects.get", "resourcemanager.projects.list")
			case strings.HasPrefix(tok, "@"):
				perms = append(perms, byID[tok[1:]].Perms...)
			default:
				perms = append(perms, expand(tok)...)
			}
		}
		slices.Sort(perms)
		r := &role{Name: "roles/" + f[0], Title: f[1], Description: f[1], Perms: slices.Compact(perms), Stage: adminpb.Role_GA, Etag: []byte{0}}
		byID[f[0]], out = r, append(out, r)
		for _, p := range r.Perms {
			all[p] = true
		}
	}
	basic := func(id, title string, keep func(p string) bool) *role {
		r := &role{Name: "roles/" + id, Title: title, Description: title, Stage: adminpb.Role_GA, Etag: []byte{0}}
		for p := range all {
			if keep(p) {
				r.Perms = append(r.Perms, p)
			}
		}
		slices.Sort(r.Perms)
		return r
	}
	isIAM := func(p string) bool {
		return strings.HasSuffix(p, ".setIamPolicy") || strings.HasPrefix(p, "iam.roles.") || p == "resourcemanager.projects.delete"
	}
	return append([]*role{
		basic("owner", "Owner", func(string) bool { return true }),
		basic("editor", "Editor", func(p string) bool { return !isIAM(p) }),
		basic("viewer", "Viewer", func(p string) bool { return strings.HasSuffix(p, ".get") || strings.HasSuffix(p, ".list") }),
	}, out...)
}
