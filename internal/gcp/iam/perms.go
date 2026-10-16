package iam

import (
	"strings"
	"unicode"
)

// The permission each call needs, by Google's convention: <service>.<resourcePlural>.<verb>, so
// CreateTopic is pubsub.topics.create. A service lists its collections, the methods the convention
// covers, and the exceptions ("Publish=topics.publish"). Anything not listed here is not modelled:
// it is allowed, and recorded as such.
type svcDef struct {
	prefix string // the permission's service
	colls  string // collections the convention covers
	ops    string // methods the convention maps: Create/Get/List/Update/Delete + a noun, or any verb + a noun
	over   string // exceptions, "Method=collection.verb"
	iam    string // collections that hold an IAM policy
}

// services are keyed by the pogi service name the request log shows.
var services = map[string]svcDef{
	"pubsub": {"pubsub", "topics subscriptions snapshots schemas",
		"CreateTopic GetTopic UpdateTopic DeleteTopic ListTopics CreateSubscription GetSubscription UpdateSubscription DeleteSubscription ListSubscriptions " +
			"CreateSnapshot GetSnapshot UpdateSnapshot DeleteSnapshot ListSnapshots CreateSchema GetSchema DeleteSchema ListSchemas",
		"Publish=topics.publish ListTopicSubscriptions=topics.get ListTopicSnapshots=topics.get DetachSubscription=topics.detachSubscription " +
			"Pull=subscriptions.consume StreamingPull=subscriptions.consume Acknowledge=subscriptions.consume ModifyAckDeadline=subscriptions.consume " +
			"ModifyPushConfig=subscriptions.update Seek=subscriptions.consume ValidateSchema=schemas.validate ValidateMessage=schemas.validate",
		"topics subscriptions snapshots schemas"},
	"gcs": {"storage", "", "",
		"buckets.list=buckets.list buckets.insert=buckets.create buckets.get=buckets.get buckets.patch=buckets.update buckets.delete=buckets.delete " +
			"buckets.getIamPolicy=buckets.getIamPolicy buckets.setIamPolicy=buckets.setIamPolicy objects.list=objects.list objects.insert=objects.create " +
			"objects.get=objects.get objects.delete=objects.delete objects.patch=objects.update objects.compose=objects.create objects.copy=objects.create " +
			"objects.rewrite=objects.create xml.getObject=objects.get xml.putObject=objects.create xml.deleteObject=objects.delete",
		"buckets"},
	"secretmanager": {"secretmanager", "secrets", "CreateSecret GetSecret UpdateSecret DeleteSecret ListSecrets",
		"AddSecretVersion=versions.add AccessSecretVersion=versions.access GetSecretVersion=versions.get ListSecretVersions=versions.list " +
			"EnableSecretVersion=versions.enable DisableSecretVersion=versions.disable DestroySecretVersion=versions.destroy",
		"secrets"},
	// Firestore's permissions are Datastore's. Commit and the writes need create, update or delete by what they write; they count as update.
	"firestore": {"datastore", "", "",
		"GetDocument=entities.get BatchGetDocuments=entities.get ListDocuments=entities.list ListCollectionIds=entities.list RunQuery=entities.list " +
			"RunAggregationQuery=entities.list PartitionQuery=entities.list CreateDocument=entities.create UpdateDocument=entities.update " +
			"Commit=entities.update BatchWrite=entities.update Write=entities.update DeleteDocument=entities.delete", ""},
	"run": {"run", "services jobs revisions executions",
		"CreateService GetService UpdateService DeleteService ListServices CreateJob GetJob UpdateJob DeleteJob ListJobs GetRevision DeleteRevision ListRevisions " +
			"GetExecution DeleteExecution ListExecutions RunJob CancelExecution", "", "services jobs"},
	"cloudfunctions": {"cloudfunctions", "functions", "CreateFunction GetFunction UpdateFunction DeleteFunction ListFunctions",
		"GenerateUploadUrl=functions.sourceCodeSet GenerateDownloadUrl=functions.sourceCodeGet ListRuntimes=runtimes.list", "functions"},
	"cloudkms": {"cloudkms", "keyRings cryptoKeys cryptoKeyVersions importJobs",
		"CreateKeyRing GetKeyRing ListKeyRings CreateCryptoKey GetCryptoKey UpdateCryptoKey ListCryptoKeys CreateCryptoKeyVersion GetCryptoKeyVersion " +
			"UpdateCryptoKeyVersion ListCryptoKeyVersions DestroyCryptoKeyVersion RestoreCryptoKeyVersion CreateImportJob GetImportJob ListImportJobs",
		"Encrypt=cryptoKeyVersions.useToEncrypt Decrypt=cryptoKeyVersions.useToDecrypt AsymmetricSign=cryptoKeyVersions.useToSign " +
			"AsymmetricDecrypt=cryptoKeyVersions.useToDecrypt MacSign=cryptoKeyVersions.useToSign MacVerify=cryptoKeyVersions.useToVerify " +
			"GetPublicKey=cryptoKeyVersions.viewPublicKey UpdateCryptoKeyPrimaryVersion=cryptoKeys.update",
		"keyRings cryptoKeys importJobs"},
	"cloudtasks": {"cloudtasks", "queues tasks",
		"CreateQueue GetQueue UpdateQueue DeleteQueue ListQueues PauseQueue ResumeQueue PurgeQueue CreateTask GetTask DeleteTask ListTasks RunTask", "", "queues"},
	"resourcemanager": {"resourcemanager", "projects", "GetProject ListProjects UpdateProject DeleteProject", "SearchProjects=projects.list", "projects"},
}

// opPerms is every modelled call: pogi service + "/" + method.
var opPerms = func() map[string]string {
	m := map[string]string{}
	for svc, d := range services {
		for _, op := range strings.Fields(d.ops) {
			if p := conventional(d, op); p != "" {
				m[svc+"/"+op] = p
			}
		}
		for _, kv := range strings.Fields(d.over) {
			op, p, _ := strings.Cut(kv, "=")
			m[svc+"/"+op] = d.prefix + "." + p
		}
	}
	return m
}()

// conventional is the permission of "<Verb><Noun>" when the noun's plural is one of the service's
// collections: CreateTopic is pubsub.topics.create, PauseQueue cloudtasks.queues.pause.
func conventional(d svcDef, op string) string {
	i := strings.IndexFunc(op[1:], unicode.IsUpper) + 1
	if i < 2 {
		return ""
	}
	verb, noun := strings.ToLower(op[:i]), op[i:]
	coll := strings.ToLower(noun[:1]) + noun[1:]
	if !strings.HasSuffix(coll, "s") {
		coll += "s"
	}
	for _, c := range strings.Fields(d.colls) {
		if c == coll {
			return d.prefix + "." + coll + "." + verb
		}
	}
	return ""
}

// lastColl is the collection a resource name ends in: "topics" for projects/p/topics/t.
func lastColl(resource string) string {
	seg := strings.Split(resource, "/")
	if len(seg)%2 == 1 {
		return seg[len(seg)-1]
	}
	return seg[len(seg)-2]
}

// Permission is the permission a call needs on a resource; ok is false for a call that is not
// modelled. getIamPolicy and setIamPolicy follow the collection of the resource they name.
// TestIamPermissions needs none, so it is never modelled.
func Permission(service, op, resource string) (perm string, ok bool) {
	d, found := services[service]
	if found && (op == "GetIamPolicy" || op == "SetIamPolicy") {
		verb := strings.ToLower(op[:1]) + op[1:]
		if c := lastColl(resource); strings.Contains(" "+d.iam+" ", " "+c+" ") {
			return d.prefix + "." + c + "." + verb, true
		}
		return "", false
	}
	perm, ok = opPerms[service+"/"+op]
	return perm, ok
}

// callPerms is every permission the modelled calls can need.
func callPerms() map[string]bool {
	out := map[string]bool{}
	for _, p := range opPerms {
		out[p] = true
	}
	for _, d := range services {
		for _, c := range strings.Fields(d.iam) {
			out[d.prefix+"."+c+".getIamPolicy"], out[d.prefix+"."+c+".setIamPolicy"] = true, true
		}
	}
	return out
}

// policyColls are the collections that hold a policy, for the service of a request.
func policyColls(service string) []string { return strings.Fields(services[service].iam) }
