// Package pogi runs local AWS, Google Cloud and Azure emulators in one process.
//
// Use it from Go tests:
//
//	srv := pogi.Start(t)                // in-process, random port
//	cfg := aws.Config{BaseEndpoint: aws.String(srv.URL), ...}
//
// or run the pogi binary (CLI, TUI and web console).
package pogi

// Rewrites the generated docs and the README Services table (see test/coverage_test.go).
//go:generate go test ./test -run TestCoverageDocs -update

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/acm"
	"github.com/nrndev/pogi/internal/aws/apigateway"
	"github.com/nrndev/pogi/internal/aws/appautoscaling"
	"github.com/nrndev/pogi/internal/aws/appconfig"
	"github.com/nrndev/pogi/internal/aws/appsync"
	"github.com/nrndev/pogi/internal/aws/athena"
	"github.com/nrndev/pogi/internal/aws/autoscaling"
	"github.com/nrndev/pogi/internal/aws/batch"
	"github.com/nrndev/pogi/internal/aws/cloudformation"
	"github.com/nrndev/pogi/internal/aws/cloudfront"
	"github.com/nrndev/pogi/internal/aws/cloudtrail"
	"github.com/nrndev/pogi/internal/aws/cloudwatch"
	"github.com/nrndev/pogi/internal/aws/codebuild"
	"github.com/nrndev/pogi/internal/aws/codedeploy"
	"github.com/nrndev/pogi/internal/aws/codepipeline"
	"github.com/nrndev/pogi/internal/aws/cognito"
	"github.com/nrndev/pogi/internal/aws/cognitoidentity"
	"github.com/nrndev/pogi/internal/aws/dynamodb"
	"github.com/nrndev/pogi/internal/aws/ec2"
	"github.com/nrndev/pogi/internal/aws/ecr"
	"github.com/nrndev/pogi/internal/aws/ecs"
	"github.com/nrndev/pogi/internal/aws/eks"
	"github.com/nrndev/pogi/internal/aws/elasticache"
	"github.com/nrndev/pogi/internal/aws/elbv2"
	"github.com/nrndev/pogi/internal/aws/events"
	"github.com/nrndev/pogi/internal/aws/firehose"
	"github.com/nrndev/pogi/internal/aws/fis"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/aws/glue"
	"github.com/nrndev/pogi/internal/aws/iam"
	"github.com/nrndev/pogi/internal/aws/iot"
	"github.com/nrndev/pogi/internal/aws/kafka"
	"github.com/nrndev/pogi/internal/aws/kinesis"
	"github.com/nrndev/pogi/internal/aws/kms"
	"github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/aws/logs"
	"github.com/nrndev/pogi/internal/aws/memorydb"
	"github.com/nrndev/pogi/internal/aws/mq"
	"github.com/nrndev/pogi/internal/aws/opensearch"
	"github.com/nrndev/pogi/internal/aws/organizations"
	"github.com/nrndev/pogi/internal/aws/pipes"
	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/aws/rdsdata"
	"github.com/nrndev/pogi/internal/aws/redshift"
	"github.com/nrndev/pogi/internal/aws/redshiftdata"
	"github.com/nrndev/pogi/internal/aws/redshiftserverless"
	"github.com/nrndev/pogi/internal/aws/resourceexplorer"
	"github.com/nrndev/pogi/internal/aws/route53"
	"github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/aws/s3vectors"
	"github.com/nrndev/pogi/internal/aws/scheduler"
	"github.com/nrndev/pogi/internal/aws/secretsmanager"
	"github.com/nrndev/pogi/internal/aws/servicediscovery"
	"github.com/nrndev/pogi/internal/aws/ses"
	"github.com/nrndev/pogi/internal/aws/sns"
	"github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/aws/ssm"
	"github.com/nrndev/pogi/internal/aws/stepfunctions"
	"github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/aws/tagging"
	"github.com/nrndev/pogi/internal/aws/timestreaminfluxdb"
	"github.com/nrndev/pogi/internal/aws/verifiedpermissions"
	"github.com/nrndev/pogi/internal/azure/aci"
	"github.com/nrndev/pogi/internal/azure/acr"
	azaks "github.com/nrndev/pogi/internal/azure/aks"
	"github.com/nrndev/pogi/internal/azure/apim"
	azappconfig "github.com/nrndev/pogi/internal/azure/appconfig"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/blob"
	azcompute "github.com/nrndev/pogi/internal/azure/compute"
	"github.com/nrndev/pogi/internal/azure/containerapps"
	"github.com/nrndev/pogi/internal/azure/cosmos"
	azdatabases "github.com/nrndev/pogi/internal/azure/databases"
	"github.com/nrndev/pogi/internal/azure/email"
	"github.com/nrndev/pogi/internal/azure/entra"
	"github.com/nrndev/pogi/internal/azure/eventgrid"
	"github.com/nrndev/pogi/internal/azure/eventhubs"
	azfunctions "github.com/nrndev/pogi/internal/azure/functions"
	"github.com/nrndev/pogi/internal/azure/graph"
	"github.com/nrndev/pogi/internal/azure/keyvault"
	azmonitor "github.com/nrndev/pogi/internal/azure/monitor"
	aznetwork "github.com/nrndev/pogi/internal/azure/network"
	"github.com/nrndev/pogi/internal/azure/queue"
	"github.com/nrndev/pogi/internal/azure/rbac"
	"github.com/nrndev/pogi/internal/azure/servicebus"
	"github.com/nrndev/pogi/internal/azure/signalr"
	"github.com/nrndev/pogi/internal/azure/table"
	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/artifactregistry"
	"github.com/nrndev/pogi/internal/gcp/bigquery"
	gcpcompute "github.com/nrndev/pogi/internal/gcp/compute"
	"github.com/nrndev/pogi/internal/gcp/datastore"
	"github.com/nrndev/pogi/internal/gcp/eventarc"
	"github.com/nrndev/pogi/internal/gcp/firebaseauth"
	"github.com/nrndev/pogi/internal/gcp/firestore"
	"github.com/nrndev/pogi/internal/gcp/functions"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"github.com/nrndev/pogi/internal/gcp/gke"
	gcpiam "github.com/nrndev/pogi/internal/gcp/iam"
	gcpkms "github.com/nrndev/pogi/internal/gcp/kms"
	"github.com/nrndev/pogi/internal/gcp/logging"
	"github.com/nrndev/pogi/internal/gcp/managedkafka"
	"github.com/nrndev/pogi/internal/gcp/monitoring"
	gcpoauth "github.com/nrndev/pogi/internal/gcp/oauth"
	"github.com/nrndev/pogi/internal/gcp/pubsub"
	"github.com/nrndev/pogi/internal/gcp/resourcemanager"
	"github.com/nrndev/pogi/internal/gcp/run"
	gcpscheduler "github.com/nrndev/pogi/internal/gcp/scheduler"
	"github.com/nrndev/pogi/internal/gcp/secretmanager"
	"github.com/nrndev/pogi/internal/gcp/serviceusage"
	gcpsql "github.com/nrndev/pogi/internal/gcp/sql"
	gcs "github.com/nrndev/pogi/internal/gcp/storage"
	"github.com/nrndev/pogi/internal/gcp/tasks"
	"github.com/nrndev/pogi/internal/mcp"
	"github.com/nrndev/pogi/internal/seed"
	"github.com/nrndev/pogi/internal/web"
)

// Version is set at build time with -ldflags "-X github.com/nrndev/pogi.Version=…".
var Version = "0.1.0"

// Config configures a pogi instance; zero values get sensible defaults.
type Config = core.Config

type factory struct {
	name, cloud string
	new         func(*core.Hub) core.Service
}

// services is every emulated service, in routing order.
var services = []factory{
	{"s3", core.AWS, func(h *core.Hub) core.Service { return s3.New(h) }},
	{"sqs", core.AWS, func(h *core.Hub) core.Service { return sqs.New(h) }},
	{"sns", core.AWS, func(h *core.Hub) core.Service { return sns.New(h) }},
	{"mq", core.AWS, func(h *core.Hub) core.Service { return mq.New(h) }},
	{"sts", core.AWS, func(h *core.Hub) core.Service { return sts.New(h) }},
	{"lambda", core.AWS, func(h *core.Hub) core.Service { return lambda.New(h) }},
	{"kinesis", core.AWS, func(h *core.Hub) core.Service { return kinesis.New(h) }},
	{"firehose", core.AWS, func(h *core.Hub) core.Service { return firehose.New(h) }},
	{"events", core.AWS, func(h *core.Hub) core.Service { return events.New(h) }},
	{"scheduler", core.AWS, func(h *core.Hub) core.Service { return scheduler.New(h) }},
	{"pipes", core.AWS, func(h *core.Hub) core.Service { return pipes.New(h) }},
	{"dynamodb", core.AWS, func(h *core.Hub) core.Service { return dynamodb.New(h) }},
	{"iam", core.AWS, func(h *core.Hub) core.Service { return iam.New(h) }},
	{"kms", core.AWS, func(h *core.Hub) core.Service { return kms.New(h) }},
	{"secretsmanager", core.AWS, func(h *core.Hub) core.Service { return secretsmanager.New(h) }},
	{"ssm", core.AWS, func(h *core.Hub) core.Service { return ssm.New(h) }},
	{"logs", core.AWS, func(h *core.Hub) core.Service { return logs.New(h) }},
	{"stepfunctions", core.AWS, func(h *core.Hub) core.Service { return stepfunctions.New(h) }},
	{"apigateway", core.AWS, func(h *core.Hub) core.Service { return apigateway.New(h) }},
	{"cloudformation", core.AWS, func(h *core.Hub) core.Service { return cloudformation.New(h) }},
	{"cognito-idp", core.AWS, func(h *core.Hub) core.Service { return cognito.New(h) }},
	{"ses", core.AWS, func(h *core.Hub) core.Service { return ses.New(h) }},
	{"cloudwatch", core.AWS, func(h *core.Hub) core.Service { return cloudwatch.New(h) }},
	{"acm", core.AWS, func(h *core.Hub) core.Service { return acm.New(h) }},
	{"ecr", core.AWS, func(h *core.Hub) core.Service { return ecr.New(h) }},
	{"ecs", core.AWS, func(h *core.Hub) core.Service { return ecs.New(h) }},
	{"route53", core.AWS, func(h *core.Hub) core.Service { return route53.New(h) }},
	{"ec2", core.AWS, func(h *core.Hub) core.Service { return ec2.New(h) }},
	{"autoscaling", core.AWS, func(h *core.Hub) core.Service { return autoscaling.New(h) }},
	{"application-autoscaling", core.AWS, func(h *core.Hub) core.Service { return appautoscaling.New(h) }},
	{"resourcegroupstaggingapi", core.AWS, func(h *core.Hub) core.Service { return tagging.New(h) }},
	{"rds", core.AWS, func(h *core.Hub) core.Service { return rds.New(h) }},
	{"rds-data", core.AWS, func(h *core.Hub) core.Service { return rdsdata.New(h) }},
	{"elasticache", core.AWS, func(h *core.Hub) core.Service { return elasticache.New(h) }},
	{"redshift", core.AWS, func(h *core.Hub) core.Service { return redshift.New(h) }},
	{"redshift-data", core.AWS, func(h *core.Hub) core.Service { return redshiftdata.New(h) }},
	{"redshift-serverless", core.AWS, func(h *core.Hub) core.Service { return redshiftserverless.New(h) }},
	{"opensearch", core.AWS, func(h *core.Hub) core.Service { return opensearch.New(h) }},
	{"memorydb", core.AWS, func(h *core.Hub) core.Service { return memorydb.New(h) }},
	{"timestream-influxdb", core.AWS, func(h *core.Hub) core.Service { return timestreaminfluxdb.New(h) }},
	{"elbv2", core.AWS, func(h *core.Hub) core.Service { return elbv2.New(h) }},
	{"eks", core.AWS, func(h *core.Hub) core.Service { return eks.New(h) }},
	{"batch", core.AWS, func(h *core.Hub) core.Service { return batch.New(h) }},
	{"codebuild", core.AWS, func(h *core.Hub) core.Service { return codebuild.New(h) }},
	{"kafka", core.AWS, func(h *core.Hub) core.Service { return kafka.New(h) }},
	{"glue", core.AWS, func(h *core.Hub) core.Service { return glue.New(h) }},
	{"athena", core.AWS, func(h *core.Hub) core.Service { return athena.New(h) }},
	{"organizations", core.AWS, func(h *core.Hub) core.Service { return organizations.New(h) }},
	{"verifiedpermissions", core.AWS, func(h *core.Hub) core.Service { return verifiedpermissions.New(h) }},
	{"resource-explorer-2", core.AWS, func(h *core.Hub) core.Service { return resourceexplorer.New(h) }},
	{"appconfig", core.AWS, func(h *core.Hub) core.Service { return appconfig.New(h) }},
	{"cloudfront", core.AWS, func(h *core.Hub) core.Service { return cloudfront.New(h) }},
	{"s3vectors", core.AWS, func(h *core.Hub) core.Service { return s3vectors.New(h) }},
	{"appsync", core.AWS, func(h *core.Hub) core.Service { return appsync.New(h) }},
	{"cloudtrail", core.AWS, func(h *core.Hub) core.Service { return cloudtrail.New(h) }},
	{"fis", core.AWS, func(h *core.Hub) core.Service { return fis.New(h) }},
	{"codedeploy", core.AWS, func(h *core.Hub) core.Service { return codedeploy.New(h) }},
	{"codepipeline", core.AWS, func(h *core.Hub) core.Service { return codepipeline.New(h) }},
	{"cognito-identity", core.AWS, func(h *core.Hub) core.Service { return cognitoidentity.New(h) }},
	{"iot", core.AWS, func(h *core.Hub) core.Service { return iot.New(h) }},
	{"iotdata", core.AWS, func(h *core.Hub) core.Service { return iot.NewData(h) }}, // after iot: it shares iot's broker
	{"servicediscovery", core.AWS, func(h *core.Hub) core.Service { return servicediscovery.New(h) }},
	{"sqladmin", core.GCP, func(h *core.Hub) core.Service { return gcpsql.New(h) }},    // before operations: it claims the polls of its own /v1/projects/p/operations/<id>
	{"operations", core.GCP, func(h *core.Hub) core.Service { return gapi.NewOps(h) }}, // shared google.longrunning.Operations
	{"pubsub", core.GCP, func(h *core.Hub) core.Service { return pubsub.New(h) }},      // before gcs: its matcher claims Google clients
	{"secretmanager", core.GCP, func(h *core.Hub) core.Service { return secretmanager.New(h) }},
	{"cloudkms", core.GCP, func(h *core.Hub) core.Service { return gcpkms.New(h) }},
	{"firebaseauth", core.GCP, func(h *core.Hub) core.Service { return firebaseauth.New(h) }},
	{"container", core.GCP, func(h *core.Hub) core.Service { return gke.New(h) }}, // before managedkafka: both have projects/p/locations/l/clusters; its matcher claims only what is GKE's
	{"managedkafka", core.GCP, func(h *core.Hub) core.Service { return managedkafka.New(h) }},
	{"cloudtasks", core.GCP, func(h *core.Hub) core.Service { return tasks.New(h) }}, // these three before gcs: its matcher claims Google clients
	{"cloudscheduler", core.GCP, func(h *core.Hub) core.Service { return gcpscheduler.New(h) }},
	{"eventarc", core.GCP, func(h *core.Hub) core.Service { return eventarc.New(h) }},
	{"firestore", core.GCP, func(h *core.Hub) core.Service { return firestore.New(h) }},
	{"datastore", core.GCP, func(h *core.Hub) core.Service { return datastore.New(h) }}, // before gcs: its matcher claims POST /reset
	{"run", core.GCP, func(h *core.Hub) core.Service { return run.New(h) }},             // before gcs: Google clients send X-Goog-Api-Client
	{"compute", core.GCP, func(h *core.Hub) core.Service { return gcpcompute.New(h) }},  // before gcs: its matcher claims Google clients
	// Artifact Registry: before gcs, like run
	{"artifactregistry", core.GCP, func(h *core.Hub) core.Service { return artifactregistry.New(h) }},
	{"cloudfunctions", core.GCP, func(h *core.Hub) core.Service { return functions.New(h) }},
	// identity and platform, before gcs: its matcher claims Google clients. "gcpiam" because AWS has an "iam"
	{"gcpiam", core.GCP, func(h *core.Hub) core.Service { return gcpiam.New(h) }},
	{"iamcredentials", core.GCP, func(h *core.Hub) core.Service { return gcpiam.NewCredentials(h) }}, // after gcpiam: it shares its accounts
	{"gcpoauth", core.GCP, func(h *core.Hub) core.Service { return gcpoauth.New(h) }},
	{"resourcemanager", core.GCP, func(h *core.Hub) core.Service { return resourcemanager.New(h) }},
	{"serviceusage", core.GCP, func(h *core.Hub) core.Service { return serviceusage.New(h) }},
	{"bigquery", core.GCP, func(h *core.Hub) core.Service { return bigquery.New(h) }}, // these three before gcs: its matcher claims Google clients
	{"logging", core.GCP, func(h *core.Hub) core.Service { return logging.New(h) }},
	{"monitoring", core.GCP, func(h *core.Hub) core.Service { return monitoring.New(h) }},
	{"gcs", core.GCP, func(h *core.Hub) core.Service { return gcs.New(h) }},
	{"apim", core.Azure, func(h *core.Hub) core.Service { return apim.New(h) }}, // before the other Azure services: it claims everything on <svc>.azure-api.localhost and /_azure/apim/, SignalR's paths included
	{"azblob", core.Azure, func(h *core.Hub) core.Service { return blob.New(h) }},
	{"azqueue", core.Azure, func(h *core.Hub) core.Service { return queue.New(h) }},
	{"aztable", core.Azure, func(h *core.Hub) core.Service { return table.New(h) }},
	{"azappconfig", core.Azure, func(h *core.Hub) core.Service { return azappconfig.New(h) }}, // before keyvault: a Bearer GET <store>.azconfig.localhost/keys is ours
	{"keyvault", core.Azure, func(h *core.Hub) core.Service { return keyvault.New(h) }},
	{"cosmos", core.Azure, func(h *core.Hub) core.Service { return cosmos.New(h) }},
	{"servicebus", core.Azure, func(h *core.Hub) core.Service { return servicebus.New(h) }},
	{"eventgrid", core.Azure, func(h *core.Hub) core.Service { return eventgrid.New(h) }},
	{"signalr", core.Azure, func(h *core.Hub) core.Service { return signalr.New(h) }},
	{"azemail", core.Azure, func(h *core.Hub) core.Service { return email.New(h) }},
	{"eventhubs", core.Azure, func(h *core.Hub) core.Service { return eventhubs.New(h) }},
	{"azfunctions", core.Azure, func(h *core.Hub) core.Service { return azfunctions.New(h) }},
	{"azdatabases", core.Azure, func(h *core.Hub) core.Service { return azdatabases.New(h) }},
	{"azaks", core.Azure, func(h *core.Hub) core.Service { return azaks.New(h) }},
	// Container Apps: its data plane is *.azurecontainerapps.localhost and /_azure/containerapps/
	{"azcontainerapps", core.Azure, func(h *core.Hub) core.Service { return containerapps.New(h) }},
	// Container Instances: its data plane is *.azurecontainer.localhost and /_azure/aci/
	{"azaci", core.Azure, func(h *core.Hub) core.Service { return aci.New(h) }},
	// Container Registry: the sign-in endpoints of <name>.azurecr.localhost; its /v2/ is ECR's
	{"azacr", core.Azure, func(h *core.Hub) core.Service { return acr.New(h) }},
	{"azmonitor", core.Azure, func(h *core.Hub) core.Service { return azmonitor.New(h) }}, // matches only its own hosts, /_azure/monitor/ and Microsoft.Insights metrics paths
	{"aznetwork", core.Azure, func(h *core.Hub) core.Service { return aznetwork.New(h) }},
	{"azcompute", core.Azure, func(h *core.Hub) core.Service { return azcompute.New(h) }}, // matches only the instance metadata paths
	{"arm", core.Azure, func(h *core.Hub) core.Service { return arm.New(h) }},
	{"entra", core.Azure, func(h *core.Hub) core.Service { rbac.New(h); return entra.New(h) }}, // rbac is the Azure authorizer: no service of its own, it comes with the sign-in service
	{"graph", core.Azure, func(h *core.Hub) core.Service { return graph.New(h) }},
}

// The generated long-tail services come after the hand-written ones, and only
// for names the registry does not have.
func init() {
	for _, g := range generic.Services() {
		if !slices.ContainsFunc(services, func(f factory) bool { return f.name == g.Name }) {
			services = append(services, factory{g.Name, core.AWS, g.New})
		}
	}
}

// ServiceNames lists every service pogi can run.
func ServiceNames() []string {
	var out []string
	for _, f := range services {
		out = append(out, f.name)
	}
	return out
}

// Server is a configured pogi instance; it is an http.Handler.
type Server struct {
	Hub   *core.Hub
	URL   string            // set once listening
	Ports map[string]string // base URLs of services on their own ports (Azure Storage)

	closePorts []func() // record the extra ports pogi opened (the services close them) for the shutdown list
}

// New builds a server with the enabled services.
func New(cfg Config, log *slog.Logger) *Server {
	if cfg.Port == 0 {
		cfg.Port = 7644
	}
	if cfg.ECRPort == 0 {
		cfg.ECRPort = 4510
	}
	if cfg.SMTPPort == 0 {
		cfg.SMTPPort = 2525
	}
	if cfg.MQTTPort == 0 {
		cfg.MQTTPort = 1883
	}
	if cfg.AMQPPort == 0 {
		cfg.AMQPPort = 5672
	}
	if cfg.TransferPort == 0 {
		cfg.TransferPort = 2222
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Region == "" {
		cfg.Region = core.DefaultRegion
	}
	if cfg.DataDir == "" {
		home, _ := os.UserHomeDir()
		cfg.DataDir = filepath.Join(home, ".pogi")
	}
	if cfg.Lambda == "" {
		cfg.Lambda = "auto"
	}
	if cfg.IAM == "" {
		cfg.IAM = "off"
	}
	cfg.Version = Version
	hub := core.NewHub(cfg, log)
	addChaos(hub, cfg.File)
	hub.CheckInput = generic.CheckInput
	for _, f := range services {
		on := enabled(cfg.Services, f.name, f.cloud)
		if f.name == "operations" { // every GCP service's long-running operations: on unless excluded
			on = enabled(slices.DeleteFunc(slices.Clone(cfg.Services), func(s string) bool { return !strings.HasPrefix(strings.TrimSpace(s), "-") }), f.name, f.cloud)
		}
		if on {
			hub.Register(f.new(hub))
		}
	}
	hub.Mount("/_pogi/", web.New(hub))
	hub.Mount("/_pogi/mcp", mcp.Handler(hub))
	hub.Mount("/favicon.ico", web.Favicon)
	seed.Hook(hub, cfg.File)
	return &Server{Hub: hub, Ports: map[string]string{}}
}

// enabled reports whether list (services, clouds or "all"; "-name" or
// "-cloud" excludes) turns a service on. Empty, or only exclusions, means all.
func enabled(list []string, name, cloud string) bool {
	on, picked := false, false
	for _, s := range list {
		s = strings.TrimSpace(strings.ToLower(s))
		if x, ok := strings.CutPrefix(s, "-"); ok {
			if x == name || x == cloud {
				return false
			}
			continue
		}
		picked = true
		on = on || s == name || s == cloud || s == "all"
	}
	return on || !picked
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.Hub.ServeHTTP(w, r) }

// Run serves until ctx ends: it loads persisted state, runs init hooks
// (boot, start, ready, shutdown) and calls ready once requests are accepted.
func (s *Server) Run(ctx context.Context, ready func(url string)) error {
	h := s.Hub
	cfg := h.Cfg
	if !cfg.AllowExternal && !core.Loopback(cfg.Host) && !inContainer() {
		return fmt.Errorf("refusing to listen on %s, where other machines can reach pogi: pass --allow-external "+
			"(or set POGI_ALLOW_EXTERNAL=1, or allowExternal: true under server: in pogi.yaml)", cfg.Host)
	}
	if err := config.CheckRelay(cfg.SMTPRelay); err != nil {
		return fmt.Errorf("SMTP relay: %w", err)
	}
	core.Go("docker.Sweep", func() { h.Docker().Sweep() }) // containers a crashed pogi left behind
	if err := h.OpenStorage(); err != nil {
		h.Log.Warn("could not load saved state", "err", err)
	}
	initDirs := []string{filepath.Join(cfg.DataDir, "init"), "/etc/localstack/init"}
	var tc *tls.Config
	if !cfg.NoTLS {
		var err error
		if tc, err = h.TLS(); err != nil {
			if cfg.TLSCert+cfg.TLSKey != "" {
				return err // the user's own certificate: no silent fallback to plain HTTP
			}
			h.Log.Warn("TLS disabled", "err", err)
		}
	}
	// Other local users have no business in the data dir (the CA key, state, snapshots).
	if cfg.DataDir != "" {
		if err := os.Chmod(cfg.DataDir, 0o700); err != nil {
			h.Log.Debug("could not restrict the data dir", "err", err)
		}
	}
	// A network-facing pogi with no IAM lets anyone who reaches the port use every API.
	if !core.Loopback(cfg.Host) && h.IAMMode() == "off" {
		h.Log.Warn("pogi listens beyond this machine and IAM is off: anyone who can reach the port can use every API; " +
			"publish the port as -p 127.0.0.1:7644:7644 (Docker) or turn IAM on")
	}
	// listen opens a port that speaks HTTP and HTTPS; on 127.0.0.1 it also
	// takes ::1, since "localhost" (and *.localhost) often resolve there first.
	listen := func(port int) ([]net.Listener, error) {
		ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(port)))
		if err != nil {
			return nil, err
		}
		lns := []net.Listener{ln}
		if cfg.Host == "127.0.0.1" {
			if ln6, err := net.Listen("tcp", net.JoinHostPort("::1", strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))); err == nil {
				lns = append(lns, ln6)
			}
		}
		if tc != nil {
			for i := range lns {
				lns[i] = core.SniffTLS(lns[i], tc)
			}
		}
		return lns, nil
	}
	lns, err := listen(cfg.Port)
	if err != nil {
		return err
	}
	host := cfg.Host
	if host == "0.0.0.0" || host == "::" || host == "" {
		host = "localhost"
	}
	s.URL = "http://" + net.JoinHostPort(host, strconv.Itoa(lns[0].Addr().(*net.TCPAddr).Port))
	h.URL = s.URL
	env := core.AWSEnv(s.URL, cfg.Region)
	h.Init.Run("boot", env, initDirs...)
	srv := &http.Server{Handler: s, Protocols: core.Protocols(), ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
	errc := make(chan error, len(lns))
	for _, ln := range lns {
		go func() { errc <- srv.Serve(ln) }()
	}
	var extra []*http.Server
	for _, svc := range h.Services() {
		p, ok := svc.(core.Porter)
		if !ok || cfg.NoPorts {
			continue
		}
		plns, err := listen(p.Port())
		if err != nil {
			h.Log.Warn("extra port unavailable", "service", svc.Info().Name, "port", p.Port(), "err", err)
			continue
		}
		es := &http.Server{Handler: h.Handler(svc), Protocols: core.Protocols(), ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute}
		extra = append(extra, es)
		s.closePorts = append(s.closePorts, func() { h.ClosedPort(svc.Info().Name, "HTTP", p.Port()) })
		s.Ports[svc.Info().Name] = "http://" + net.JoinHostPort(host, strconv.Itoa(p.Port()))
		h.SetEndpoint("port:"+svc.Info().Name, net.JoinHostPort(host, strconv.Itoa(p.Port()))) // Azure Functions hosts use the storage ports
		for _, pln := range plns {
			core.Go("pogi.Serve", func() { es.Serve(pln) })
		}
	}
	s.smtp(host)
	s.amqp(host)
	h.Init.Run("start", env, initDirs...)
	if cfg.File != nil { // seeded before Ready., so whoever waits for it sees the resources
		if err := seed.Apply(h, cfg.File); err != nil {
			for _, es := range append(extra, srv) {
				es.Close()
			}
			h.Close()
			return err
		}
	}
	flushed := make(chan struct{})
	core.Go("pogi.Flush", func() { defer close(flushed); h.Flush(ctx) })
	core.Go("pogi.Run", func() { h.Init.Run("ready", env, initDirs...) })
	h.Ready.Store(true)
	if ready != nil {
		ready(s.URL)
	}
	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	h.BeginShutdown()
	h.Init.Run("shutdown", env, initDirs...)
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
	for _, es := range extra {
		es.Shutdown(sctx)
	}
	for _, f := range s.closePorts {
		f()
	}
	<-flushed
	h.Close() // after in-flight requests: it writes the state one last time
	return nil
}

// smtp serves SES's SMTP interface on Config.SMTPPort; host names it in Ports["ses"].
func (s *Server) smtp(host string) {
	cfg := s.Hub.Cfg
	svc, ok := s.Hub.Service("ses").(*ses.Service)
	if !ok || cfg.SMTPPort <= 0 || cfg.NoPorts {
		return
	}
	port := strconv.Itoa(cfg.SMTPPort)
	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, port))
	if err != nil {
		s.Hub.Log.Warn("SMTP port unavailable", "port", cfg.SMTPPort, "err", err)
		return
	}
	core.Go("pogi.ServeSMTP", func() { svc.ServeSMTP(ln) })
	if cfg.Host == "127.0.0.1" { // "localhost" may resolve to ::1 first, as on the main port
		if ln6, err := net.Listen("tcp", net.JoinHostPort("::1", port)); err == nil {
			core.Go("pogi.ServeSMTP", func() { svc.ServeSMTP(ln6) })
		}
	}
	s.closePorts = append(s.closePorts, func() { s.Hub.ClosedPort("ses", "SMTP", cfg.SMTPPort) }) // SES closes its listeners itself
	s.Ports["ses"] = "smtp://" + net.JoinHostPort(host, port)
}

// amqp serves the AMQP 1.0 broker on Config.AMQPPort; host names it in Ports["amqp"].
func (s *Server) amqp(host string) {
	cfg := s.Hub.Cfg
	if cfg.AMQPPort <= 0 || cfg.NoPorts {
		return
	}
	port := strconv.Itoa(cfg.AMQPPort)
	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, port))
	if err != nil {
		s.Hub.Log.Warn("AMQP port unavailable", "port", cfg.AMQPPort, "err", err)
		return
	}
	go s.Hub.AMQP().Serve(ln)
	if cfg.Host == "127.0.0.1" { // "localhost" may resolve to ::1 first, as on the main port
		if ln6, err := net.Listen("tcp", net.JoinHostPort("::1", port)); err == nil {
			go s.Hub.AMQP().Serve(ln6)
		}
	}
	s.closePorts = append(s.closePorts, func() { s.Hub.ClosedPort("amqp", "AMQP", cfg.AMQPPort) }) // Hub.Close closes it
	s.Ports["amqp"] = "amqp://" + net.JoinHostPort(host, port)
	s.Hub.SetEndpoint("amqp", net.JoinHostPort(host, port)) // the resource manager's connection strings
	s.Hub.Log.Info("AMQP listening", "addr", s.Ports["amqp"])
}

// inContainer: Docker and Podman mark their containers, where listening on
// 0.0.0.0 is how ports get published.
// ponytail: other runtimes (containerd) leave no mark; set POGI_ALLOW_EXTERNAL there
func inContainer() bool {
	for _, f := range []string{"/.dockerenv", "/run/.containerenv"} {
		if _, err := os.Stat(f); err == nil {
			return true
		}
	}
	return false
}

// Running is an in-process pogi started by Start.
type Running struct {
	*Server
}

// Start runs pogi in-process on a random port for the duration of a test.
// State lives in memory and a temporary data dir; it stops at cleanup.
func Start(tb core.TB, cfg ...Config) *Running {
	tb.Helper()
	var c Config
	if len(cfg) > 0 {
		c = cfg[0]
	}
	if c.DataDir == "" {
		c.DataDir = tb.TempDir()
	}
	c.ValidateSigs = c.ValidateSigs || os.Getenv("POGI_TEST_VALIDATE_SIGNATURES") == "1"
	if c.ECRPort == 0 {
		c.ECRPort = -1 // no fixed port 4510 in tests; set ECRPort to docker push
	}
	if c.SMTPPort == 0 {
		c.SMTPPort = -1 // no fixed port 2525 in tests; set SMTPPort to send mail over SMTP
	}
	if c.MQTTPort == 0 {
		c.MQTTPort = -1 // no fixed port 1883 in tests; set MQTTPort for MQTT over TCP
	}
	if c.AMQPPort == 0 {
		c.AMQPPort = -1 // no fixed port 5672 in tests; AMQP is on a random port, Ports["amqp"]
	}
	if c.TransferPort == 0 {
		c.TransferPort = -1 // no fixed port 2222 in tests; SFTP is on a free port, Hub.Endpoint("sftp:transfer")
	}
	s := New(c, nil)
	if err := s.Hub.OpenStorage(); err != nil { // memory unless cfg.Storage says otherwise
		tb.Fatalf("pogi: %v", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	core.Go("pogi.Flush", func() { s.Hub.Flush(ctx) })
	tb.Cleanup(stop)
	u, _ := core.ListenTLS(tb, s.Hub, s) // HTTP and HTTPS on one port, like `pogi start`
	s.URL = "http://127.0.0.1:" + u[strings.LastIndexByte(u, ':')+1:]
	s.Hub.URL = s.URL
	for _, svc := range s.Hub.Services() {
		if _, ok := svc.(core.Porter); ok { // own random port each
			s.Ports[svc.Info().Name] = core.Listen(tb, s.Hub.Handler(svc))
			s.Hub.SetEndpoint("port:"+svc.Info().Name, core.Hostport(s.Ports[svc.Info().Name]))
		}
	}
	s.smtp("127.0.0.1")
	if aln, err := net.Listen("tcp", "127.0.0.1:0"); err == nil { // Service Bus and Event Hubs tests need AMQP
		go s.Hub.AMQP().Serve(aln)
		s.Ports["amqp"] = "amqp://" + aln.Addr().String()
		s.Hub.SetEndpoint("amqp", aln.Addr().String())
		tb.Cleanup(func() { aln.Close() })
	}
	tb.Cleanup(s.Hub.Close)
	if c.File != nil {
		if err := seed.Apply(s.Hub, c.File); err != nil {
			tb.Fatalf("%v", err)
		}
	}
	s.Hub.Ready.Store(true)
	return &Running{s}
}

// Env returns the environment variables that point AWS, Google Cloud and
// Azure SDKs at this server.
func (s *Server) Env() []string {
	return core.Env(s.URL, s.Hub.Cfg.Region, s.Ports["azblob"], s.Ports["azqueue"], s.Ports["aztable"])
}

// Setenv points SDKs created by the code under test at this server, for the
// rest of the test (t.Setenv).
func (s *Server) Setenv(t interface{ Setenv(k, v string) }) {
	for _, kv := range s.Env() {
		k, v, _ := strings.Cut(kv, "=")
		t.Setenv(k, v)
	}
}

// Reset clears all state (or the named services).
func (s *Server) Reset(services ...string) { s.Hub.Reset(services...) }
