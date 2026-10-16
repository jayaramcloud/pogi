module github.com/nrndev/pogi

go 1.27.2

require (
	charm.land/bubbletea/v2 v2.1.0
	charm.land/lipgloss/v2 v2.0.6
	cloud.google.com/go v0.123.0
	cloud.google.com/go/artifactregistry v1.27.0
	cloud.google.com/go/bigquery v1.79.0
	cloud.google.com/go/cloudtasks v1.20.0
	cloud.google.com/go/compute v1.72.0
	cloud.google.com/go/container v1.55.0
	cloud.google.com/go/datastore v1.27.0
	cloud.google.com/go/eventarc v1.26.0
	cloud.google.com/go/firestore v1.26.0
	cloud.google.com/go/functions v1.26.0
	cloud.google.com/go/iam v1.12.0
	cloud.google.com/go/kms v1.35.0
	cloud.google.com/go/logging v1.19.0
	cloud.google.com/go/longrunning v1.2.0
	cloud.google.com/go/managedkafka v1.2.0
	cloud.google.com/go/monitoring v1.30.0
	cloud.google.com/go/pubsub/v2 v2.7.0
	cloud.google.com/go/resourcemanager v1.17.0
	cloud.google.com/go/run v1.23.0
	cloud.google.com/go/scheduler v1.17.0
	cloud.google.com/go/secretmanager v1.22.0
	cloud.google.com/go/serviceusage v1.16.0
	cloud.google.com/go/storage v1.69.0
	firebase.google.com/go/v4 v4.22.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.23.2
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.14.1
	github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos v1.5.0
	github.com/Azure/azure-sdk-for-go/sdk/data/aztables v1.4.1
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2 v2.0.2
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.11.0
	github.com/Azure/azure-sdk-for-go/sdk/messaging/eventgrid/azeventgrid v1.0.0
	github.com/Azure/azure-sdk-for-go/sdk/monitor/ingestion/azlogs v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/monitor/query/azlogs v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2 v2.1.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v3 v3.1.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7 v7.3.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2 v2.4.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v6 v6.6.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/msi/armmsi v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork v1.1.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2 v2.1.1
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/privatedns/armprivatedns v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources v1.2.0
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys v1.5.0
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets v1.5.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.8.2
	github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue v1.0.1
	github.com/Azure/go-amqp v1.7.0
	github.com/aws/aws-sdk-go-v2 v1.47.3
	github.com/aws/aws-sdk-go-v2/config v1.33.9
	github.com/aws/aws-sdk-go-v2/credentials v1.20.9
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.21.11
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.7.6
	github.com/aws/aws-sdk-go-v2/service/accessanalyzer v1.57.4
	github.com/aws/aws-sdk-go-v2/service/account v1.42.3
	github.com/aws/aws-sdk-go-v2/service/acm v1.50.4
	github.com/aws/aws-sdk-go-v2/service/amp v1.54.4
	github.com/aws/aws-sdk-go-v2/service/apigateway v1.52.1
	github.com/aws/aws-sdk-go-v2/service/apigatewaymanagementapi v1.41.1
	github.com/aws/aws-sdk-go-v2/service/apigatewayv2 v1.46.1
	github.com/aws/aws-sdk-go-v2/service/appconfig v1.54.4
	github.com/aws/aws-sdk-go-v2/service/appconfigdata v1.32.4
	github.com/aws/aws-sdk-go-v2/service/appintegrations v1.51.1
	github.com/aws/aws-sdk-go-v2/service/applicationautoscaling v1.51.4
	github.com/aws/aws-sdk-go-v2/service/appsync v1.62.4
	github.com/aws/aws-sdk-go-v2/service/athena v1.66.4
	github.com/aws/aws-sdk-go-v2/service/autoscaling v1.78.4
	github.com/aws/aws-sdk-go-v2/service/backup v1.69.1
	github.com/aws/aws-sdk-go-v2/service/batch v1.78.3
	github.com/aws/aws-sdk-go-v2/service/bcmdataexports v1.25.4
	github.com/aws/aws-sdk-go-v2/service/bcmpricingcalculator v1.21.4
	github.com/aws/aws-sdk-go-v2/service/bedrock v1.73.4
	github.com/aws/aws-sdk-go-v2/service/bedrockagent v1.68.3
	github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol v1.72.3
	github.com/aws/aws-sdk-go-v2/service/bedrockagentruntime v1.64.3
	github.com/aws/aws-sdk-go-v2/service/bedrockruntime v1.63.4
	github.com/aws/aws-sdk-go-v2/service/budgets v1.53.1
	github.com/aws/aws-sdk-go-v2/service/cloudcontrol v1.38.4
	github.com/aws/aws-sdk-go-v2/service/cloudformation v1.82.1
	github.com/aws/aws-sdk-go-v2/service/cloudfront v1.74.3
	github.com/aws/aws-sdk-go-v2/service/cloudhsmv2 v1.43.4
	github.com/aws/aws-sdk-go-v2/service/cloudtrail v1.65.4
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.73.3
	github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs v1.89.3
	github.com/aws/aws-sdk-go-v2/service/codeartifact v1.46.4
	github.com/aws/aws-sdk-go-v2/service/codebuild v1.78.4
	github.com/aws/aws-sdk-go-v2/service/codedeploy v1.45.4
	github.com/aws/aws-sdk-go-v2/service/codegurureviewer v1.43.4
	github.com/aws/aws-sdk-go-v2/service/codepipeline v1.55.4
	github.com/aws/aws-sdk-go-v2/service/cognitoidentity v1.42.4
	github.com/aws/aws-sdk-go-v2/service/comprehend v1.49.4
	github.com/aws/aws-sdk-go-v2/service/configservice v1.74.4
	github.com/aws/aws-sdk-go-v2/service/connect v1.204.3
	github.com/aws/aws-sdk-go-v2/service/controlcatalog v1.23.4
	github.com/aws/aws-sdk-go-v2/service/controltower v1.37.4
	github.com/aws/aws-sdk-go-v2/service/costandusagereportservice v1.43.4
	github.com/aws/aws-sdk-go-v2/service/costexplorer v1.74.1
	github.com/aws/aws-sdk-go-v2/service/databasemigrationservice v1.75.1
	github.com/aws/aws-sdk-go-v2/service/datasync v1.67.4
	github.com/aws/aws-sdk-go-v2/service/detective v1.47.4
	github.com/aws/aws-sdk-go-v2/service/dlm v1.45.4
	github.com/aws/aws-sdk-go-v2/service/docdb v1.57.4
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.70.3
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.45.1
	github.com/aws/aws-sdk-go-v2/service/ec2 v1.338.4
	github.com/aws/aws-sdk-go-v2/service/ecr v1.66.4
	github.com/aws/aws-sdk-go-v2/service/ecs v1.100.3
	github.com/aws/aws-sdk-go-v2/service/efs v1.49.4
	github.com/aws/aws-sdk-go-v2/service/eks v1.104.1
	github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk v1.43.4
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing v1.41.4
	github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2 v1.63.4
	github.com/aws/aws-sdk-go-v2/service/elasticsearchservice v1.51.4
	github.com/aws/aws-sdk-go-v2/service/emr v1.70.4
	github.com/aws/aws-sdk-go-v2/service/emrserverless v1.50.1
	github.com/aws/aws-sdk-go-v2/service/eventbridge v1.55.3
	github.com/aws/aws-sdk-go-v2/service/firehose v1.52.4
	github.com/aws/aws-sdk-go-v2/service/fis v1.46.4
	github.com/aws/aws-sdk-go-v2/service/globalaccelerator v1.45.3
	github.com/aws/aws-sdk-go-v2/service/glue v1.169.1
	github.com/aws/aws-sdk-go-v2/service/guardduty v1.99.1
	github.com/aws/aws-sdk-go-v2/service/iam v1.64.4
	github.com/aws/aws-sdk-go-v2/service/identitystore v1.49.1
	github.com/aws/aws-sdk-go-v2/service/inspector2 v1.61.3
	github.com/aws/aws-sdk-go-v2/service/iot v1.84.4
	github.com/aws/aws-sdk-go-v2/service/iotdataplane v1.40.4
	github.com/aws/aws-sdk-go-v2/service/kafka v1.65.4
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.56.4
	github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2 v1.47.4
	github.com/aws/aws-sdk-go-v2/service/kms v1.61.4
	github.com/aws/aws-sdk-go-v2/service/lakeformation v1.55.4
	github.com/aws/aws-sdk-go-v2/service/lambda v1.112.1
	github.com/aws/aws-sdk-go-v2/service/lambdamicrovms v1.11.1
	github.com/aws/aws-sdk-go-v2/service/lightsail v1.66.4
	github.com/aws/aws-sdk-go-v2/service/macie2 v1.59.4
	github.com/aws/aws-sdk-go-v2/service/marketplacecatalog v1.52.4
	github.com/aws/aws-sdk-go-v2/service/marketplaceentitlementservice v1.46.4
	github.com/aws/aws-sdk-go-v2/service/marketplacemetering v1.46.1
	github.com/aws/aws-sdk-go-v2/service/memorydb v1.42.4
	github.com/aws/aws-sdk-go-v2/service/mq v1.45.4
	github.com/aws/aws-sdk-go-v2/service/mwaa v1.48.4
	github.com/aws/aws-sdk-go-v2/service/neptune v1.53.4
	github.com/aws/aws-sdk-go-v2/service/networkfirewall v1.76.1
	github.com/aws/aws-sdk-go-v2/service/oam v1.31.4
	github.com/aws/aws-sdk-go-v2/service/opensearch v1.85.1
	github.com/aws/aws-sdk-go-v2/service/organizations v1.61.3
	github.com/aws/aws-sdk-go-v2/service/pipes v1.32.4
	github.com/aws/aws-sdk-go-v2/service/pricing v1.49.4
	github.com/aws/aws-sdk-go-v2/service/ram v1.45.4
	github.com/aws/aws-sdk-go-v2/service/rds v1.130.3
	github.com/aws/aws-sdk-go-v2/service/rdsdata v1.40.4
	github.com/aws/aws-sdk-go-v2/service/redshift v1.71.4
	github.com/aws/aws-sdk-go-v2/service/redshiftdata v1.49.3
	github.com/aws/aws-sdk-go-v2/service/redshiftserverless v1.44.4
	github.com/aws/aws-sdk-go-v2/service/rekognition v1.60.3
	github.com/aws/aws-sdk-go-v2/service/resourceexplorer2 v1.33.4
	github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi v1.41.4
	github.com/aws/aws-sdk-go-v2/service/route53 v1.70.4
	github.com/aws/aws-sdk-go-v2/service/route53resolver v1.54.4
	github.com/aws/aws-sdk-go-v2/service/rum v1.38.4
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.3
	github.com/aws/aws-sdk-go-v2/service/s3control v1.79.4
	github.com/aws/aws-sdk-go-v2/service/s3tables v1.23.4
	github.com/aws/aws-sdk-go-v2/service/s3vectors v1.16.3
	github.com/aws/aws-sdk-go-v2/service/sagemaker v1.282.3
	github.com/aws/aws-sdk-go-v2/service/sagemakerruntime v1.49.4
	github.com/aws/aws-sdk-go-v2/service/scheduler v1.25.4
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.50.4
	github.com/aws/aws-sdk-go-v2/service/securityhub v1.84.1
	github.com/aws/aws-sdk-go-v2/service/servicecatalog v1.47.4
	github.com/aws/aws-sdk-go-v2/service/servicediscovery v1.49.4
	github.com/aws/aws-sdk-go-v2/service/servicequotas v1.43.4
	github.com/aws/aws-sdk-go-v2/service/ses v1.42.4
	github.com/aws/aws-sdk-go-v2/service/sesv2 v1.79.1
	github.com/aws/aws-sdk-go-v2/service/sfn v1.51.4
	github.com/aws/aws-sdk-go-v2/service/sns v1.47.5
	github.com/aws/aws-sdk-go-v2/service/sqs v1.52.4
	github.com/aws/aws-sdk-go-v2/service/ssm v1.79.3
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.4
	github.com/aws/aws-sdk-go-v2/service/ssoadmin v1.49.4
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.4
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.4
	github.com/aws/aws-sdk-go-v2/service/swf v1.43.4
	github.com/aws/aws-sdk-go-v2/service/textract v1.49.4
	github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb v1.29.4
	github.com/aws/aws-sdk-go-v2/service/transcribe v1.66.4
	github.com/aws/aws-sdk-go-v2/service/transfer v1.85.3
	github.com/aws/aws-sdk-go-v2/service/translate v1.43.1
	github.com/aws/aws-sdk-go-v2/service/verifiedpermissions v1.41.4
	github.com/aws/aws-sdk-go-v2/service/wafv2 v1.83.4
	github.com/aws/smithy-go v1.28.5
	github.com/cedar-policy/cedar-go v1.8.0
	github.com/charmbracelet/x/ansi v0.11.9
	github.com/coder/websocket v1.8.15
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.4.1
	github.com/dop251/goja v0.0.0-20261011122007-9b0700c7e02e
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/go-sql-driver/mysql v1.10.1
	github.com/googleapis/gax-go/v2 v2.26.2
	github.com/gorilla/websocket v1.5.3
	github.com/jackc/pgx/v5 v5.11.0
	github.com/klauspost/compress v1.20.1
	github.com/microsoft/go-mssqldb v1.11.2
	github.com/mochi-mqtt/server/v2 v2.7.9
	github.com/modelcontextprotocol/go-sdk v1.8.0
	github.com/pkg/sftp v1.13.11
	github.com/rabbitmq/amqp091-go v1.15.0
	github.com/twmb/franz-go v1.22.1
	github.com/twmb/franz-go/pkg/kadm v1.19.0
	github.com/twmb/franz-go/pkg/kmsg v1.14.0
	github.com/vektah/gqlparser/v2 v2.5.62
	go.yaml.in/yaml/v3 v3.0.5
	golang.org/x/crypto v0.58.0
	golang.org/x/net v0.60.0
	golang.org/x/oauth2 v0.36.0
	google.golang.org/api v0.288.0
	google.golang.org/genproto v0.0.0-20260715232425-e75dac1f907d
	google.golang.org/genproto/googleapis/api v0.0.0-20260715232425-e75dac1f907d
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260715232425-e75dac1f907d
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.11
)

require (
	cel.dev/expr v0.25.2 // indirect
	cloud.google.com/go/auth v0.20.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.12.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/internal v1.2.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/internal v0.1.0 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.8.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.35.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.57.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.57.0 // indirect
	github.com/MicahParks/keyfunc v1.9.0 // indirect
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/apache/arrow-go/v18 v18.7.0 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.22 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.13.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.4 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/charmbracelet/colorprofile v0.4.3 // indirect
	github.com/charmbracelet/ultraviolet v0.0.0-20261008173134-6b8d4baf91b4 // indirect
	github.com/charmbracelet/x/term v0.2.2 // indirect
	github.com/charmbracelet/x/termios v0.1.1 // indirect
	github.com/charmbracelet/x/windows v0.2.2 // indirect
	github.com/clipperhouse/displaywidth v0.11.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/dlclark/regexp2/v2 v2.8.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.37.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/jsonschema-go v0.4.3 // indirect
	github.com/google/pprof v0.0.0-20230207041349-798e818bf904 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.17 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lucasb-eyer/go-colorful v1.4.1 // indirect
	github.com/mattn/go-runewidth v0.0.30 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/segmentio/encoding v0.5.4 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/spiffe/go-spiffe/v2 v2.8.1 // indirect
	github.com/xo/terminfo v1.0.0 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.45.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.68.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0 // indirect
	go.opentelemetry.io/otel v1.45.0 // indirect
	go.opentelemetry.io/otel/metric v1.45.0 // indirect
	go.opentelemetry.io/otel/sdk v1.45.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.45.0 // indirect
	go.opentelemetry.io/otel/trace v1.45.0 // indirect
	golang.org/x/exp v0.0.0-20260813180055-c1d0aacb2297 // indirect
	golang.org/x/mod v0.41.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/telemetry v0.0.0-20260924152758-ed294f943157 // indirect
	golang.org/x/text v0.43.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	golang.org/x/tools v0.51.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/appengine/v2 v2.0.6 // indirect
)
