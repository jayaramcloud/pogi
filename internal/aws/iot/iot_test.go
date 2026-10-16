package iot_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

const role = "arn:aws:iam::000000000000:role/iot"

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantErr(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

// start runs pogi with the MQTT broker on a free port.
func start(t *testing.T) (*pogi.Running, int) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return pogi.Start(t, pogi.Config{MQTTPort: port}), port
}

// connect is a real MQTT client of broker (tcp:// or ws://).
func connect(t *testing.T, broker, id string) paho.Client {
	t.Helper()
	c := paho.NewClient(paho.NewClientOptions().AddBroker(broker).SetClientID(id).SetConnectTimeout(5 * time.Second))
	if tok := c.Connect(); !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		t.Fatalf("connect %s: %v", broker, tok.Error())
	}
	t.Cleanup(func() { c.Disconnect(100) })
	return c
}

func publish(t *testing.T, c paho.Client, topic, payload string) {
	t.Helper()
	if tok := c.Publish(topic, 1, false, payload); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish %s: %v", topic, tok.Error())
	}
}

// subscribe returns the messages that arrive on topic.
func subscribe(t *testing.T, c paho.Client, topic string) chan []byte {
	t.Helper()
	ch := make(chan []byte, 16)
	if tok := c.Subscribe(topic, 1, func(_ paho.Client, m paho.Message) { ch <- m.Payload() }); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe %s: %v", topic, tok.Error())
	}
	return ch
}

func next(t *testing.T, ch chan []byte) map[string]any {
	t.Helper()
	select {
	case b := <-ch:
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("message %s: %v", b, err)
		}
		return m
	case <-time.After(10 * time.Second):
		t.Fatal("no message")
		return nil
	}
}

// receive collects the bodies of the messages in a queue until there are n,
// then checks that no more come.
func receive(t *testing.T, q *sqs.Client, url string, n int) []string {
	t.Helper()
	var out []string
	for end := time.Now().Add(15 * time.Second); len(out) < n && time.Now().Before(end); {
		res := must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), MaxNumberOfMessages: 10, WaitTimeSeconds: 1}))
		for _, m := range res.Messages {
			out = append(out, *m.Body)
			must(q.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(url), ReceiptHandle: m.ReceiptHandle}))
		}
	}
	if extra := must(q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(url), WaitTimeSeconds: 1})).Messages; len(extra) > 0 {
		t.Fatalf("unexpected message %s", *extra[0].Body)
	}
	slices.Sort(out)
	return out
}

// An MQTT message fires a topic rule that delivers to SQS: over TCP, over
// WebSocket on pogi's port and through the HTTP Publish API.
func TestRuleToSQS(t *testing.T) {
	srv, port := start(t)
	cfg := awstest.Config(t, srv.URL)
	c, data, q := iot.NewFromConfig(cfg), iotdataplane.NewFromConfig(cfg), sqs.NewFromConfig(cfg)

	if ep := must(c.DescribeEndpoint(ctx, &iot.DescribeEndpointInput{EndpointType: aws.String("iot:Data-ATS")})); *ep.EndpointAddress != "localhost" {
		t.Fatalf("endpoint = %s", *ep.EndpointAddress)
	}
	queue := *must(q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("hot")})).QueueUrl
	sql := "SELECT temp, topic(2) AS device FROM 'sensors/+/data' WHERE temp > 30"
	must(c.CreateTopicRule(ctx, &iot.CreateTopicRuleInput{RuleName: aws.String("hot"), Tags: aws.String("team=iot"), TopicRulePayload: &types.TopicRulePayload{
		Sql: aws.String(sql), AwsIotSqlVersion: aws.String("2016-03-23"),
		Actions: []types.Action{{Sqs: &types.SqsAction{QueueUrl: aws.String(queue), RoleArn: aws.String(role)}}}}}))

	tcp := connect(t, fmt.Sprintf("tcp://127.0.0.1:%d", port), "sensor-tcp")
	publish(t, tcp, "sensors/dev1/data", `{"temp": 35, "humidity": 40}`)
	publish(t, tcp, "sensors/dev1/data", `{"temp": 20}`)
	publish(t, tcp, "sensors/dev1/status", `{"temp": 99}`)
	ws := connect(t, "ws://"+core.Hostport(srv.URL)+"/mqtt", "sensor-ws")
	publish(t, ws, "sensors/dev2/data", `{"temp": 50.5}`)
	publish(t, ws, "sensors/dev2/data", `{"temp": 10}`)
	must(data.Publish(ctx, &iotdataplane.PublishInput{Topic: aws.String("sensors/dev3/data"), Payload: []byte(`{"temp": 41}`)}))
	must(data.Publish(ctx, &iotdataplane.PublishInput{Topic: aws.String("sensors/dev3/data"), Payload: []byte(`{"temp": 3}`)}))
	want := []string{`{"device":"dev1","temp":35}`, `{"device":"dev2","temp":50.5}`, `{"device":"dev3","temp":41}`}
	if got := receive(t, q, queue, 3); !slices.Equal(got, want) {
		t.Fatalf("SQS got %q, want %q", got, want)
	}

	// MQTT subscribers get what the HTTP API publishes, retained messages stay
	sub := subscribe(t, ws, "alerts/#")
	must(data.Publish(ctx, &iotdataplane.PublishInput{Topic: aws.String("alerts/fire"), Payload: []byte(`{"level":3}`), Retain: true}))
	if m := next(t, sub); m["level"] != 3.0 {
		t.Fatalf("alert = %v", m)
	}
	if r := must(data.GetRetainedMessage(ctx, &iotdataplane.GetRetainedMessageInput{Topic: aws.String("alerts/fire")})); string(r.Payload) != `{"level":3}` {
		t.Fatalf("retained = %s", r.Payload)
	}
	if l := must(data.ListRetainedMessages(ctx, &iotdataplane.ListRetainedMessagesInput{})).RetainedTopics; len(l) != 1 || *l[0].Topic != "alerts/fire" {
		t.Fatalf("retained topics = %+v", l)
	}

	// the rule API: disabled rules do not run, a republish rule feeds MQTT
	rule := must(c.GetTopicRule(ctx, &iot.GetTopicRuleInput{RuleName: aws.String("hot")}))
	if *rule.Rule.Sql != sql || *rule.Rule.AwsIotSqlVersion != "2016-03-23" || *rule.Rule.Actions[0].Sqs.QueueUrl != queue {
		t.Fatalf("rule = %+v", rule.Rule)
	}
	list := must(c.ListTopicRules(ctx, &iot.ListTopicRulesInput{}))
	if len(list.Rules) != 1 || *list.Rules[0].TopicPattern != "sensors/+/data" {
		t.Fatalf("rules = %+v", list.Rules)
	}
	if tags := must(c.ListTagsForResource(ctx, &iot.ListTagsForResourceInput{ResourceArn: rule.RuleArn})).Tags; len(tags) != 1 || *tags[0].Value != "iot" {
		t.Fatalf("tags = %+v", tags)
	}
	must(c.DisableTopicRule(ctx, &iot.DisableTopicRuleInput{RuleName: aws.String("hot")}))
	must(c.CreateTopicRule(ctx, &iot.CreateTopicRuleInput{RuleName: aws.String("echo"), TopicRulePayload: &types.TopicRulePayload{
		Sql:     aws.String("SELECT *, upper(topic(2)) AS who FROM 'sensors/+/data'"),
		Actions: []types.Action{{Republish: &types.RepublishAction{Topic: aws.String("echo/${topic(2)}"), RoleArn: aws.String(role)}}}}}))
	echo := subscribe(t, tcp, "echo/+")
	publish(t, tcp, "sensors/dev4/data", `{"temp": 90}`)
	if m := next(t, echo); m["who"] != "DEV4" || m["temp"] != 90.0 {
		t.Fatalf("republished = %v", m)
	}
	receive(t, q, queue, 0) // the disabled rule sent nothing

	_, err := c.CreateTopicRule(ctx, &iot.CreateTopicRuleInput{RuleName: aws.String("echo"), TopicRulePayload: &types.TopicRulePayload{
		Sql: aws.String("SELECT * FROM 'x'"), Actions: []types.Action{}}})
	wantErr(t, err, "ResourceAlreadyExistsException")
	_, err = c.CreateTopicRule(ctx, &iot.CreateTopicRuleInput{RuleName: aws.String("bad"), TopicRulePayload: &types.TopicRulePayload{
		Sql: aws.String("SELECT FROM 'x'"), Actions: []types.Action{}}})
	wantErr(t, err, "SqlParseException")
	must(c.DeleteTopicRule(ctx, &iot.DeleteTopicRuleInput{RuleName: aws.String("hot")}))
	_, err = c.GetTopicRule(ctx, &iot.GetTopicRuleInput{RuleName: aws.String("hot")})
	wantErr(t, err, "UnauthorizedException") // what AWS answers for a missing rule
}

// Shadows: desired state, a partial report, the delta over HTTP and MQTT.
func TestShadows(t *testing.T) {
	srv, port := start(t)
	cfg := awstest.Config(t, srv.URL)
	data := iotdataplane.NewFromConfig(cfg)
	must(iot.NewFromConfig(cfg).DescribeEndpoint(ctx, &iot.DescribeEndpointInput{})) // opens the MQTT port
	mc := connect(t, fmt.Sprintf("tcp://127.0.0.1:%d", port), "lamp")
	deltas := subscribe(t, mc, "$aws/things/lamp/shadow/update/delta")
	accepted := subscribe(t, mc, "$aws/things/lamp/shadow/update/accepted")
	docs := subscribe(t, mc, "$aws/things/lamp/shadow/update/documents")

	_, err := data.GetThingShadow(ctx, &iotdataplane.GetThingShadowInput{ThingName: aws.String("lamp")})
	wantErr(t, err, "ResourceNotFoundException")
	out := must(data.UpdateThingShadow(ctx, &iotdataplane.UpdateThingShadowInput{ThingName: aws.String("lamp"),
		Payload: []byte(`{"state":{"desired":{"color":"red","power":"on"}},"clientToken":"t1"}`)}))
	var ack map[string]any
	json.Unmarshal(out.Payload, &ack)
	if ack["version"] != 1.0 || ack["clientToken"] != "t1" {
		t.Fatalf("accepted = %s", out.Payload)
	}
	if d := next(t, deltas); !reflect.DeepEqual(d["state"], map[string]any{"color": "red", "power": "on"}) || d["version"] != 1.0 {
		t.Fatalf("delta = %v", d)
	}
	next(t, accepted)
	if d := next(t, docs); d["previous"] != nil || d["current"].(map[string]any)["version"] != 1.0 {
		t.Fatalf("documents = %v", d)
	}

	// the device reports part of it over MQTT
	publish(t, mc, "$aws/things/lamp/shadow/update", `{"state":{"reported":{"power":"on","bulb":{"watts":9}}},"clientToken":"t2"}`)
	if a := next(t, accepted); a["clientToken"] != "t2" || a["version"] != 2.0 {
		t.Fatalf("accepted over MQTT = %v", a)
	}
	var doc struct {
		State struct {
			Desired, Reported, Delta map[string]any
		}
		Metadata struct{ Desired map[string]map[string]float64 }
		Version  int
	}
	json.Unmarshal(must(data.GetThingShadow(ctx, &iotdataplane.GetThingShadowInput{ThingName: aws.String("lamp")})).Payload, &doc)
	if !reflect.DeepEqual(doc.State.Delta, map[string]any{"color": "red"}) || doc.Version != 2 || doc.State.Reported["power"] != "on" ||
		doc.Metadata.Desired["color"]["timestamp"] == 0 {
		t.Fatalf("shadow = %+v", doc)
	}
	select {
	case d := <-deltas:
		t.Fatalf("a report alone sent a delta: %s", d)
	case <-time.After(200 * time.Millisecond):
	}

	// get over MQTT; a stale version and a bad document are rejected
	gets := subscribe(t, mc, "$aws/things/lamp/shadow/get/accepted")
	publish(t, mc, "$aws/things/lamp/shadow/get", `{"clientToken":"g1"}`)
	if g := next(t, gets); g["clientToken"] != "g1" || g["version"] != 2.0 {
		t.Fatalf("get = %v", g)
	}
	rejected := subscribe(t, mc, "$aws/things/lamp/shadow/update/rejected")
	publish(t, mc, "$aws/things/lamp/shadow/update", `{"state":{"reported":{"color":"red"}},"version":1}`)
	if r := next(t, rejected); r["code"] != 409.0 {
		t.Fatalf("rejected = %v", r)
	}
	_, err = data.UpdateThingShadow(ctx, &iotdataplane.UpdateThingShadowInput{ThingName: aws.String("lamp"), Payload: []byte(`{"desired":{}}`)})
	wantErr(t, err, "InvalidRequestException")

	// named shadows
	must(data.UpdateThingShadow(ctx, &iotdataplane.UpdateThingShadowInput{ThingName: aws.String("lamp"), ShadowName: aws.String("config"),
		Payload: []byte(`{"state":{"reported":{"mode":"eco"}}}`)}))
	if names := must(data.ListNamedShadowsForThing(ctx, &iotdataplane.ListNamedShadowsForThingInput{ThingName: aws.String("lamp")})).Results; !slices.Equal(names, []string{"config"}) {
		t.Fatalf("named shadows = %v", names)
	}
	must(data.DeleteThingShadow(ctx, &iotdataplane.DeleteThingShadowInput{ThingName: aws.String("lamp"), ShadowName: aws.String("config")}))
	_, err = data.GetThingShadow(ctx, &iotdataplane.GetThingShadowInput{ThingName: aws.String("lamp"), ShadowName: aws.String("config")})
	wantErr(t, err, "ResourceNotFoundException")
}

// Things, groups, certificates, policies and principals.
func TestRegistry(t *testing.T) {
	srv, _ := start(t)
	c := iot.NewFromConfig(awstest.Config(t, srv.URL))

	thing := must(c.CreateThing(ctx, &iot.CreateThingInput{ThingName: aws.String("lamp"),
		AttributePayload: &types.AttributePayload{Attributes: map[string]string{"room": "hall"}}}))
	must(c.UpdateThing(ctx, &iot.UpdateThingInput{ThingName: aws.String("lamp"), AttributePayload: &types.AttributePayload{
		Attributes: map[string]string{"floor": "1"}, Merge: true}}))
	desc := must(c.DescribeThing(ctx, &iot.DescribeThingInput{ThingName: aws.String("lamp")}))
	if *desc.ThingArn != *thing.ThingArn || desc.Version != 2 || desc.Attributes["room"] != "hall" || desc.Attributes["floor"] != "1" || *desc.DefaultClientId != "lamp" {
		t.Fatalf("thing = %+v", desc)
	}
	if l := must(c.ListThings(ctx, &iot.ListThingsInput{AttributeName: aws.String("room"), AttributeValue: aws.String("hall")})).Things; len(l) != 1 {
		t.Fatalf("things = %+v", l)
	}
	_, err := c.DescribeThing(ctx, &iot.DescribeThingInput{ThingName: aws.String("nope")})
	wantErr(t, err, "ResourceNotFoundException")

	must(c.CreateThingGroup(ctx, &iot.CreateThingGroupInput{ThingGroupName: aws.String("home")}))
	must(c.CreateThingGroup(ctx, &iot.CreateThingGroupInput{ThingGroupName: aws.String("kitchen"), ParentGroupName: aws.String("home")}))
	must(c.AddThingToThingGroup(ctx, &iot.AddThingToThingGroupInput{ThingName: aws.String("lamp"), ThingGroupName: aws.String("kitchen")}))
	if l := must(c.ListThingsInThingGroup(ctx, &iot.ListThingsInThingGroupInput{ThingGroupName: aws.String("home"), Recursive: true})).Things; !slices.Equal(l, []string{"lamp"}) {
		t.Fatalf("group things = %v", l)
	}
	if g := must(c.DescribeThingGroup(ctx, &iot.DescribeThingGroupInput{ThingGroupName: aws.String("kitchen")})); *g.ThingGroupMetadata.ParentGroupName != "home" {
		t.Fatalf("group = %+v", g)
	}
	_, err = c.DeleteThingGroup(ctx, &iot.DeleteThingGroupInput{ThingGroupName: aws.String("home")})
	wantErr(t, err, "InvalidRequestException") // it has a child

	// certificates: a real X.509 certificate and key from pogi's CA
	cert := must(c.CreateKeysAndCertificate(ctx, &iot.CreateKeysAndCertificateInput{SetAsActive: true}))
	block, _ := pem.Decode([]byte(*cert.CertificatePem))
	x := must(x509.ParseCertificate(block.Bytes))
	keyBlock, _ := pem.Decode([]byte(*cert.KeyPair.PrivateKey))
	key := must(x509.ParsePKCS1PrivateKey(keyBlock.Bytes))
	if !key.PublicKey.Equal(x.PublicKey) || x.Issuer.CommonName != "pogi IoT CA" {
		t.Fatalf("certificate issuer %s, key matches: %v", x.Issuer, key.PublicKey.Equal(x.PublicKey))
	}
	cd := must(c.DescribeCertificate(ctx, &iot.DescribeCertificateInput{CertificateId: cert.CertificateId})).CertificateDescription
	if cd.Status != types.CertificateStatusActive || *cd.CertificatePem != *cert.CertificatePem || cd.Validity.NotAfter.Year() != 2049 {
		t.Fatalf("certificate = %+v", cd)
	}
	csrKey := must(rsa.GenerateKey(rand.Reader, 2048))
	csr := must(x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "lamp"}}, csrKey))
	fromCSR := must(c.CreateCertificateFromCsr(ctx, &iot.CreateCertificateFromCsrInput{
		CertificateSigningRequest: aws.String(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr})))}))
	_, err = c.RegisterCertificateWithoutCA(ctx, &iot.RegisterCertificateWithoutCAInput{CertificatePem: fromCSR.CertificatePem})
	wantErr(t, err, "ResourceAlreadyExistsException")

	// principals and policies keep the certificate from being deleted
	must(c.AttachThingPrincipal(ctx, &iot.AttachThingPrincipalInput{ThingName: aws.String("lamp"), Principal: cert.CertificateArn}))
	if p := must(c.ListThingPrincipals(ctx, &iot.ListThingPrincipalsInput{ThingName: aws.String("lamp")})).Principals; !slices.Equal(p, []string{*cert.CertificateArn}) {
		t.Fatalf("principals = %v", p)
	}
	if th := must(c.ListPrincipalThings(ctx, &iot.ListPrincipalThingsInput{Principal: cert.CertificateArn})).Things; !slices.Equal(th, []string{"lamp"}) {
		t.Fatalf("principal things = %v", th)
	}
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iot:*","Resource":"*"}]}`
	pol := must(c.CreatePolicy(ctx, &iot.CreatePolicyInput{PolicyName: aws.String("devices"), PolicyDocument: aws.String(doc)}))
	must(c.AttachPolicy(ctx, &iot.AttachPolicyInput{PolicyName: aws.String("devices"), Target: cert.CertificateArn}))
	if l := must(c.ListAttachedPolicies(ctx, &iot.ListAttachedPoliciesInput{Target: cert.CertificateArn})).Policies; len(l) != 1 || *l[0].PolicyArn != *pol.PolicyArn {
		t.Fatalf("attached = %+v", l)
	}
	if l := must(c.ListTargetsForPolicy(ctx, &iot.ListTargetsForPolicyInput{PolicyName: aws.String("devices")})).Targets; !slices.Equal(l, []string{*cert.CertificateArn}) {
		t.Fatalf("targets = %v", l)
	}
	v2 := must(c.CreatePolicyVersion(ctx, &iot.CreatePolicyVersionInput{PolicyName: aws.String("devices"), PolicyDocument: aws.String(doc), SetAsDefault: true}))
	if gp := must(c.GetPolicy(ctx, &iot.GetPolicyInput{PolicyName: aws.String("devices")})); *gp.DefaultVersionId != *v2.PolicyVersionId || *v2.PolicyVersionId != "2" {
		t.Fatalf("policy = %+v", gp)
	}

	_, err = c.DeleteCertificate(ctx, &iot.DeleteCertificateInput{CertificateId: cert.CertificateId})
	wantErr(t, err, "CertificateStateException")
	must(c.UpdateCertificate(ctx, &iot.UpdateCertificateInput{CertificateId: cert.CertificateId, NewStatus: types.CertificateStatusInactive}))
	_, err = c.DeleteCertificate(ctx, &iot.DeleteCertificateInput{CertificateId: cert.CertificateId})
	wantErr(t, err, "DeleteConflictException") // attached to a thing
	must(c.DetachThingPrincipal(ctx, &iot.DetachThingPrincipalInput{ThingName: aws.String("lamp"), Principal: cert.CertificateArn}))
	_, err = c.DeleteCertificate(ctx, &iot.DeleteCertificateInput{CertificateId: cert.CertificateId})
	wantErr(t, err, "DeleteConflictException") // a policy still is
	_, err = c.DeletePolicy(ctx, &iot.DeletePolicyInput{PolicyName: aws.String("devices")})
	wantErr(t, err, "DeleteConflictException")
	must(c.DetachPolicy(ctx, &iot.DetachPolicyInput{PolicyName: aws.String("devices"), Target: cert.CertificateArn}))
	must(c.DeleteCertificate(ctx, &iot.DeleteCertificateInput{CertificateId: cert.CertificateId}))
	_, err = c.DescribeCertificate(ctx, &iot.DescribeCertificateInput{CertificateId: cert.CertificateId})
	wantErr(t, err, "ResourceNotFoundException")
	must(c.DeletePolicyVersion(ctx, &iot.DeletePolicyVersionInput{PolicyName: aws.String("devices"), PolicyVersionId: aws.String("1")}))
	must(c.DeletePolicy(ctx, &iot.DeletePolicyInput{PolicyName: aws.String("devices")}))
	if l := must(c.ListCertificates(ctx, &iot.ListCertificatesInput{})).Certificates; len(l) != 1 || *l[0].CertificateId != *fromCSR.CertificateId {
		t.Fatalf("certificates = %+v", l)
	}
	must(c.DeleteThing(ctx, &iot.DeleteThingInput{ThingName: aws.String("lamp")}))
	if l := must(c.ListThingsInThingGroup(ctx, &iot.ListThingsInThingGroupInput{ThingGroupName: aws.String("kitchen")})).Things; len(l) != 0 {
		t.Fatalf("group things after delete = %v", l)
	}
}
