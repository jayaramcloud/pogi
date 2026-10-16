package kinesis_test

import (
	"context"
	"crypto/md5"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	kinesissvc "github.com/nrndev/pogi/internal/aws/kinesis"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func setup(t *testing.T) (*kinesis.Client, *core.Hub) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return kinesissvc.New(h) })
	return kinesis.NewFromConfig(awstest.Config(t, url)), h
}

func data(recs []types.Record) string {
	var s string
	for _, r := range recs {
		s += string(r.Data)
	}
	return s
}

// read returns everything in a shard from the trim horizon.
func read(t *testing.T, c *kinesis.Client, stream, shard string) []types.Record {
	t.Helper()
	it := must(c.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{StreamName: aws.String(stream), ShardId: aws.String(shard),
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon}))
	return must(c.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: it.ShardIterator})).Records
}

func TestStreamLifecycle(t *testing.T) {
	c, _ := setup(t)
	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("orders"), ShardCount: aws.Int32(3), Tags: map[string]string{"team": "a"}}))
	_, err := c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("orders"), ShardCount: aws.Int32(1)})
	var inUse *types.ResourceInUseException
	if !errors.As(err, &inUse) {
		t.Fatalf("want ResourceInUseException, got %v", err)
	}
	d := must(c.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String("orders")})).StreamDescription
	if d.StreamStatus != types.StreamStatusActive || len(d.Shards) != 3 || *d.HasMoreShards || *d.RetentionPeriodHours != 24 ||
		*d.StreamARN != "arn:aws:kinesis:us-east-1:000000000000:stream/orders" {
		t.Fatalf("describe = %+v", d)
	}
	// Three shards split 2^128 evenly, like AWS.
	if *d.Shards[0].HashKeyRange.StartingHashKey != "0" || *d.Shards[1].HashKeyRange.StartingHashKey != "113427455640312821154458202477256070485" ||
		*d.Shards[2].HashKeyRange.EndingHashKey != "340282366920938463463374607431768211455" || d.Shards[0].SequenceNumberRange.StartingSequenceNumber == nil {
		t.Fatalf("hash ranges = %+v %+v %+v", d.Shards[0].HashKeyRange, d.Shards[1].HashKeyRange, d.Shards[2].HashKeyRange)
	}
	paged := must(c.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String("orders"), Limit: aws.Int32(2)})).StreamDescription
	if len(paged.Shards) != 2 || !*paged.HasMoreShards {
		t.Fatalf("shard paging = %d more=%v", len(paged.Shards), *paged.HasMoreShards)
	}

	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("clicks"), StreamModeDetails: &types.StreamModeDetails{StreamMode: types.StreamModeOnDemand}}))
	sum := must(c.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: aws.String("clicks")})).StreamDescriptionSummary
	if *sum.OpenShardCount != 4 || sum.StreamModeDetails.StreamMode != types.StreamModeOnDemand || sum.EncryptionType != types.EncryptionTypeNone {
		t.Fatalf("on-demand summary = %+v", sum)
	}
	l := must(c.ListStreams(ctx, &kinesis.ListStreamsInput{Limit: aws.Int32(1)}))
	if len(l.StreamNames) != 1 || l.StreamNames[0] != "clicks" || !*l.HasMoreStreams || len(l.StreamSummaries) != 1 || l.NextToken == nil {
		t.Fatalf("list page 1 = %+v", l)
	}
	l = must(c.ListStreams(ctx, &kinesis.ListStreamsInput{NextToken: l.NextToken}))
	if len(l.StreamNames) != 1 || l.StreamNames[0] != "orders" || *l.HasMoreStreams {
		t.Fatalf("list page 2 = %+v", l)
	}

	must(c.AddTagsToStream(ctx, &kinesis.AddTagsToStreamInput{StreamName: aws.String("orders"), Tags: map[string]string{"env": "dev"}}))
	must(c.RemoveTagsFromStream(ctx, &kinesis.RemoveTagsFromStreamInput{StreamName: aws.String("orders"), TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsForStream(ctx, &kinesis.ListTagsForStreamInput{StreamName: aws.String("orders")})); len(tags.Tags) != 1 || *tags.Tags[0].Key != "env" {
		t.Fatalf("tags = %+v", tags.Tags)
	}
	must(c.TagResource(ctx, &kinesis.TagResourceInput{ResourceARN: d.StreamARN, Tags: map[string]string{"x": "1"}}))
	if tags := must(c.ListTagsForResource(ctx, &kinesis.ListTagsForResourceInput{ResourceARN: d.StreamARN})); len(tags.Tags) != 2 {
		t.Fatalf("resource tags = %+v", tags.Tags)
	}

	_, err = c.DecreaseStreamRetentionPeriod(ctx, &kinesis.DecreaseStreamRetentionPeriodInput{StreamName: aws.String("orders"), RetentionPeriodHours: aws.Int32(48)})
	var bad *types.InvalidArgumentException
	if !errors.As(err, &bad) {
		t.Fatalf("decreasing to a longer period: want InvalidArgumentException, got %v", err)
	}
	must(c.IncreaseStreamRetentionPeriod(ctx, &kinesis.IncreaseStreamRetentionPeriodInput{StreamName: aws.String("orders"), RetentionPeriodHours: aws.Int32(48)}))
	mon := must(c.EnableEnhancedMonitoring(ctx, &kinesis.EnableEnhancedMonitoringInput{StreamName: aws.String("orders"), ShardLevelMetrics: []types.MetricsName{types.MetricsNameAll}}))
	if len(mon.CurrentShardLevelMetrics) != 0 || len(mon.DesiredShardLevelMetrics) != 7 {
		t.Fatalf("monitoring = %+v", mon)
	}
	must(c.StartStreamEncryption(ctx, &kinesis.StartStreamEncryptionInput{StreamName: aws.String("orders"), EncryptionType: types.EncryptionTypeKms, KeyId: aws.String("alias/aws/kinesis")}))
	sum = must(c.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: aws.String("orders")})).StreamDescriptionSummary
	if *sum.RetentionPeriodHours != 48 || sum.EncryptionType != types.EncryptionTypeKms || *sum.KeyId != "alias/aws/kinesis" || len(sum.EnhancedMonitoring[0].ShardLevelMetrics) != 7 {
		t.Fatalf("summary after settings = %+v", sum)
	}
	if lim := must(c.DescribeLimits(ctx, &kinesis.DescribeLimitsInput{})); *lim.OpenShardCount != 7 || *lim.OnDemandStreamCount != 1 {
		t.Fatalf("limits = %+v", lim)
	}

	must(c.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String("orders")}))
	_, err = c.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String("orders")})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("want ResourceNotFoundException, got %v", err)
	}
}

func TestPutAndRead(t *testing.T) {
	c, _ := setup(t)
	name := aws.String("events")
	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: name, ShardCount: aws.Int32(2)}))

	// MD5 of the partition key picks the shard: the top bit splits two shards.
	want := "shardId-000000000000"
	if sum := md5.Sum([]byte("user-1")); sum[0] >= 0x80 {
		want = "shardId-000000000001"
	}
	a := must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("user-1"), Data: []byte("a")}))
	b := must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("user-1"), Data: []byte("b")}))
	if *a.ShardId != want || *b.ShardId != want || len(*a.SequenceNumber) != 56 || *b.SequenceNumber <= *a.SequenceNumber {
		t.Fatalf("put = %s %s, %s %s (want shard %s)", *a.ShardId, *a.SequenceNumber, *b.ShardId, *b.SequenceNumber, want)
	}
	batch := must(c.PutRecords(ctx, &kinesis.PutRecordsInput{StreamName: name, Records: []types.PutRecordsRequestEntry{
		{PartitionKey: aws.String("user-1"), Data: []byte("c")},
		{PartitionKey: aws.String("user-1"), Data: []byte("d")},
	}}))
	if *batch.FailedRecordCount != 0 || len(batch.Records) != 2 || *batch.Records[1].ShardId != want {
		t.Fatalf("put records = %+v", batch)
	}

	// TRIM_HORIZON pages with Limit and NextShardIterator.
	it := must(c.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{StreamName: name, ShardId: &want, ShardIteratorType: types.ShardIteratorTypeTrimHorizon}))
	p1 := must(c.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: it.ShardIterator, Limit: aws.Int32(3)}))
	p2 := must(c.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: p1.NextShardIterator}))
	if data(p1.Records) != "abc" || data(p2.Records) != "d" || *p2.MillisBehindLatest != 0 || p2.NextShardIterator == nil {
		t.Fatalf("pages = %q %q", data(p1.Records), data(p2.Records))
	}
	if r := p1.Records[0]; *r.PartitionKey != "user-1" || *r.SequenceNumber != *a.SequenceNumber || time.Since(*r.ApproximateArrivalTimestamp) > time.Minute {
		t.Fatalf("record = %+v", r)
	}
	if p3 := must(c.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: p2.NextShardIterator})); len(p3.Records) != 0 || p3.NextShardIterator == nil {
		t.Fatalf("caught up = %+v", p3)
	}

	// LATEST only sees what comes next.
	latest := must(c.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{StreamName: name, ShardId: &want, ShardIteratorType: types.ShardIteratorTypeLatest}))
	must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("user-1"), Data: []byte("e")}))
	if r := must(c.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: latest.ShardIterator})); data(r.Records) != "e" {
		t.Fatalf("latest = %q", data(r.Records))
	}

	from := func(typ types.ShardIteratorType, seq *string, ts *time.Time) string {
		it := must(c.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{StreamName: name, ShardId: &want, ShardIteratorType: typ,
			StartingSequenceNumber: seq, Timestamp: ts}))
		return data(must(c.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: it.ShardIterator})).Records)
	}
	if got := from(types.ShardIteratorTypeAfterSequenceNumber, a.SequenceNumber, nil); got != "bcde" {
		t.Fatalf("after a = %q", got)
	}
	if got := from(types.ShardIteratorTypeAtSequenceNumber, b.SequenceNumber, nil); got != "bcde" {
		t.Fatalf("at b = %q", got)
	}
	time.Sleep(20 * time.Millisecond)
	ts := time.Now()
	time.Sleep(20 * time.Millisecond)
	must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("user-1"), Data: []byte("f")}))
	if got := from(types.ShardIteratorTypeAtTimestamp, nil, &ts); got != "f" {
		t.Fatalf("at timestamp = %q", got)
	}

	_, err := c.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: aws.String("bogus")})
	var bad *types.InvalidArgumentException
	if !errors.As(err, &bad) {
		t.Fatalf("bad iterator: want InvalidArgumentException, got %v", err)
	}
	_, err = c.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{StreamName: name, ShardId: aws.String("shardId-000000000009"), ShardIteratorType: types.ShardIteratorTypeLatest})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("missing shard: want ResourceNotFoundException, got %v", err)
	}

	// An explicit hash key pins the shard regardless of the partition key.
	low := must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("x"), ExplicitHashKey: aws.String("0"), Data: []byte("lo")}))
	high := must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("x"),
		ExplicitHashKey: aws.String("340282366920938463463374607431768211455"), Data: []byte("hi")}))
	if *low.ShardId != "shardId-000000000000" || *high.ShardId != "shardId-000000000001" {
		t.Fatalf("explicit hash keys went to %s and %s", *low.ShardId, *high.ShardId)
	}
	_, err = c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("x"),
		ExplicitHashKey: aws.String("340282366920938463463374607431768211456"), Data: []byte("x")})
	if !errors.As(err, &bad) {
		t.Fatalf("hash key 2^128: want InvalidArgumentException, got %v", err)
	}
}

// Newer SDKs address everything by StreamARN.
func TestStreamARNOnly(t *testing.T) {
	c, _ := setup(t)
	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("arnonly"), ShardCount: aws.Int32(1)}))
	arn := aws.String("arn:aws:kinesis:us-east-1:000000000000:stream/arnonly")
	if sum := must(c.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamARN: arn})); *sum.StreamDescriptionSummary.StreamName != "arnonly" {
		t.Fatalf("summary = %+v", sum.StreamDescriptionSummary)
	}
	must(c.PutRecords(ctx, &kinesis.PutRecordsInput{StreamARN: arn, Records: []types.PutRecordsRequestEntry{{PartitionKey: aws.String("k"), Data: []byte("v")}}}))
	shards := must(c.ListShards(ctx, &kinesis.ListShardsInput{StreamARN: arn}))
	it := must(c.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{StreamARN: arn, ShardId: shards.Shards[0].ShardId, ShardIteratorType: types.ShardIteratorTypeTrimHorizon}))
	if r := must(c.GetRecords(ctx, &kinesis.GetRecordsInput{StreamARN: arn, ShardIterator: it.ShardIterator})); data(r.Records) != "v" {
		t.Fatalf("records = %q", data(r.Records))
	}

	con := must(c.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{StreamARN: arn, ConsumerName: aws.String("app")})).Consumer
	if con.ConsumerStatus != types.ConsumerStatusActive {
		t.Fatalf("consumer = %+v", con)
	}
	desc := must(c.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{ConsumerARN: con.ConsumerARN})).ConsumerDescription
	if *desc.ConsumerName != "app" || *desc.StreamARN != *arn {
		t.Fatalf("consumer description = %+v", desc)
	}
	if l := must(c.ListStreamConsumers(ctx, &kinesis.ListStreamConsumersInput{StreamARN: arn})); len(l.Consumers) != 1 {
		t.Fatalf("consumers = %+v", l.Consumers)
	}
	must(c.PutResourcePolicy(ctx, &kinesis.PutResourcePolicyInput{ResourceARN: arn, Policy: aws.String(`{"Version":"2012-10-17","Statement":[]}`)}))
	if p := must(c.GetResourcePolicy(ctx, &kinesis.GetResourcePolicyInput{ResourceARN: arn})); *p.Policy != `{"Version":"2012-10-17","Statement":[]}` {
		t.Fatalf("policy = %s", *p.Policy)
	}
	_, err := c.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamARN: arn})
	var inUse *types.ResourceInUseException
	if !errors.As(err, &inUse) {
		t.Fatalf("delete with consumers: want ResourceInUseException, got %v", err)
	}
	must(c.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{ConsumerARN: con.ConsumerARN}))
	must(c.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamARN: arn}))
}

func TestUpdateShardCount(t *testing.T) {
	c, _ := setup(t)
	name := aws.String("scale")
	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: name, ShardCount: aws.Int32(2)}))
	before := must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("k"), Data: []byte("before")}))
	out := must(c.UpdateShardCount(ctx, &kinesis.UpdateShardCountInput{StreamName: name, TargetShardCount: aws.Int32(4), ScalingType: types.ScalingTypeUniformScaling}))
	if *out.CurrentShardCount != 2 || *out.TargetShardCount != 4 {
		t.Fatalf("update = %+v", out)
	}
	all := must(c.ListShards(ctx, &kinesis.ListShardsInput{StreamName: name})).Shards
	if len(all) != 6 || all[0].SequenceNumberRange.EndingSequenceNumber == nil || all[2].SequenceNumberRange.EndingSequenceNumber != nil ||
		*all[2].ParentShardId != "shardId-000000000000" || *all[5].ParentShardId != "shardId-000000000001" {
		t.Fatalf("shards after resharding = %+v", all)
	}
	open := must(c.ListShards(ctx, &kinesis.ListShardsInput{StreamName: name, ShardFilter: &types.ShardFilter{Type: types.ShardFilterTypeAtLatest}})).Shards
	if len(open) != 4 {
		t.Fatalf("open shards = %d", len(open))
	}
	pg := must(c.ListShards(ctx, &kinesis.ListShardsInput{StreamName: name, MaxResults: aws.Int32(4)}))
	rest := must(c.ListShards(ctx, &kinesis.ListShardsInput{NextToken: pg.NextToken}))
	if len(pg.Shards) != 4 || len(rest.Shards) != 2 || rest.NextToken != nil {
		t.Fatalf("shard pages = %d + %d", len(pg.Shards), len(rest.Shards))
	}

	if after := must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: name, PartitionKey: aws.String("k"), Data: []byte("after")})); *after.ShardId <= "shardId-000000000001" {
		t.Fatalf("new records must go to open shards, went to %s", *after.ShardId)
	}
	// A closed shard still serves its records, then hands over to its children.
	it := must(c.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{StreamName: name, ShardId: before.ShardId, ShardIteratorType: types.ShardIteratorTypeTrimHorizon}))
	r := must(c.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: it.ShardIterator}))
	if data(r.Records) != "before" || r.NextShardIterator != nil || len(r.ChildShards) != 2 {
		t.Fatalf("closed shard read = %q next=%v children=%+v", data(r.Records), r.NextShardIterator, r.ChildShards)
	}

	_, err := c.UpdateShardCount(ctx, &kinesis.UpdateShardCountInput{StreamName: name, TargetShardCount: aws.Int32(9), ScalingType: types.ScalingTypeUniformScaling})
	var bad *types.InvalidArgumentException
	if !errors.As(err, &bad) {
		t.Fatalf("more than double: want InvalidArgumentException, got %v", err)
	}
}

// EventBridge, SNS and friends put records through the hub.
func TestHubDeliver(t *testing.T) {
	c, h := setup(t)
	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("bus"), ShardCount: aws.Int32(1)}))
	if err := h.Deliver("arn:aws:kinesis:us-east-1:000000000000:stream/bus", &core.Message{Body: `{"detail":1}`, GroupID: "g"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Deliver("arn:aws:kinesis:us-east-1:000000000000:stream/bus", &core.Message{Body: "no key"}); err != nil {
		t.Fatal(err)
	}
	recs := read(t, c, "bus", "shardId-000000000000")
	if len(recs) != 2 || string(recs[0].Data) != `{"detail":1}` || *recs[0].PartitionKey != "g" || len(*recs[1].PartitionKey) != 36 {
		t.Fatalf("delivered = %+v", recs)
	}
	if err := h.Deliver("arn:aws:kinesis:us-east-1:000000000000:stream/missing", &core.Message{Body: "x"}); err == nil {
		t.Fatal("delivering to a missing stream must fail")
	}
}

// Records, shards and the sequence counter survive a snapshot.
func TestSnapshot(t *testing.T) {
	c, h := setup(t)
	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("keep"), ShardCount: aws.Int32(1)}))
	a := must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: aws.String("keep"), PartitionKey: aws.String("k"), Data: []byte("a")}))
	snap := must(h.Snapshot())
	h.Reset()
	if _, err := c.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: aws.String("keep")}); err == nil {
		t.Fatal("reset must drop the stream")
	}
	if err := h.Restore(snap); err != nil {
		t.Fatal(err)
	}
	b := must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: aws.String("keep"), PartitionKey: aws.String("k"), Data: []byte("b")}))
	if *b.SequenceNumber <= *a.SequenceNumber {
		t.Fatalf("sequence went backwards: %s after %s", *b.SequenceNumber, *a.SequenceNumber)
	}
	if got := data(read(t, c, "keep", "shardId-000000000000")); got != "ab" {
		t.Fatalf("records after restore = %q", got)
	}
	if rows := h.Resources("kinesis")["kinesis"][0].Rows; len(rows) != 1 || rows[0][3] != "1" || rows[0][4] != "2" || rows[0][5] != "PROVISIONED" {
		t.Fatalf("console rows = %v", rows)
	}
}

// The Java SDK's default CBOR gets a clear error instead of a parse failure.
// The Java SDK's legacy CBOR (X-Amz-Target + application/x-amz-cbor-1.1) is answered in CBOR.
func TestLegacyCBOR(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return kinesissvc.New(h) })
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader("\xa0")) // {} in CBOR
	req.Header.Set("Content-Type", "application/x-amz-cbor-1.1")
	req.Header.Set("X-Amz-Target", "Kinesis_20131202.ListStreams")
	res := must(http.DefaultClient.Do(req))
	b, _ := io.ReadAll(res.Body)
	var out struct{ HasMoreStreams bool }
	if res.StatusCode != 200 || res.Header.Get("Content-Type") != "application/x-amz-cbor-1.1" || core.DecodeCBOR(b, &out) != nil {
		t.Fatalf("cbor: %d %q", res.StatusCode, b)
	}
}

// floci #562: SubscribeToShard streams existing and new records, and a closed shard ends with its children.
func TestSubscribeToShard(t *testing.T) {
	c, _ := setup(t)
	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("efo"), ShardCount: aws.Int32(1)}))
	sum := must(c.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: aws.String("efo")})).StreamDescriptionSummary
	con := must(c.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{StreamARN: sum.StreamARN, ConsumerName: aws.String("c1")})).Consumer
	put := func(d string) {
		must(c.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: aws.String("efo"), PartitionKey: aws.String("k"), Data: []byte(d)}))
	}
	put("one")
	shard := must(c.ListShards(ctx, &kinesis.ListShardsInput{StreamName: aws.String("efo")})).Shards[0].ShardId

	_, err := c.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{ConsumerARN: aws.String(*con.ConsumerARN + "x"), ShardId: shard,
		StartingPosition: &types.StartingPosition{Type: types.ShardIteratorTypeTrimHorizon}})
	if !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("unknown consumer: %v", err)
	}
	out := must(c.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{ConsumerARN: con.ConsumerARN, ShardId: shard,
		StartingPosition: &types.StartingPosition{Type: types.ShardIteratorTypeTrimHorizon}}))
	es := out.GetStream()
	defer es.Close()
	var got string
	timeout := time.After(10 * time.Second)
	for sent := false; len(got) < len("onetwo"); {
		select {
		case ev := <-es.Events():
			if e, ok := ev.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent); ok {
				got += data(e.Value.Records)
				if e.Value.ContinuationSequenceNumber == nil {
					t.Fatal("no ContinuationSequenceNumber")
				}
			}
			if !sent {
				put("two") // arrives while subscribed
				sent = true
			}
		case <-timeout:
			t.Fatalf("got %q, stream error %v", got, es.Err())
		}
	}
	if got != "onetwo" {
		t.Fatalf("records = %q", got)
	}
}

// floci #2516: Data is required in PutRecord and PutRecords (an empty blob is fine).
// The SDK refuses a missing Data itself, so the requests are sent by hand.
func TestPutRequiresData(t *testing.T) {
	c, h := setup(t)
	must(c.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("req"), ShardCount: aws.Int32(1)}))
	call := func(op, body string) int {
		req, _ := http.NewRequest(http.MethodPost, h.URL, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Kinesis_20131202."+op)
		res := must(http.DefaultClient.Do(req))
		res.Body.Close()
		return res.StatusCode
	}
	for name, body := range map[string]string{
		"missing": `{"StreamName":"req","PartitionKey":"k"}`,
		"null":    `{"StreamName":"req","PartitionKey":"k","Data":null}`,
	} {
		if got := call("PutRecord", body); got != 400 {
			t.Errorf("PutRecord with %s Data: status %d", name, got)
		}
	}
	if got := call("PutRecords", `{"StreamName":"req","Records":[{"PartitionKey":"k"}]}`); got != 400 {
		t.Errorf("PutRecords with a missing Data: status %d", got)
	}
	if got := call("PutRecord", `{"StreamName":"req","PartitionKey":"k","Data":""}`); got != 200 {
		t.Errorf("PutRecord with an empty Data: status %d", got)
	}
}
