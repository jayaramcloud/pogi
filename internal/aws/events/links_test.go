package events_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
)

// A bus feeds its rules and archives, and a rule can target another bus.
func TestLinks(t *testing.T) {
	e := setup(t)
	other := "arn:aws:events:us-east-1:000000000000:event-bus/other"
	must(e.eb.CreateEventBus(ctx, &eventbridge.CreateEventBusInput{Name: aws.String("other")}))
	e.rule("fwd", `{"source":["app"]}`, target("b", other))
	must(e.eb.CreateArchive(ctx, &eventbridge.CreateArchiveInput{ArchiveName: aws.String("keep"), EventSourceArn: aws.String(defaultBus)}))
	rule := "arn:aws:events:us-east-1:000000000000:rule/fwd"
	want := map[string]bool{defaultBus + ">" + rule: true, rule + ">" + other: true,
		defaultBus + ">arn:aws:events:us-east-1:000000000000:archive/keep": true}
	for _, l := range e.svc.Links() {
		delete(want, l.From+">"+l.To)
	}
	if len(want) != 0 {
		t.Fatalf("missing links: %v", want)
	}
}
