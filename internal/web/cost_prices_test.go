package web

import (
	"testing"

	"github.com/nrndev/pogi/internal/cost"
)

// Step Functions and API Gateway rows are priced from the embedded list, in the chosen region.
func TestCostStepFunctionsAndAPIGateway(t *testing.T) {
	db, err := cost.Default()
	if err != nil {
		t.Fatal(err)
	}
	cols := map[string]string{"Executions": "1000", "Type": "STANDARD"}
	in := staticIn{share: 2e6, perMonth: 1, db: db, region: "eu-west-1", col: func(c string) string { return cols[c] }}
	transition, _, _ := db.Rate("aws/sfn/transition", "eu-west-1")
	if _, got, ok := staticPrice("stepfunctions", in); !ok || got != 1000*sfnTransitions*transition/1000 {
		t.Errorf("standard state machine: %v %v", got, ok)
	}
	in.table = "HTTP APIs"
	httpReq, _, _ := db.Rate("aws/apigw/http/request", "eu-west-1")
	if _, got, ok := staticPrice("apigateway", in); !ok || got != 2e6*httpReq*perM || got == 0 {
		t.Errorf("HTTP API: %v %v", got, ok)
	}
}
