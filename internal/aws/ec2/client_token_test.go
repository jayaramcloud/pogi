package ec2_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

// RunInstances with a ClientToken: same token and parameters give the first answer (also when 24 calls
// arrive at once), and the same token with other parameters is an IdempotentParameterMismatch.
func TestRunInstancesClientTokenIsIdempotent(t *testing.T) {
	_, _, c := serve(t)
	run := func(image string) (*ec2.RunInstancesOutput, error) {
		return c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(image), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
			ClientToken: aws.String("tok-1")})
	}
	const n = 24
	outs := make([]*ec2.RunInstancesOutput, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, err := run(alpine)
			if err != nil {
				t.Errorf("call %d: %v", i, err)
				return
			}
			outs[i] = o
		}()
	}
	wg.Wait()
	first := outs[0]
	for i, o := range outs {
		if o == nil || aws.ToString(o.ReservationId) != aws.ToString(first.ReservationId) ||
			aws.ToString(o.Instances[0].InstanceId) != aws.ToString(first.Instances[0].InstanceId) {
			t.Fatalf("call %d did not return the first call's reservation and instance", i)
		}
	}
	d := must(c.DescribeInstances(ctx, &ec2.DescribeInstancesInput{}))
	if got := len(d.Reservations); got != 1 || len(d.Reservations[0].Instances) != 1 {
		t.Errorf("one token made %d reservations; want 1 reservation with 1 instance", got)
	}
	_, err := run("ami-0abcdef1234567890")
	if err == nil || !strings.Contains(err.Error(), "IdempotentParameterMismatch") {
		t.Errorf("same token, other ImageId: err = %v, want IdempotentParameterMismatch", err)
	}
}
