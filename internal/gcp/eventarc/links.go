package eventarc

import (
	"fmt"

	pb "cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"github.com/nrndev/pogi/internal/core"
)

// Links are each trigger's wires: its event source (a bucket, or the topic
// behind a Pub/Sub trigger) to the trigger, and the trigger to its destination.
func (s *Service) Links() []core.Link {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Link
	for _, n := range core.SortedKeys(s.triggers) {
		p := s.triggers[n].p
		id := "//eventarc.googleapis.com/" + p.Name
		for _, f := range p.EventFilters {
			if f.Attribute == "bucket" && f.Operator != "match-path-pattern" {
				out = append(out, core.Link{From: "//storage.googleapis.com/projects/_/buckets/" + f.Value, To: id, Kind: "configured", Label: "event source"})
			}
		}
		if t := p.GetTransport().GetPubsub().GetTopic(); t != "" {
			out = append(out, core.Link{From: "//pubsub.googleapis.com/" + t, To: id, Kind: "configured", Label: "event source"})
		}
		project, loc := split(p.Name)
		to := ""
		switch d := p.GetDestination().GetDescriptor_().(type) {
		case *pb.Destination_CloudRun:
			to = fmt.Sprintf("//run.googleapis.com/projects/%s/locations/%s/services/%s", project, or(d.CloudRun.Region, loc), d.CloudRun.Service)
		case *pb.Destination_CloudFunction:
			to = "//cloudfunctions.googleapis.com/" + d.CloudFunction
		case *pb.Destination_HttpEndpoint:
			to = d.HttpEndpoint.Uri
		}
		if to != "" {
			out = append(out, core.Link{From: id, To: to, Kind: "configured", Label: "target"})
		}
	}
	return out
}
