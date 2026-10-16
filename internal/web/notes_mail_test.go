package web_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// The notes poll carries the newest captured mail, so the console can toast it and refresh the Inbox.
func TestNotesPollNewestMail(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"ses"}})
	poll := func() (m struct {
		Mail *struct{ ID, To, Subject string }
	}) {
		res, err := http.Get(srv.URL + "/_pogi/notes/poll")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
			t.Fatal(err)
		}
		return
	}
	if poll().Mail != nil {
		t.Fatal("empty inbox should report no mail")
	}
	c := sesv2.NewFromConfig(awstest.Config(t, srv.URL))
	if _, err := c.SendEmail(context.Background(), &sesv2.SendEmailInput{FromEmailAddress: aws.String("ann@example.com"),
		Destination: &types.Destination{ToAddresses: []string{"alice@x.com"}},
		Content: &types.EmailContent{Simple: &types.Message{Subject: &types.Content{Data: aws.String("Hello")},
			Body: &types.Body{Text: &types.Content{Data: aws.String("hi")}}}}}); err != nil {
		t.Fatal(err)
	}
	if m := poll().Mail; m == nil || m.ID == "" || m.To != "alice@x.com" || m.Subject != "Hello" {
		t.Fatalf("mail = %+v", m)
	}
	// the shell: logo first in the top bar, one scope pill, the collapse button in the sidebar footer
	h := page(t, srv, "/_pogi/")
	has(t, h, `class="collapse" data-js="side"`, "All clouds", `id="scopeBtn"`)
	if strings.Index(h, `class="logo"`) > strings.Index(h, `class="search"`) || strings.Contains(h, "cloudsel") {
		t.Error("logo must come first and cloudsel must be gone")
	}
}
