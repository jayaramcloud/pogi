package web

import "testing"

func TestDisambiguate(t *testing.T) {
	inUse := []navSvc{
		{Name: "iam", Title: "IAM", Abbr: "IAM", Cloud: "aws", Full: "IAM · AWS"},
		{Name: "gcpiam", Title: "IAM", Abbr: "IAM", Cloud: "gcp", Full: "IAM · GCP"},
		{Name: "secretsmanager", Title: "Secrets Manager", Abbr: "SM", Cloud: "aws", Full: "Secrets Manager · AWS"},
		{Name: "secretmanager", Title: "Secret Manager", Abbr: "SM", Cloud: "gcp", Full: "Secret Manager · GCP"},
		{Name: "s3", Title: "S3", Abbr: "S3", Cloud: "aws", Full: "S3 · AWS"},
	}
	pinned := []navSvc{inUse[1]} // a pinned service already in use is one service, not a clash
	disambiguate(inUse, pinned)
	got := map[string][2]string{}
	for _, s := range inUse {
		got[s.Name] = [2]string{s.Title, s.Abbr}
	}
	for name, w := range map[string][2]string{
		"iam": {"IAM · AWS", "IAMa"}, "gcpiam": {"IAM · GCP", "IAMg"}, // same title: the label and the abbreviation say the cloud
		"secretsmanager": {"Secrets Manager", "SMa"}, "secretmanager": {"Secret Manager", "SMg"}, // same abbreviation only
		"s3": {"S3", "S3"}, // no clash: untouched
	} {
		if got[name] != w {
			t.Errorf("%s = %v, want %v", name, got[name], w)
		}
	}
	if pinned[0].Title != "IAM · GCP" {
		t.Errorf("the pinned copy = %q", pinned[0].Title)
	}
}

func TestPerMonth(t *testing.T) {
	for v, w := range map[float64]string{0.004: "<$1", 0.99: "<$1", 1: "$1.0", 4.25: "$4.2", 29.4: "$29", 1234: "$1234"} {
		if got := perMonth(v); got != w {
			t.Errorf("perMonth(%v) = %q, want %q", v, got, w)
		}
	}
}
