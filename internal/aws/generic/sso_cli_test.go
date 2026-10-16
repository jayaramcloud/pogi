package generic_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/identitystore"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
	"github.com/nrndev/pogi/internal/awstest"
)

// TestSSOCLI runs the real AWS CLI v2 through `aws sso login` (the device code grant, then the
// browser-less authorization-code grant) and a profile whose credentials come from the cached token.
func TestSSOCLI(t *testing.T) {
	bin, err := exec.LookPath("aws")
	if err != nil {
		t.Skip("the aws CLI is not installed")
	}
	if out, err := exec.Command(bin, "--version").Output(); err != nil || !strings.HasPrefix(string(out), "aws-cli/2") {
		t.Skipf("need aws CLI v2: %s %v", out, err)
	}
	_, base := startWith(t, "sso-oidc", "sso", "sso-admin", "identitystore")
	ctx := context.Background()
	cfg := awstest.Config(t, base)
	const acct = "123456789012"
	admin, ids := ssoadmin.NewFromConfig(cfg), identitystore.NewFromConfig(cfg)
	in, _ := admin.ListInstances(ctx, &ssoadmin.ListInstancesInput{})
	inst := in.Instances[0]
	ps, err := admin.CreatePermissionSet(ctx, &ssoadmin.CreatePermissionSetInput{InstanceArn: inst.InstanceArn, Name: aws.String("ReadOnly")})
	if err != nil {
		t.Fatal(err)
	}
	user, err := ids.CreateUser(ctx, &identitystore.CreateUserInput{IdentityStoreId: inst.IdentityStoreId, UserName: aws.String("jdoe")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.CreateAccountAssignment(ctx, &ssoadmin.CreateAccountAssignmentInput{InstanceArn: inst.InstanceArn, PermissionSetArn: ps.PermissionSet.PermissionSetArn,
		PrincipalType: types.PrincipalTypeUser, PrincipalId: user.UserId, TargetType: types.TargetTypeAwsAccount, TargetId: aws.String(acct)}); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	conf := filepath.Join(home, "config")
	// The CLI only checks that the start URL is an AWS-owned host; the endpoints come from the environment.
	os.WriteFile(conf, []byte("[profile x]\nsso_session = pogi\nsso_account_id = "+acct+"\nsso_role_name = ReadOnly\nregion = us-east-1\n\n"+
		"[sso-session pogi]\nsso_start_url = https://pogi.awsapps.com/start\nsso_region = us-east-1\nsso_registration_scopes = sso:account:access\n"), 0o600)
	var env []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "AWS_") && !strings.HasPrefix(e, "HOME=") {
			env = append(env, e)
		}
	}
	env = append(env, "HOME="+home, "AWS_CONFIG_FILE="+conf, "AWS_SHARED_CREDENTIALS_FILE="+filepath.Join(home, "creds"),
		"AWS_ENDPOINT_URL_SSO_OIDC="+base, "AWS_ENDPOINT_URL_SSO="+base, "AWS_ENDPOINT_URL_STS="+base)
	cli := func(args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Env = env
		return cmd
	}
	whoami := func() {
		t.Helper()
		out, err := cli("sts", "get-caller-identity", "--profile", "x", "--output", "json").CombinedOutput()
		var who struct{ Account, Arn string }
		json.Unmarshal(out, &who)
		if err != nil || who.Account != acct || !regexp.MustCompile(`^arn:aws:sts::`+acct+`:assumed-role/AWSReservedSSO_ReadOnly_[0-9a-f]{16}/jdoe$`).MatchString(who.Arn) {
			t.Fatalf("get-caller-identity: %v %s", err, out)
		}
	}

	// The device code grant: the CLI polls until the sign-in is approved.
	out, err := cli("sso", "login", "--profile", "x", "--no-browser", "--use-device-code").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "Successfully logged into") {
		t.Fatalf("device login: %v %s", err, out)
	}
	whoami()

	// The authorization-code grant: the CLI prints the URL and waits for the browser to come back to it.
	os.RemoveAll(filepath.Join(home, ".aws"))
	cmd := cli("sso", "login", "--profile", "x", "--no-browser")
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		if strings.Contains(sc.Text(), "/authorize?") {
			res, err := http.Get(strings.TrimSpace(sc.Text())) // the browser follows the redirect to the CLI's local server
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
		}
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("browser login: %v", err)
	}
	whoami()
}
