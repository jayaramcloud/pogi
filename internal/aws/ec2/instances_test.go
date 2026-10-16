package ec2_test

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	iamsdk "github.com/aws/aws-sdk-go-v2/service/iam"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	"github.com/nrndev/pogi/internal/aws/iam"
	"github.com/nrndev/pogi/internal/aws/sts"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"golang.org/x/crypto/ssh"
)

const alpine = "ami-0a1b2c3d4e5f60006" // the catalog's Alpine Linux

// serve starts EC2 with IAM and STS, and makes a role in an instance profile.
func serve(t *testing.T) (*core.Hub, string, *ec2.Client) {
	t.Helper()
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) },
		func(h *core.Hub) core.Service { return iam.New(h) }, func(h *core.Hub) core.Service { return sts.New(h) })
	ic := iamsdk.NewFromConfig(awstest.Config(t, url))
	must(ic.CreateRole(ctx, &iamsdk.CreateRoleInput{RoleName: aws.String("web"),
		AssumeRolePolicyDocument: aws.String(`{"Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`)}))
	must(ic.CreateInstanceProfile(ctx, &iamsdk.CreateInstanceProfileInput{InstanceProfileName: aws.String("web")}))
	must(ic.AddRoleToInstanceProfile(ctx, &iamsdk.AddRoleToInstanceProfileInput{InstanceProfileName: aws.String("web"), RoleName: aws.String("web")}))
	return hub, url, ec2.NewFromConfig(awstest.Config(t, url))
}

func describe(t *testing.T, c *ec2.Client, id string) types.Instance {
	t.Helper()
	r := must(c.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{id}}))
	return r.Reservations[0].Instances[0]
}

func TestInstancesWithoutDocker(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	hub, url, c := serve(t)

	sg := must(c.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{GroupName: aws.String("web"), Description: aws.String("web")}))
	must(c.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{GroupId: sg.GroupId, IpPermissions: []types.IpPermission{
		{IpProtocol: aws.String("tcp"), FromPort: aws.Int32(80), ToPort: aws.Int32(80), IpRanges: []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}}}}}))
	userData := base64.StdEncoding.EncodeToString([]byte("#!/bin/sh\necho hi > /tmp/hi\n"))
	lt := must(c.CreateLaunchTemplate(ctx, &ec2.CreateLaunchTemplateInput{LaunchTemplateName: aws.String("web"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{ImageId: aws.String(alpine), InstanceType: types.InstanceTypeT3Micro,
			SecurityGroupIds: []string{*sg.GroupId}, UserData: aws.String(userData),
			IamInstanceProfile: &types.LaunchTemplateIamInstanceProfileSpecificationRequest{Name: aws.String("web")},
			TagSpecifications: []types.LaunchTemplateTagSpecificationRequest{{ResourceType: types.ResourceTypeInstance,
				Tags: []types.Tag{{Key: aws.String("Name"), Value: aws.String("web")}}}}}})).LaunchTemplate
	if *lt.LatestVersionNumber != 1 || *lt.DefaultVersionNumber != 1 {
		t.Fatalf("template = %+v", lt)
	}
	v := must(c.DescribeLaunchTemplateVersions(ctx, &ec2.DescribeLaunchTemplateVersionsInput{LaunchTemplateId: lt.LaunchTemplateId,
		Versions: []string{"$Latest"}})).LaunchTemplateVersions[0].LaunchTemplateData
	if *v.ImageId != alpine || v.InstanceType != types.InstanceTypeT3Micro || v.SecurityGroupIds[0] != *sg.GroupId || *v.IamInstanceProfile.Name != "web" ||
		*v.TagSpecifications[0].Tags[0].Value != "web" || *v.UserData != userData {
		t.Fatalf("template data = %+v", v)
	}

	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{MinCount: aws.Int32(2), MaxCount: aws.Int32(2),
		LaunchTemplate: &types.LaunchTemplateSpecification{LaunchTemplateId: lt.LaunchTemplateId}}))
	if len(run.Instances) != 2 || run.Instances[0].State.Name != types.InstanceStateNamePending {
		t.Fatalf("run = %+v", run.Instances)
	}
	id := *run.Instances[0].InstanceId
	i := describe(t, c, id)
	tags := map[string]string{}
	for _, tg := range i.Tags {
		tags[*tg.Key] = *tg.Value
	}
	switch {
	case i.State.Name != types.InstanceStateNameRunning, *i.ImageId != alpine, i.InstanceType != types.InstanceTypeT3Micro,
		!strings.HasPrefix(*i.PrivateIpAddress, "172.31."), *i.PrivateIpAddress == *run.Instances[1].PrivateIpAddress,
		*i.SecurityGroups[0].GroupId != *sg.GroupId, tags["Name"] != "web", tags["aws:ec2launchtemplate:id"] != *lt.LaunchTemplateId,
		!strings.HasSuffix(*i.IamInstanceProfile.Arn, ":instance-profile/web"), *i.RootDeviceName != "/dev/xvda", !*i.SourceDestCheck,
		len(i.BlockDeviceMappings) != 1:
		t.Fatalf("instance = %+v", i)
	}
	vols := must(c.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{Filters: []types.Filter{filter("attachment.instance-id", id)}})).Volumes
	if len(vols) != 1 || *vols[0].Size != 8 || vols[0].State != types.VolumeStateInUse || *vols[0].Attachments[0].Device != "/dev/xvda" {
		t.Fatalf("volumes = %+v", vols)
	}
	ud := must(c.DescribeInstanceAttribute(ctx, &ec2.DescribeInstanceAttributeInput{InstanceId: &id, Attribute: types.InstanceAttributeNameUserData}))
	if *ud.UserData.Value != userData {
		t.Fatalf("user data = %+v", ud.UserData)
	}
	if it := must(c.DescribeInstanceTypes(ctx, &ec2.DescribeInstanceTypesInput{InstanceTypes: []types.InstanceType{"t3.micro"}})).InstanceTypes; len(it) != 1 ||
		!*it[0].BurstablePerformanceSupported || *it[0].MemoryInfo.SizeInMiB != 1024 {
		t.Fatalf("instance types = %+v", it)
	}

	// IMDS, v1 and v2, and the role's credentials
	get := func(path, token string) (int, string) {
		req, _ := http.NewRequest(http.MethodGet, url+"/_aws/imds/"+id+"/latest/"+path, nil)
		if token != "" {
			req.Header.Set("X-aws-ec2-metadata-token", token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	if code, body := get("meta-data/instance-id", ""); code != 200 || body != id {
		t.Fatalf("instance-id = %d %q", code, body)
	}
	req, _ := http.NewRequest(http.MethodPut, url+"/_aws/imds/"+id+"/latest/api/token", nil)
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "60")
	resp := must(http.DefaultClient.Do(req))
	token := string(must(io.ReadAll(resp.Body)))
	resp.Body.Close()
	if code, _ := get("meta-data/instance-id", "bogus"); code != 401 {
		t.Fatalf("a bad token answered %d", code)
	}
	if _, body := get("meta-data/iam/security-credentials/", token); body != "web" {
		t.Fatalf("roles = %q", body)
	}
	_, body := get("meta-data/iam/security-credentials/web", token)
	var cr struct{ AccessKeyId, SecretAccessKey, Token string }
	json.Unmarshal([]byte(body), &cr)
	who := must(stssdk.NewFromConfig(awstest.ConfigAs(t, url, cr.AccessKeyId, cr.SecretAccessKey, cr.Token)).GetCallerIdentity(ctx, &stssdk.GetCallerIdentityInput{}))
	if *who.Arn != "arn:aws:sts::000000000000:assumed-role/web/"+id {
		t.Fatalf("caller = %s", *who.Arn)
	}
	// floci #5264: the instance identity credentials (the SSM agent asks for them) are served, with or without a role
	if code, body := get("meta-data/identity-credentials/ec2/security-credentials/ec2-instance", token); code != 200 || !strings.Contains(body, `"AccessKeyId": "ASIA`) {
		t.Fatalf("identity credentials = %d %q", code, body)
	}
	if _, body := get("user-data", token); body != "#!/bin/sh\necho hi > /tmp/hi\n" {
		t.Fatalf("user-data = %q", body)
	}
	if _, body := get("meta-data/placement/availability-zone", token); body != *i.Placement.AvailabilityZone {
		t.Fatalf("zone = %q", body)
	}

	// a second template version inherits from the first and becomes the default
	must(c.CreateLaunchTemplateVersion(ctx, &ec2.CreateLaunchTemplateVersionInput{LaunchTemplateId: lt.LaunchTemplateId, SourceVersion: aws.String("1"),
		LaunchTemplateData: &types.RequestLaunchTemplateData{InstanceType: types.InstanceTypeT3Small}}))
	must(c.ModifyLaunchTemplate(ctx, &ec2.ModifyLaunchTemplateInput{LaunchTemplateId: lt.LaunchTemplateId, DefaultVersion: aws.String("2")}))
	v2 := must(c.DescribeLaunchTemplateVersions(ctx, &ec2.DescribeLaunchTemplateVersionsInput{LaunchTemplateName: aws.String("web"),
		Versions: []string{"$Default"}})).LaunchTemplateVersions[0]
	if *v2.VersionNumber != 2 || !*v2.DefaultVersion || *v2.LaunchTemplateData.ImageId != alpine || v2.LaunchTemplateData.InstanceType != types.InstanceTypeT3Small {
		t.Fatalf("version 2 = %+v", v2)
	}
	// so does a template another service creates in-process, with JSON
	if err := hub.Call("000000000000", "us-east-1", "AmazonEC2", "CreateLaunchTemplate", map[string]any{"LaunchTemplateName": "json",
		"LaunchTemplateData": map[string]any{"ImageId": alpine, "InstanceType": "c5.large", "SecurityGroupIds": []string{*sg.GroupId}}}, nil); err != nil {
		t.Fatal(err)
	}
	var fromJSON struct {
		Instances []struct {
			InstanceType   string
			SecurityGroups []struct{ GroupId string }
		}
	}
	if err := hub.Call("000000000000", "us-east-1", "AmazonEC2", "RunInstances", map[string]any{"MinCount": 1, "MaxCount": 1,
		"LaunchTemplate": map[string]string{"LaunchTemplateName": "json"}}, &fromJSON); err != nil {
		t.Fatal(err)
	}
	if i := fromJSON.Instances[0]; i.InstanceType != "c5.large" || i.SecurityGroups[0].GroupId != *sg.GroupId {
		t.Fatalf("from a JSON template = %+v", i)
	}

	// an EBS volume comes and goes
	vol := must(c.CreateVolume(ctx, &ec2.CreateVolumeInput{AvailabilityZone: i.Placement.AvailabilityZone, Size: aws.Int32(20), VolumeType: types.VolumeTypeGp3}))
	if *vol.Iops != 3000 || vol.State != types.VolumeStateAvailable {
		t.Fatalf("volume = %+v", vol)
	}
	must(c.AttachVolume(ctx, &ec2.AttachVolumeInput{VolumeId: vol.VolumeId, InstanceId: &id, Device: aws.String("/dev/sdf")}))
	if v := must(c.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{*vol.VolumeId}})).Volumes[0]; v.State != types.VolumeStateInUse ||
		*v.Attachments[0].InstanceId != id || len(describe(t, c, id).BlockDeviceMappings) != 2 {
		t.Fatalf("attached volume = %+v", v)
	}
	_, err := c.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: vol.VolumeId})
	wantCode(t, err, "VolumeInUse")
	must(c.DetachVolume(ctx, &ec2.DetachVolumeInput{VolumeId: vol.VolumeId}))
	must(c.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: vol.VolumeId}))
	if st := must(c.DescribeInstanceStatus(ctx, &ec2.DescribeInstanceStatusInput{InstanceIds: []string{id}})).InstanceStatuses; len(st) != 1 ||
		st[0].InstanceStatus.Status != types.SummaryStatusOk {
		t.Fatalf("status = %+v", st)
	}

	// lifecycle
	_, err = c.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{InstanceId: &id, InstanceType: &types.AttributeValue{Value: aws.String("t3.small")}})
	wantCode(t, err, "IncorrectInstanceState")
	must(c.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{InstanceId: &id, DisableApiTermination: &types.AttributeBooleanValue{Value: aws.Bool(true)}}))
	_, err = c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}})
	wantCode(t, err, "OperationNotPermitted")
	must(c.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{InstanceId: &id, DisableApiTermination: &types.AttributeBooleanValue{Value: aws.Bool(false)}}))
	if st := must(c.StopInstances(ctx, &ec2.StopInstancesInput{InstanceIds: []string{id}})).StoppingInstances[0]; st.CurrentState.Name != types.InstanceStateNameStopping {
		t.Fatalf("stop = %+v", st)
	}
	if i := describe(t, c, id); i.State.Name != types.InstanceStateNameStopped {
		t.Fatalf("state = %s", i.State.Name)
	}
	must(c.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{InstanceId: &id, InstanceType: &types.AttributeValue{Value: aws.String("t3.small")}}))
	_, err = c.RebootInstances(ctx, &ec2.RebootInstancesInput{InstanceIds: []string{id}})
	wantCode(t, err, "IncorrectInstanceState")
	must(c.StartInstances(ctx, &ec2.StartInstancesInput{InstanceIds: []string{id}}))
	roundTrip(t, hub)
	if i := describe(t, c, id); i.State.Name != types.InstanceStateNameRunning || i.InstanceType != types.InstanceTypeT3Small {
		t.Fatalf("after start = %+v", i)
	}
	if st := must(c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}})).TerminatingInstances[0]; st.CurrentState.Name != types.InstanceStateNameShuttingDown {
		t.Fatalf("terminate = %+v", st)
	}
	if i := describe(t, c, id); i.State.Name != types.InstanceStateNameTerminated || len(i.BlockDeviceMappings) != 0 {
		t.Fatalf("terminated = %+v", i)
	}
	if v := must(c.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{Filters: []types.Filter{filter("volume-id", *vols[0].VolumeId)}})).Volumes; len(v) != 0 {
		t.Fatalf("root volume survived: %+v", v)
	}
	if code, _ := get("meta-data/instance-id", ""); code != 404 {
		t.Fatalf("IMDS of a terminated instance answered %d", code)
	}

	_, err = c.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{"i-0123456789abcdef0"}})
	wantCode(t, err, "InvalidInstanceID.NotFound")
	_, err = c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String("bogus"), MinCount: aws.Int32(1), MaxCount: aws.Int32(1)})
	wantCode(t, err, "InvalidAMIID.Malformed")
	_, err = c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1), KeyName: aws.String("nope")})
	wantCode(t, err, "InvalidKeyPair.NotFound")

	// pogi's own services call EC2 in-process with JSON
	var out struct{ Instances []struct{ InstanceId string } }
	if err := hub.Call("000000000000", "us-east-1", "AmazonEC2", "RunInstances", map[string]any{"ImageId": "ami-0abcdef1234567890", "MinCount": 1,
		"MaxCount": 1, "TagSpecifications": []map[string]any{{"ResourceType": "instance", "Tags": []map[string]string{{"Key": "team", "Value": "a"}}}}}, &out); err != nil {
		t.Fatal(err)
	}
	var found struct {
		Reservations []struct {
			Instances []struct{ InstanceId, InstanceType string }
		}
	}
	if err := hub.Call("000000000000", "us-east-1", "AmazonEC2", "DescribeInstances",
		map[string]any{"Filters": []map[string]any{{"Name": "tag:team", "Values": []string{"a"}}}}, &found); err != nil {
		t.Fatal(err)
	}
	if len(found.Reservations) != 1 || found.Reservations[0].Instances[0].InstanceId != out.Instances[0].InstanceId ||
		found.Reservations[0].Instances[0].InstanceType != "m1.small" {
		t.Fatalf("in-process = %+v", found)
	}
	err = hub.Call("000000000000", "us-east-1", "AmazonEC2", "TerminateInstances", map[string]any{"InstanceIds": []string{"i-0000000000000000a"}}, nil)
	if core.AsError(err).Code != "InvalidInstanceID.NotFound" {
		t.Fatalf("in-process error = %v", err)
	}
}

// TestInstanceContainer runs an Alpine instance in Docker: its UserData
// writes a file, and from inside it IMDSv2 hands out the role's credentials.
func TestInstanceContainer(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	hub, url, c := serve(t)
	if !hub.Docker().Available() {
		t.Skip("no container engine")
	}
	sg := must(c.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{GroupName: aws.String("http"), Description: aws.String("http")}))
	must(c.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{GroupId: sg.GroupId, IpPermissions: []types.IpPermission{
		{IpProtocol: aws.String("tcp"), FromPort: aws.Int32(8080), ToPort: aws.Int32(8080), IpRanges: []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}}}}}))
	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		SecurityGroupIds: []string{*sg.GroupId}, IamInstanceProfile: &types.IamInstanceProfileSpecification{Name: aws.String("web")},
		UserData: aws.String(base64.StdEncoding.EncodeToString([]byte("#!/bin/sh\necho hello from user data > /tmp/hello\n" +
			"while :; do printf 'HTTP/1.0 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\nok' | nc -l -p 8080; done &\n")))}))
	cc := *must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		UserData: aws.String(base64.StdEncoding.EncodeToString([]byte("#cloud-config\npackages: [curl]\nruncmd:\n" +
			"  - echo from cloud-config > /tmp/cc\n  - [ sh, -c, 'echo \"a list\" > /tmp/cc2' ]\nfinal_message: done\n")))})).Instances[0].InstanceId
	id := *run.Instances[0].InstanceId
	t.Cleanup(func() { c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id, cc}}) })
	name := docker.Name("ec2", id)
	execIn := func(name, cmd string) string {
		out, code, err := hub.Docker().Exec(ctx, name, "sh", "-c", cmd)
		if err != nil || code != 0 {
			return ""
		}
		return strings.TrimSpace(string(out))
	}
	exec := func(cmd string) string { return execIn(name, cmd) }
	deadline := time.Now().Add(2 * time.Minute)
	for describe(t, c, id).State.Name != types.InstanceStateNameRunning || exec("cat /tmp/hello") == "" ||
		execIn(docker.Name("ec2", cc), "cat /tmp/cc /tmp/cc2") == "" {
		if time.Now().After(deadline) {
			t.Fatalf("instance %s: state %s, user data did not run", id, describe(t, c, id).State.Name)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := exec("cat /tmp/hello"); got != "hello from user data" {
		t.Fatalf("user data wrote %q", got)
	}
	if got := execIn(docker.Name("ec2", cc), "cat /tmp/cc /tmp/cc2"); got != "from cloud-config\na list" {
		t.Fatalf("cloud-config runcmd wrote %q", got)
	}

	// IMDSv2 from inside, with busybox: nc sends the token PUT, wget the GETs
	creds := exec(`E=$AWS_EC2_METADATA_SERVICE_ENDPOINT; HP=${E#http://}; HOST=${HP%%/*}; P=/${HP#*/}
T=$(printf 'PUT %slatest/api/token HTTP/1.0\r\nHost: %s\r\nX-aws-ec2-metadata-token-ttl-seconds: 60\r\n\r\n' "$P" "$HOST" | nc ${HOST%:*} ${HOST#*:} | tail -n 1)
R=$(wget -q -O - --header "X-aws-ec2-metadata-token: $T" "${E}latest/meta-data/iam/security-credentials/")
wget -q -O - --header "X-aws-ec2-metadata-token: $T" "${E}latest/meta-data/iam/security-credentials/$R"`)
	var cr struct{ AccessKeyId, SecretAccessKey, Token string }
	if err := json.Unmarshal([]byte(creds), &cr); err != nil || cr.AccessKeyId == "" {
		t.Fatalf("credentials from inside the instance: %q (%v)", creds, err)
	}
	who := must(stssdk.NewFromConfig(awstest.ConfigAs(t, url, cr.AccessKeyId, cr.SecretAccessKey, cr.Token)).GetCallerIdentity(ctx, &stssdk.GetCallerIdentityInput{}))
	if *who.Arn != "arn:aws:sts::000000000000:assumed-role/web/"+id {
		t.Fatalf("caller = %s", *who.Arn)
	}

	// the port its security group opens is published, and recorded for load balancers
	var addr string
	for addr == "" && time.Now().Before(deadline) {
		addr, _ = hub.Endpoint("instance:" + id + ":8080")
		time.Sleep(100 * time.Millisecond)
	}
	ip, _ := hub.Endpoint("ip:" + *run.Instances[0].PrivateIpAddress + ":8080")
	var page string
	for page != "ok" && time.Now().Before(deadline) {
		if resp, err := http.Get("http://" + addr + "/"); err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			page = strings.TrimSpace(string(b))
		}
		time.Sleep(200 * time.Millisecond)
	}
	if page != "ok" || ip != addr {
		t.Fatalf("instance:%s:8080 = %q answered %q; ip endpoint %q", id, addr, page, ip)
	}

	must(c.StopInstances(ctx, &ec2.StopInstancesInput{InstanceIds: []string{id}}))
	for describe(t, c, id).State.Name != types.InstanceStateNameStopped {
		if time.Now().After(deadline) {
			t.Fatal("instance did not stop")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, ok := hub.Endpoint("instance:" + id + ":8080"); ok {
		t.Fatal("a stopped instance keeps its endpoint")
	}
	must(c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}}))
	for describe(t, c, id).State.Name != types.InstanceStateNameTerminated {
		if time.Now().After(deadline) {
			t.Fatal("instance did not terminate")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if _, err := hub.Docker().Inspect(ctx, name); err == nil {
		t.Fatal("the container of a terminated instance is still there")
	}
}

// floci #1644 and #1178: an instance shows its network interface, with the security groups and the attach time.
func TestInstanceNetworkInterface(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := serve(t)
	sg := must(c.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{GroupName: aws.String("nic"), Description: aws.String("nic")}))
	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{MinCount: aws.Int32(1), MaxCount: aws.Int32(1), ImageId: aws.String(alpine),
		InstanceType: types.InstanceTypeT3Micro, SecurityGroupIds: []string{*sg.GroupId}}))
	if len(run.Instances[0].NetworkInterfaces) != 1 {
		t.Fatalf("run output = %+v", run.Instances[0].NetworkInterfaces)
	}
	i := describe(t, c, *run.Instances[0].InstanceId)
	if len(i.SecurityGroups) != 1 || *i.SecurityGroups[0].GroupId != *sg.GroupId || len(i.NetworkInterfaces) != 1 {
		t.Fatalf("instance = %+v", i)
	}
	n := i.NetworkInterfaces[0]
	if len(n.Groups) != 1 || *n.Groups[0].GroupId != *sg.GroupId || n.Attachment == nil || n.Attachment.AttachTime == nil || *n.PrivateIpAddress != *i.PrivateIpAddress ||
		!strings.HasPrefix(*n.NetworkInterfaceId, "eni-") || *n.NetworkInterfaceId != *describe(t, c, *i.InstanceId).NetworkInterfaces[0].NetworkInterfaceId {
		t.Fatalf("network interface = %+v", n)
	}
}

// waitRunning waits until the instance is running.
func waitRunning(t *testing.T, c *ec2.Client, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for describe(t, c, id).State.Name != types.InstanceStateNameRunning {
		if time.Now().After(deadline) {
			t.Fatalf("instance %s did not run", id)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestInstanceMetadataAddress reads the instance id from 169.254.169.254 inside
// an Alpine instance with IMDSv2 and with IMDSv1, and UserData reads it at boot.
func TestInstanceMetadataAddress(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	hub, _, c := serve(t)
	if !hub.Docker().Available() {
		t.Skip("no container engine")
	}
	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		UserData: aws.String(base64.StdEncoding.EncodeToString([]byte("#!/bin/sh\nwget -q -O /tmp/boot-id http://169.254.169.254/latest/meta-data/instance-id\n")))}))
	id := *run.Instances[0].InstanceId
	t.Cleanup(func() { c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}}) })
	waitRunning(t, c, id)
	exec := func(cmd string) string {
		out, _, _ := hub.Docker().Exec(ctx, docker.Name("ec2", id), "sh", "-c", cmd)
		return strings.TrimSpace(string(out))
	}
	// the token PUT by nc, the GETs by wget, as the SDKs do
	const v2 = `T=$(printf 'PUT /latest/api/token HTTP/1.0\r\nHost: 169.254.169.254\r\nX-aws-ec2-metadata-token-ttl-seconds: 60\r\n\r\n' | nc 169.254.169.254 80 | tail -n 1)
wget -q -O - --header "X-aws-ec2-metadata-token: $T" http://169.254.169.254/latest/meta-data/instance-id`
	deadline := time.Now().Add(time.Minute)
	for exec(v2) != id || exec("cat /tmp/boot-id") != id {
		if time.Now().After(deadline) {
			t.Fatalf("IMDSv2 at 169.254.169.254 gave %q, UserData read %q; want %s", exec(v2), exec("cat /tmp/boot-id"), id)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if got := exec("wget -q -O - http://169.254.169.254/latest/meta-data/instance-id"); got != id {
		t.Fatalf("IMDSv1 gave %q, want %s", got, id)
	}
}

// TestInstanceSSH installs sshd at boot in an Alpine instance with a created
// key pair (the image has none) and logs in with the private key.
func TestInstanceSSH(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	hub, _, c := serve(t)
	if !hub.Docker().Available() {
		t.Skip("no container engine")
	}
	key := must(c.CreateKeyPair(ctx, &ec2.CreateKeyPairInput{KeyName: aws.String("login")}))
	signer, err := ssh.ParsePrivateKey([]byte(*key.KeyMaterial))
	if err != nil {
		t.Fatal(err)
	}
	sg := must(c.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{GroupName: aws.String("ssh"), Description: aws.String("ssh")}))
	must(c.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{GroupId: sg.GroupId, IpPermissions: []types.IpPermission{
		{IpProtocol: aws.String("tcp"), FromPort: aws.Int32(22), ToPort: aws.Int32(22), IpRanges: []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}}}}}))
	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
		KeyName: aws.String("login"), SecurityGroupIds: []string{*sg.GroupId}}))
	id := *run.Instances[0].InstanceId
	t.Cleanup(func() { c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}}) })
	waitRunning(t, c, id)
	cfg := &ssh.ClientConfig{User: "root", Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)}, HostKeyCallback: ssh.InsecureIgnoreHostKey(), Timeout: 5 * time.Second}
	deadline := time.Now().Add(2 * time.Minute) // apk has to fetch openssh first
	for {
		addr, _ := hub.Endpoint("instance:" + id + ":22")
		client, err := ssh.Dial("tcp", addr, cfg)
		if err == nil {
			sess, err := client.NewSession()
			if err != nil {
				t.Fatal(err)
			}
			out, err := sess.Output("echo logged in")
			sess.Close()
			client.Close()
			if err != nil || strings.TrimSpace(string(out)) != "logged in" {
				t.Fatalf("ssh command: %q, %v", out, err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ssh to %s (%q): %v", id, addr, err)
		}
		time.Sleep(time.Second)
	}
	con := must(c.GetConsoleOutput(ctx, &ec2.GetConsoleOutputInput{InstanceId: aws.String(id)}))
	if b, _ := base64.StdEncoding.DecodeString(*con.Output); !strings.Contains(string(b), "pogi: sshd started") {
		t.Fatalf("console output = %q", b)
	}
}
