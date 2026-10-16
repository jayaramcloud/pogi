package generic_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/transfer"
	"github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// sshKey makes a key pair: the signer to log in with and the line to import.
func sshKey(t *testing.T) (ssh.Signer, string) {
	t.Helper()
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return signer, string(ssh.MarshalAuthorizedKey(signer.PublicKey()))
}

func TestTransferSFTP(t *testing.T) {
	srv := pogi.Start(t)
	cfg := awstest.Config(t, srv.URL)
	ctx := context.Background()
	c := transfer.NewFromConfig(cfg)
	sc := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := sc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("sftp-bucket")}); err != nil {
		t.Fatal(err)
	}
	object := func(key string) (string, error) {
		o, err := sc.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("sftp-bucket"), Key: aws.String(key)})
		if err != nil {
			return "", err
		}
		b, _ := io.ReadAll(o.Body)
		return string(b), nil
	}

	// control plane
	sv, err := c.CreateServer(ctx, &transfer.CreateServerInput{Protocols: []types.Protocol{types.ProtocolSftp}, IdentityProviderType: types.IdentityProviderTypeServiceManaged,
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(sv.ServerId)
	if !strings.HasPrefix(id, "s-") {
		t.Fatalf("server id %q", id)
	}
	ds, err := c.DescribeServer(ctx, &transfer.DescribeServerInput{ServerId: sv.ServerId})
	if err != nil || ds.Server.State != types.StateOnline || !strings.HasPrefix(aws.ToString(ds.Server.HostKeyFingerprint), "SHA256:") || len(ds.Server.Tags) != 1 || ds.Server.UserCount == nil {
		t.Fatalf("describe server: %v %+v", err, ds)
	}
	aliceKey, alicePub := sshKey(t)
	u, err := c.CreateUser(ctx, &transfer.CreateUserInput{ServerId: sv.ServerId, UserName: aws.String("alice"), Role: aws.String("arn:aws:iam::000000000000:role/sftp"),
		HomeDirectory: aws.String("/sftp-bucket/home/alice"), SshPublicKeyBody: aws.String(alicePub)})
	if err != nil || aws.ToString(u.UserName) != "alice" {
		t.Fatalf("create user: %v %+v", err, u)
	}
	if _, err := c.CreateUser(ctx, &transfer.CreateUserInput{ServerId: sv.ServerId, UserName: aws.String("bad"), Role: aws.String("arn:aws:iam::000000000000:role/r"), SshPublicKeyBody: aws.String("not a key")}); err == nil {
		t.Fatal("a bad key was accepted")
	}
	du, err := c.DescribeUser(ctx, &transfer.DescribeUserInput{ServerId: sv.ServerId, UserName: aws.String("alice")})
	if err != nil || len(du.User.SshPublicKeys) != 1 || du.User.HomeDirectoryType != types.HomeDirectoryTypePath || aws.ToString(du.User.HomeDirectory) != "/sftp-bucket/home/alice" ||
		!strings.Contains(aws.ToString(du.User.Arn), ":user/"+id+"/alice") {
		t.Fatalf("describe user: %v %+v", err, du)
	}

	addr, ok := srv.Hub.Endpoint("sftp:transfer")
	if !ok {
		t.Fatal("no SFTP endpoint")
	}
	fingerprint := aws.ToString(ds.Server.HostKeyFingerprint)
	login := func(user string, signer ssh.Signer) (*sftp.Client, error) {
		conn, err := ssh.Dial("tcp", addr, &ssh.ClientConfig{User: user, Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
			HostKeyCallback: func(_ string, _ net.Addr, k ssh.PublicKey) error {
				if ssh.FingerprintSHA256(k) != fingerprint {
					return errors.New("host key is not the server's")
				}
				return nil
			}})
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { conn.Close() })
		return sftp.NewClient(conn)
	}

	// a user key logs in; a stranger's key does not
	otherKey, _ := sshKey(t)
	if _, err := login("alice", otherKey); err == nil {
		t.Fatal("logged in with a key that was not imported")
	}
	if _, err := login("nobody", aliceKey); err == nil {
		t.Fatal("logged in as a user that does not exist")
	}
	sc1, err := login("alice", aliceKey)
	if err != nil {
		t.Fatal(err)
	}
	defer sc1.Close()

	// upload, then see it in S3
	f, err := sc1.Create("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello over sftp")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := object("home/alice/hello.txt"); err != nil || got != "hello over sftp" {
		t.Fatalf("object in S3: %q %v", got, err)
	}
	// a large file arrives in pieces
	big := strings.Repeat("0123456789abcdef", 100_000)
	bf, _ := sc1.Create("data/big.bin")
	if _, err := io.Copy(bf, strings.NewReader(big)); err != nil {
		t.Fatal(err)
	}
	bf.Close()
	if got, _ := object("home/alice/data/big.bin"); got != big {
		t.Fatalf("big object: %d bytes", len(got))
	}
	// a file put with the S3 API is there to list and download
	if _, err := sc.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("sftp-bucket"), Key: aws.String("home/alice/from-s3.txt"), Body: strings.NewReader("from s3")}); err != nil {
		t.Fatal(err)
	}
	if _, err := sc.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("sftp-bucket"), Key: aws.String("home/bob/private.txt"), Body: strings.NewReader("not alice's")}); err != nil {
		t.Fatal(err)
	}
	ents, err := sc1.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
		if e.Name() == "hello.txt" && (e.Size() != 15 || e.IsDir()) || e.Name() == "data" && !e.IsDir() {
			t.Fatalf("entry %s: size %d dir %v", e.Name(), e.Size(), e.IsDir())
		}
	}
	if !slices.Equal(names, []string{"data", "from-s3.txt", "hello.txt"}) {
		t.Fatalf("listing: %v", names)
	}
	if ents, err = sc1.ReadDir("data"); err != nil || len(ents) != 1 || ents[0].Name() != "big.bin" || ents[0].Size() != int64(len(big)) {
		t.Fatalf("listing data: %v %v", ents, err)
	}
	rf, err := sc1.Open("hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rf)
	rf.Close()
	if string(got) != "hello over sftp" {
		t.Fatalf("download: %q", got)
	}
	rf, _ = sc1.Open("data/big.bin")
	if got, _ := io.ReadAll(rf); string(got) != big {
		t.Fatalf("big download: %d bytes", len(got))
	}
	rf.Close()
	// the absolute path works too, and a missing file is not found
	if fi, err := sc1.Stat("/sftp-bucket/home/alice/hello.txt"); err != nil || fi.Size() != 15 {
		t.Fatalf("stat by absolute path: %v %v", fi, err)
	}
	if _, err := sc1.Stat("nothing.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat of a missing file: %v", err)
	}
	if _, err := sc1.Open("nothing.txt"); err == nil {
		t.Fatal("opened a missing file")
	}
	// mkdir, rename, remove
	if err := sc1.Mkdir("empty"); err != nil {
		t.Fatal(err)
	}
	if fi, err := sc1.Stat("empty"); err != nil || !fi.IsDir() {
		t.Fatalf("stat of a new directory: %v %v", fi, err)
	}
	if err := sc1.Rename("hello.txt", "data/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if got, err := object("home/alice/data/renamed.txt"); err != nil || got != "hello over sftp" {
		t.Fatalf("renamed object: %q %v", got, err)
	}
	if _, err := object("home/alice/hello.txt"); err == nil {
		t.Fatal("the old name is still there")
	}
	if err := sc1.Rename("data", "archive"); err != nil {
		t.Fatal(err)
	}
	if got, err := object("home/alice/archive/renamed.txt"); err != nil || got != "hello over sftp" {
		t.Fatalf("renamed folder: %q %v", got, err)
	}
	if err := sc1.Remove("archive/renamed.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := object("home/alice/archive/renamed.txt"); err == nil {
		t.Fatal("the removed file is still in S3")
	}
	if err := sc1.Remove("archive/renamed.txt"); err == nil {
		t.Fatal("removed a file twice")
	}
	if err := sc1.RemoveDirectory("empty"); err != nil {
		t.Fatal(err)
	}

	// a logical home directory shows other places as folders
	bobKey, bobPub := sshKey(t)
	if _, err := c.CreateUser(ctx, &transfer.CreateUserInput{ServerId: sv.ServerId, UserName: aws.String("bob"), Role: aws.String("arn:aws:iam::000000000000:role/r"), HomeDirectoryType: types.HomeDirectoryTypeLogical,
		HomeDirectoryMappings: []types.HomeDirectoryMapEntry{{Entry: aws.String("/inbox"), Target: aws.String("/sftp-bucket/home/bob")}, {Entry: aws.String("/shared"), Target: aws.String("/sftp-bucket/home/alice/archive")}},
		SshPublicKeyBody:      aws.String(bobPub)}); err != nil {
		t.Fatal(err)
	}
	sc2, err := login("bob", bobKey)
	if err != nil {
		t.Fatal(err)
	}
	defer sc2.Close()
	ents, err = sc2.ReadDir("/")
	names = nil
	for _, e := range ents {
		names = append(names, e.Name())
	}
	if err != nil || !slices.Equal(names, []string{"inbox", "shared"}) {
		t.Fatalf("logical root: %v %v", names, err)
	}
	if ents, err = sc2.ReadDir("/inbox"); err != nil || len(ents) != 1 || ents[0].Name() != "private.txt" {
		t.Fatalf("logical inbox: %v %v", ents, err)
	}
	pf, _ := sc2.Create("/inbox/new.txt")
	pf.Write([]byte("bob's"))
	pf.Close()
	if got, err := object("home/bob/new.txt"); err != nil || got != "bob's" {
		t.Fatalf("object of a logical path: %q %v", got, err)
	}
	if _, err := sc2.Stat("/home/alice"); err == nil {
		t.Fatal("bob saw outside his mappings")
	}

	// keys come and go; a stopped server refuses everyone
	key2, pub2 := sshKey(t)
	imp, err := c.ImportSshPublicKey(ctx, &transfer.ImportSshPublicKeyInput{ServerId: sv.ServerId, UserName: aws.String("alice"), SshPublicKeyBody: aws.String(pub2)})
	if err != nil || !strings.HasPrefix(aws.ToString(imp.SshPublicKeyId), "key-") {
		t.Fatalf("import key: %v %+v", err, imp)
	}
	if _, err := c.ImportSshPublicKey(ctx, &transfer.ImportSshPublicKeyInput{ServerId: sv.ServerId, UserName: aws.String("alice"), SshPublicKeyBody: aws.String(pub2)}); err == nil {
		t.Fatal("imported a key twice")
	}
	if cl, err := login("alice", key2); err != nil {
		t.Fatalf("login with the second key: %v", err)
	} else {
		cl.Close()
	}
	if _, err := c.DeleteSshPublicKey(ctx, &transfer.DeleteSshPublicKeyInput{ServerId: sv.ServerId, UserName: aws.String("alice"), SshPublicKeyId: imp.SshPublicKeyId}); err != nil {
		t.Fatal(err)
	}
	if _, err := login("alice", key2); err == nil {
		t.Fatal("logged in with a deleted key")
	}
	lu, err := c.ListUsers(ctx, &transfer.ListUsersInput{ServerId: sv.ServerId})
	if err != nil || len(lu.Users) != 2 {
		t.Fatalf("list users: %v %+v", err, lu)
	}
	if _, err := c.StopServer(ctx, &transfer.StopServerInput{ServerId: sv.ServerId}); err != nil {
		t.Fatal(err)
	}
	if ds, _ = c.DescribeServer(ctx, &transfer.DescribeServerInput{ServerId: sv.ServerId}); ds.Server.State != types.StateOffline || aws.ToInt32(ds.Server.UserCount) != 2 {
		t.Fatalf("stopped server: %+v", ds.Server)
	}
	if _, err := login("alice", aliceKey); err == nil {
		t.Fatal("logged in to a stopped server")
	}
	if _, err := c.StartServer(ctx, &transfer.StartServerInput{ServerId: sv.ServerId}); err != nil {
		t.Fatal(err)
	}
	if cl, err := login("alice", aliceKey); err != nil {
		t.Fatalf("login after start: %v", err)
	} else {
		cl.Close()
	}

	// tags, lists, deletes, not-found
	arn := ds.Server.Arn
	if _, err := c.TagResource(ctx, &transfer.TagResourceInput{Arn: arn, Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &transfer.UntagResourceInput{Arn: arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tg, err := c.ListTagsForResource(ctx, &transfer.ListTagsForResourceInput{Arn: arn}); err != nil || len(tg.Tags) != 1 || aws.ToString(tg.Tags[0].Key) != "a" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if ls, err := c.ListServers(ctx, &transfer.ListServersInput{}); err != nil || len(ls.Servers) != 1 || aws.ToInt32(ls.Servers[0].UserCount) != 2 {
		t.Fatalf("list servers: %v %+v", err, ls)
	}
	if _, err := c.UpdateUser(ctx, &transfer.UpdateUserInput{ServerId: sv.ServerId, UserName: aws.String("alice"), HomeDirectory: aws.String("/sftp-bucket/home/alice2")}); err != nil {
		t.Fatal(err)
	}
	if du, _ = c.DescribeUser(ctx, &transfer.DescribeUserInput{ServerId: sv.ServerId, UserName: aws.String("alice")}); aws.ToString(du.User.HomeDirectory) != "/sftp-bucket/home/alice2" || len(du.User.SshPublicKeys) != 1 {
		t.Fatalf("updated user: %+v", du.User)
	}
	if _, err := c.DeleteUser(ctx, &transfer.DeleteUserInput{ServerId: sv.ServerId, UserName: aws.String("alice")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeUser(ctx, &transfer.DescribeUserInput{ServerId: sv.ServerId, UserName: aws.String("alice")}); !errors.As(err, &nf) {
		t.Fatalf("user not found: %T %v", err, err)
	}
	if _, err := login("alice", aliceKey); err == nil {
		t.Fatal("logged in as a deleted user")
	}
	if _, err := c.DeleteServer(ctx, &transfer.DeleteServerInput{ServerId: sv.ServerId}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeServer(ctx, &transfer.DescribeServerInput{ServerId: sv.ServerId}); !errors.As(err, &nf) {
		t.Fatalf("server not found: %T %v", err, err)
	}
	if _, err := c.DescribeUser(ctx, &transfer.DescribeUserInput{ServerId: sv.ServerId, UserName: aws.String("bob")}); !errors.As(err, &nf) {
		t.Fatalf("a deleted server's user: %T %v", err, err)
	}
}
