package iam

import (
	"crypto/x509"
	"encoding/pem"
	"slices"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/nrndev/pogi/internal/core"
)

// SSHKey is a public key a user uploaded (for CodeCommit). The body is kept
// as uploaded; the fingerprint is the MD5 one ssh-keygen prints.
type SSHKey struct {
	UserName, SSHPublicKeyId, Fingerprint, SSHPublicKeyBody, Status string
	UploadDate                                                      core.Time
}

// sshMeta is the ListSSHPublicKeys shape.
type sshMeta struct {
	UserName, SSHPublicKeyId, Status string
	UploadDate                       core.Time
}

func (s *Service) sshKey(u *User, id string) (int, error) {
	if i := slices.IndexFunc(u.SSHKeys, func(k SSHKey) bool { return k.SSHPublicKeyId == id }); i >= 0 {
		return i, nil
	}
	return 0, core.Errorf(404, "NoSuchEntity", "The SSH Public Key with id %s cannot be found.", id)
}

func (s *Service) uploadSSHPublicKey(c *core.Ctx, in *struct{ UserName, SSHPublicKeyBody string }) (any, error) {
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(in.SSHPublicKeyBody))
	if err != nil {
		return nil, core.Errorf(400, "InvalidPublicKey", "Unsupported public key format. Use an ssh-rsa, ecdsa-sha2 or ssh-ed25519 key in OpenSSH format.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := get(s.acct(c.Account).Users, "user", in.UserName)
	if err != nil {
		return nil, err
	}
	fp := ssh.FingerprintLegacyMD5(pub)
	switch {
	case len(u.SSHKeys) >= 5:
		return nil, core.Errorf(409, "LimitExceeded", "Cannot exceed quota for SSHPublicKeysPerUser: 5")
	case slices.ContainsFunc(u.SSHKeys, func(k SSHKey) bool { return k.Fingerprint == fp }):
		return nil, core.Errorf(400, "DuplicateSSHPublicKey", "The SSH public key is already associated with the specified IAM user.")
	}
	k := SSHKey{SSHPublicKeyId: "APKA" + core.RandString(17, core.UpperNum), Fingerprint: fp,
		SSHPublicKeyBody: strings.TrimSpace(in.SSHPublicKeyBody), Status: "Active", UploadDate: now()}
	u.SSHKeys = append(u.SSHKeys, k)
	k.UserName = u.UserName
	return map[string]any{"SSHPublicKey": k}, nil
}

func (s *Service) getSSHPublicKey(c *core.Ctx, in *struct{ UserName, SSHPublicKeyId, Encoding string }) (any, error) {
	if in.Encoding != "SSH" && in.Encoding != "PEM" {
		return nil, core.Errorf(400, "ValidationError", "Encoding must be SSH or PEM.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := get(s.acct(c.Account).Users, "user", in.UserName)
	if err != nil {
		return nil, err
	}
	i, err := s.sshKey(u, in.SSHPublicKeyId)
	if err != nil {
		return nil, err
	}
	k := u.SSHKeys[i]
	k.UserName = u.UserName
	if in.Encoding == "PEM" {
		pub, _, _, _, _ := ssh.ParseAuthorizedKey([]byte(k.SSHPublicKeyBody))
		der, err := x509.MarshalPKIXPublicKey(pub.(ssh.CryptoPublicKey).CryptoPublicKey())
		if err != nil {
			return nil, core.Errorf(400, "InvalidPublicKey", "This key cannot be shown as PEM.")
		}
		k.SSHPublicKeyBody = string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	}
	return map[string]any{"SSHPublicKey": k}, nil
}

func (s *Service) listSSHPublicKeys(c *core.Ctx, in *struct {
	UserName, Marker string
	MaxItems         int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.userOrCaller(c, in.UserName)
	if err != nil {
		return nil, err
	}
	keys := make([]sshMeta, len(u.SSHKeys))
	for i, k := range u.SSHKeys {
		keys[i] = sshMeta{u.UserName, k.SSHPublicKeyId, k.Status, k.UploadDate}
	}
	return page("SSHPublicKeys", keys, in.Marker, in.MaxItems), nil
}

func (s *Service) changeSSHPublicKey(c *core.Ctx, in *struct{ UserName, SSHPublicKeyId, Status string }) (any, error) {
	if c.Op == "UpdateSSHPublicKey" && in.Status != "Active" && in.Status != "Inactive" {
		return nil, core.Errorf(400, "ValidationError", "Status must be Active or Inactive.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := get(s.acct(c.Account).Users, "user", in.UserName)
	if err != nil {
		return nil, err
	}
	i, err := s.sshKey(u, in.SSHPublicKeyId)
	if err != nil {
		return nil, err
	}
	if c.Op == "DeleteSSHPublicKey" {
		u.SSHKeys = without(u.SSHKeys, i)
	} else {
		u.SSHKeys[i].Status = in.Status
	}
	return nil, nil
}
