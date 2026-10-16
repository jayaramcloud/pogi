package cognito

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"time"
)

// Software token MFA is TOTP as RFC 6238 defines it: HMAC-SHA1, 30-second
// steps, 6 digits, which every authenticator app uses.

func newTOTPSecret() string {
	b := make([]byte, 20)
	rand.Read(b)
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b)
}

func totpAt(key []byte, step int64) string {
	m := hmac.New(sha1.New, key)
	binary.Write(m, binary.BigEndian, step)
	sum := m.Sum(nil)
	o := sum[len(sum)-1] & 0xf
	n := binary.BigEndian.Uint32(sum[o:o+4]) & 0x7fffffff
	return fmt.Sprintf("%06d", n%1000000)
}

// totpOK accepts the code of the current step and of the one before and after,
// like AWS, which tolerates a little clock drift.
func totpOK(secret, code string, now time.Time) bool {
	key, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(secret)
	if err != nil {
		return false
	}
	step := now.Unix() / 30
	for d := int64(-1); d <= 1; d++ {
		if hmac.Equal([]byte(totpAt(key, step+d)), []byte(code)) {
			return true
		}
	}
	return false
}
