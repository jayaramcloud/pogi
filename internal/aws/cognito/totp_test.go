package cognito

import "testing"

// RFC 6238 appendix B: the SHA-1 test key at T=59 gives 94287082, so 287082 with 6 digits.
func TestTOTPVector(t *testing.T) {
	if got := totpAt([]byte("12345678901234567890"), 59/30); got != "287082" {
		t.Fatalf("totpAt = %s, want 287082", got)
	}
}
