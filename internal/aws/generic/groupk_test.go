package generic_test

import (
	"errors"
	"testing"
)

// kCode fails the test unless err is an API error with the given code.
func kCode(t *testing.T, err error, code string) {
	t.Helper()
	var ae interface{ ErrorCode() string }
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}
