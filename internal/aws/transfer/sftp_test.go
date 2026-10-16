package transfer

import "testing"

// A client-chosen offset must not panic or ask for huge memory.
func TestWriteAtRejectsBadOffsets(t *testing.T) {
	w := &writer{max: 100}
	for _, off := range []int64{-1, 101, 1 << 40, 1<<63 - 1} {
		if _, err := w.WriteAt([]byte("x"), off); err == nil {
			t.Errorf("offset %d was accepted", off)
		}
	}
	if _, err := w.WriteAt(make([]byte, 20), 90); err == nil {
		t.Error("a write past the limit was accepted")
	}
	if n, err := w.WriteAt([]byte("abc"), 10); err != nil || n != 3 || len(w.buf) != 13 {
		t.Errorf("a normal write: %d %v %d", n, err, len(w.buf))
	}
}
