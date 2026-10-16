package amp

import (
	"encoding/binary"
	"testing"
)

// A 5-byte body that claims to unpack to 4 GiB is refused before any decoding.
func TestWriteRefusesHugeDecodedSize(t *testing.T) {
	body := binary.AppendUvarint(nil, 0xFFFFFFFF)
	if _, err := New().Write("ws", body); err == nil {
		t.Fatal("a body declaring 4 GiB was accepted")
	}
	if _, err := New().Write("ws", EncodeWrite(&Series{Labels: map[string]string{"__name__": "up"}, Samples: []Sample{{T: 1, V: 1}}})); err != nil {
		t.Fatalf("a normal body: %v", err)
	}
}
