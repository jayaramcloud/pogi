package blob

import (
	"encoding/json"

	"github.com/nrndev/pogi/internal/core"
)

// blobEvent is what Event Grid is told about one blob change.
type blobEvent struct {
	api, etag, contentType, blobType string
	size                             int
	deleted                          bool
}

func created(api string, b *Blob) *blobEvent {
	return &blobEvent{api: api, etag: b.ETag, contentType: b.Props["Content-Type"], blobType: b.Type, size: len(b.Data)}
}

// emit reports a committed change as Microsoft.Storage.BlobCreated or BlobDeleted to Event Grid's
// system topics. Call it without s.mu held. Nothing happens when Event Grid is off.
func (s *Service) emit(trace, account, container, name string, ev *blobEvent) {
	if s.hub == nil || ev == nil {
		return
	}
	typ := "Microsoft.Storage.BlobCreated"
	data := map[string]any{
		"api": ev.api, "clientRequestId": core.UUID(), "requestId": core.UUID(), "blobType": ev.blobType,
		// ponytail: the public Azure host; the emulator's own URL is not known here
		"url":                "https://" + account + ".blob.core.windows.net/" + container + "/" + name,
		"sequencer":          core.RandHex(8),
		"storageDiagnostics": map[string]any{"batchId": core.UUID()},
	}
	if ev.deleted {
		typ = "Microsoft.Storage.BlobDeleted"
	} else {
		data["eTag"], data["contentType"], data["contentLength"] = ev.etag, ev.contentType, ev.size
	}
	body, _ := json.Marshal(data)
	scope := "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup
	s.hub.Deliver(scope+"/providers/Microsoft.EventGrid/events", &core.Message{ // the error is Event Grid being off
		Source:     scope + "/providers/Microsoft.Storage/storageAccounts/" + account,
		DetailType: typ,
		Subject:    "/blobServices/default/containers/" + container + "/blobs/" + name,
		Body:       string(body),
		Trace:      trace,
		From:       scope + "/providers/Microsoft.Storage/storageAccounts/" + account,
	})
}
