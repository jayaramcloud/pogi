package generic

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// IVS: a channel has the defaults real IVS gives (type STANDARD, latency LOW), an ingest endpoint and a playback
// URL, and a create call also returns its first stream key. Nothing can be streamed to the endpoints.
func init() {
	Derive("ivs", "Channel", func(r *Resource) map[string]any {
		id := r.ARN[strings.LastIndex(r.ARN, "/")+1:]
		out := map[string]any{
			"ingestEndpoint": id + ".global-contribute.live-video.net",
			"playbackUrl":    "https://" + id + "." + r.Region + ".playback.live-video.net/api/video/v1/" + r.Region + "." + r.Account + ".channel." + id + ".m3u8",
		}
		for k, v := range map[string]string{"type": "STANDARD", "latencyMode": "LOW"} {
			if str(r.Data[k]) == "" {
				out[k] = v
			}
		}
		return out
	})
	Override("ivs", "CreateChannel", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		arn := c.arnOf("stream-key/"+core.UUID(), nil)
		key := map[string]any{"channelArn": c.Res.ARN, "value": "sk_" + c.Region + "_" + randHex(24)}
		s2Store(c, "StreamKey", arn, "", arn, key, "arn")
		out["streamKey"] = map[string]any{"arn": arn, "channelArn": key["channelArn"], "value": key["value"]}
		return out, nil
	})
}
