package generic

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aimodel"
	"github.com/nrndev/pogi/internal/core"
)

// Helpers shared by the AI services (Bedrock, SageMaker, Comprehend and the like).

// aiGateway is the local model aiGateway of the call's hub (pogi.yaml `ai:`).
func aiGateway(c *Call) *aimodel.Gateway {
	if h := c.Hub(); h != nil {
		return aimodel.New(h.Cfg.AIEndpoint, h.Cfg.AIKey, h.Cfg.AIModels)
	}
	return aimodel.New("", "", nil)
}

func aiAmap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func aiNum(v any) float64 {
	f, _ := v.(float64)
	return f
}

// aiOptNum is a pointer to a number member, or nil when it is absent.
func aiOptNum(v any) *float64 {
	if f, ok := v.(float64); ok {
		return &f
	}
	return nil
}

func aiStrList(v any) []string {
	var out []string
	for _, x := range asList(v) {
		out = append(out, str(x))
	}
	return out
}

// aiBlobJSON decodes the blob member of a request (base64 in c.In) as a JSON object.
func aiBlobJSON(v any) (map[string]any, bool) {
	raw, err := base64.StdEncoding.DecodeString(str(v))
	if err != nil {
		return nil, false
	}
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil || m == nil {
		return nil, false
	}
	return m, true
}

// aiJsonBlob is the blob member (base64) for a JSON document.
func aiJsonBlob(v any) string {
	b, _ := json.Marshal(v)
	return base64.StdEncoding.EncodeToString(b)
}

// aiPieces splits a text into aiPieces that join back to it, for streaming.
func aiPieces(s string) []string {
	var out []string
	for _, p := range strings.SplitAfter(s, " ") {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// aiEventFrame is one message of the AWS aiEvent stream encoding: prelude, headers, payload, CRCs.
func aiEventFrame(kind string, payload []byte) []byte {
	var hdr []byte
	for _, h := range [][2]string{{":event-type", kind}, {":content-type", "application/json"}, {":message-type", "event"}} {
		hdr = append(hdr, byte(len(h[0])))
		hdr = append(hdr, h[0]...)
		hdr = append(hdr, 7)
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(h[1])))
		hdr = append(hdr, h[1]...)
	}
	b := binary.BigEndian.AppendUint32(nil, uint32(12+len(hdr)+len(payload)+4))
	b = binary.BigEndian.AppendUint32(b, uint32(len(hdr)))
	b = binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
	b = append(b, hdr...)
	b = append(b, payload...)
	return binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
}

// aiEvent is one named aiEvent of a stream; its payload is the JSON of Body.
type aiEvent struct {
	Kind string
	Body any
}

// aiWriteEvents sends events as an event-stream response.
func aiWriteEvents(w http.ResponseWriter, header map[string]string, evs []aiEvent) {
	for k, v := range header {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	w.WriteHeader(http.StatusOK)
	for _, e := range evs {
		b, _ := json.Marshal(e.Body)
		w.Write(aiEventFrame(e.Kind, b))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}

// aiSeed wraps a create operation: once the engine has stored the resource, set adds the members
// it would not make up itself (versions, status, derived ARNs) to its data, so that every
// later describe and list shows them. A check refuses the call before anything is stored.
func aiSeed(service, op string, set func(c *Call, r *Resource), check ...func(*Call) error) {
	Override(service, op, func(c *Call) (map[string]any, error) {
		for _, f := range check {
			if err := f(c); err != nil {
				return nil, err
			}
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		if r == nil { // an operation that is not a create (StartIngestionJob) found nothing to seed
			return nil, c.Err("ResourceNotFoundException", "%s: the resource was not found", c.Op)
		}
		if r.Data == nil {
			r.Data = map[string]any{}
		}
		set(c, r)
		c.Put(r)
		return c.Fill(r), nil
	})
}

// aiPatch wraps an operation that changes a resource: after the engine has applied it, set may adjust
// the stored data.
func aiPatch(service, op string, set func(c *Call, r *Resource)) {
	Override(service, op, func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		if r := c.Res; r != nil {
			if r.Data == nil {
				r.Data = map[string]any{}
			}
			set(c, r)
			c.Put(r)
		}
		return out, nil
	})
}

// aiNewResource stores a resource that an override makes besides the one the engine made.
func aiNewResource(c *Call, typ, name, arn string, data map[string]any) *Resource {
	id := randHex(12)
	now := time.Now().UTC()
	r := &Resource{Type: typ, Key: id, IDs: map[string]string{typ + "Id": id}, Name: name, ID: id, ARN: arn, Account: c.Account, Region: c.Svc.region(c.Region),
		Data: data, Created: now, Updated: now}
	c.Put(r)
	return r
}

// aiFoundationModelARN is the ARN of a Bedrock foundation model, or id itself when it is one.
func aiFoundationModelARN(c *Call, id string) string {
	if strings.HasPrefix(id, "arn:") {
		return id
	}
	return core.ARN("bedrock", c.Region, "", "foundation-model/"+id)
}

// aiS3Get reads an object through the hub's S3; ok is false when there is none (or no S3).
func aiS3Get(c *Call, bucket, key string) ([]byte, bool) {
	h := c.Hub()
	if h == nil || bucket == "" || key == "" {
		return nil, false
	}
	u := url.URL{Path: "/" + bucket + "/" + key}
	status, _, body := h.Do(c.Account, c.Region, "s3", http.MethodGet, u.EscapedPath(), nil, nil)
	return body, status == http.StatusOK
}

// aiS3Put writes an object through the hub's S3.
func aiS3Put(c *Call, bucket, key string, body []byte, contentType string) bool {
	h := c.Hub()
	if h == nil || bucket == "" {
		return false
	}
	u := url.URL{Path: "/" + bucket + "/" + key}
	status, _, _ := h.Do(c.Account, c.Region, "s3", http.MethodPut, u.EscapedPath(), http.Header{"Content-Type": {contentType}}, body)
	return status == http.StatusOK
}

// aiFind is the resource of a type that a request names in an input member, by id, name or ARN.
func aiFind(c *Call, typ, member string) *Resource {
	v := str(c.In[member])
	for _, r := range c.List(typ) {
		if r.ID == v || r.Name == v || r.ARN == v {
			return r
		}
	}
	return nil
}

// aiRekey gives a resource that the engine named from a loose pattern a tidier id.
func aiRekey(c *Call, r *Resource, id string) {
	t := c.m.types[norm(r.Type)]
	c.Svc.Delete(c.Account, c.Region, r.Type, r.Key)
	var vals []string
	for _, k := range t.ids {
		if r.IDs[k] == r.ID {
			r.IDs[k] = id
		}
		vals = append(vals, r.IDs[k])
	}
	r.ID, r.Key = id, strings.Join(vals, "/")
	r.ARN = c.arn(t, r.IDs, "")
}

// aiAlnum10 is an id like the ones Bedrock agents and knowledge bases have: ten upper-case letters and digits.
func aiAlnum10() string { return strings.ToUpper(randHex(10)) }
