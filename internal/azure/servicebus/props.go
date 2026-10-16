package servicebus

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Dur is a duration that reads and writes ISO 8601 ("PT1M", as the ATOM API
// has it) and also reads Go's ("30s") and a bare number of seconds. Zero
// means "not set": no expiry, no idle deletion.
type Dur time.Duration

// maxTimeSpan is .NET's TimeSpan.MaxValue, how Service Bus spells "forever".
const maxTimeSpan = "P10675199DT2H48M5.4775807S"

var isoDur = regexp.MustCompile(`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

// ParseDur parses an ISO 8601 or Go duration; the "forever" TimeSpan is zero.
func ParseDur(s string) (Dur, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == maxTimeSpan {
		return 0, nil
	}
	if m := isoDur.FindStringSubmatch(strings.ToUpper(s)); m != nil && len(s) > 1 {
		n := func(x string) float64 { f, _ := strconv.ParseFloat(x, 64); return f }
		sec := n(m[1])*86400 + n(m[2])*3600 + n(m[3])*60 + n(m[4])
		if sec >= float64(100*365*86400) {
			return 0, nil
		}
		return Dur(sec * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", s)
	}
	return Dur(d), nil
}

// ISO renders the duration as ISO 8601; zero is the "forever" TimeSpan.
func (d Dur) ISO() string {
	if d <= 0 {
		return maxTimeSpan
	}
	t := time.Duration(d)
	days := int64(t / (24 * time.Hour))
	t -= time.Duration(days) * 24 * time.Hour
	var b strings.Builder
	b.WriteByte('P')
	if days > 0 {
		fmt.Fprintf(&b, "%dD", days)
	}
	if t > 0 || days == 0 {
		b.WriteByte('T')
		if h := int64(t / time.Hour); h > 0 {
			fmt.Fprintf(&b, "%dH", h)
			t -= time.Duration(h) * time.Hour
		}
		if m := int64(t / time.Minute); m > 0 {
			fmt.Fprintf(&b, "%dM", m)
			t -= time.Duration(m) * time.Minute
		}
		if t > 0 || b.Len() == 2 {
			b.WriteString(strings.TrimRight(strings.TrimRight(strconv.FormatFloat(t.Seconds(), 'f', 7, 64), "0"), ".") + "S")
		}
	}
	return b.String()
}

func (d Dur) MarshalJSON() ([]byte, error) {
	if d == 0 {
		return []byte(`""`), nil
	}
	return json.Marshal(d.ISO())
}

func (d *Dur) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case float64:
		*d = Dur(x * float64(time.Second))
	case string:
		p, err := ParseDur(x)
		if err != nil {
			return err
		}
		*d = p
	case nil:
		*d = 0
	default:
		return fmt.Errorf("invalid duration %v", v)
	}
	return nil
}

// Props are the settings of a queue, topic or subscription; each kind uses
// the ones that apply to it. Zero values mean "service default".
type Props struct {
	LockDuration                      Dur    `json:"lockDuration,omitempty"`
	MaxSizeInMegabytes                int32  `json:"maxSizeInMegabytes,omitempty"` // stored only
	RequiresDuplicateDetection        bool   `json:"requiresDuplicateDetection,omitempty"`
	RequiresSession                   bool   `json:"requiresSession,omitempty"`
	DefaultMessageTimeToLive          Dur    `json:"defaultMessageTimeToLive,omitempty"`
	DeadLetteringOnMessageExpiration  bool   `json:"deadLetteringOnMessageExpiration,omitempty"`
	DeadLetteringOnFilterEvalFailures bool   `json:"deadLetteringOnFilterEvaluationExceptions,omitempty"`
	DuplicateDetectionHistoryWindow   Dur    `json:"duplicateDetectionHistoryTimeWindow,omitempty"`
	MaxDeliveryCount                  int32  `json:"maxDeliveryCount,omitempty"`
	EnableBatchedOperations           bool   `json:"enableBatchedOperations,omitempty"`
	Status                            string `json:"status,omitempty"`           // Active, Disabled, SendDisabled, ReceiveDisabled
	AutoDeleteOnIdle                  Dur    `json:"autoDeleteOnIdle,omitempty"` // stored only
	EnablePartitioning                bool   `json:"enablePartitioning,omitempty"`
	ForwardTo                         string `json:"forwardTo,omitempty"`
	ForwardDeadLetteredMessagesTo     string `json:"forwardDeadLetteredMessagesTo,omitempty"`
	UserMetadata                      string `json:"userMetadata,omitempty"`
	SupportOrdering                   bool   `json:"supportOrdering,omitempty"`
	MaxMessageSizeInKilobytes         int64  `json:"maxMessageSizeInKilobytes,omitempty"`
}

// defaults fills what Service Bus fills when a client leaves it out.
func (p *Props) defaults(kind string) {
	if p.LockDuration == 0 && kind != "topic" {
		p.LockDuration = Dur(time.Minute)
	}
	if p.MaxDeliveryCount == 0 && kind != "topic" {
		p.MaxDeliveryCount = 10
	}
	if p.MaxSizeInMegabytes == 0 && kind != "subscription" {
		p.MaxSizeInMegabytes = 1024
	}
	if p.DuplicateDetectionHistoryWindow == 0 && kind != "subscription" {
		p.DuplicateDetectionHistoryWindow = Dur(10 * time.Minute)
	}
	if p.Status == "" {
		p.Status = "Active"
	}
	if p.MaxMessageSizeInKilobytes == 0 && kind != "subscription" {
		p.MaxMessageSizeInKilobytes = 256
	}
}
