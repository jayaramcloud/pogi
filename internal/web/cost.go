package web

// The Cost page: a rough monthly bill for what is running, from the resources the services hold
// and the traffic they have served (each service's request counter over the uptime, as a rate, times
// 30 days). AWS and Azure prices come from the embedded price list in internal/cost; the Google
// constants in cost_prices.go stand in until the Google price list is generated.
//
// ponytail: the request log's last 1,000 entries only split a service's traffic among its S3, SQS,
// DynamoDB and Lambda resources; a DynamoDB request counts as one request unit.

import (
	"cmp"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/cost"
)

const monthSeconds = 30 * 24 * 3600 // traffic is scaled to 30 days; always-on resources bill 730 h

// costCache keeps the sidebar badge and the Home card from walking every service on each page: an
// estimate is reused for 5 seconds while the request count has not moved. One per console.
var costCache sync.Map // *UI → *costEntry

type costEntry struct {
	sync.Mutex
	at    time.Time
	n     int
	total map[string]float64 // by cloud; "" is all of them
}

func init() {
	cached := func(ui *UI) map[string]float64 {
		v, _ := costCache.LoadOrStore(ui, &costEntry{})
		e := v.(*costEntry)
		e.Lock()
		defer e.Unlock()
		n := 0
		for _, c := range ui.hub.Requests.Totals() {
			n += c
		}
		if n != e.n || time.Since(e.at) > 5*time.Second {
			est := ui.estimate(ui.defaultRegion(), scope{Account: all, Region: all}, "")
			e.total, e.n, e.at = map[string]float64{"": est.Total}, n, time.Now()
			for _, c := range est.Clouds {
				e.total[c.Service] = c.Monthly
			}
		}
		return e.total
	}
	costTotal = func(ui *UI, r *http.Request) (float64, bool) { t := cached(ui)[ui.cloud(r)]; return t, t > 0 }
	badgeFuncs = append(badgeFuncs, func(ui *UI, r *http.Request, b map[string]badge) {
		if t := cached(ui)[ui.cloud(r)]; t > 0 {
			b["cost"] = badge{perMonth(t) + "/mo", ""}
		}
	})
}

// perMonth is a short monthly estimate for the badge and Home card: "<$1", "$4.2", "$29". The
// Cost page itself keeps cents.
func perMonth(v float64) string {
	switch {
	case v < 1:
		return "<$1"
	case v < 10:
		return fmt.Sprintf("$%.1f", v)
	}
	return fmt.Sprintf("$%.0f", v)
}

func (ui *UI) costRoutes(m *http.ServeMux) {
	ui.pages["cost"] = template.Must(template.Must(shell().Clone()).ParseFS(files, "templates/cost.html"))
	m.HandleFunc("GET /_pogi/cost", ui.costPage)
	ui.tfRoutes(m)
	m.HandleFunc("GET /_pogi/cost.json", ui.costAPI)
}

// costRegions is every AWS region the price list has prices for.
func costRegions() []string {
	db, _ := cost.Default()
	return db.Regions("aws")
}

// defaultRegion is the pogi region when it is in the price list, else us-east-1.
func (ui *UI) defaultRegion() string {
	if slices.Contains(costRegions(), ui.hub.Cfg.Region) {
		return ui.hub.Cfg.Region
	}
	return "us-east-1"
}

type costRow struct {
	Cloud, Service, Name, Detail, Href string
	Monthly                            float64
	Priced                             bool
	Pct                                int // bar width, of the largest row
}

type costBar struct {
	Service string
	Monthly float64
	Pct     int
}

type costEstimate struct {
	Rows   []costRow
	Bars   []costBar
	Clouds []costBar // per-cloud subtotals (Service is the cloud id), for all clouds only
	Total  float64
	More   int    // not-priced resources beyond the listing cap
	Basis  string // what the traffic figures come from
}

type costData struct {
	costEstimate
	Region  string
	Regions []string
	PerDay  float64
}

func (ui *UI) costPage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	region := q.Get("region")
	if region != "" && !slices.Contains(costRegions(), region) {
		http.Error(w, "unknown region "+region, http.StatusBadRequest)
		return
	}
	if region == "" {
		region = ui.defaultRegion()
	}
	d := costData{costEstimate: ui.estimate(region, ui.scope(r), ui.cloud(r)), Region: region}
	d.PerDay = d.Total / 30
	d.Regions = costRegions()
	ui.render(w, r, "cost", "Cost estimate", "cost", d)
}

// traffic is what the request log shows, per resource name.
type traffic struct {
	s3Tier1, s3Tier2, sqs, ddbW, ddbR, lambda, lambdaMs map[string]float64
}

var (
	reQueueURL = regexp.MustCompile(`"QueueUrl"\s*:\s*"([^"]+)"|QueueUrl=([^&\s]+)`)
	reTable    = regexp.MustCompile(`"TableName"\s*:\s*"([^"]+)"`)
)

// traffic reads the request log and returns it with how many log entries each service has, so a
// resource's share of its service's counted requests is its entries over that sample.
func (ui *UI) traffic() (t traffic, sample map[string]float64) {
	t = traffic{map[string]float64{}, map[string]float64{}, map[string]float64{}, map[string]float64{}, map[string]float64{}, map[string]float64{}, map[string]float64{}}
	sample = map[string]float64{}
	all := ui.hub.Requests.Query(0, 0, "", "", false)
	for _, e := range all {
		sample[e.Service]++
		switch e.Service {
		case "s3":
			bucket, _, _ := strings.Cut(strings.TrimPrefix(strings.SplitN(e.Path, "?", 2)[0], "/"), "/")
			switch {
			case bucket == "", strings.HasPrefix(e.Op, "Delete"):
			case slices.ContainsFunc([]string{"Put", "Copy", "Post", "List", "Create", "Upload", "Complete"}, func(p string) bool { return strings.HasPrefix(e.Op, p) }):
				t.s3Tier1[bucket]++
			default:
				t.s3Tier2[bucket]++
			}
		case "sqs":
			full, _ := ui.hub.Requests.Get(e.ID)
			name := ""
			if m := reQueueURL.FindStringSubmatch(full.ReqBody); m != nil {
				name = lastPart(strings.NewReplacer("%2F", "/").Replace(m[1] + m[2]))
			} else if p := strings.Split(strings.SplitN(e.Path, "?", 2)[0], "/"); len(p) > 2 {
				name = p[len(p)-1]
			}
			if name != "" {
				t.sqs[name]++
			}
		case "dynamodb":
			full, _ := ui.hub.Requests.Get(e.ID)
			if m := reTable.FindStringSubmatch(full.ReqBody); m != nil {
				if strings.HasPrefix(e.Op, "Put") || strings.HasPrefix(e.Op, "Update") || strings.HasPrefix(e.Op, "Delete") && e.Op != "DeleteTable" || strings.HasSuffix(e.Op, "WriteItem") || e.Op == "TransactWriteItems" {
					t.ddbW[m[1]]++
				} else if e.Op == "GetItem" || e.Op == "Query" || e.Op == "Scan" || strings.HasPrefix(e.Op, "BatchGet") || e.Op == "TransactGetItems" {
					t.ddbR[m[1]]++
				}
			}
		case "lambda":
			if e.Op == "Invoke" {
				if _, rest, ok := strings.Cut(e.Path, "/functions/"); ok {
					name, _, _ := strings.Cut(rest, "/")
					t.lambda[name]++
					t.lambdaMs[name] += max(e.Ms, 1)
				}
			}
		}
	}
	return t, sample
}

func lastPart(s string) string { return s[strings.LastIndex(s, "/")+1:] }

// estimate prices every resource the priced services hold, and lists the others as not priced.
func (ui *UI) estimate(region string, sc scope, cloud string) costEstimate {
	tr, sample := ui.traffic()
	status := ui.hub.Status()
	up := max(float64(status.Uptime), 60)
	rate, total := map[string]float64{}, 0 // requests a month by service, from its counter and the uptime
	for _, st := range status.Services {
		rate[st.Name] = float64(st.Requests) / up * monthSeconds
		if cloud == "" || st.Cloud == cloud {
			total += st.Requests
		}
	}
	scale := func(svc string) float64 { // the log's entries of a service to its counted requests a month
		if sample[svc] == 0 {
			return 0
		}
		return rate[svc] / sample[svc]
	}
	db, _ := cost.Default()
	rt := func(key string) float64 { // a unit price in the chosen region; 0 when the list has none
		p, _, _ := db.Rate(key, region)
		return p
	}
	var out costEstimate
	out.Basis = fmt.Sprintf("%s requests counted in %s of uptime, as a rate over 30 days", count(float64(total)), (time.Duration(up) * time.Second).Round(time.Second))
	add := func(cloud, svc, name, detail string, monthly float64) {
		out.Rows = append(out.Rows, costRow{Cloud: cloud, Service: svc, Name: name, Detail: detail, Monthly: monthly, Priced: true, Href: "/_pogi/resources/" + strings.ToLower(svc)})
	}
	inUse := map[string]bool{} // services that have seen traffic: built-in defaults stay out of the list
	for _, st := range status.Services {
		inUse[st.Name] = st.Requests > 0
	}
	for _, s := range ui.hub.Services() {
		b, ok := s.(core.Browsable)
		if !ok {
			continue
		}
		info := s.Info()
		if cloud != "" && info.Cloud != cloud {
			continue
		}
		rowScope := sc.forCloud(info.Cloud)
		if info.Cloud != core.AWS {
			rowScope.Location, rowScope.Group = all, all // only AWS is priced per region; the others are cut by project or subscription
		}
		for ti, t := range b.Resources() {
			col := func(row []string, name string) string {
				if i := slices.Index(t.Columns, name); i >= 0 && i < len(row) {
					return row[i]
				}
				return ""
			}
			for _, row := range t.Rows {
				if !rowScope.allows(t.Columns, row) { // costs are priced per region, so even a global list (S3) is cut to the region
					continue
				}
				name := row[0]
				switch {
				case info.Name == "s3" && t.Name == "Buckets":
					gb := parseSize(col(row, "Size")) / (1 << 30)
					t1, t2 := tr.s3Tier1[name]*scale("s3"), tr.s3Tier2[name]*scale("s3")
					add("aws", "S3", name, fmt.Sprintf("%s stored · %s requests", col(row, "Size"), count(t1+t2)),
						gb*rt("aws/s3/standard/storage")+(t1*rt("aws/s3/put")+t2*rt("aws/s3/get"))*perM)
				case info.Name == "sqs" && t.Name == "Queues":
					n, kind := tr.sqs[name]*scale("sqs"), "standard"
					if strings.HasSuffix(name, ".fifo") {
						kind = "fifo"
					}
					add("aws", "SQS", name, count(n)+" requests", n*rt("aws/sqs/"+kind+"/request")*perM)
				case info.Name == "dynamodb" && t.Name == "Tables":
					w, rd := tr.ddbW[name]*scale("dynamodb"), tr.ddbR[name]*scale("dynamodb")
					bytes, _ := strconv.ParseFloat(col(row, "Size"), 64)
					add("aws", "DynamoDB", name, fmt.Sprintf("%s writes, %s reads", count(w), count(rd)),
						(w*rt("aws/dynamodb/write-request")+rd*rt("aws/dynamodb/read-request"))*perM+bytes/(1<<30)*rt("aws/dynamodb/storage"))
				case info.Name == "lambda" && t.Name == "Functions":
					mb, _ := strconv.ParseFloat(strings.TrimSuffix(col(row, "Memory"), " MB"), 64)
					n, ms := tr.lambda[name]*scale("lambda"), tr.lambdaMs[name]*scale("lambda")
					avg := 0.0
					if n > 0 {
						avg = ms / n
					}
					add("aws", "Lambda", name, fmt.Sprintf("%s inv · %.0f MB · %.0f ms", count(n), mb, avg),
						n*rt("aws/lambda/request")*perM+mb/1024*ms/1000*rt("aws/lambda/gb-second"))
				case info.Name == "ec2" && t.Name == "Instances":
					if col(row, "State") != "running" {
						continue
					}
					typ := col(row, "Type")
					p, ok := db.Hourly("aws/ec2/linux", region, typ)
					if !ok {
						out.Rows = append(out.Rows, costRow{Cloud: "aws", Service: "EC2", Name: name, Detail: "no price for " + typ + " in " + region, Href: "/_pogi/resources/ec2"})
						continue
					}
					add("aws", "EC2", name, typ+" · 730 h", p*730)
				case info.Name == "rds" && t.Name == "DB instances":
					class, engine := col(row, "Class"), strings.ToLower(col(row, "Engine"))
					eng, _, _ := strings.Cut(engine, " ") // "postgres 17.4" → postgres; mysql and mariadb too, aurora-* and the rest are not priced
					var m struct {
						AllocatedStorage int
						StorageType      string
						MultiAZ          bool
					}
					if v, ok := s.(core.Inspectable); ok {
						b, _ := json.Marshal(v.Inspect(t.Name, name))
						_ = json.Unmarshal(b, &m)
					}
					variant, sfx := "single-az", ""
					if m.MultiAZ {
						variant, sfx = "multi-az", "/multi-az"
					}
					p, ok := db.Hourly("aws/rds/"+eng+"/"+variant, region, class)
					if !ok || (eng != "postgres" && eng != "mysql" && eng != "mariadb") {
						out.Rows = append(out.Rows, costRow{Cloud: "aws", Service: "RDS", Name: name, Detail: "no price for " + class + " " + engine + " in " + region, Href: "/_pogi/resources/rds"})
						continue
					}
					storage, storageCost := "", 0.0
					if m.AllocatedStorage > 0 {
						st := cmp.Or(m.StorageType, "gp2")
						storage = fmt.Sprintf(" + %d GB %s", m.AllocatedStorage, st)
						storageCost = float64(m.AllocatedStorage) * rt("aws/rds/"+eng+"/storage/"+st+sfx)
					}
					add("aws", "RDS", name, class+" · 730 h"+storage, p*730+storageCost)
				case staticPriced[info.Name+"/"+t.Name]:
					n := 0
					for _, t2 := range b.Resources() {
						if staticPriced[info.Name+"/"+t2.Name] {
							n += len(t2.Rows)
						}
					}
					in := staticIn{share: rate[info.Name] / float64(max(n, 1)), perMonth: monthSeconds / up, db: db, region: region, table: t.Name, col: func(c string) string { return col(row, c) }}
					if i, ok := s.(core.Inspectable); ok {
						in.inspect = i.Inspect
					}
					if detail, monthly, ok := staticPrice(info.Name, in); ok {
						out.Rows = append(out.Rows, costRow{Cloud: info.Cloud, Service: info.Title, Name: name, Detail: detail, Monthly: monthly, Priced: true, Href: "/_pogi/resources/" + info.Name})
					} else if ti == 0 && inUse[info.Name] {
						out.Rows = append(out.Rows, costRow{Cloud: info.Cloud, Service: info.Title, Name: name, Detail: strings.TrimSuffix(t.Name, "s") + ": " + detail, Href: "/_pogi/resources/" + info.Name})
					}
				default:
					if ti > 0 || !inUse[info.Name] {
						continue // ponytail: a service's first table is its main resource
					}
					out.Rows = append(out.Rows, costRow{Cloud: info.Cloud, Service: info.Title, Name: name, Detail: strings.TrimSuffix(t.Name, "s"), Href: "/_pogi/resources/" + info.Name})
				}
			}
		}
	}
	slices.SortStableFunc(out.Rows, func(a, b costRow) int {
		switch {
		case a.Priced != b.Priced:
			if a.Priced {
				return -1
			}
			return 1
		case a.Monthly > b.Monthly:
			return -1
		case a.Monthly < b.Monthly:
			return 1
		}
		return 0
	})
	bars, byCloud := map[string]float64{}, map[string]float64{}
	maxRow := 0.0
	for _, r := range out.Rows {
		if r.Priced {
			out.Total += r.Monthly
			byCloud[r.Cloud] += r.Monthly
			bars[r.Service] += r.Monthly
			maxRow = max(maxRow, r.Monthly)
		}
	}
	if cloud == "" {
		for _, c := range cloudLanes {
			out.Clouds = append(out.Clouds, costBar{Service: c, Monthly: byCloud[c]})
		}
	}
	const limit = 40
	if n := len(out.Rows); n > limit {
		out.Rows, out.More = out.Rows[:limit], n-limit
	}
	for i, r := range out.Rows {
		if r.Priced && maxRow > 0 {
			out.Rows[i].Pct = int(r.Monthly / maxRow * 100)
		}
	}
	for _, s := range core.SortedKeys(bars) {
		out.Bars = append(out.Bars, costBar{Service: s, Monthly: bars[s]})
	}
	slices.SortFunc(out.Bars, func(a, b costBar) int {
		if a.Monthly > b.Monthly {
			return -1
		} else if a.Monthly < b.Monthly {
			return 1
		}
		return 0
	})
	for i := range out.Bars {
		if out.Total > 0 {
			out.Bars[i].Pct = int(out.Bars[i].Monthly / out.Total * 100)
		}
	}
	return out
}

// parseSize reads "4.2 MB" (the buckets table's size) back to bytes.
func parseSize(s string) float64 {
	f := strings.Fields(s)
	if len(f) != 2 {
		return 0
	}
	n, _ := strconv.ParseFloat(f[0], 64)
	return n * map[string]float64{"B": 1, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30}[f[1]]
}

// count is 13400000 → "13.4M", 4100 → "4.1k".
func count(n float64) string {
	switch {
	case n >= 1e6:
		return fmt.Sprintf("%.1fM", n/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1fk", n/1e3)
	}
	return fmt.Sprintf("%.0f", n)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
