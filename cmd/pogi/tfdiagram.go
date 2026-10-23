package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"maps"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/cost"
	tfmodel "github.com/nrndev/pogi/internal/terraform"
	"github.com/nrndev/pogi/internal/web"
)

// openURL opens a page in the browser; a test swaps it out.
var openURL = openBrowser

// openBrowser starts the system's opener and does not wait for it.
func openBrowser(url string) error {
	var c *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		c = exec.Command("open", url)
	case "windows":
		c = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		c = exec.Command("xdg-open", url)
	}
	return c.Start()
}

var htmlTag = regexp.MustCompile(`<[^>]*>`)

// tfDiagramCmd is `pogi tf diagram`: send a folder or plan to the running pogi and open its Terraform
// page, or with -o write the SVG with no pogi running. Without a terminal (term false, as in CI) it
// prints the page's address instead of opening a browser, as --no-open does.
func tfDiagramCmd(w io.Writer, term bool, args []string) error {
	fs := flag.NewFlagSet("tf diagram", flag.ContinueOnError)
	setUsage(fs, "Draw a Terraform folder or plan (with no path, the current folder) in pogi's console, or write it\nto an SVG file with -o, which needs no running pogi.",
		"pogi tf diagram [folder | plan.json] [flags]", "pogi tf diagram ./infra -o diagram.svg")
	var vars strList
	fs.Var(&vars, "var-file", "a .tfvars file (repeatable); to send it to pogi it must be inside the folder")
	noOpen := fs.Bool("no-open", false, "print the page's address instead of opening the browser")
	out := fs.String("o", "", "write the diagram to this .svg file instead")
	fs.StringVar(out, "output", "", "same as -o")
	group := fs.String("group", "module", "module or network")
	ep := endpointFlag(fs)
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	switch {
	case !slices.Contains([]string{"module", "network"}, *group):
		return usagef("unknown --group %q (use module or network)", *group)
	case len(pos) > 1:
		return usagef("one path at a time, got %d", len(pos))
	case *out != "" && !strings.HasSuffix(strings.ToLower(*out), ".svg"):
		return usagef("-o must name a .svg file, got %q", *out)
	}
	path := "."
	if len(pos) == 1 {
		path = pos[0]
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%s was not found", path)
	}
	if *out != "" {
		return tfDiagramFile(w, path, vars, *group == "network", *out)
	}
	if !info.IsDir() && len(vars) > 0 {
		return errors.New("--var-file goes with a folder, not a plan.json")
	}
	keys, err := tfInsideFolder(path, vars)
	if err != nil {
		return err
	}
	files, keys, _, err := tfmodel.ReadFiles(path, keys)
	if err != nil {
		return err
	}
	page, err := tfSend(*ep, files, keys, *group)
	if err != nil {
		return err
	}
	if *noOpen || !term {
		fmt.Fprintln(w, page)
		return nil
	}
	if err := openURL(page); err != nil {
		fmt.Fprintf(w, "Could not open the browser (%v). Open this address: %s\n", err, page)
		return nil
	}
	fmt.Fprintln(w, "Opened", page)
	return nil
}

// tfInsideFolder turns var files into paths relative to the folder, and fails for one outside it:
// pogi is sent only what is inside the folder.
func tfInsideFolder(folder string, vars []string) ([]string, error) {
	var keys []string
	for _, v := range vars {
		rel := v
		if filepath.IsAbs(v) {
			r, err := filepath.Rel(folder, v)
			if err != nil {
				r = ".."
			}
			rel = r
		}
		if rel = filepath.Clean(rel); rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("var file %q must be inside the folder %s, so pogi can read it (with -o any path works)", v, folder)
		}
		keys = append(keys, rel)
	}
	return keys, nil
}

// tfDiagramFile draws the diagram without a server: read, price, draw.
func tfDiagramFile(w io.Writer, path string, vars []string, network bool, out string) error {
	cfg, err := tfmodel.Load(path, vars)
	if err != nil {
		return err
	}
	db, err := cost.Default()
	if err != nil {
		return err
	}
	est, err := cost.Price(cfg, cost.Options{DB: db})
	if err != nil {
		return err
	}
	if err := os.WriteFile(out, []byte(web.TerraformSVG(cfg, est, network)), 0o644); err != nil {
		return err
	}
	fmt.Fprintln(w, "Wrote", out)
	return nil
}

// tfSend posts the files the way the Terraform page does, and returns the page's address.
func tfSend(endpoint string, files map[string][]byte, vars []string, group string) (string, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for _, p := range slices.Sorted(maps.Keys(files)) {
		fw, err := mw.CreateFormFile(p, p)
		if err != nil {
			return "", err
		}
		fw.Write(files[p])
	}
	for _, v := range vars {
		mw.WriteField("vars", v)
	}
	mw.WriteField("layout", group)
	mw.WriteField("view", "diagram")
	if err := mw.Close(); err != nil {
		return "", err
	}
	base := strings.TrimRight(endpoint, "/")
	req, err := http.NewRequest("POST", base+"/_pogi/terraform", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	res, err := (&http.Client{Timeout: 2 * time.Minute}).Do(req)
	if err != nil {
		if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
			return "", fmt.Errorf("pogi at %s took too long to read the files; try a smaller folder", endpoint)
		}
		return "", fmt.Errorf("pogi isn't running at %s; start it with `pogi start`, or write a file with -o", endpoint)
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		msg := strings.TrimSpace(html.UnescapeString(htmlTag.ReplaceAllString(string(b), "")))
		return "", fmt.Errorf("pogi said no (%s): %s", res.Status, msg)
	}
	return base + "/_pogi/terraform", nil
}
