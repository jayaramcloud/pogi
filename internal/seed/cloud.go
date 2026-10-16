package seed

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"

	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
)

// send serves a JSON request to a Google Cloud or Azure service in-process.
// A 409 (the resource exists) is not an error.
func (s *seeder) send(svc, method, path string, body any) (int, error) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, "http://localhost"+path, r)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer pogi") // Key Vault wants a token; the others ignore it
	status, resp := s.h.DoService(svc, req)
	if status >= 300 && status != http.StatusConflict {
		return status, fmt.Errorf("%s %s: %d %s", method, path, status, bytes.TrimSpace(resp))
	}
	return status, nil
}

// project is the Google Cloud project of an item: its project property, or pogi.
func project(p map[string]any) string { return cmp.Or(text(p["project"]), "pogi") }

func pubsubTopic(s *seeder, it config.Item) error {
	if err := allow(it.Props, "project"); err != nil {
		return err
	}
	_, err := s.send("pubsub", http.MethodPut, "/v1/projects/"+project(it.Props)+"/topics/"+it.Name, map[string]any{})
	return err
}

// subscription: topic, ackDeadline, pushEndpoint.
func subscription(s *seeder, it config.Item) error {
	p := it.Props
	if err := allow(p, "project", "topic", "ackDeadline", "pushEndpoint"); err != nil {
		return err
	}
	proj, topic := project(p), text(p["topic"])
	if !strings.HasPrefix(topic, "projects/") {
		topic = "projects/" + proj + "/topics/" + topic
	}
	body := map[string]any{"topic": topic}
	rename(body, p, "ackDeadline=ackDeadlineSeconds")
	if ep, ok := p["pushEndpoint"]; ok {
		body["pushConfig"] = map[string]any{"pushEndpoint": ep}
	}
	_, err := s.send("pubsub", http.MethodPut, "/v1/projects/"+proj+"/subscriptions/"+it.Name, body)
	return err
}

func gcsBucket(s *seeder, it config.Item) error {
	if err := allow(it.Props, "project"); err != nil {
		return err
	}
	_, err := s.send("gcs", http.MethodPost, "/storage/v1/b?project="+url.QueryEscape(project(it.Props)), map[string]any{"name": it.Name})
	return err
}

// gcpSecret: value, as the first version.
func gcpSecret(s *seeder, it config.Item) error {
	if err := allow(it.Props, "project", "value"); err != nil {
		return err
	}
	base := "/v1/projects/" + project(it.Props) + "/secrets"
	status, err := s.send("secretmanager", http.MethodPost, base+"?secretId="+url.QueryEscape(it.Name),
		map[string]any{"replication": map[string]any{"automatic": map[string]any{}}})
	if err != nil || status == http.StatusConflict {
		return err
	}
	_, err = s.send("secretmanager", http.MethodPost, base+"/"+it.Name+":addVersion", map[string]any{"payload": map[string]any{"data": []byte(text(it.Props["value"]))}})
	return err
}

// document: the properties are the fields of the document at the item's path
// ("users/alice"); project is taken out.
func document(s *seeder, it config.Item) error {
	fs, ok := s.h.Service("firestore").(interface {
		Inspect(table, id string) any
		SetDocument(project, database, path string, data map[string]any) error
	})
	if !ok {
		return errors.New("service firestore is not enabled")
	}
	proj, fields := project(it.Props), maps.Clone(it.Props)
	delete(fields, "project")
	if fs.Inspect("Documents", "projects/"+proj+"/databases/(default)/documents/"+it.Name) != nil {
		return nil
	}
	return fs.SetDocument(proj, "", it.Name, fields)
}

func container(s *seeder, it config.Item) error {
	if err := allow(it.Props); err != nil {
		return err
	}
	_, err := s.send("azblob", http.MethodPut, "/devstoreaccount1/"+it.Name+"?restype=container", nil)
	return err
}

func azureQueue(s *seeder, it config.Item) error {
	if err := allow(it.Props); err != nil {
		return err
	}
	_, err := s.send("azqueue", http.MethodPut, "/devstoreaccount1/"+it.Name, nil)
	return err
}

func azureTable(s *seeder, it config.Item) error {
	if err := allow(it.Props); err != nil {
		return err
	}
	_, err := s.send("aztable", http.MethodPost, "/devstoreaccount1/Tables", map[string]any{"TableName": it.Name})
	return err
}

// appConfigSvc is what seeding needs of the App Configuration service, which has no HTTP way to make a store.
type appConfigSvc interface {
	CreateStore(name string) error
	SetKeyValue(store, key, label string, value, contentType *string, tags map[string]string) error
}

func appConfig(s *seeder) (appConfigSvc, error) {
	svc, ok := s.h.Service("azappconfig").(appConfigSvc)
	if !ok {
		return nil, errors.New("service azappconfig is not enabled")
	}
	return svc, nil
}

// appConfigStore: an App Configuration store; its access keys are random
// (pogi env or the console show the connection string).
func appConfigStore(s *seeder, it config.Item) error {
	if err := allow(it.Props); err != nil {
		return err
	}
	svc, err := appConfig(s)
	if err != nil {
		return err
	}
	return svc.CreateStore(it.Name)
}

// appConfigKeyValue: value, label, contentType, tags and store (default "default",
// the store requests without a store name reach). A key-value that exists is left alone.
func appConfigKeyValue(s *seeder, it config.Item) error {
	if err := allow(it.Props, "value", "label", "contentType", "tags", "store"); err != nil {
		return err
	}
	svc, err := appConfig(s)
	if err != nil {
		return err
	}
	var value, ct *string
	if v, ok := it.Props["value"]; ok && v != nil {
		value = ptr(text(v))
	}
	if v := text(it.Props["contentType"]); v != "" {
		ct = &v
	}
	tags := map[string]string{}
	if m, ok := it.Props["tags"].(map[string]any); ok {
		for k, v := range m {
			tags[k] = text(v)
		}
	}
	return svc.SetKeyValue(cmp.Or(text(it.Props["store"]), "default"), it.Name, text(it.Props["label"]), value, ct, tags)
}

func ptr[T any](v T) *T { return &v }

// vaultSecret: value, in the default vault.
func vaultSecret(s *seeder, it config.Item) error {
	if err := allow(it.Props, "value"); err != nil {
		return err
	}
	path := "/secrets/" + it.Name + "?api-version=7.4"
	if status, _ := s.send("keyvault", http.MethodGet, path, nil); status == http.StatusOK {
		return nil // setting it again would add a version
	}
	_, err := s.send("keyvault", http.MethodPut, path, map[string]any{"value": text(it.Props["value"])})
	return err
}

// cosmosDB: containers [{name, partitionKey (default /id)}].
func cosmosDB(s *seeder, it config.Item) error {
	if err := allow(it.Props, "containers"); err != nil {
		return err
	}
	colls, err := nested(it.Props["containers"], "name", "partitionKey")
	if err != nil {
		return err
	}
	if _, err := s.send("cosmos", http.MethodPost, "/dbs", map[string]any{"id": it.Name}); err != nil {
		return err
	}
	for _, c := range colls {
		pk := "/" + strings.TrimPrefix(cmp.Or(text(c["partitionKey"]), "id"), "/")
		if _, err := s.send("cosmos", http.MethodPost, "/dbs/"+it.Name+"/colls", map[string]any{"id": c["name"],
			"partitionKey": map[string]any{"paths": []string{pk}, "kind": "Hash"}}); err != nil {
			return err
		}
	}
	return nil
}

// serviceBus: queues and topics of the "localhost" namespace; their properties are Service Bus's own
// (lockDuration: 30s, maxDeliveryCount, requiresSession, ...), and a topic lists subscriptions.
func serviceBus(kind string) func(*seeder, config.Item) error {
	return func(s *seeder, it config.Item) error {
		sb, ok := s.h.Service("servicebus").(interface {
			Seed(kind, ns, name string, props map[string]any) error
		})
		if !ok {
			return errors.New("service servicebus is not enabled")
		}
		return sb.Seed(kind, "localhost", it.Name, it.Props)
	}
}

// functionApp: runtime, path (a code folder, relative to this file), settings.
func functionApp(s *seeder, it config.Item) error {
	if err := allow(it.Props, "runtime", "path", "settings"); err != nil {
		return err
	}
	svc, ok := s.h.Service("azfunctions").(interface {
		CreateApp(name, runtime, path string, settings map[string]string) error
	})
	if !ok {
		return errors.New("service azfunctions is not enabled")
	}
	settings := map[string]string{}
	if m, ok := it.Props["settings"].(map[string]any); ok {
		for k, v := range m {
			settings[k] = text(v)
		}
	}
	path := text(it.Props["path"])
	if path != "" {
		path = rel(s.file, path)
	}
	if err := svc.CreateApp(it.Name, text(it.Props["runtime"]), path, settings); err != nil && core.AsError(err).Status != http.StatusConflict {
		return err
	}
	return nil
}
