package generic

import (
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

type errKind int

const (
	eNotFound errKind = iota
	eExists
	eInvalid
)

var errPatterns = map[errKind][]string{
	eNotFound: {"notfound", "nosuch", "doesnotexist", "unknownresource", "nonexistent"},
	eExists:   {"alreadyexist", "conflict", "duplicate"},
	eInvalid:  {"validation", "invalidparametervalue", "invalidparameter", "invalidinput", "invalidrequest", "badrequest"},
}

// modelErr builds the model's own error for a kind of failure: the operation's
// declared errors first, then any in the model, preferring names that mention
// the resource type. Without a match it falls back to a generic code.
func (m *model) modelErr(op *operation, k errKind, typ, msg string) *core.Error {
	if k == eNotFound && m.meta.Name == "sagemaker" { // SageMaker has no not-found error: it is a ValidationException
		return core.Errorf(http.StatusBadRequest, "ValidationException", "%s", sagemakerNotFound(typ, msg))
	}
	var cands []*shape
	if op != nil {
		for _, e := range op.Errors {
			if e.sh != nil {
				cands = append(cands, e.sh)
			}
		}
	}
	nops := len(cands)
	cands = append(cands, m.errs...)
	tn := norm(typ)
	for pass := range 4 { // declared and about this type, declared, about this type, any
		for _, pat := range errPatterns[k] {
			for i, sh := range cands {
				n := norm(sh.Name)
				if !strings.Contains(n, pat) {
					continue
				}
				declared, typed := i < nops, tn != "" && strings.Contains(n, tn)
				if pass == 0 && declared && typed || pass == 1 && declared || pass == 2 && typed || pass == 3 {
					return m.shapeErr(sh, msg)
				}
			}
		}
	}
	switch k {
	case eNotFound:
		// a service without a not-found error of its own (DataSync) says InvalidRequestException
		for _, pat := range errPatterns[eInvalid] {
			for _, sh := range cands[:nops] {
				if strings.Contains(norm(sh.Name), pat) {
					return m.shapeErr(sh, msg)
				}
			}
		}
		return core.Errorf(http.StatusBadRequest, "ResourceNotFoundException", "%s", msg)
	case eExists:
		return core.Errorf(http.StatusConflict, "ConflictException", "%s", msg)
	}
	if m.meta.Protocol == "query" || m.meta.Protocol == "ec2" {
		return core.Errorf(http.StatusBadRequest, "ValidationError", "%s", msg)
	}
	return core.Errorf(http.StatusBadRequest, "ValidationException", "%s", msg)
}

func (m *model) shapeErr(sh *shape, msg string) *core.Error {
	code, status := sh.Name, http.StatusBadRequest
	if sh.Error != nil {
		if sh.Error.HTTPStatusCode != 0 {
			status = sh.Error.HTTPStatusCode
		}
		if sh.Error.Code != "" && (m.meta.Protocol == "query" || m.meta.Protocol == "ec2") {
			code = sh.Error.Code
		}
	}
	return core.Errorf(status, code, "%s", msg)
}

// sagemakerNotFound words the ValidationException that SageMaker gives for a missing resource the way
// Terraform looks for it: Could not find endpoint, Could not find endpoint configuration, RecordNotFound.
func sagemakerNotFound(typ, msg string) string {
	if typ == "NotebookInstance" {
		return "RecordNotFound"
	}
	if typ == "" || strings.HasPrefix(msg, "Could not find") {
		return msg
	}
	var parts []string
	for _, w := range words(typ) {
		parts = append(parts, strings.ToLower(w))
	}
	name := strings.Join(parts, " ")
	if name == "endpoint config" {
		name = "endpoint configuration"
	}
	return "Could not find " + name + "."
}
