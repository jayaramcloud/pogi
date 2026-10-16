# pogi's Lambda Runtime API client for Python handlers (python3.8+).
import importlib
import json
import os
import sys
import time
import traceback
import urllib.request

BASE = "http://%s/2018-06-01/runtime" % os.environ["AWS_LAMBDA_RUNTIME_API"]


def post(path, body, headers=None):
    req = urllib.request.Request(BASE + path, data=body, method="POST", headers=headers or {})
    urllib.request.urlopen(req).read()


def error_body(e, etype=None, msg=None):
    msg = msg or str(e)
    if isinstance(e, ModuleNotFoundError) and (e.name or "").split(".")[0] in ("boto3", "botocore"):
        msg += ("\n(pogi runs Python functions natively: run `pogi lambda-sdks` once to install the boto3 "
                "the Lambda runtime includes, or start pogi with --lambda docker)")
    return json.dumps({
        "errorMessage": msg,
        "errorType": etype or type(e).__name__,
        "stackTrace": traceback.format_tb(e.__traceback__),
    }).encode()


def fail_init(e, etype=None, msg=None):
    traceback.print_exc()
    post("/init/error", error_body(e, etype, msg), {"Lambda-Runtime-Function-Error-Type": etype or type(e).__name__})
    sys.exit(1)


class Context:
    def __init__(self, headers):
        self.aws_request_id = headers.get("Lambda-Runtime-Aws-Request-Id")
        self.invoked_function_arn = headers.get("Lambda-Runtime-Invoked-Function-Arn")
        self._deadline = int(headers.get("Lambda-Runtime-Deadline-Ms") or 0)
        self.function_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
        self.function_version = os.environ.get("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
        self.memory_limit_in_mb = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
        self.log_group_name = os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME")
        self.log_stream_name = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME")
        self.identity = None
        cc = headers.get("Lambda-Runtime-Client-Context")
        self.client_context = json.loads(cc) if cc else None

    def get_remaining_time_in_millis(self):
        return max(0, self._deadline - int(time.time() * 1000))


def main():
    root = os.environ["LAMBDA_TASK_ROOT"]
    sys.path.insert(0, root)
    module, _, name = os.environ["_HANDLER"].rpartition(".")
    try:
        mod = importlib.import_module(module.replace("/", "."))
        handler = getattr(mod, name, None)
        if handler is None:
            fail_init(AttributeError(name), "Runtime.HandlerNotFound", "Handler '%s' missing on module '%s'" % (name, module))
    except SyntaxError as e:
        fail_init(e, "Runtime.UserCodeSyntaxError", "Syntax error in module '%s': %s" % (module, e))
    except ImportError as e:
        fail_init(e, "Runtime.ImportModuleError", "Unable to import module '%s': %s" % (module, e))
    except Exception as e:  # noqa: BLE001 - any other error while loading keeps its own type
        fail_init(e)
    while True:
        with urllib.request.urlopen(BASE + "/invocation/next") as res:
            headers = res.headers
            event = json.loads(res.read() or b"null")
        os.environ["_X_AMZN_TRACE_ID"] = headers.get("Lambda-Runtime-Trace-Id", "")  # the SDK sends it as X-Amzn-Trace-Id
        ctx = Context(headers)
        try:
            result = handler(event, ctx)
            body = json.dumps(result).encode()
        except Exception as e:  # noqa: BLE001 - handler errors go back to the caller
            traceback.print_exc()
            post("/invocation/%s/error" % ctx.aws_request_id, error_body(e), {"Lambda-Runtime-Function-Error-Type": "Unhandled"})
            continue
        post("/invocation/%s/response" % ctx.aws_request_id, body)


main()
