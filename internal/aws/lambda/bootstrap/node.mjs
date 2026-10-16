// pogi's Lambda Runtime API client for Node.js handlers (node 18+): async,
// callback and CommonJS/ES module handlers.
import fs from "node:fs";
import https from "node:https";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

// CloudFormation custom-resource handlers such as the CDK provider framework
// PUT their result with https.request and no port (443); send those requests
// for pogi's host to pogi's port, which also speaks TLS.
{
  const ep = new URL(process.env.AWS_ENDPOINT_URL ?? "http://localhost:7644");
  const request = https.request;
  https.request = function (opts, ...rest) {
    if (opts && typeof opts === "object" && !(opts instanceof URL) && !opts.port && (opts.hostname ?? opts.host) === ep.hostname) {
      opts = { ...opts, port: ep.port };
    }
    return request.call(this, opts, ...rest);
  };
  syncBuiltinESMExports();
}

const base = `http://${process.env.AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime`;

async function post(p, body, headers = {}) {
  await fetch(base + p, { method: "POST", body, headers });
}

function errorBody(e) {
  let msg = e?.message ?? String(e);
  if (/Cannot find (module|package) '@aws-sdk\//.test(msg)) {
    msg += "\n(pogi runs Node.js functions natively: run `pogi lambda-sdks` once to install the AWS SDK the Lambda runtime includes, or start pogi with --lambda docker)";
  }
  return JSON.stringify({
    errorType: e?.name ?? "Error",
    errorMessage: msg,
    stack: String(e?.stack ?? "").split("\n"),
  });
}

async function load() {
  const h = process.env._HANDLER;
  const dot = h.lastIndexOf(".");
  const mod = h.slice(0, dot), name = h.slice(dot + 1);
  const root = process.env.LAMBDA_TASK_ROOT;
  for (const ext of [".mjs", ".js", ".cjs"]) {
    const file = path.join(root, mod + ext);
    if (fs.existsSync(file)) {
      const m = await import(pathToFileURL(file).href);
      const fn = m[name] ?? m.default?.[name];
      if (typeof fn !== "function") throw new Error(`${h} is undefined or not exported`);
      return fn;
    }
  }
  throw new Error(`Cannot find module '${mod}'`);
}

function call(handler, event, context) {
  if (handler.length < 3) return handler(event, context);
  return new Promise((resolve, reject) => {
    const r = handler(event, context, (err, res) => (err ? reject(err) : resolve(res)));
    if (r && typeof r.then === "function") r.then(resolve, reject);
  });
}

let handler;
try {
  handler = await load();
} catch (e) {
  console.error(e);
  await post("/init/error", errorBody(e), { "Lambda-Runtime-Function-Error-Type": "Runtime.ImportModuleError" });
  process.exit(1);
}

for (;;) {
  let res;
  try {
    res = await fetch(`${base}/invocation/next`);
  } catch {
    continue; // long poll timed out; ask again
  }
  const id = res.headers.get("lambda-runtime-aws-request-id");
  process.env._X_AMZN_TRACE_ID = res.headers.get("lambda-runtime-trace-id") ?? ""; // the SDK sends it as X-Amzn-Trace-Id
  const deadline = Number(res.headers.get("lambda-runtime-deadline-ms"));
  const text = await res.text();
  const context = {
    awsRequestId: id,
    invokedFunctionArn: res.headers.get("lambda-runtime-invoked-function-arn"),
    functionName: process.env.AWS_LAMBDA_FUNCTION_NAME,
    functionVersion: process.env.AWS_LAMBDA_FUNCTION_VERSION,
    memoryLimitInMB: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE,
    logGroupName: process.env.AWS_LAMBDA_LOG_GROUP_NAME,
    logStreamName: process.env.AWS_LAMBDA_LOG_STREAM_NAME,
    callbackWaitsForEmptyEventLoop: true,
    getRemainingTimeInMillis: () => Math.max(0, deadline - Date.now()),
  };
  try {
    const result = await call(handler, text ? JSON.parse(text) : null, context);
    await post(`/invocation/${id}/response`, JSON.stringify(result ?? null));
  } catch (e) {
    console.error(e);
    await post(`/invocation/${id}/error`, errorBody(e), { "Lambda-Runtime-Function-Error-Type": "Unhandled" });
  }
}
