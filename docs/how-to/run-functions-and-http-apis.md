---
title: Run functions and HTTP APIs
description: Deploy a function on pogi and call it, on AWS Lambda, API Gateway, Cloud Functions and Azure Functions.
---

# Run functions and HTTP APIs

pogi runs your function code for real, and you call it the way you would call the cloud. You'll deploy one function on each cloud and get an answer back.

## Before you start

Start pogi in another terminal:

```bash
pogi start
```

Load the settings that point your tools at pogi:

```bash
eval "$(pogi env)"
```

Each section needs the following:

| Section | You need |
|---|---|
| AWS: Lambda and API Gateway | The AWS CLI, `zip`, `curl`, and `python3` on your machine (or Docker, which runs the function in a container). |
| Google Cloud: Cloud Functions | `gcloud`, `curl`, and Docker (or the Functions Framework, see the section). |
| Azure: Functions | `curl`, `zip`, `jq` and Docker (or Azure Functions Core Tools). |

## AWS: Lambda

You'll make a Python function, call it, and read the result. Lambda runs Python on your machine when `python3` is installed, and in Docker when it is not.

1. Write the handler and zip it:

   ```bash
   mkdir -p fn && cd fn
   cat > app.py <<'PY'
   def handler(event, context):
       return {"hello": event.get("name", "world")}
   PY
   zip -q ../fn.zip app.py && cd ..
   ```

2. Create the function. The role can be any ARN (the ID AWS gives a resource):

   ```bash
   aws lambda create-function --function-name hello --runtime python3.12 \
     --handler app.handler --role arn:aws:iam::000000000000:role/lambda \
     --zip-file fileb://fn.zip --query '[FunctionName,State]' --output text
   ```

   ```text title="Output"
   hello	Active
   ```

3. Call it:

   ```bash
   aws lambda invoke --function-name hello --payload '{"name":"pogi"}' \
     --cli-binary-format raw-in-base64-out out.json
   cat out.json; echo
   ```

   ```text title="Output"
   {
       "StatusCode": 200,
       "ExecutedVersion": "$LATEST"
   }
   {"hello": "pogi"}
   ```

## AWS: API Gateway in front of Lambda

You'll put an HTTP API in front of a function and call it with `curl`.

1. Make a function that answers as a web handler:

   ```bash
   mkdir -p web && cd web
   cat > app.py <<'PY'
   import json

   def handler(event, context):
       body = {"path": event["rawPath"], "method": event["requestContext"]["http"]["method"]}
       return {"statusCode": 200, "headers": {"content-type": "application/json"}, "body": json.dumps(body)}
   PY
   zip -q ../web.zip app.py && cd ..
   FN=$(aws lambda create-function --function-name web --runtime python3.12 \
     --handler app.handler --role arn:aws:iam::000000000000:role/lambda \
     --zip-file fileb://web.zip --query FunctionArn --output text)
   echo "$FN"
   ```

   ```text title="Output"
   arn:aws:lambda:us-east-1:000000000000:function:web
   ```

2. Create the HTTP API. Giving it a `--target` makes one default route that sends every request to the function (a Lambda proxy integration):

   ```bash
   API=$(aws apigatewayv2 create-api --name demo --protocol-type HTTP --target "$FN" \
     --query ApiId --output text)
   echo "$API"
   ```

   ```text title="Output"
   ewfdgikwm6
   ```

3. Call it. pogi answers at `<api id>.execute-api.localhost`:

   ```bash
   curl -s "http://$API.execute-api.localhost:7644/hello?x=1"; echo
   ```

   ```json title="Response"
   {"path": "/hello", "method": "GET"}
   ```

4. Clean up:

   ```bash
   aws apigatewayv2 delete-api --api-id "$API"
   aws lambda delete-function --function-name hello
   aws lambda delete-function --function-name web
   ```

## Google Cloud: Cloud Functions

You'll deploy a Python HTTP function with `gcloud` and call it with `curl`. pogi runs it with the Functions Framework (Google's library that serves a function over HTTP). If the framework is not installed on your machine, pogi runs the function in a `python` Docker container. The first deploy pulls that image and installs the framework, so it takes longer than the next ones.

1. Write the function and its requirements file:

   ```bash
   mkdir -p cf && cd cf
   cat > main.py <<'PY'
   import functions_framework

   @functions_framework.http
   def hello(request):
       return "Hello, " + request.args.get("name", "world") + "!\n"
   PY
   echo functions-framework > requirements.txt
   ```

2. Deploy it. `--allow-unauthenticated` lets anyone call it:

   ```bash
   gcloud functions deploy hello --gen2 --runtime python312 --region us-central1 \
     --source . --entry-point hello --trigger-http --allow-unauthenticated \
     --format "value(state,url)"
   cd ..
   ```

   ```text title="Output"
   WARNING: Service run.googleapis.com is not enabled. This operation may not succeed.
   WARNING: Service cloudbuild.googleapis.com is not enabled. This operation may not succeed.
   WARNING: Service artifactregistry.googleapis.com is not enabled. This operation may not succeed.
   Preparing function...
   ..................done.
   Deploying function...
   Done.
   You can view your function in the Cloud Console here: https://console.cloud.google.com/functions/details/us-central1/hello?project=pogi

   ACTIVE	http://hello.cloudfunctions.localhost:7644/
   ```

3. Call it. The function answers at `<name>.cloudfunctions.localhost`:

   ```bash
   curl -s "http://hello.cloudfunctions.localhost:7644/?name=pogi"
   ```

   ```text title="Output"
   Hello, pogi!
   ```

4. Delete the function. This also stops its container:

   ```bash
   gcloud functions delete hello --gen2 --region us-central1 --quiet
   ```

   ```text title="Output"
   Preparing function...
   done.
   Deleting function...
   Done.
   Deleted [projects/pogi/locations/us-central1/functions/hello].
   ```

## Azure: Functions

You'll create a function app through Azure Resource Manager (ARM, the API behind the Azure portal), deploy a zip, and call it with `curl`. pogi runs the real Azure Functions host: Azure Functions Core Tools (`func`) if it is installed, or the official host image in Docker. The first call pulls that image, which is large. The steps use `curl`, because `az functionapp create` asks Azure for its list of regions and language stacks, and pogi doesn't serve those.

1. Create a resource group and a function app. `FUNCTIONS_WORKER_RUNTIME` picks the language:

   ```bash
   ARM="http://localhost:7644/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/demo"
   curl -s -X PUT "$ARM?api-version=2021-04-01" -H "authorization: Bearer x" \
     -H "content-type: application/json" -d '{"location":"eastus"}' > /dev/null
   curl -s -X PUT "$ARM/providers/Microsoft.Web/sites/hello?api-version=2023-01-01" \
     -H "authorization: Bearer x" -H "content-type: application/json" \
     -d '{"location":"eastus","kind":"functionapp","properties":{"siteConfig":{"appSettings":[{"name":"FUNCTIONS_WORKER_RUNTIME","value":"node"}]}}}' \
     | jq -r '.name, .properties.state'
   ```

   ```text title="Output"
   hello
   Running
   ```

2. Write a function and zip it:

   ```bash
   mkdir -p az/hello && cd az
   echo '{"version":"2.0","extensionBundle":{"id":"Microsoft.Azure.Functions.ExtensionBundle","version":"[4.*, 5.0.0)"}}' > host.json
   echo '{"bindings":[{"authLevel":"anonymous","type":"httpTrigger","direction":"in","name":"req","methods":["get"]},{"type":"http","direction":"out","name":"res"}]}' > hello/function.json
   cat > hello/index.js <<'JS'
   module.exports = async function (context, req) {
     context.res = { body: "Hello, " + (req.query.name || "world") + "!\n" };
   };
   JS
   zip -qr ../az.zip . && cd ..
   ```

3. Deploy the zip. pogi accepts the Kudu zip deploy (the call `az functionapp deployment` makes) on its main port:

   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" -X POST --data-binary @az.zip \
     "http://localhost:7644/_azure/functions/hello/api/zipdeploy"
   ```

   ```text title="Output"
   200
   ```

4. Call the function. The first call starts the host, so it can take a while:

   ```bash
   curl -s "http://hello.azurewebsites.localhost:7644/api/hello?name=pogi"
   ```

   ```text title="Output"
   Hello, pogi!
   ```

5. Delete the app. This stops its container. The response code is 200:

   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" -X DELETE -H "authorization: Bearer x" \
     "$ARM/providers/Microsoft.Web/sites/hello?api-version=2023-01-01"
   ```

   ```text title="Output"
   200
   ```

## Limits

- **Lambda:** Java, .NET and Ruby functions run only in Docker. Code signing and SnapStart are not supported. A native function runs on your machine's own architecture, whatever `Architectures` says. A failed asynchronous invoke retries after 1 and 2 seconds, where AWS waits 1 and 2 minutes, so tests stay fast.
- **API Gateway:** WebSocket invoke does not work. Custom domains answer over HTTP only. A route that needs `AWS_IAM` wants a signed request, but pogi does not check the signature or the caller's permissions.
- **Cloud Functions:** only Python runs, and a function can't come from a repository.
- **Azure Functions:** zip deploy works. Other Kudu calls do not, and `requirements.txt` is not installed in containers.

The [coverage page](../reference/coverage.md#aws) lists every limit.

## See also

- [Connect your tools](connect-your-tools.md)
- [Run events and workflows](run-events-and-workflows.md)
- [Read logs and metrics](read-logs-and-metrics.md)
- [Coverage](../reference/coverage.md)
- [Troubleshoot](troubleshoot.md)
