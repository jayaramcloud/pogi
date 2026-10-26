---
title: Store secrets and keys
description: Store and read secrets, and encrypt and decrypt data with keys, on AWS, Google Cloud and Azure.
---

# Store secrets and keys

You'll keep a secret in pogi and read it back, and encrypt a short text with a key and decrypt it again. Each section uses the tool for that cloud.

## Before you start

- Run `pogi start` in another terminal.
- Load the settings that point your tools at pogi. The lines after `eval` sign the Azure CLI (`az`) in to pogi, for the Key Vault section. [Azure CLI](connect-your-tools.md#azure-cli) explains them, and you can run them again:

  ```bash
  eval "$(pogi env)"
  export REQUESTS_CA_BUNDLE="$HOME/.pogi/certs/ca.pem"   # pogi's CA; `pogi cert` prints its path
  export AZURE_CONFIG_DIR="$HOME/.pogi/azure"            # keeps your own ~/.azure as it is
  az cloud show -n pogi > /dev/null 2>&1 || az cloud register -n pogi --endpoint-resource-manager https://localhost:7644/
  az cloud set -n pogi --only-show-errors
  az login --service-principal -u 00000000-0000-0000-0000-000000000001 -p secret \
    --tenant 00000000-0000-0000-0000-000000000000 --output none
  ```

- Install the tools for the sections you use: the AWS CLI, `gcloud` and the Azure CLI.

## AWS: Secrets Manager

1. Store a secret:

   ```bash
   aws secretsmanager create-secret --name db-password --secret-string hunter2 --query Name --output text
   ```

   ```text title="Output"
   db-password
   ```

2. Read it back:

   ```bash
   aws secretsmanager get-secret-value --secret-id db-password --query SecretString --output text
   ```

   ```text title="Output"
   hunter2
   ```

3. Store a new value, which makes a new version, and read the current one:

   ```bash
   aws secretsmanager put-secret-value --secret-id db-password --secret-string hunter3 --query VersionStages --output text
   aws secretsmanager get-secret-value --secret-id db-password --query SecretString --output text
   ```

   ```text title="Output"
   AWSCURRENT
   hunter3
   ```

## AWS: Parameter Store

Parameter Store is part of Systems Manager (SSM).

1. Store a plain parameter and read it:

   ```bash
   aws ssm put-parameter --name /demo/greeting --value hello --type String --query Version --output text
   aws ssm get-parameter --name /demo/greeting --query Parameter.Value --output text
   ```

   ```text title="Output"
   1
   hello
   ```

2. Store a `SecureString`, which is encrypted:

   ```bash
   aws ssm put-parameter --name /demo/token --value s3cret --type SecureString --query Version --output text
   ```

   ```text title="Output"
   1
   ```

3. Read it with and without `--with-decryption`:

   ```bash
   aws ssm get-parameter --name /demo/token --with-decryption --query Parameter.Value --output text
   aws ssm get-parameter --name /demo/token --query Parameter.Value --output text
   ```

   ```text title="Output"
   s3cret
   AQICAHnEIZHCd6bN3F6BShOO+bLVzqVAKaQ0jsA2eV8ln9YUw=
   ```

## AWS: KMS

1. Make a key:

   ```bash
   KEY=$(aws kms create-key --query KeyMetadata.KeyId --output text)
   echo $KEY
   ```

   ```text title="Output"
   68bb7c1e-ccda-4105-9790-c40700615388
   ```

2. Encrypt a text. The result is binary, so it goes to a file. The second command prints the file's size in bytes:

   ```bash
   aws kms encrypt --key-id $KEY --plaintext "hello kms" --cli-binary-format raw-in-base64-out --query CiphertextBlob --output text | base64 --decode > cipher.bin
   wc -c < cipher.bin | tr -d ' '
   ```

   ```text title="Output"
   78
   ```

3. Decrypt it:

   ```bash
   aws kms decrypt --ciphertext-blob fileb://cipher.bin --query Plaintext --output text | base64 --decode; echo
   ```

   ```text title="Output"
   hello kms
   ```

## Google Cloud: Secret Manager

1. Make a secret and add a version that holds the value:

   ```bash
   gcloud secrets create db-password --replication-policy=automatic
   printf hunter2 | gcloud secrets versions add db-password --data-file=-
   ```

   ```text title="Output"
   Created secret [db-password].
   Created version [1] of the secret [db-password].
   ```

2. Read the latest version:

   ```bash
   gcloud secrets versions access latest --secret=db-password; echo
   ```

   ```text title="Output"
   hunter2
   ```

## Google Cloud: Cloud KMS

1. Make a key ring (a group of keys) and a key. These print nothing:

   ```bash
   gcloud kms keyrings create demo-ring --location=global
   gcloud kms keys create demo-key --keyring=demo-ring --location=global --purpose=encryption
   ```

2. Encrypt a file. The last command prints the encrypted file's size in bytes:

   ```bash
   printf "hello kms" > plain.txt
   gcloud kms encrypt --key=demo-key --keyring=demo-ring --location=global --plaintext-file=plain.txt --ciphertext-file=cipher-gcp.bin
   wc -c < cipher-gcp.bin | tr -d ' '
   ```

   ```text title="Output"
   42
   ```

3. Decrypt it:

   ```bash
   gcloud kms decrypt --key=demo-key --keyring=demo-ring --location=global --ciphertext-file=cipher-gcp.bin --plaintext-file=-; echo
   ```

   ```text title="Output"
   hello kms
   ```

## Azure: Key Vault

You'll make a vault, store a secret and read it back, and make a key. `az` makes the vault through Azure Resource Manager (ARM, Azure's control API). It then reaches the vault itself over HTTPS, and `REQUESTS_CA_BUNDLE` from "Before you start" lets it trust pogi's own certificate authority (CA).

1. Make a resource group and a vault:

   ```bash
   az group create --name demo --location eastus --output none
   az keyvault create --resource-group demo --name vault1 --location eastus --query '{name:name,uri:properties.vaultUri}'
   ```

   ```json title="Output"
   {
     "name": "vault1",
     "uri": "https://vault1.vault.localhost:7644/"
   }
   ```

2. Store a secret:

   ```bash
   az keyvault secret set --vault-name vault1 --name db-password --value hunter2 --query '{name:name,value:value}'
   ```

   ```json title="Output"
   {
     "name": "db-password",
     "value": "hunter2"
   }
   ```

3. Read it back:

   ```bash
   az keyvault secret show --vault-name vault1 --name db-password --query value --output tsv
   ```

   ```text title="Output"
   hunter2
   ```

4. Make an RSA key, and show its key type:

   ```bash
   az keyvault key create --vault-name vault1 --name demo-key --kty RSA --size 2048 --query key.kty --output tsv
   ```

   ```text title="Output"
   RSA
   ```

Run `pogi cert` to see where the CA file is and how to trust it for your whole system.

## Limits

- Secrets Manager stores replication settings but does not replicate.
- Parameter Store has no parameter policies.
- KMS has no grants, imported key material or multi-region keys.
- Secret Manager stores rotation and notification settings but does not act on them.
- Cloud KMS has no raw encryption, import jobs, or ED25519 and post-quantum keys.
- Key Vault has no certificate issuers or key release.

The [coverage page](../reference/coverage.md) lists every row: [Secrets Manager, Parameter Store and KMS](../reference/coverage.md#aws), [Secret Manager and Cloud KMS](../reference/coverage.md#google-cloud) and [Key Vault](../reference/coverage.md#azure).

## See also

- [First run](../tutorials/first-run.md)
- [Connect your tools](connect-your-tools.md)
- [Use any service](use-any-service.md)
- [Coverage](../reference/coverage.md)
