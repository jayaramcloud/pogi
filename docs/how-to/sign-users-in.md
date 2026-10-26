---
title: Sign users in
description: Create a user, sign in and get tokens with Cognito, Firebase Auth and Azure managed identity against pogi.
---

# Sign users in

pogi issues real sign-in tokens, so your login code can run against it with no changes. You'll sign a user in with Cognito, sign one up with Firebase Auth, and get an Azure managed identity token.

## Before you start

1. Start pogi in another terminal:

   ```bash
   pogi start
   ```

2. Load the settings that point your tools at pogi:

   ```bash
   eval "$(pogi env)"
   ```

You need the AWS CLI for the Cognito section and `curl` for the other two. No Docker needed.

## AWS: Cognito user pools

You'll make a user pool (the directory of users), an app client, and a user who signs in with a password.

1. Create the pool and the app client. The client allows password sign-in:

   ```bash
   POOL=$(aws cognito-idp create-user-pool --pool-name app --query UserPool.Id --output text)
   CLIENT=$(aws cognito-idp create-user-pool-client --user-pool-id $POOL --client-name web \
     --explicit-auth-flows ALLOW_USER_PASSWORD_AUTH ALLOW_REFRESH_TOKEN_AUTH \
     --query UserPoolClient.ClientId --output text)
   echo $POOL $CLIENT
   ```

   ```text title="Output"
   us-east-1_zLy18XeCk pdokh1rhgnlxzk2ycgkmp1rzbj
   ```

2. Sign up a user:

   ```bash
   aws cognito-idp sign-up --client-id $CLIENT --username alice@example.com --password 'Passw0rd!x'
   ```

   ```text title="Output"
   {
       "UserConfirmed": false,
       "UserSub": "aa1bb775-cc1b-4e80-8a2c-1a6705a90148"
   }
   ```

   `UserConfirmed` is false until the user is confirmed.

3. Confirm the user as an admin. In a real pool the user would enter a code from an email. The command prints nothing:

   ```bash
   aws cognito-idp admin-confirm-sign-up --user-pool-id $POOL --username alice@example.com
   ```

4. Sign in:

   ```bash
   aws cognito-idp initiate-auth --client-id $CLIENT --auth-flow USER_PASSWORD_AUTH \
     --auth-parameters USERNAME=alice@example.com,PASSWORD='Passw0rd!x' \
     --query 'AuthenticationResult.{type:TokenType,expiresIn:ExpiresIn,tokens:keys(@)}'
   ```

   ```text title="Output"
   {
       "type": "Bearer",
       "expiresIn": 3600,
       "tokens": [
           "AccessToken",
           "ExpiresIn",
           "TokenType",
           "RefreshToken",
           "IdToken"
       ]
   }
   ```

   You get an access token, an ID token and a refresh token. They are signed JWTs (JSON Web Tokens).

5. Check the user's status:

   ```bash
   aws cognito-idp list-users --user-pool-id $POOL --query 'Users[].[Username,UserStatus]' --output text
   ```

   ```text title="Output"
   alice@example.com	CONFIRMED
   ```

6. Fetch the pool's public keys, which your own code uses to check a token. This prints the key ID of each key:

   ```bash
   curl -s "http://localhost:7644/$POOL/.well-known/jwks.json" | grep -o '"kid":"[^"]*"'
   ```

   ```text title="Response"
   "kid":"JLXaVrHR4pRUtIQlKVCxUgW2pmn1U8bhK+i5APCte9I="
   ```


## Google Cloud: Firebase Auth

You'll sign up a user through the Identity Toolkit REST API, the same one the Firebase client SDKs call. The Firebase Admin SDK reads `FIREBASE_AUTH_EMULATOR_HOST`, which `pogi env` sets. The API key can be any text.

1. Sign up a user. `sed` cuts the long tokens:

   ```bash
   curl -s -X POST "http://$FIREBASE_AUTH_EMULATOR_HOST/identitytoolkit.googleapis.com/v1/accounts:signUp?key=any-key" \
     -H "content-type: application/json" \
     -d '{"email":"bob@example.com","password":"Passw0rd!x","returnSecureToken":true}' \
     | sed -E 's/"(idToken|refreshToken)": ?"[^"]*"/"\1":"…"/g'
   ```

   ```json title="Response"
   {"email":"bob@example.com","expiresIn":"3600","idToken":"…","kind":"identitytoolkit#SignupNewUserResponse","localId":"8ZrFPc7TEnHcj0M2L12CIng9v8tD","refreshToken":"…"}
   ```

2. Sign in with the same email and password:

   ```bash
   curl -s -X POST "http://$FIREBASE_AUTH_EMULATOR_HOST/identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=any-key" \
     -H "content-type: application/json" \
     -d '{"email":"bob@example.com","password":"Passw0rd!x","returnSecureToken":true}' \
     | sed -E 's/"(idToken|refreshToken)": ?"[^"]*"/"\1":"…"/g'
   ```

   ```json title="Response"
   {"email":"bob@example.com","expiresIn":"3600","idToken":"…","kind":"identitytoolkit#VerifyPasswordResponse","localId":"8ZrFPc7TEnHcj0M2L12CIng9v8tD","refreshToken":"…","registered":true}
   ```

## Azure: managed identity tokens

You'll ask pogi for a token the way code running on Azure does. `pogi env` sets `IDENTITY_ENDPOINT` and `IDENTITY_HEADER`, so the Azure SDKs' managed identity credential finds pogi on its own.

1. Request a token for Azure Resource Manager:

   ```bash
   curl -s -H "X-IDENTITY-HEADER: $IDENTITY_HEADER" \
     "$IDENTITY_ENDPOINT?api-version=2019-08-01&resource=https://management.azure.com/" \
     | sed -E 's/"access_token":"[^"]*"/"access_token":"…"/'
   ```

   ```json title="Response"
   {"access_token":"…","client_id":"766e4da8-5949-5329-896a-b7932ccf55c9","expires_in":"3600","expires_on":"1792957417","not_before":"1792953817","refresh_token":"","resource":"https://management.azure.com/","token_type":"Bearer"}
   ```

## Limits

- Cognito: signing in through an outside identity provider, passkeys and the hosted sign-up page are not supported yet. A user whose MFA (second factor) method was only switched on has no authenticator, so the code 123456 passes.
- Firebase Auth: tenants, MFA and email-link sign-in are not supported yet.
- Azure: pogi serves managed identity tokens, and Microsoft Entra ID tokens for client secrets, passwords and device codes. The Azure CLI signs in with a service principal, as in [Azure CLI](connect-your-tools.md#azure-cli). Its browser sign-in is not served.

See the [Cognito](../reference/coverage.md#aws), [Firebase Auth](../reference/coverage.md#google-cloud) and [managed identity](../reference/coverage.md#azure) rows in the coverage page.

## See also

- [Connect your tools](connect-your-tools.md)
- [Run functions and HTTP APIs](run-functions-and-http-apis.md)
- [Coverage](../reference/coverage.md)
