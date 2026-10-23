---
title: "IAM: record, then enforce"
description: Why pogi records what your code calls before it enforces anything, and what soft and strict mode are for.
---

# IAM: record, then enforce

## What is the problem?

IAM (the rules for who may call what) controls access in a cloud. Least privilege means code gets the permissions it needs and no more. It is easy to say and hard to do. A tight policy needs every action your code calls and every resource it touches. Most teams do not know that list. So they start with a wide policy such as `AdministratorAccess` and mean to narrow it later. Or they write a narrow one, deploy, and find the missing permission when production fails.

Both are slow and risky, because the feedback comes late, from the real cloud.

## Why record first?

The calls your code makes are the best list of what it needs. pogi sees every call, so it can write that list for you. In `off` or `soft` mode every call is allowed, and pogi writes down the action and resource of each call that comes from an IAM user's access key or an assumed role. A policy built from that list has exactly the actions your code used, narrowed to the resources it touched.

Recording changes nothing about how your code runs. That is why pogi does it by default, even with IAM off. Run your whole test suite once and get a policy, instead of guessing.

## Why enforce second?

A recorded policy is only a claim until something checks it. Strict mode is the check. With the policy attached and strict mode on, a call the policy does not allow fails with `AccessDenied` and the same message AWS gives. If your code still passes its tests, the policy is enough. If it fails, the failure names the action you forgot, on your machine, in seconds.

## What is soft mode for?

Soft mode is the middle step. Every call is allowed, but pogi flags the calls that strict mode would deny, in the IAM page and in Activity. Use it while you build up the policy. Your tests keep passing, and you see the gaps. Switch to strict when the list of "would deny" calls is empty.

| Mode | Calls | Use it to |
|---|---|---|
| `off` | All allowed, still recorded. | Work without thinking about IAM. |
| `soft` | All allowed, would-deny flagged. | Find what your code needs. |
| `strict` | Denied when no policy allows. | Prove the policy is enough. |

## Who is checked?

pogi checks callers that have an identity worth checking.

| Caller | Result |
|---|---|
| Root and the default `test` credentials | Always allowed, never recorded. Your first steps never hit a wall. |
| An IAM user's access key, or an assumed role's credentials | Checked. |
| A Lambda function | Acts as its execution role, so a missing grant shows up locally, not in production. |
| Google Cloud | Checks access tokens of service accounts you created. |
| Azure | Checks tokens of service principals and managed identities you created. |

## What does pogi not check?

IAM in pogi is not a full copy of IAM in the cloud. pogi enforces identity policies, permissions boundaries, service control policies (in an organization), S3 bucket policies and the `Deny` statements of API Gateway resource policies. It does not yet check other resource-based policies, such as SQS, SNS and KMS key policies. A policy that allows a call only under a condition can read as "would deny" after an attach, because the check has no condition keys.

A pass in pogi is strong evidence, not proof. The real cloud can still deny a call.

## See also

- [Least privilege tutorial](../tutorials/least-privilege.md)
- [Switch IAM modes](../how-to/switch-iam-modes.md)
- [REST API](../reference/rest-api.md)
- [Console](../reference/console.md)
