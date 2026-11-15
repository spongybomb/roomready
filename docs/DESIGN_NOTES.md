# ROOMREADY - design notes and simplifications

This document explains how the course-demo implementation relates to two sources, and lists
honest limitations:

* the **source proposal** (ROOMREADY.pdf, Group 1), whose later sections add tooling and services
  that contradict its own five-service core; and
* the **final Architecture & Design report** (Tianyi Zhang and Haifeng Wang), which already
  resolves those contradictions: five runtime services, one DynamoDB table, IAM/SigV4 caller
  identity on API Gateway, and mock providers. The final report does not require Cognito,
  LocalStack, or multiple physical tables.

## Consistency with the source proposal and the final report

| Topic | Source proposal | Final architecture report | This implementation |
|---|---|---|---|
| Runtime services | Core five, but later sections add CloudWatch, X-Ray, SAM and SSM | Exactly five: API Gateway, EventBridge, Step Functions (Standard), Lambda, DynamoDB | Same five as the final report. CloudFormation is deployment tooling only. |
| Storage | Not specified as a single-table design | One DynamoDB table holding orders, action ledger, due work, outbox, profiles and audit items | One table (`PK`/`SK`) plus one sparse GSI (`GSI1`), as in the final report |
| Identity | - | API Gateway IAM/SigV4; caller from the request context | `AWS_IAM` route authorization; caller id = `requestContext.authorizer.iam.userArn`; optional `X-Demo-User` header only when `DEMO_ALLOW_HEADER_IDENTITY=true` (insecure, demo only) |
| Providers | Mocked third parties | Mock adapters | Simulated providers with deterministic fault injection |

## Simplifications vs. the final architecture report

| Topic | Final architecture report | This implementation | Why / consequence |
|---|---|---|---|
| Due-work scheduling | Sparse GSI keyed by hourly bucket + shard, persisted cursor with overlap and cyclic historical re-query | Sparse GSI `GSI1PK=DUE`, `GSI1SK={dueAt}#{orderId}`, polled every minute | Demo volume is tiny; one partition is enough. The claim is still a conditional update, so concurrent pollers are safe. Sharding and cursors would be the scale-out step. |
| Outbox retirement | Outbox/intent stays pending until the **workflow acknowledges** it (starter confirms the execution exists and ownership is recorded), so lost target delivery or a crash before `StartExecution` is repaired by replay | Outbox item is retired (marked `PUBLISHED`, removed from the index) as soon as **PutEvents reports success for that entry** (`service.drain_outbox`). Failed entries stay in the outbox. | **Not an equivalent guarantee.** If EventBridge accepts the event but the target delivery to the starter is lost, or the starter crashes before `StartExecution`, nothing replays it automatically; the order would stay `PREPARING` (or `COMPENSATING`) without an execution. This is a cloud recovery limitation (see Honest limitations). Locally the publisher invokes the starter synchronously, so the gap is not exercised. |
| Local orchestration | Not specified | In-process interpreter that executes `infra/statemachine.asl.json` itself (Task/Choice/Pass/Succeed/Fail, Parameters, ResultPath, Retry, Catch) and dispatches Tasks to the same `saga.dispatch` the Lambda uses | No Docker, no Java, no network; the local path still exercises the real state-machine definition. Retry delays are recorded, not slept. |
| Step Lambdas | Provider adapters as Lambdas | One step-handler Lambda; `Parameters.step` selects the handler | Fewer resources to deploy and update; identical code paths. |
| Provider state | Mock adapters | Simulated providers storing state in the same table (`PROVIDER#...` items) | State must survive across Lambda invocations; deterministic fault injection. |
| Reverse generation after READY | Starter claims a new reverse generation only after `DescribeExecution` shows the earlier execution ended and all PENDING/UNKNOWN ledger entries are reconciled | Cancel of a `READY` order does a conditional `READY -> COMPENSATING` update that bumps `generation`. **No `DescribeExecution` check is implemented.** | Safety relies on generation/status fencing (see Invariant notes), not on observing the earlier execution's end. |
| Lambda code upload | Not specified | Plain CFN with placeholder inline code, then `aws lambda update-function-code --zip-file` | Avoids an S3 artifact bucket. The ASL is embedded by `scripts/render_template.py`, keeping one source of truth; deploy via `scripts/deploy.ps1`, not the raw template. |

## Invariant notes

* **Action keys** are `ORDER#{id}/ACTION#{name}` with no version component, so a retry or a
  re-delivered step reuses the same provider idempotency key.
* **Ambiguous timeouts** leave the ledger in `UNKNOWN`. The next attempt (Step Functions Retry)
  calls `get_status` first and only reissues if the provider has no record.
* **Generation fencing**: `generation` is bumped only by (a) the scheduler claim
  `ACCEPTED -> PREPARING` and (b) a cancel of a `READY` order `READY -> COMPENSATING`. Both are
  conditional updates on the current status and generation. `READY` does **not** prove that the
  forward Step Functions execution has already ended (it may still be returning from
  `mark_ready`). The enforced invariant is narrower: forward side effects are gated on
  `status = PREPARING` and the execution's own `generation`, so once an order is `READY` (or has
  moved to a newer generation) the earlier forward execution cannot cause any new side effect.
  Each side-effecting step writes its `PENDING` ledger entry in a transaction that checks
  `generation` and `status`, so a stale execution is fenced out.
* **Cancellation during preparation** only sets the monotonic `cancelRequested` flag. Forward
  steps check it before and after each side effect, and `mark_ready` is conditional on
  `cancelRequested = false`, so a cancel is never lost between "last step done" and "READY".
* **Compensation** visits steps in reverse; each compensation reads provider state first
  (idempotent, reconciles its own ambiguous timeouts), chooses void vs. refund from the actual
  payment status, records its own result, and never blocks the others. Any unconfirmed
  compensation or unreconciled effect yields `MANUAL_REVIEW`, never `CANCELLED`.

## Honest limitations

* **No AWS deployment has been performed.** The template, scripts and Lambda handlers are
  tested only locally (handlers with fake boto3 clients). LabRole's actual permissions
  (e.g. `states:StartExecution`, `events:PutEvents`, Step Functions invoking Lambda) are unverified.
* No performance, cost or latency measurements are claimed.
* The scheduler resolution is one minute on AWS; the local demo uses a manual clock.
* The local starter remembers execution names in memory only; after a server restart, fencing
  (not the name check) prevents duplicate effects.
* Outbox items are retired on PutEvents acknowledgement, not on workflow acknowledgement. A lost
  EventBridge target delivery or a starter crash before `StartExecution` is not replayed
  automatically on AWS; the order would need the (not implemented) stale-order sweeper below or
  manual intervention.
* `GET /admin/orders` and `GET /orders` use a table scan - acceptable for a demo, not for scale.
* If `begin_compensation` itself fails persistently, the execution fails and the order stays in
  `PREPARING`/`COMPENSATING`; there is no automatic sweeper for stuck executions (would be the
  next step: a poller query for stale non-final orders).
* Supplies, prices and provider responses are simulated; no real payments, bookings or PMS calls.
* The JSON file persistence of the local server is a convenience, not a database (single process).
