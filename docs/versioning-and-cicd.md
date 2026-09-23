# Versioning and deploying this pipeline: the official options

We deploy PriceGuard with a hand-written bash script (`scripts/recreate-all.sh`)
driving the `confluent` CLI. This document records what Confluent *officially*
supports instead, so the choice is deliberate rather than accidental.

Researched 2026-09-23 against docs.confluent.io, registry.terraform.io and
github.com/confluentinc.

---

## Summary

| # | Question | Verdict |
|---|----------|---------|
| 1 | Official Terraform provider, incl. Flink statements? | **YES** |
| 2 | First-party GitHub App / native git-sync? | **NO** |
| 3 | Declarative YAML (CFK) for Confluent *Cloud*? | **NO** — on-prem only |
| 4 | Documented CI/CD promotion story for Flink SQL? | **PARTIAL** |
| 5 | Is the REST API we already use the public one? | **YES** — `sql/v1` |

---

## 1. Terraform provider — YES

`confluentinc/confluent`, currently 2.x.
<https://registry.terraform.io/providers/confluentinc/confluent/latest>

Every object in this project has a resource type:

| Our object | Terraform resource |
|---|---|
| Environment | `confluent_environment` |
| Kafka cluster | `confluent_kafka_cluster` |
| Topics | `confluent_kafka_topic` |
| API keys | `confluent_api_key` |
| Datagen connectors | `confluent_connector` |
| Flink pool | `confluent_flink_compute_pool` |
| **Flink SQL statements** | **`confluent_flink_statement`** |

`confluent_flink_statement` is real and GA — that is the load-bearing fact,
since a Flink pipeline that cannot version its statements is not really
versioned. Arguments: `organization`, `environment`, `compute_pool`,
`principal`, `statement`, `statement_name`, `rest_endpoint`, `credentials`,
`properties`, `stopped`.
<https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/confluent_flink_statement>

Two caveats worth knowing before adopting it:

- **Editing SQL replaces the statement**, it is not an in-place update. Docs
  recommend `lifecycle { prevent_destroy = true }` on production statements.
  For a stateful streaming job, replacement means losing accumulated state —
  the same reason our `06`/`07` CEP statements need a warm-up period after
  every redeploy.
- **Flink credentials land in Terraform state.** State must be treated as a
  secret and stored in a locked-down backend.

## 2. GitHub integration — NO

There is no first-party Confluent GitHub App, and no "connect your repo"
feature that syncs Flink statements or connector configs from git. A
Marketplace search for "confluent" returns one action, `cp-all-in-one`, which
runs Confluent *Platform* in containers locally — unrelated to Cloud deployment.

So: GitHub holds the source of truth, and *you* run Terraform from your own CI.
Confluent does not reach into the repo. Any claim otherwise is describing
something adjacent.

## 3. Confluent for Kubernetes — NO, not for Cloud

CFK is the operator for **Confluent Platform on Kubernetes** (on-prem/self-managed).
It does not manage Confluent Cloud clusters, managed connectors, Flink pools or
Cloud Flink SQL. <https://docs.confluent.io/operator/current/overview.html>

Not a GitOps path for this project.

## 4. Flink CI/CD — PARTIAL

Confluent documents the statement lifecycle (create/stop/resume/delete) and
ships a Flink Terraform quickstart, but there is **no official documented
dev→staging→prod promotion workflow** for Flink SQL, and no native statement
versioning feature. The supported pattern is assembled by the user:

    SQL in git -> separate Terraform workspace + state per environment
               -> terraform plan/apply in your own CI
               -> prevent_destroy on production statements

That is official *tooling*, not an official *product*. Worth stating plainly so
nobody expects a managed GitOps service.

## 5. REST API — YES, `sql/v1` is public and current

    https://flink.<region>.<cloud>.confluent.cloud/sql/v1/
      organizations/{org}/environments/{env}/statements

This is the documented public Flink Gateway API — the same one the Terraform
provider calls internally. `scripts/query.sh` is therefore using a supported
interface, not an undocumented one.
<https://docs.confluent.io/cloud/current/api.html>

---

## Why we are still on bash

Not because Terraform is unavailable — it plainly is available, and for anything
long-lived it is the better answer.

For this project the bash script is the right size of tool:

- The stack is **ephemeral by design**. It exists to be built, demonstrated, and
  torn down. `teardown.sh` deleting the environment is a feature, whereas
  Terraform's `prevent_destroy` guidance is aimed at the opposite goal.
- There is **one environment**, so per-environment state/workspace separation —
  Terraform's main advantage here — buys nothing yet.
- Terraform state would hold live Flink credentials, adding a secret-management
  problem to a project that currently keeps every secret in a gitignored
  `infra.env` generated at runtime.
- The script **doubles as documentation**: each step explains *why* in comments,
  which a reviewer reads top-to-bottom in one pass.

The honest caveat: bash gives no drift detection and no plan/apply dry run. It
is also only as good as its error handling — the teardown-and-rebuild drill on
2026-09-23 found a silent failure where `jget` could not parse the trailing
`Using API Key "..."` line that `api-key create --use` appends after its JSON.
`set -e` turned that into a wordless death at step 3/9, having already created
a cloud resource. Terraform would have surfaced that as a typed error.

**Recommendation:** if PriceGuard ever becomes long-lived or gains a second
environment, port it to the Terraform provider. `confluent_flink_statement`
makes that a complete migration rather than a partial one. Until then the
script is deliberate, and the rebuild drill is what keeps it honest.
