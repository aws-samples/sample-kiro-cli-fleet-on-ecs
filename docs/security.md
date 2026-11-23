# Security

Part of the [sample README](../README.md).

Key material is never baked into an image or committed to this repository. Tasks load subscription keys
at runtime from AWS Secrets Manager using the task role, after the allocator has decided which
subscription the task holds — nothing at task start knows that yet, which is why the key is not injected
as a container secret. The task execution role and the task role are separate: the execution role can
pull the image and open a log stream and cannot read a subscription key; the task role can take work from
the queue and store results and cannot create work or delete either. Its one ECS permission is
`ecs:UpdateTaskProtection` on the cluster's own tasks, for scale-in protection.

**Do not trust the shell tool for a workflow that reads untrusted input.** The agent (`kiro-cli`) runs in
the same container and as the same user as the worker, so any tool it is trusted with acts with the
task role. A trusted `shell` tool (`execute_bash` in older CLI versions) can therefore call AWS with
that role: read every subscription key in the pool, because the role's Secrets Manager grant covers the
pool prefix (a task learns which subscription it holds only after it claims one), and write any key in
the results bucket. Input content can steer the agent (prompt injection), so treat every file a manifest
lists as untrusted unless you produced it yourself. The example workflows trust only read and write for
this reason, and writes outside `./output` fail the run. If a workflow genuinely needs a shell, run it
only on inputs you control, or move key retrieval out of the task role before you do.

Egress has three modes, set with `fleet:egressControl`. In every mode nothing reaches a task from
outside.

- **`private` (default).** The tasks' subnets have no route to the internet at all: no internet gateway,
  no NAT gateway, no firewall. Every destination the workload needs has a VPC endpoint. The AWS services
  it calls (ECR, SQS, Secrets Manager, CloudWatch Logs, STS) use interface endpoints, S3 and DynamoDB use
  gateway endpoints, and the Kiro service is reached through the `q` interface endpoint, which provides
  private DNS for `runtime.<region>.kiro.dev` (inference), `management.<region>.kiro.dev` and
  `telemetry.<region>.kiro.dev`. A task cannot reach any other host, which is a stronger control than an
  allowlist and costs nothing extra. The measurements in `bench/README.md` ran in this mode.
- **`inspection`.** For a workflow that must reach the internet. Tasks are routed through a NAT gateway
  and an AWS Network Firewall that allows only the domains in `fleet:kiroEgressDomains`. The firewall
  has delete protection on, which affects teardown (see [Removing it](deploying.md#removing-it)).
- **`open`.** A NAT gateway with the security group restricted to port 443 and no destination control.
  Synthesis warns, because a task can reach any host on 443. That is a real reduction in the security
  property, not a tidying-up.

The endpoint list and the allowlist are measured rather than assumed. A capture of a credentialed run,
reading hostnames and byte counts only, corrected three things the design had wrong: inference goes to
`runtime.<region>.kiro.dev`, which the `q` endpoint covers, rather than to `codewhisperer`, which was
never contacted and whose endpoint is gone; and the MCP server was being downloaded from npm on every
run, which `private` and `inspection` would both block, so the image now installs it. Ongoing traffic is
about 0.39 MB per run, so the endpoints are justified by the image pull (0.69 GB per host) and by keeping
credentials and results off the public internet, not by inference volume.

## What it costs while idle

The fleet scales to zero. The network around it does not, and that distinction is easy to miss:

| | rate (us-east-1) | `private` | `inspection` | `open` |
|---|---|---|---|---|
| Interface endpoints, 7 x 2 AZs | $0.01/hour each per AZ | ~$102 | ~$102 | ~$102 |
| Gateway endpoints (S3, DynamoDB) | free | $0 | $0 | $0 |
| NAT gateway | $0.045/hour | none | ~$33 | ~$33 |
| Firewall endpoint | $0.395/hour | none | ~$288 | none |
| **Monthly, idle** | | **~$102** (about $3.4 a day) | **~$423** (about $14 a day) | **~$135** |

Rates from the AWS Pricing API; check them for your region. Data processing charges come on top and
are small at 0.39 MB per run. In `inspection` the firewall dominates, and it is one endpoint rather than
one per zone for the same reason there is one NAT gateway: a path carrying 0.39 MB per run does not need
zonal independence.

## Before adapting this for production or shared use

This is a sample. The security model assumes one operator running their own batches in an account they
own. A non-exhaustive list of what to address first:

- **Cost controls.** Set AWS Budgets alarms. Kiro subscription credits are billed separately from AWS
  charges, so watching only AWS cost leaves half the picture, and a runaway batch spends both.
- **Subscription key rotation.** Keys are long-lived here and rotation is manual. Decide a rotation
  period and who is notified when a key is disabled.
- **IAM review against your own requirements.** The roles are scoped to named resources, but "what this
  workload needs" is a judgement about your workflows, not ours. A workflow that calls further AWS APIs
  needs grants this sample does not make.
- **Egress allowlist maintenance.** The allowed domains are a point-in-time list. A Kiro endpoint added
  later is a silent failure, so treat the list as something that expires.
- **Workflow prompt review.** A workflow is a prompt plus a tool allowlist, and both decide what an
  unattended agent can do. Review changes to them as you would review code.
- **Result integrity.** Results are versioned but not signed. If a downstream system trusts them, decide
  how it establishes which run produced what.
- **Observability retention and alarm routing.** Log retention is configured; where alarms go is not.
- **Dependency scanning in the build.** The build pins versions and verifies a checksum, but nothing
  audits them for known vulnerabilities on a schedule.

See [CONTRIBUTING](../CONTRIBUTING.md#security-issue-notifications) for reporting a security issue.
