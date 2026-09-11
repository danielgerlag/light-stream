# Configurable security and optional review

The user requested both configurable broker security and an optional independent security review.
These are separate controls.
Implement the runtime capability in LS08.
Always verify the enabled and disabled modes.
Run the independent review only when requested.

## Runtime profiles

The configuration names below are proposed and must be finalized with the API contract in LS01.
Use runtime selection in one tested binary initially.
Compile-time removal of security is not required by this plan.

| Profile | Client access | Peer access | Intended use |
| --- | --- | --- | --- |
| `local-insecure` | No TLS or authentication. Report the mode prominently. Bind to loopback by default. | Explicit trusted local membership only. | Development and isolated local functional tests. |
| `secured` | TLS plus authenticated principals and stream-scoped authorization. | Mutual TLS with cluster and node identity bound to configured membership. | Remote, shared, and production-like deployments. |

Refuse non-loopback insecure binding unless the operator supplies an explicit unsafe development override.
Never enable that override in the reference-host release profile.
Security configuration errors must stop startup before serving requests.
Missing certificates, invalid token configuration, or a failed authenticator must not fall back to insecure mode.

Use established TLS and cryptographic libraries.
Start with high-entropy opaque bearer tokens for client principals.
Do not add password authentication, an identity provider, custom cryptography, or a JWT issuer to the first release.
Keep the authentication interface replaceable without spreading policy decisions across handlers.

Separate public client RPCs from peer and administrative RPCs.
A client credential is not a node credential.
A valid certificate alone does not authorize an unknown node to join a Raft group.
Validate the cluster identity, node identity, and committed membership.

## Authorization and session ownership

Define permissions for publish, read, bookmark management, consumer progress, and stream or cluster administration.
Apply them at every unary and streaming entry point.
Unknown operations and missing permissions fail closed.
Keep resource quotas and input bounds active in both security modes.

Bind producer sessions and retry receipts to a principal, not just a caller-supplied sequence.
Check authorization before returning a cached receipt or bookmark.
Credential rotation for the same principal must not invent a new committed request.
Revocation must not permit another principal to claim the original session.

Define revocation behavior explicitly.
The initial proposal permits a bounded policy-cache window of at most five seconds.
After that window, a node that cannot refresh authorization state refuses secured work.
Use fresh authorization for destructive administration.
Record the actual bound in the security profile and exercise it across leaders.
Do not claim instantaneous global revocation from an asynchronous cache.

## Secrets and observability

Read private material from protected files or the environment, not command-line arguments.
Restrict file permissions and avoid echoing secret values in errors.
Do not put raw tokens or TLS private keys in Raft logs, snapshots, backups, diagnostics, or source archives.
Use synthetic credentials in tests and redact them from publishable artifacts.

Record principal identifiers, action, target, result, and request identity in security events without recording payloads or credentials.
Keep security diagnostics distinct from latency-sensitive payload logs.
Do not disable logging or input checks to improve the secured benchmark.

Provide certificate and token rotation without an insecure interval.
Document certificate validity, clock assumptions, overlapping trust, and the rollback procedure.
Reject unsupported security-profile mixtures during node admission or deployment preflight.

## Mandatory functional security cases

| Case | Required result |
| --- | --- |
| Local no-credential workflow | Create, publish, bookmark, replay, and restart succeed in the approved insecure local profile. |
| Accidental insecure exposure | Non-loopback binding without explicit authorization is refused before the listener serves traffic. |
| Valid secured workflow | The same workflow succeeds over verified TLS with authorized identities. |
| Missing or invalid client credential | The request fails and the committed state is unchanged. |
| Insufficient stream permission | Cross-stream reads, publishes, marker operations, and receipt lookups are denied. |
| Untrusted or wrong-name certificate | The connection fails without fallback. |
| Unknown or wrong-cluster peer | The node cannot join, vote, append, or install a snapshot. |
| Credential rotation | The intended overlap works; expired or revoked credentials cease working within the declared bound. |
| Revocation during an active stream | Reauthorization or stream termination follows the documented bound; authorization is not frozen forever at connection time. |
| Disabled versus enabled mode | Performance reports name the mode and never compare unlike security contracts as a code speedup. |
| Secret canaries | Synthetic secret values do not appear in ordinary logs, evidence manifests, command capture, or source snapshots. |
| Restore and restart | Secured state cannot start with missing required trust material or silently become insecure. |

## Optional independent security review

Set `independent_security_review` to true or false in the execution manifest.
The default is false.
This flag affects independent review, not implementation or functional security verification.

When false, record `NOT_REQUESTED` with the revision and declared scope.
Do not label the release security-reviewed.

When true, run a read-only security specialist at the exact candidate revision.
Use the agent environment's dedicated security-review workflow when available.
Follow its approval and remediation process.
Do not substitute an unscoped general review or a dependency scanner for the specialist assessment.

Give the reviewer the threat model, protocol schema, exposure map, auth implementation, storage and snapshot parsers, secret handling, and E2E evidence.
Ask for reproducible, high-confidence findings with severity, confidence, affected files, and a concrete impact.
Require evidence for findings from automated tools.

A selected review is incomplete until its findings are triaged.
Confirmed critical or high-severity defects block the release candidate until fixed and reverified.
Any accepted residual risk needs an explicit operator decision.
Rerun affected functional and performance gates after fixes.

Save the scope, revision, findings, dispositions, and final verdict in `artifacts/<revision>/security-review/`.
Never report a skipped review as a clean review.
The current planning task does not perform this review or claim that the implementation is secure.
