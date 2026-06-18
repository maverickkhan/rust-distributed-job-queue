# Security policy

## Scope and threat model

This service is designed to run **inside a trusted network**, behind an
authenticated API gateway (e.g. the companion `rust-api-gateway`). It is a
job-processing backend, not an internet-facing edge service.

- **Authentication/authorization:** intentionally **not** implemented here.
  Front the API with a gateway that enforces auth. Do not expose `:8080`
  directly to untrusted clients.
- **Input handling:** submissions are validated (queue/type non-empty, bounded
  `max_attempts`/`timeout_secs`, length limits) and the request body size is
  capped (`API_MAX_BODY_BYTES`).
- **SQL injection:** every query is fully parameterized; no user input is ever
  string-concatenated into SQL.
- **Payloads:** stored as JSONB. **Do not put secrets in job payloads** — they
  are persisted and visible via the API.
- **Denial of service:** an unauthenticated caller with network access could
  submit unbounded jobs. Rate limiting is a roadmap item; in the meantime rely
  on the gateway.

## Secrets handling

- Configuration is environment-based; **no credentials are committed**.
- `.env` is git-ignored; only `.env.example` (with placeholders) is committed.
- Container images carry no secrets; inject them at runtime.

## Dependency hygiene

- `cargo audit` and `cargo deny` configs ship in the repo; CI runs `cargo audit`.
- Dependencies are pinned via `Cargo.lock` (committed for the binaries).

## Reporting a vulnerability

Open a private security advisory on the GitHub repository, or email the
maintainer. Please do not file public issues for security problems.

## Honest limitations

This project has **not** undergone a formal security audit. It should not be
treated as hardened for hostile, internet-facing deployment without adding
authentication, rate limiting, and a review of your deployment topology.
