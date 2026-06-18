# Contributing

Thanks for your interest. This is a portfolio project but contributions and
feedback are welcome.

## Development setup

```bash
rustup toolchain install stable
make db                       # local Postgres on :5432
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/djq
make ci                       # fmt-check + clippy + test
```

## Before opening a PR

Run the same gates CI runs:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

- Keep `djq-core` pure — no I/O, no sqlx, no Tokio runtime dependencies.
- New storage behaviour belongs behind the `Store` trait and needs an integration test.
- Prefer typed `thiserror` errors in libraries; `anyhow` only in binaries.
- No `unwrap()`/`expect()` on production paths unless clearly justified.

## Commit style

Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`, `ci:`),
one logical change per commit. Review your `git diff` and staged files, and
confirm no secrets are included, before committing.

## Tests

Unit tests live next to the code (`#[cfg(test)]`); integration tests that need
Postgres live in `crates/integration-tests/tests`. Tests must not be disabled
to make CI pass.
