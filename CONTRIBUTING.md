# Contributing Guide — `dtnpm-cla-mercury-arq`

This guide outlines the development rules and security requirements for the Mercury Connected ARQ CLA in `dtnpm-cla-mercury-arq`.

---

## 1. Core Principles

- **Zero Panic / Zero Unwrap**: No `.unwrap()` or `.expect()` calls in production code.
- **ARQ Security and Robustness**:
  - Every transient connecting or disconnecting state must be protected by an expiration timer.
  - Bundle transmission must strictly verify that the target matches the currently connected radio peer.
- **Architectural Traceability (ADR)**:
  - Any protocol addition or adjustment must be documented in an ADR file under `adr/` and indexed in `adr/README.md`.

---

## 2. Local Verification

```bash
cargo fmt --all -- --check
cargo clippy --offline --workspace --lib --bins -- -D warnings -D clippy::unwrap_used -D clippy::panic
cargo test --offline --all-features
./scripts/check-security.sh --strict
```
