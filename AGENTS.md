# Agent Guide — `dtnpm-cla-mercury-arq`

## 🎯 Mission
Provide the Convergence Layer Adapter (CLA) for Mercury Connected ARQ HF modems in the DTN Postmaster ecosystem, handling interfacing with the modem control and data sockets, resolving amateur radio callsigns to EIDs, and scheduling radio contact windows.

---

## 🛡️ Security Frameworks and Requirements

### 1. Cyber Resilience Act (CRA) & HF Security
- **Identity & Amateur Radio Callsign Validation**:
  - Callsigns are normalized (`normalize_callsign`) and syntactically validated (`is_valid_callsign`: 3 to 10 characters, without control characters `\r`, `\n`, `\0`).
- **Explicit Opt-In for Off-Air Testing**:
  - Operating without a callsign (or with `N0CALL`) emits a warning and is only tolerated in lab/dummy-load mode via `--allow-unidentified-rf` or `require_callsign: false`.
  - The `--require-callsign` flag prevents startup without a legitimate callsign.
- **Supply-Chain & VEX**:
  - `cargo-audit` and `cargo-deny` audits; VEX `not_affected` statement for `RUSTSEC-2026-0459`.
  - CI actions pinned by 40-character SHA-1.
- **Security Policy**: Documented in [SECURITY.md](SECURITY.md).

### 2. Protocol Robustness & Deadlock Prevention
- **ARQ State Deadlock Resolution**:
  - Systematic session reset (`SessionState::Disconnected`) upon a `"WRONG"` control line from the modem, unblocking pending connect or disconnect promises (preventing permanent `Error::Busy` deadlocks).
  - Automatic detection and purging of stalls in transient states `Connecting` (default 30s timeout) and `Disconnecting` (5s timeout).
  - Hard 5s timeout wrapping `MercuryCla::disconnect`.
- **Routing and Resource Bounds**:
  - Maximum frame cap `MAX_MERCURY_FRAME` set to 16 MiB with pre-check before buffering.
  - Mandatory verification that the bundle's target address (`cla_addr`) matches the currently connected peer's callsign (`target == *peer_callsign`) before any transmission, returning `ForwardBundleResult::NoNeighbour` on mismatch.
  - Peer table bounded to a maximum of 256 entries (`MAX_PEERS`).

---

## 🦀 Rust Engineering Standards

1. **Zero Panic / Zero Unwrap Rule**:
   - All code under `src/` must be 100% free of `.unwrap()`, `.expect()`, and the `panic!` macro.
   - Strict linter: `-D warnings -D clippy::unwrap_used -D clippy::panic`.
2. **Mandatory ADRs**:
   - Any Mercury ARQ architectural decision must be recorded under `adr/` and indexed in [adr/README.md](adr/README.md).
3. **Documentation Language**:
   - English must be used as the language for all documentation, unless the file is suffixed with `.fr.md` or another language is explicitly requested in the prompt.

---

## ⚡ Critical Verification Commands

- `cargo fmt --all -- --check`
- `cargo clippy --offline --workspace --lib --bins -- -D warnings -D clippy::unwrap_used -D clippy::panic`
- `cargo test --offline --all-features`
- `./scripts/check-security.sh --strict`
- `./scripts/check-coverage.sh --summary-only`
