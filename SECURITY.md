# Security Policy — `dtnpm-cla-mercury-arq`

## Supported Versions

| Version | Supported | Security Updates |
|---------|-----------|------------------|
| 0.2.x   | Yes       | Active           |
| < 0.2   | No        | End of Life      |

## Security Architecture & EU Cyber Resilience Act (CRA) Alignment

`dtnpm-cla-mercury-arq` implements the Mercury Connected ARQ Convergence Layer Adapter for Hardy BPv7 (`dtn-postmaster`):

- **Memory Safety & Panic-Free Production Targets**: Built in Rust (`edition = "2024"`) and checked in CI with `-D warnings -D clippy::unwrap_used -D clippy::panic`.
- **Bounded Session State Machine (`src/driver.rs`, `src/cla.rs`)**:
  - Tracks `entered_at` timestamps in `SessionState::Connecting` and `SessionState::Disconnecting` so half-open/stalled modem handshakes automatically reset back to `SessionState::Disconnected` upon timeout (`connect_timeout` / 5s disconnect timeout).
  - Resets `SessionState` to `Disconnected` on `"WRONG"` control frames and on TCP disconnect before reconnecting, removing stale BPA peers from `Sink`.
  - Wraps `MercuryCla::disconnect` in a 5-second `tokio::time::timeout` so contact termination and shutdown cannot deadlock.
- **Destination Verification, Frame Bounds & Peer Cap (`src/cla.rs`, `src/driver.rs`)**:
  - Enforces `MAX_MERCURY_FRAME` (16 MiB) on outbound forwarding (`total_len` before stream buffering as well as driver write path) and inbound `data_read_loop`.
  - Verifies that `cla_addr` matches the active connected remote callsign in `ForwardBundle` before writing to the ARQ data socket.
  - Validates callsigns strictly (`3..=10` ASCII alphanumeric chars containing at least one ASCII letter plus optional `-SSID` `0..=15`, rejecting `\r`/`\n`/`\0`), normalizes with `.trim().to_uppercase()`, and caps `Driver::peers` at `MAX_PEERS` (256).
- **RF Identification Policy (`src/config.rs`, `src/main.rs`)**: Warns on blank or `N0CALL` local callsigns by default while supporting strict rejection via `require-callsign: true` or CLI `--require-callsign` (and explicit opt-in override `--allow-unidentified-rf`).
- **Secret Handling**: Standalone CLI authentication supports `--token-file <PATH>` (`DTN_HDY_TOKEN_FILE`) to avoid passing Bearer tokens on process command lines, alongside Unix Domain Socket transport (`--socket` / `DTN_HDY_SOCKET`).
- **Supply Chain & VEX**: Replaced `serde_yaml` (`unsafe-libyaml`, `RUSTSEC-2024-0449`) with pure-Rust `yaml_serde`. `RUSTSEC-2026-0459` (`libsqlite3-sys` SQLite `session` extension) is documented as VEX Not Affected in `.cargo/audit.toml` and `deny.toml` because transitive `dtnpm-core` enables only `["array", "bundled", "time"]` without the SQLite `session` extension.
- **SBOM**: Generated on every CI run in CycloneDX JSON format via `anchore/sbom-action`.

## Reporting a Vulnerability

Do **not** open a public GitHub issue for security vulnerabilities.
Report vulnerabilities privately via GitHub Security Advisories for `dtn-postmaster/dtnpm-cla-mercury-arq` or by contacting the maintainers directly. Please include:

- Component version and commit hash
- Configuration flags (`my-callsign`, `require-callsign`, `connect-timeout`, `idle-timeout`)
- Minimal proof-of-concept control/data sequences or reproduction steps

We aim to acknowledge reports within 72 hours and coordinate a fix before public disclosure.
