# dtnpm-cla-mercury-arq

Mercury Connected ARQ HF modem Convergence Layer Adapter (CLA) for Hardy BPv7 and **DTN Postmaster** (`dtnpm`).

## Overview

`dtnpm-cla-mercury-arq` bridges Hardy BPv7 bundles with a Mercury ARQ HF modem over TCP control and data sockets, integrating callsign-to-EID peer resolution and contact window coordination (`ContactEndpoint`).

## Features

- `serde`: Enable serde deserialization for `Config` / `PeerEntry`.
- `cli`: Build the standalone `dtnpm-cla-mercury-arq` gRPC CLA binary (default).

## Build and Test

Requires sibling checkouts of `../dtnpm-core` and `../../hardy/*` as referenced in `Cargo.toml`.

```bash
cargo test
```

## Security & Compliance

- **Cyber Resilience Act (CRA)**: Strict callsign syntax validation, token security via `--token-file`, VEX audit documentation (`RUSTSEC-2026-0459`), and SHA-pinned CI actions.
- **Explicit Permissive Mode (Opt-in)**: Operation without identified callsign (`N0CALL`) is restricted to lab use via `--allow-unidentified-rf` / `require_callsign: false`.
- **Resource Bounds & Protocol Safety**: Deadlock-free state machine (timeout sweeps for `Connecting`/`Disconnecting`, 5s disconnect timeout, recovery on `"WRONG"` control lines), 16 MiB max frame cap, strict target callsign matching before RF transmission, and 256-peer table limit.
- **Rust Quality Standards**: 100% `unwrap`-free and `panic!`-free production codebase.
- **Documentation**: See [`AGENTS.md`](AGENTS.md), [`CONTRIBUTING.md`](CONTRIBUTING.md), [`SECURITY.md`](SECURITY.md), and Architectural Decision Records in [`adr/`](adr/).

## License

Dual-licensed under the Apache License, Version 2.0 or the MIT License. See [LICENSE.md](LICENSE.md).
