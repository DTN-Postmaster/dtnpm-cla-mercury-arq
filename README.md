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

## License

Dual-licensed under the Apache License, Version 2.0 or the MIT License. See [LICENSE.md](LICENSE.md).
