# ADR 0001: Mercury Connected ARQ Dual-Socket Session Architecture

- **Status**: Accepted
- **Date**: 2026-11-14

## Context

Mercury HF modem TNCs expose two separate local TCP interfaces when operating in connected ARQ mode:

1. A line-oriented CRLF ASCII control socket (`control-addr`, default `127.0.0.1:8300`) handling `MYCALL`, `BW<hz>`, `LISTEN ON`, `CONNECT <src> <dst>`, `DISCONNECT`, and unsolicited `CONNECTED`, `DISCONNECTED`, and `WRONG` status lines.
2. A binary length-prefixed (`u32` big-endian) stream data socket (`data-addr`, default `127.0.0.1:8301`) carrying BPv7 bundle frames once an ARQ link is established.

Hardy BPA (`Cla`) and the `dtnpm-core` contact scheduler (`ContactExecutionEndpoint`) require a non-blocking actor that manages link lifecycle, peer route advertisement (`Sink::add_peer` / `Sink::remove_peer`), and bundle forwarding safely across reconnects.

## Decision

1. **Dedicated Background Actor (`Driver`)**: Spawn a Tokio actor owning both TCP connections and communicating with `MercuryCla` via bounded `mpsc` commands and `oneshot` replies, with background tasks splitting control reads (`MAX_CTL_BUF = 4096`), length-prefixed data reads (`MAX_MERCURY_FRAME = 16 MiB`), and framed data writes.
2. **`ContactExecutionEndpoint` & BPA Integration**: Implement both `hardy_bpa::cla::Cla` and `dtnpm_core::coordinator::ContactExecutionEndpoint` on `MercuryCla` so contact schedules can proactively dial (`connect_peer`) and tear down (`disconnect`) HF ARQ links while also accepting inbound RF connections via `LISTEN ON`.
3. **Pure-Rust Supply Chain**: Parse YAML configurations with `yaml_serde` (`RUSTSEC-2024-0449` eliminated) and pass gRPC auth tokens via `--token-file` or Unix Domain Sockets.

## Consequences

- Control-plane commands (`CONNECT`, `DISCONNECT`) never block bundle data writes once a session is established.
- Loss of either TCP socket cleanly tears down the active session, removes stale peer routes from the BPA, and reconnects with exponential backoff without restarting the CLA process.
