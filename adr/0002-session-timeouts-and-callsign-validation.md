# ADR 0002: Session State Timeouts, Frame Bounds, and Callsign Validation

- **Status**: Accepted
- **Date**: 2026-11-14

## Context

Several fault and abuse vectors were identified in the Mercury session driver and CLA bridge:

1. **Stuck FSM Transitions & Shutdown Hang**: A `"WRONG"` reply or unanswered `CONNECT`/`DISCONNECT` command could leave `SessionState` permanently stuck in `Connecting` or `Disconnecting`, blocking future contacts (`Error::Busy`), while `MercuryCla::disconnect()` had no outer timeout. Similarly, mid-session TCP drops failed to clear `SessionState::Connected` and stale `Sink` peer routes before reconnecting.
2. **Unbounded Outbound Buffers & Misrouted Bundles**: `forward()` previously ignored `_cla_addr` and buffered arbitrary `total_len` before checking `MAX_MERCURY_FRAME`, allowing multi-gigabyte allocations or bundles destined for `STATC` to transmit over an active `STATB` link.
3. **Unvalidated Dynamic `CONNECTED` Callsigns & Unbounded Peer Map**: Unsanitized control lines could inject malformed callsigns or insert unlimited entries into `Driver::peers`.
4. **Amateur Radio Identification**: `Config::default()` defaults `my_callsign` to `"N0CALL"` for convenience in mock/bench environments, necessitating a warning by default with optional strict compliance enforcement.

## Decision

1. **Deadlock-Free FSM & Explicit Reset Paths**:
   - Store `entered_at: Instant` on `SessionState::Connecting` and `SessionState::Disconnecting`; sweep stale transitions during `idle_interval` ticks using `connect_timeout` (default 30s) and `DISCONNECT_TIMEOUT` (5s).
   - Reset `session = SessionState::Disconnected` on `"WRONG"` control frames and on TCP disconnect cleanup (invoking `Sink::remove_peer` and failing/resolving pending replies).
   - Wrap `MercuryCla::disconnect` in `tokio::time::timeout(Duration::from_secs(5), ...)`.
2. **Outbound Size Bounds & Destination Matching**:
   - Enforce `MAX_MERCURY_FRAME` (16 MiB) before `buffer_stream` in `forward()` and again inside `DriverCommand::ForwardBundle`, mapping `Error::PayloadTooLarge` to `hardy_bpa::cla::Error::PayloadTooLarge`.
   - Extract `target_callsign` via `cla_address_to_callsign(cla_addr)` and verify `target == *peer_callsign` before writing, returning `ForwardBundleResult::NoNeighbour` on mismatch.
3. **Callsign Normalization, Validation, Peer Cap, and Strict RF Flag**:
   - Normalize all callsigns via `normalize_callsign` (`.trim().to_uppercase()`).
   - Enforce `is_valid_callsign` (`3..=10` ASCII alphanumeric chars with >=1 letter, optional `-SSID` `0..=15`, no `\r`/`\n`/`\0`) on outbound and incoming `CONNECTED` frames, and cap `Driver::peers` at `MAX_PEERS = 256`.
   - Keep `Config::require_callsign: false` by default (emitting `tracing::warn!` on `N0CALL`/blank) with opt-in `require-callsign: true` / `--require-callsign` and explicit override `--allow-unidentified-rf`.

## Consequences

- Stuck RF calls or silent TNC disconnects self-heal instead of locking the contact scheduler.
- Oversized or misaddressed bundles fail fast without OOM risk or wrong-station transmission.
- Dynamic peer state stays strictly bounded at 256 entries even under malformed TNC input.
