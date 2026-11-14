use std::net::SocketAddr;
use std::time::Duration;

use hardy_bpv7::eid::NodeId;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Normalizes an amateur radio callsign by trimming whitespace and uppercasing ASCII characters.
#[must_use]
pub fn normalize_callsign(call: &str) -> String {
    call.trim().to_uppercase()
}

/// Validates Mercury ARQ / AX.25 callsign syntax:
/// `3..=10` ASCII alphanumeric characters (at least one ASCII letter) plus optional `-SSID` (`0..=15`),
/// rejecting control characters such as `\r` or `\n`.
#[must_use]
pub fn is_valid_callsign(call: &str) -> bool {
    if call.contains(['\r', '\n', '\0']) {
        return false;
    }
    let trimmed = call.trim();
    if trimmed.is_empty() {
        return false;
    }
    let mut parts = trimmed.split('-');
    let Some(base) = parts.next() else {
        return false;
    };
    if !(3..=10).contains(&base.len())
        || !base.chars().all(|c| c.is_ascii_alphanumeric())
        || !base.chars().any(|c| c.is_ascii_alphabetic())
    {
        return false;
    }
    match parts.next() {
        Some(ssid) => {
            if parts.next().is_some()
                || ssid.is_empty()
                || ssid.len() > 2
                || !ssid.chars().all(|c| c.is_ascii_digit())
            {
                return false;
            }
            ssid.parse::<u8>().is_ok_and(|n| n <= 15)
        }
        None => true,
    }
}

/// Returns `true` if `callsign` is blank or uses placeholder `N0CALL` (with optional `-SSID`).
#[must_use]
pub fn is_unidentified_callsign(callsign: &str) -> bool {
    let trimmed = callsign.trim();
    if trimmed.is_empty() {
        return true;
    }
    let base = trimmed.split('-').next().unwrap_or(trimmed);
    base.is_empty() || base.eq_ignore_ascii_case("N0CALL")
}

fn default_control_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8300))
}

fn default_data_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8301))
}

fn default_bandwidth() -> u32 {
    2300
}

fn default_connect_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_grace_period() -> Duration {
    Duration::from_secs(30)
}

/// Configuration entry for a known Mercury peer.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(rename_all = "kebab-case")
)]
pub struct PeerEntry {
    /// Amateur radio callsign of the remote station.
    pub callsign: String,

    /// Node EID of the remote station. If omitted, defaults to `dtn://<callsign>/`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub node_id: Option<NodeId>,
}

impl PeerEntry {
    /// Returns the effective NodeId for this peer.
    #[must_use]
    pub fn effective_node_id(&self) -> NodeId {
        if let Some(ref nid) = self.node_id {
            nid.clone()
        } else {
            let normalized = self.callsign.trim().to_lowercase();
            let uri = format!("dtn://{normalized}/");
            uri.parse().unwrap_or_else(|_| {
                NodeId::Dtn(hardy_bpv7::eid::DtnNodeId {
                    node_name: normalized.into_boxed_str(),
                })
            })
        }
    }
}

/// Configuration for the Mercury Connected ARQ CLA.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(default, rename_all = "kebab-case")
)]
pub struct Config {
    /// Local amateur radio callsign.
    pub my_callsign: String,

    /// Require an identified, syntactically valid amateur radio callsign (reject `N0CALL` / blank).
    #[cfg_attr(feature = "serde", serde(default))]
    pub require_callsign: bool,

    /// Mercury TNC control TCP port address (default: 127.0.0.1:8300).
    pub control_addr: SocketAddr,

    /// Mercury TNC ARQ data TCP port address (default: 127.0.0.1:8301).
    pub data_addr: SocketAddr,

    /// ARQ bandwidth in Hz (e.g. 500, 2300, 2750; default 2300).
    pub bandwidth: u32,

    /// Connect handshake timeout (default: 30s).
    #[cfg_attr(feature = "serde", serde(with = "humantime_duration_opt", default))]
    pub connect_timeout: Option<Duration>,

    /// Grace period after contact duration expires to drain an in-flight bundle (default: 30s).
    #[cfg_attr(feature = "serde", serde(with = "humantime_duration_opt", default))]
    pub grace_period: Option<Duration>,

    /// Idle timeout after which an active session with no traffic is closed (optional).
    #[cfg_attr(feature = "serde", serde(with = "humantime_duration_opt", default))]
    pub idle_timeout: Option<Duration>,

    /// Static or scheduled peers.
    pub peers: Vec<PeerEntry>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            my_callsign: "N0CALL".to_string(),
            require_callsign: false,
            control_addr: default_control_addr(),
            data_addr: default_data_addr(),
            bandwidth: default_bandwidth(),
            connect_timeout: Some(default_connect_timeout()),
            grace_period: Some(default_grace_period()),
            idle_timeout: None,
            peers: Vec::new(),
        }
    }
}

impl Config {
    /// Validate local callsign configuration according to [`Self::require_callsign`].
    ///
    /// Syntactically invalid non-empty callsigns always return [`crate::Error::InvalidCallsign`].
    /// Blank or `N0CALL` callsigns emit a warning when `require_callsign` is `false`, or fail
    /// with [`crate::Error::InvalidCallsign`] when `require_callsign` is `true`.
    pub fn validate_callsign(&self) -> crate::Result<()> {
        if self.my_callsign.contains(['\r', '\n', '\0']) {
            return Err(crate::Error::InvalidCallsign(format!(
                "callsign contains control characters: {:?}",
                self.my_callsign
            )));
        }
        let trimmed = self.my_callsign.trim();
        if !trimmed.is_empty() && !is_valid_callsign(trimmed) {
            return Err(crate::Error::InvalidCallsign(format!(
                "invalid callsign syntax '{}'",
                self.my_callsign
            )));
        }
        if is_unidentified_callsign(trimmed) {
            if self.require_callsign {
                return Err(crate::Error::InvalidCallsign(
                    "identified amateur radio callsign is required (got blank or N0CALL); set `my_callsign` or pass `--allow-unidentified-rf`".to_string(),
                ));
            }
            tracing::warn!(
                my_callsign = %self.my_callsign,
                "Mercury CLA running with unidentified RF callsign (blank or N0CALL); set `my_callsign` or pass `--require-callsign` before on-air transmission"
            );
        }
        Ok(())
    }
}

#[cfg(feature = "serde")]
mod humantime_duration_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(duration: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match duration {
            Some(d) => s.collect_str(&humantime::format_duration(*d)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        let opt: Option<String> = Option::deserialize(d)?;
        match opt {
            Some(text) => humantime::parse_duration(&text)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_callsign_normalization_and_validation() {
        assert_eq!(normalize_callsign("  f4jxq-1 "), "F4JXQ-1");
        assert!(is_valid_callsign("F4JXQ"));
        assert!(is_valid_callsign("F4JXQ-15"));
        assert!(is_valid_callsign("STATA"));
        assert!(is_valid_callsign("STATB"));
        assert!(is_valid_callsign("N0CALL"));

        assert!(!is_valid_callsign(""));
        assert!(!is_valid_callsign("AB"));
        assert!(!is_valid_callsign("12345"));
        assert!(!is_valid_callsign("F4JXQ-16"));
        assert!(!is_valid_callsign("F4JXQ-99"));
        assert!(!is_valid_callsign("F4JXQ\r\nDISCONNECT"));
        assert!(!is_valid_callsign("TOO_LONG_CALLSIGN"));

        let mut cfg = Config::default();
        assert!(!cfg.require_callsign);
        assert!(cfg.validate_callsign().is_ok());

        cfg.require_callsign = true;
        assert!(matches!(
            cfg.validate_callsign(),
            Err(crate::Error::InvalidCallsign(_))
        ));

        cfg.my_callsign = "F4JXQ".into();
        assert!(cfg.validate_callsign().is_ok());

        cfg.require_callsign = false;
        cfg.my_callsign = "INVALID\r\n".into();
        assert!(matches!(
            cfg.validate_callsign(),
            Err(crate::Error::InvalidCallsign(_))
        ));
    }
}
