//! Mercury connected ARQ Convergence Layer Adapter for Hardy DTN BPv7.

pub mod cla;
pub mod config;
pub mod driver;
pub mod error;

pub use cla::MercuryCla;
pub use config::{Config, PeerEntry};
pub use driver::callsign_to_cla_address;
pub use error::{Error, Result};

use dtnpm_core::coordinator::ContactExecutionEndpoint;
use hardy_bpa::async_trait;
use hardy_bpv7::eid::NodeId;

#[async_trait]
impl ContactExecutionEndpoint for MercuryCla {
    async fn start_contact(&self, callsign: &str, node_id: Option<&NodeId>) -> std::result::Result<(), String> {
        self.connect_peer(callsign, node_id.cloned())
            .await
            .map_err(|e| e.to_string())
    }

    async fn end_contact(&self, _callsign: &str) -> std::result::Result<(), String> {
        self.disconnect().await.map_err(|e| e.to_string())
    }

    async fn is_active(&self) -> bool {
        self.is_connected().await
    }
}
