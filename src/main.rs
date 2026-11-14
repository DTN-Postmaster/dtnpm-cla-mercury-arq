use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use dtnpm_cla_mercury_arq::{Config, MercuryCla};
use dtnpm_core::{BpaClient, GrpcBpaClient, resolve_grpc_port};
use hardy_bpa::bpa::BpaRegistration;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "dtnpm-cla-mercury-arq", about = "Standalone Mercury Connected ARQ CLA process connecting to a Hardy BPA via gRPC")]
struct Args {
    /// gRPC endpoint of the target BPA (e.g. http://127.0.0.1:50051)
    #[arg(long)]
    bpa_grpc: Option<String>,

    /// Name used to register this CLA with the BPA
    #[arg(long, default_value = "mercury-arq")]
    name: String,

    /// Optional YAML or TOML configuration file path
    #[arg(short, long)]
    config: Option<PathBuf>,
}

fn load_config(path: Option<&PathBuf>) -> anyhow::Result<Config> {
    let Some(path) = path else {
        return Ok(Config::default());
    };
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file {}", path.display()))?;
    match path.extension().and_then(|e| e.to_str()) {
        Some("toml") => toml::from_str(&raw).context("Invalid TOML config"),
        _ => serde_yaml::from_str(&raw).context("Invalid YAML config"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let cfg = load_config(args.config.as_ref())?;
    let grpc_addr = args
        .bpa_grpc
        .unwrap_or_else(|| format!("http://127.0.0.1:{}", resolve_grpc_port(None)));

    info!(name = %args.name, grpc = %grpc_addr, "Starting dtnpm-cla-mercury-arq");
    let client = Arc::new(GrpcBpaClient::new(&grpc_addr));
    let cla = Arc::new(MercuryCla::new(cfg));
    let init = cla.cla_init();

    let node_ids = client
        .register_cla(args.name.clone(), cla, None, init)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register Mercury CLA on {}: {e}", client.transport_name()))?;

    info!(?node_ids, "Mercury CLA registered; waiting for Ctrl+C");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down dtnpm-cla-mercury-arq");
    Ok(())
}
