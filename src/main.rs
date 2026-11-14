use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use dtnpm_cla_mercury_arq::{Config, MercuryCla};
use dtnpm_core::{BpaClient, GrpcBpaClient, resolve_grpc_port};
use hardy_bpa::bpa::BpaRegistration;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "dtnpm-cla-mercury-arq",
    about = "Standalone Mercury Connected ARQ CLA process connecting to a Hardy BPA via gRPC"
)]
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

    /// Optional Bearer authentication token for protected gRPC BPA endpoints
    #[arg(long, env = "DTN_HDY_TOKEN", conflicts_with = "token_file")]
    token: Option<String>,

    /// Path to a file containing the Bearer authentication token for protected gRPC BPA endpoints
    #[arg(long, env = "DTN_HDY_TOKEN_FILE", conflicts_with = "token")]
    token_file: Option<PathBuf>,

    /// Optional Unix Domain Socket path for connecting to a protected local BPA
    #[arg(long, env = "DTN_HDY_SOCKET")]
    socket: Option<String>,

    /// Require a valid identified amateur radio callsign (reject blank or N0CALL)
    #[arg(long, conflicts_with = "allow_unidentified_rf")]
    require_callsign: bool,

    /// Explicitly allow unidentified RF operation (blank callsign or N0CALL) for lab/bench use
    #[arg(long, conflicts_with = "require_callsign")]
    allow_unidentified_rf: bool,
}

fn load_config(path: Option<&Path>) -> anyhow::Result<Config> {
    let Some(path) = path else {
        return Ok(Config::default());
    };
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file {}", path.display()))?;
    match path.extension().and_then(|e| e.to_str()) {
        Some("toml") => toml::from_str(&raw).context("Invalid TOML config"),
        _ => yaml_serde::from_str(&raw).context("Invalid YAML config"),
    }
}

fn resolve_token(token: Option<&str>, token_file: Option<&Path>) -> anyhow::Result<Option<String>> {
    if let Some(path) = token_file {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read token file {}", path.display()))?;
        let trimmed = raw.trim().to_string();
        if trimmed.is_empty() {
            anyhow::bail!("Token file {} is empty", path.display());
        }
        return Ok(Some(trimmed));
    }
    Ok(token.map(str::to_owned))
}

fn apply_cli_overrides(cfg: &mut Config, args: &Args) -> anyhow::Result<()> {
    if args.require_callsign {
        cfg.require_callsign = true;
    } else if args.allow_unidentified_rf {
        cfg.require_callsign = false;
    }
    cfg.validate_callsign().map_err(Into::into)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let mut cfg = load_config(args.config.as_deref())?;
    apply_cli_overrides(&mut cfg, &args)?;
    let auth_token = resolve_token(args.token.as_deref(), args.token_file.as_deref())?;
    let grpc_addr = args
        .bpa_grpc
        .clone()
        .unwrap_or_else(|| format!("http://127.0.0.1:{}", resolve_grpc_port(None)));

    info!(name = %args.name, grpc = %grpc_addr, "Starting dtnpm-cla-mercury-arq");
    let client = Arc::new(GrpcBpaClient::with_auth(
        &grpc_addr,
        auth_token.as_deref(),
        args.socket.as_deref(),
    ));
    let cla = Arc::new(MercuryCla::new(cfg));
    let init = cla.cla_init();

    let node_ids = client
        .register_cla(args.name.clone(), cla, None, init)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to register Mercury CLA on {}: {e}",
                client.transport_name()
            )
        })?;

    info!(?node_ids, "Mercury CLA registered; waiting for Ctrl+C");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down dtnpm-cla-mercury-arq");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn make_temp_test_dir(suffix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dtnpm_mercury_test_{}_{suffix}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_resolve_token_from_file_and_direct() {
        let dir = make_temp_test_dir("token");
        let tok_path = dir.join("token.txt");
        {
            let mut f = std::fs::File::create(&tok_path).unwrap();
            writeln!(f, "  mercury-secret-token  ").unwrap();
        }
        assert_eq!(
            resolve_token(None, Some(&tok_path)).unwrap().as_deref(),
            Some("mercury-secret-token")
        );

        let empty_path = dir.join("empty.txt");
        std::fs::write(&empty_path, "  \n").unwrap();
        assert!(resolve_token(None, Some(&empty_path)).is_err());

        assert_eq!(
            resolve_token(Some("inline-tok"), None).unwrap().as_deref(),
            Some("inline-tok")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_cli_callsign_flags_and_token_file_parsing() {
        let args = Args::try_parse_from(["dtnpm-cla-mercury-arq", "--require-callsign"]).unwrap();
        let mut cfg = Config::default();
        assert!(apply_cli_overrides(&mut cfg, &args).is_err());

        let args_allow =
            Args::try_parse_from(["dtnpm-cla-mercury-arq", "--allow-unidentified-rf"]).unwrap();
        let mut cfg_strict = Config {
            require_callsign: true,
            ..Config::default()
        };
        assert!(apply_cli_overrides(&mut cfg_strict, &args_allow).is_ok());
        assert!(!cfg_strict.require_callsign);

        assert!(
            Args::try_parse_from([
                "dtnpm-cla-mercury-arq",
                "--require-callsign",
                "--allow-unidentified-rf",
            ])
            .is_err()
        );
        assert!(
            Args::try_parse_from([
                "dtnpm-cla-mercury-arq",
                "--token",
                "abc",
                "--token-file",
                "/tmp/t",
            ])
            .is_err()
        );
    }

    #[test]
    fn test_load_yaml_config_with_yaml_serde() {
        let dir = make_temp_test_dir("yaml");
        let cfg_path = dir.join("cfg.yaml");
        std::fs::write(
            &cfg_path,
            "my-callsign: F4JXQ\nrequire-callsign: true\nbandwidth: 2300\n",
        )
        .unwrap();
        let cfg = load_config(Some(&cfg_path)).unwrap();
        assert_eq!(cfg.my_callsign, "F4JXQ");
        assert!(cfg.require_callsign);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
