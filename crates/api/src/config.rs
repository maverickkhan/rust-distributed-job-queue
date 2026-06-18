//! Environment-driven API configuration.

use std::net::SocketAddr;

/// Configuration for the API server.
#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub database_url: String,
    pub bind_addr: SocketAddr,
    pub db_max_connections: u32,
    /// Max accepted request body size in bytes.
    pub max_body_bytes: usize,
    pub json_logs: bool,
}

impl ApiConfig {
    /// Load from the environment. `DATABASE_URL` is required; everything else
    /// has a sensible default.
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL environment variable is required"))?;
        let bind_addr = std::env::var("API_BIND_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid API_BIND_ADDR: {e}"))?;
        let db_max_connections = std::env::var("DB_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);
        let max_body_bytes = std::env::var("API_MAX_BODY_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024 * 1024); // 1 MiB
        let json_logs = std::env::var("LOG_JSON")
            .map(|v| v == "true")
            .unwrap_or(false);

        Ok(Self {
            database_url,
            bind_addr,
            db_max_connections,
            max_body_bytes,
            json_logs,
        })
    }
}
