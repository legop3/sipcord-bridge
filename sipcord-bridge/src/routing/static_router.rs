//! Static dialplan router — routes calls based on a TOML file.
//!
//! This is the open-source-friendly backend that doesn't require the SIPcord API.
//! It reads a `dialplan.toml` file mapping extensions to Discord voice channels.
//!
//! Required env var: `DISCORD_BOT_TOKEN`
//!
//! Example `dialplan.toml`:
//! ```toml
//! [extensions]
//! 1000 = { guild = "123456789012345678", channel = "987654321012345678" }
//! 2000 = { guild = "123456789012345678", channel = "111222333444555666" }
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::info;

use crate::config::ConfigError;
use crate::routing::{
    Backend, CallError, CallStartedInfo, HangupCallRequest, OutboundCallRequest, RouteDecision,
};
use crate::services::snowflake::Snowflake;
use crate::transport::sip::DigestAuthParams;

#[derive(Deserialize, Clone)]
struct ExtensionTarget {
    guild: Snowflake,
    channel: Snowflake,
}

#[derive(Deserialize)]
struct Dialplan {
    extensions: HashMap<String, ExtensionTarget>,
}

/// Static file-based routing backend.
///
/// Routes calls by looking up the dialed extension in a TOML dialplan file.
/// No authentication is performed — any caller dialing a known extension is connected.
/// Outbound calls can also be queued by the self-host Discord `/call` command.
pub struct StaticBackend {
    bot_token: String,
    extensions: HashMap<String, ExtensionTarget>,
    outbound_rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<OutboundCallRequest>>>,
    hangup_rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<HangupCallRequest>>>,
}

impl StaticBackend {
    /// Load the dialplan from a TOML file. `bot_token` comes from the environment.
    pub fn load(
        path: &Path,
        bot_token: String,
        outbound_rx: tokio::sync::mpsc::UnboundedReceiver<OutboundCallRequest>,
        hangup_rx: tokio::sync::mpsc::UnboundedReceiver<HangupCallRequest>,
    ) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let dialplan: Dialplan = toml::from_str(&content).map_err(|source| ConfigError::TomlParse {
            path: path.to_path_buf(),
            source,
        })?;

        info!(
            "Loaded dialplan from {} ({} extensions)",
            path.display(),
            dialplan.extensions.len(),
        );
        for (ext, target) in &dialplan.extensions {
            info!(
                "  ext {} -> guild {} channel {}",
                ext, target.guild, target.channel
            );
        }

        Ok(Self {
            bot_token,
            extensions: dialplan.extensions,
            outbound_rx: Arc::new(Mutex::new(outbound_rx)),
            hangup_rx: Arc::new(Mutex::new(hangup_rx)),
        })
    }
}

#[async_trait]
impl Backend for StaticBackend {
    fn bot_token(&self) -> &str {
        &self.bot_token
    }

    async fn route_call(&self, _digest_auth: &DigestAuthParams, extension: &str) -> RouteDecision {
        match self.extensions.get(extension) {
            Some(target) => RouteDecision::Connect {
                channel_id: target.channel,
                guild_id: target.guild,
                user_id: "static".to_string(),
                bot_token: self.bot_token.clone(),
            },
            None => {
                tracing::warn!("Extension {} not found in dialplan", extension);
                RouteDecision::RejectWithError {
                    error: CallError::NoChannelMapping,
                }
            }
        }
    }

    async fn on_call_started(&self, info: &CallStartedInfo) {
        info!(
            "Call started: {} -> channel {} (ext {})",
            info.sip_call_id, info.channel_id, info.extension
        );
    }

    async fn on_call_ended(&self, sip_call_id: &str) {
        info!("Call ended: {}", sip_call_id);
    }

    async fn heartbeat(&self, _active_channel_ids: &[String]) {}

    fn report_call_status(&self, _call_id: &str, _status: &str) {}

    async fn next_outbound_request(&self) -> Option<OutboundCallRequest> {
        self.outbound_rx.lock().await.recv().await
    }

    async fn next_hangup_request(&self) -> Option<HangupCallRequest> {
        self.hangup_rx.lock().await.recv().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_valid_dialplan() {
        let toml_content = r#"
[extensions]
1000 = { guild = "123456789012345678", channel = "987654321012345678" }
2000 = { guild = "123456789012345678", channel = "111222333444555666" }
"#;
        let dir = std::env::temp_dir().join("sipcord_test_dialplan");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_dialplan.toml");
        std::fs::write(&path, toml_content).unwrap();

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (_hangup_tx, hangup_rx) = tokio::sync::mpsc::unbounded_channel();
        let backend =
            StaticBackend::load(&path, "test_token".to_string(), rx, hangup_rx).unwrap();
        assert_eq!(backend.extensions.len(), 2);
        assert!(backend.extensions.contains_key("1000"));
        assert!(backend.extensions.contains_key("2000"));
    }

    #[test]
    fn test_route_known_extension() {
        let toml_content = r#"
[extensions]
1000 = { guild = 111, channel = 222 }
"#;
        let dir = std::env::temp_dir().join("sipcord_test_dialplan");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_route.toml");
        std::fs::write(&path, toml_content).unwrap();

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (_hangup_tx, hangup_rx) = tokio::sync::mpsc::unbounded_channel();
        let backend = StaticBackend::load(&path, "tok".to_string(), rx, hangup_rx).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let decision = backend
                .route_call(&DigestAuthParams::default(), "1000")
                .await;
            match decision {
                RouteDecision::Connect { channel_id, .. } => {
                    assert_eq!(channel_id, Snowflake::new(222));
                }
                _ => panic!("Expected Connect"),
            }
        });
    }

    #[test]
    fn test_route_unknown_extension() {
        let toml_content = r#"
[extensions]
1000 = { guild = 111, channel = 222 }
"#;
        let dir = std::env::temp_dir().join("sipcord_test_dialplan");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_route_unknown.toml");
        std::fs::write(&path, toml_content).unwrap();

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (_hangup_tx, hangup_rx) = tokio::sync::mpsc::unbounded_channel();
        let backend = StaticBackend::load(&path, "tok".to_string(), rx, hangup_rx).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let decision = backend
                .route_call(&DigestAuthParams::default(), "9999")
                .await;
            match decision {
                RouteDecision::RejectWithError { error } => {
                    assert!(matches!(error, CallError::NoChannelMapping));
                }
                _ => panic!("Expected RejectWithError"),
            }
        });
    }

    #[test]
    fn test_load_malformed_toml() {
        let dir = std::env::temp_dir().join("sipcord_test_dialplan");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_bad.toml");
        std::fs::write(&path, "this is not valid toml [[[").unwrap();

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (_hangup_tx, hangup_rx) = tokio::sync::mpsc::unbounded_channel();
        let result = StaticBackend::load(&path, "tok".to_string(), rx, hangup_rx);
        assert!(result.is_err());
    }
}
