//! Static dialplan router — routes calls based on a TOML file.
//!
//! This is the open-source-friendly backend that doesn't require the SIPcord API.
//! It reads a `dialplan.toml` file mapping extensions to Discord voice channels
//! and optional dynamic menu extensions that browse the bot's Discord guilds.
//!
//! Required env var: `DISCORD_BOT_TOKEN`
//!
//! Example `dialplan.toml`:
//! ```toml
//! [extensions]
//! 1000 = { guild = "123456789012345678", channel = "987654321012345678" }
//! 2000 = { guild = "123456789012345678", channel = "111222333444555666" }
//!
//! [menus.main]
//! extension = "8000"
//!
//! [phones]
//! 777 = { label = "Shop speakerphone" }
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
    Backend, CallError, CallStartedInfo, HangupCallRequest, MenuRoute, OutboundCallRequest,
    PhoneDirectoryEntry, RouteDecision,
};
use crate::services::snowflake::Snowflake;
use crate::transport::sip::DigestAuthParams;

#[derive(Deserialize, Clone)]
struct ExtensionTarget {
    guild: Snowflake,
    channel: Snowflake,
}

#[derive(Deserialize, Clone)]
struct MenuConfig {
    extension: String,
    #[serde(default = "default_menu_timeout_seconds")]
    timeout_seconds: u64,
    #[serde(default = "default_menu_max_attempts")]
    max_attempts: u8,
}

#[derive(Deserialize, Clone)]
struct PhoneConfig {
    label: String,
    extension: Option<String>,
}

#[derive(Deserialize)]
struct Dialplan {
    #[serde(default)]
    extensions: HashMap<String, ExtensionTarget>,
    #[serde(default)]
    menus: HashMap<String, MenuConfig>,
    #[serde(default)]
    phones: HashMap<String, PhoneConfig>,
}

fn default_menu_timeout_seconds() -> u64 {
    10
}

fn default_menu_max_attempts() -> u8 {
    3
}

/// Static file-based routing backend.
///
/// Routes calls by looking up the dialed extension in a TOML dialplan file.
/// No authentication is performed — any caller dialing a known extension is connected.
/// Outbound calls can also be queued by the self-host Discord `/call` command.
pub struct StaticBackend {
    bot_token: String,
    extensions: HashMap<String, ExtensionTarget>,
    menus: HashMap<String, MenuConfig>,
    phones: HashMap<String, PhoneConfig>,
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
        if !dialplan.menus.is_empty() {
            info!("Loaded {} dynamic menu(s)", dialplan.menus.len());
            for (id, menu) in &dialplan.menus {
                info!("  dynamic menu {} on ext {}", id, menu.extension);
            }
        }
        if !dialplan.phones.is_empty() {
            info!("Loaded {} phone directory entries", dialplan.phones.len());
            for (id, phone) in &dialplan.phones {
                let extension = phone.extension.as_deref().unwrap_or(id);
                info!("  phone {} -> {} ({})", id, extension, phone.label);
            }
        }

        Ok(Self {
            bot_token,
            extensions: dialplan.extensions,
            menus: dialplan.menus,
            phones: dialplan.phones,
            outbound_rx: Arc::new(Mutex::new(outbound_rx)),
            hangup_rx: Arc::new(Mutex::new(hangup_rx)),
        })
    }

    /// Phone directory entries exposed through the Discord `/directory` command.
    pub fn phone_directory(&self) -> Vec<PhoneDirectoryEntry> {
        let mut entries: Vec<PhoneDirectoryEntry> = self
            .phones
            .iter()
            .map(|(id, phone)| PhoneDirectoryEntry {
                id: id.clone(),
                label: phone.label.clone(),
                extension: phone.extension.clone().unwrap_or_else(|| id.clone()),
            })
            .collect();
        entries.sort_by(|a, b| a.label.to_ascii_lowercase().cmp(&b.label.to_ascii_lowercase()));
        entries
    }
}

#[async_trait]
impl Backend for StaticBackend {
    fn bot_token(&self) -> &str {
        &self.bot_token
    }

    async fn route_call(&self, _digest_auth: &DigestAuthParams, extension: &str) -> RouteDecision {
        if let Some((id, menu)) = self
            .menus
            .iter()
            .find(|(_, menu)| menu.extension == extension)
        {
            return RouteDecision::Menu {
                menu: MenuRoute {
                    id: id.clone(),
                    timeout_seconds: menu.timeout_seconds,
                    max_attempts: menu.max_attempts,
                },
            };
        }

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
    fn test_route_menu_extension() {
        let toml_content = r#"
[menus.main]
extension = "8000"
timeout_seconds = 7
max_attempts = 2
"#;
        let dir = std::env::temp_dir().join("sipcord_test_dialplan");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_route_menu.toml");
        std::fs::write(&path, toml_content).unwrap();

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (_hangup_tx, hangup_rx) = tokio::sync::mpsc::unbounded_channel();
        let backend = StaticBackend::load(&path, "tok".to_string(), rx, hangup_rx).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(async {
            let decision = backend
                .route_call(&DigestAuthParams::default(), "8000")
                .await;
            match decision {
                RouteDecision::Menu { menu } => {
                    assert_eq!(menu.id, "main");
                    assert_eq!(menu.timeout_seconds, 7);
                    assert_eq!(menu.max_attempts, 2);
                }
                _ => panic!("Expected Menu"),
            }
        });
    }

    #[test]
    fn test_load_phone_directory() {
        let toml_content = r#"
[phones]
777 = { label = "Shop speakerphone" }
desk = { label = "Desk phone", extension = "111" }
"#;
        let dir = std::env::temp_dir().join("sipcord_test_dialplan");
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("test_phone_directory.toml");
        std::fs::write(&path, toml_content).unwrap();

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (_hangup_tx, hangup_rx) = tokio::sync::mpsc::unbounded_channel();
        let backend = StaticBackend::load(&path, "tok".to_string(), rx, hangup_rx).unwrap();

        let directory = backend.phone_directory();
        assert_eq!(directory.len(), 2);
        assert_eq!(directory[0].id, "desk");
        assert_eq!(directory[0].label, "Desk phone");
        assert_eq!(directory[0].extension, "111");
        assert_eq!(directory[1].id, "777");
        assert_eq!(directory[1].extension, "777");
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
