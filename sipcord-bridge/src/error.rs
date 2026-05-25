//! Top-level error type for the `sipcord-bridge` crate.

use crate::audio::AudioParseError;
use crate::config::ConfigError;
use crate::fax::FaxError;
use crate::routing::CallError;
use crate::services::sound::SoundError;
use crate::transport::discord::DiscordError;
use crate::transport::sip::error::SipError;

#[derive(thiserror::Error, Debug)]
pub enum BridgeError {
    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Sip(#[from] SipError),

    #[error(transparent)]
    Discord(#[from] DiscordError),

    #[error(transparent)]
    Routing(#[from] CallError),

    #[error(transparent)]
    Fax(#[from] FaxError),

    #[error(transparent)]
    Sound(#[from] SoundError),

    #[error(transparent)]
    AudioParse(#[from] AudioParseError),

    #[error("I/O ({context}): {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}
