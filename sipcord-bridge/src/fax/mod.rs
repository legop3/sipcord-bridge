//! Incoming fax support — receives faxes over SIP and posts images to Discord.
//!
//! Supports two transport modes:
//! - **G.711 passthrough**: Demodulates fax tones from audio samples (SpanDSP FaxState)
//! - **T.38 native**: Receives IFP packets via UDPTL (SpanDSP T38Terminal)
//!
//! Architecture:
//! - FaxSession: State machine managing a single fax reception (audio or T.38)
//! - DiscordPoster: Posts/edits messages in Discord text channels with fax images
//! - SpanDSP wrapper: FFI to SpanDSP for fax demodulation (FaxReceiver + FaxT38Receiver)
//! - audio_port: Conference bridge port for capturing SIP audio (G.711 mode)
//! - UDPTL: UDP transport for T.38 IFP packets

pub mod audio_port;
pub mod discord_poster;
pub mod session;
pub mod spandsp;
pub mod tiff_decoder;

#[derive(thiserror::Error, Debug)]
pub enum FaxError {
    #[error("Discord post failed: {0}")]
    Discord(#[from] serenity::Error),

    #[error("invalid Discord bot token: {0}")]
    InvalidToken(String),

    #[error("fax I/O ({context}): {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },

    #[error("path is not valid UTF-8: {0}")]
    NonUtf8Path(String),

    #[error("SpanDSP ({operation}): {detail}")]
    SpanDsp {
        operation: &'static str,
        detail: String,
    },

    #[error("TIFF decode: {0}")]
    Tiff(String),

    #[error("no pages in received fax")]
    NoPages,
}
