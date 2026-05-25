//! Audio parsing utilities for WAV and FLAC, used by the sound module.

pub mod flac;
pub mod simd;
pub mod wav;

#[derive(thiserror::Error, Debug)]
pub enum AudioParseError {
    #[error("malformed audio data: {0}")]
    Malformed(String),

    #[error("unsupported audio: {0}")]
    Unsupported(String),

    #[error("FLAC decode error: {0}")]
    Flac(#[from] claxon::Error),
}
