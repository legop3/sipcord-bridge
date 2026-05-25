//! Sound management for SIP call audio
//!
//! Provides a SoundManager that loads sounds from config.toml with two modes:
//! - Preloaded: Loaded into memory at startup for fast playback (system sounds)
//! - Streaming: Loaded on-demand from disk for large files (easter eggs)
//!
//! All audio files must be pre-resampled to 16kHz mono - no runtime resampling.

mod streaming;

use crate::audio::{AudioParseError, flac, wav};
use crate::config::{AppConfig, SoundEntry};
use crate::transport::sip::CONF_SAMPLE_RATE;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

pub use streaming::{StreamingError, StreamingPlayer};

#[derive(thiserror::Error, Debug)]
pub enum SoundError {
    #[error("failed to read sound file {path:?}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse audio for {name}: {source}")]
    Parse {
        name: String,
        #[source]
        source: AudioParseError,
    },

    #[error("sound {name} has wrong sample rate: {got} Hz (expected {expected} Hz)")]
    WrongSampleRate {
        name: String,
        got: u32,
        expected: u32,
    },

    #[error("unknown audio format for {name}: header bytes {header:02x?}")]
    UnknownFormat { name: String, header: Vec<u8> },

    #[error(transparent)]
    Streaming(#[from] StreamingError),
}

/// A preloaded sound ready for immediate playback
#[derive(Debug, Clone)]
pub struct PreloadedSound {
    /// PCM samples at 16kHz mono - NO RESAMPLING at runtime
    pub samples: Arc<Vec<i16>>,
    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Configuration for a streaming sound (loaded on-demand)
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Full path to the audio file
    pub path: PathBuf,
}

/// Sound manager for loading and playing audio files
pub struct SoundManager {
    /// Preloaded sounds (preload=true) - in memory, ready for playback
    preloaded: HashMap<String, PreloadedSound>,
    /// Streaming configs (preload=false) - path only, loaded on demand
    streaming: HashMap<String, StreamingConfig>,
    /// Extension -> sound name mapping for easter eggs
    pub extension_map: HashMap<u32, String>,
    /// Base directory for sound files
    sounds_dir: PathBuf,
}

impl SoundManager {
    /// Create a new SoundManager and load sounds from config
    pub fn new(sounds_dir: PathBuf) -> Result<Self, SoundError> {
        let config = AppConfig::global();
        let mut manager = Self {
            preloaded: HashMap::new(),
            streaming: HashMap::new(),
            extension_map: HashMap::new(),
            sounds_dir,
        };

        manager.load_sounds(&config.sounds.entries)?;
        Ok(manager)
    }

    /// Load all sounds from config entries
    fn load_sounds(&mut self, entries: &HashMap<String, SoundEntry>) -> Result<(), SoundError> {
        let mut preloaded_count = 0;
        let mut streaming_count = 0;
        let mut virtual_count = 0;

        for (name, entry) in entries {
            // Build extension map for easter eggs and test tones
            if let Some(ext) = entry.extension {
                self.extension_map.insert(ext, name.clone());
                debug!("Registered extension {} -> sound '{}'", ext, name);
            }

            // Handle virtual sounds (no src file - generated dynamically)
            let Some(ref src) = entry.src else {
                virtual_count += 1;
                info!("Registered virtual sound '{}' (no file, generated)", name);
                continue;
            };

            let file_path = self.sounds_dir.join(src);

            if entry.preload {
                // Load and store in memory
                match self.load_preloaded_sound(&file_path, name) {
                    Ok(sound) => {
                        info!(
                            "Preloaded sound '{}': {} samples ({} ms) from {}",
                            name,
                            sound.samples.len(),
                            sound.duration_ms,
                            src
                        );
                        self.preloaded.insert(name.clone(), sound);
                        preloaded_count += 1;
                    }
                    Err(e) => {
                        warn!("Failed to preload sound '{}' from {}: {}", name, src, e);
                    }
                }
            } else {
                // Just store path for streaming
                if file_path.exists() {
                    self.streaming.insert(
                        name.clone(),
                        StreamingConfig {
                            path: file_path.clone(),
                        },
                    );
                    streaming_count += 1;
                    info!("Registered streaming sound '{}' from {}", name, src);
                } else {
                    warn!(
                        "Streaming sound '{}' file not found: {}",
                        name,
                        file_path.display()
                    );
                }
            }
        }

        info!(
            "SoundManager loaded {} preloaded, {} streaming, {} virtual sounds, {} extensions",
            preloaded_count,
            streaming_count,
            virtual_count,
            self.extension_map.len()
        );

        Ok(())
    }

    /// Load a preloaded sound from a file
    fn load_preloaded_sound(
        &self,
        path: &Path,
        name: &str,
    ) -> Result<PreloadedSound, SoundError> {
        let data = std::fs::read(path).map_err(|source| SoundError::Read {
            path: path.to_path_buf(),
            source,
        })?;

        let samples = self.parse_audio(&data, name)?;

        let duration_ms = (samples.len() as u64 * 1000) / CONF_SAMPLE_RATE as u64;

        Ok(PreloadedSound {
            samples: Arc::new(samples),
            duration_ms,
        })
    }

    /// Parse audio data (auto-detect WAV or FLAC format).
    /// Expects 16kHz mono — returns `WrongSampleRate` otherwise.
    fn parse_audio(&self, data: &[u8], name: &str) -> Result<Vec<i16>, SoundError> {
        // Check for FLAC magic number: "fLaC"
        if data.len() >= 4 && &data[0..4] == b"fLaC" {
            debug!("Detected FLAC format for '{}'", name);
            let (samples, rate) = flac::parse_flac(data).map_err(|source| SoundError::Parse {
                name: name.to_string(),
                source,
            })?;
            if rate != CONF_SAMPLE_RATE {
                return Err(SoundError::WrongSampleRate {
                    name: name.to_string(),
                    got: rate,
                    expected: CONF_SAMPLE_RATE,
                });
            }
            return Ok(samples);
        }

        // Check for WAV magic number: "RIFF"
        if data.len() >= 4 && &data[0..4] == b"RIFF" {
            debug!("Detected WAV format for '{}'", name);
            let (samples, rate) = wav::parse_wav(data).map_err(|source| SoundError::Parse {
                name: name.to_string(),
                source,
            })?;
            if rate != CONF_SAMPLE_RATE {
                return Err(SoundError::WrongSampleRate {
                    name: name.to_string(),
                    got: rate,
                    expected: CONF_SAMPLE_RATE,
                });
            }
            return Ok(samples);
        }

        Err(SoundError::UnknownFormat {
            name: name.to_string(),
            header: data[..4.min(data.len())].to_vec(),
        })
    }

    /// Get a preloaded sound by name
    pub fn get_preloaded(&self, name: &str) -> Option<&PreloadedSound> {
        self.preloaded.get(name)
    }

    /// Get a streaming config by name
    pub fn get_streaming(&self, name: &str) -> Option<&StreamingConfig> {
        self.streaming.get(name)
    }

    /// Check if a sound is configured for streaming
    pub fn is_streaming(&self, name: &str) -> bool {
        self.streaming.contains_key(name)
    }

    /// Check if a sound is a virtual sound (test tone)
    pub fn is_test_tone(&self, name: &str) -> bool {
        name == "test_tone"
    }

    /// Get the sound name for an extension (if configured)
    pub fn get_extension_sound(&self, extension: u32) -> Option<&str> {
        self.extension_map.get(&extension).map(|s| s.as_str())
    }

    /// Get the connecting sound samples (used for early media loop)
    pub fn get_connecting_samples(&self) -> Option<Arc<Vec<i16>>> {
        self.preloaded.get("connecting").map(|s| s.samples.clone())
    }

    /// Get the discord_join sound samples
    pub fn get_discord_join_samples(&self) -> Option<Arc<Vec<i16>>> {
        self.preloaded
            .get("discord_join")
            .map(|s| s.samples.clone())
    }

    /// Get error sound samples by error type
    pub fn get_error_samples(&self, error_type: &str) -> Option<Arc<Vec<i16>>> {
        self.preloaded.get(error_type).map(|s| s.samples.clone())
    }
}

/// Create an Arc-wrapped SoundManager for sharing across async tasks
pub fn create_sound_manager(sounds_dir: PathBuf) -> Result<Arc<SoundManager>, SoundError> {
    Ok(Arc::new(SoundManager::new(sounds_dir)?))
}
