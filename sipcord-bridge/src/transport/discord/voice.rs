//! Voice/audio utilities for Discord

use parking_lot::Mutex;
use rtrb::Consumer;
use songbird::input::{Input, RawAdapter};
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use symphonia_core::io::MediaSource;

/// Discord expects 48kHz stereo audio
pub const DISCORD_SAMPLE_RATE: u32 = 48000;
const DISCORD_CHANNELS: u16 = 2;

/// Ring buffer capacity in samples (f32 stereo pairs)
pub fn ring_buffer_samples() -> usize {
    use std::sync::OnceLock;
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| crate::config::AppConfig::audio().ring_buffer_samples)
}

/// Pre-buffer threshold in samples before we start outputting
fn pre_buffer_samples() -> usize {
    use std::sync::OnceLock;
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| crate::config::AppConfig::audio().pre_buffer_samples)
}

/// Resample audio from one sample rate to another using linear interpolation.
/// This is a fallback - prefer using the rubato sinc resampler for quality.
pub fn resample_audio(samples: &[i16], from_rate: u32, to_rate: u32) -> Vec<i16> {
    let mut output = Vec::new();
    resample_audio_into(samples, from_rate, to_rate, &mut output);
    output
}

/// Resample audio into a pre-allocated buffer (avoids per-call Vec allocation).
/// Clears `output` and fills it with resampled data.
pub fn resample_audio_into(samples: &[i16], from_rate: u32, to_rate: u32, output: &mut Vec<i16>) {
    output.clear();

    if from_rate == to_rate {
        output.extend_from_slice(samples);
        return;
    }

    if samples.is_empty() {
        return;
    }

    let ratio = from_rate as f64 / to_rate as f64;
    let output_len = ((samples.len() as f64) / ratio).ceil() as usize;
    output.reserve(output_len.saturating_sub(output.capacity()));

    for i in 0..output_len {
        let src_pos = i as f64 * ratio;
        let src_idx = src_pos as usize;
        let frac = src_pos - src_idx as f64;

        let sample = if src_idx + 1 < samples.len() {
            let s0 = samples[src_idx] as f64;
            let s1 = samples[src_idx + 1] as f64;
            (s0 + (s1 - s0) * frac) as i16
        } else if src_idx < samples.len() {
            samples[src_idx]
        } else {
            0
        };

        output.push(sample);
    }
}

/// Streaming audio source using a lock-free ring buffer.
///
/// This implements Symphonia's MediaSource trait and provides raw f32 PCM data.
/// Uses rtrb for lock-free, wait-free audio streaming - no spinning or blocking.
///
/// The Mutex around Consumer is required to satisfy MediaSource's Sync bound,
/// but since only one thread (Songbird's audio thread) ever accesses it,
/// there's never any contention - it's essentially just satisfying the type system.
pub struct StreamingAudioSource {
    /// Ring buffer consumer for audio samples (f32)
    /// Wrapped in Mutex to satisfy Sync bound (no actual contention)
    consumer: Mutex<Consumer<f32>>,
    /// Read count for logging (atomic — single reader, no contention)
    read_count: AtomicU64,
    /// Whether we've pre-buffered enough to start output (atomic — single reader)
    pre_buffered: AtomicBool,
    /// Underrun count for diagnostics (atomic — single reader)
    underrun_count: AtomicU64,
}

impl StreamingAudioSource {
    /// Create a new streaming audio source with ring buffer.
    ///
    /// Returns the source and the rtrb Producer to push audio samples.
    /// Samples should be f32 interleaved stereo at 48kHz, normalized to [-1.0, 1.0].
    pub fn new() -> (Self, rtrb::Producer<f32>) {
        let (producer, consumer) = rtrb::RingBuffer::new(ring_buffer_samples());

        (
            Self {
                consumer: Mutex::new(consumer),
                read_count: AtomicU64::new(0),
                pre_buffered: AtomicBool::new(false),
                underrun_count: AtomicU64::new(0),
            },
            producer,
        )
    }

    /// Create a Songbird Input from this streaming source
    pub fn into_input(self) -> Input {
        RawAdapter::new(self, DISCORD_SAMPLE_RATE, DISCORD_CHANNELS as u32).into()
    }
}

impl Read for StreamingAudioSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let count = self.read_count.fetch_add(1, Ordering::Relaxed) + 1;

        // How many f32 samples can we fit in the output buffer?
        let samples_requested = buf.len() / 4; // 4 bytes per f32

        let mut consumer = self.consumer.lock();
        let samples_available = consumer.slots();

        // Pre-buffering: wait until ring buffer has enough before starting
        if !self.pre_buffered.load(Ordering::Relaxed) {
            if samples_available >= pre_buffer_samples() {
                self.pre_buffered.store(true, Ordering::Relaxed);
                let ms_buffered = samples_available as f64 / 48000.0 / 2.0 * 1000.0;
                tracing::info!(
                    "StreamingAudioSource: Pre-buffer complete ({} samples, {:.0}ms), starting output",
                    samples_available,
                    ms_buffered
                );
            } else {
                // Still pre-buffering - return silence
                if count.is_multiple_of(50) {
                    let ms_buffered = samples_available as f64 / 48000.0 / 2.0 * 1000.0;
                    let ms_target = pre_buffer_samples() as f64 / 48000.0 / 2.0 * 1000.0;
                    tracing::debug!(
                        "StreamingAudioSource: Pre-buffering {}/{} samples ({:.0}ms / {:.0}ms)",
                        samples_available,
                        pre_buffer_samples(),
                        ms_buffered,
                        ms_target
                    );
                }
                buf.fill(0);
                return Ok(buf.len());
            }
        }

        // Log buffer status periodically
        if count.is_multiple_of(50) {
            let ms_buffered = samples_available as f64 / 48000.0 / 2.0 * 1000.0;
            tracing::debug!(
                "StreamingAudioSource #{}: ring buffer has {} samples ({:.1}ms), Songbird wants {} samples",
                count,
                samples_available,
                ms_buffered,
                samples_requested
            );
        }

        // Read as many samples as we can from ring buffer
        let samples_to_read = samples_requested.min(samples_available);

        if samples_to_read == 0 {
            // Buffer empty - underrun
            let underruns = self.underrun_count.fetch_add(1, Ordering::Relaxed) + 1;
            if underruns <= 5 || underruns.is_multiple_of(100) {
                tracing::warn!(
                    "StreamingAudioSource: Ring buffer empty, filling with silence (underruns: {})",
                    underruns
                );
            }
            buf.fill(0);
            return Ok(buf.len());
        }

        // `samples_to_read <= samples_available` by construction, so this
        // should never error; on rtrb desync, silence rather than panic.
        let chunk = match consumer.read_chunk(samples_to_read) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    "StreamingAudioSource: rtrb read_chunk unexpectedly failed ({:?}), filling with silence",
                    e
                );
                buf.fill(0);
                return Ok(buf.len());
            }
        };
        let (first, second) = chunk.as_slices();

        // Bulk copy f32 samples as raw bytes (memcpy instead of per-sample loop)
        let first_bytes =
            unsafe { std::slice::from_raw_parts(first.as_ptr() as *const u8, first.len() * 4) };
        buf[..first_bytes.len()].copy_from_slice(first_bytes);
        if !second.is_empty() {
            let second_bytes = unsafe {
                std::slice::from_raw_parts(second.as_ptr() as *const u8, second.len() * 4)
            };
            buf[first_bytes.len()..first_bytes.len() + second_bytes.len()]
                .copy_from_slice(second_bytes);
        }
        chunk.commit_all();

        // Fill remainder with silence if we didn't have enough
        let bytes_written = samples_to_read * 4;
        if bytes_written < buf.len() {
            buf[bytes_written..].fill(0);
            if count.is_multiple_of(50) || count < 10 {
                let silence_samples = (buf.len() - bytes_written) / 4;
                tracing::debug!(
                    "StreamingAudioSource #{}: Partial read, filled {} samples with silence",
                    count,
                    silence_samples
                );
            }
        }

        Ok(buf.len())
    }
}

impl Seek for StreamingAudioSource {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        // Live streams are not seekable
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Live streams are not seekable",
        ))
    }
}

impl MediaSource for StreamingAudioSource {
    fn is_seekable(&self) -> bool {
        false
    }

    fn byte_len(&self) -> Option<u64> {
        None // Unknown length for live streams
    }
}

impl Drop for StreamingAudioSource {
    fn drop(&mut self) {
        tracing::debug!("StreamingAudioSource dropped (call ending)");
    }
}
