//! Sipcord Bridge - SIP to Discord Voice Bridge
//!
//! A generic SIP-to-Discord voice bridge library. Provides all the core
//! functionality for bridging SIP phone calls to Discord voice channels,
//! including fax (G.711 and T.38) support.
//!
//! Backends implement the `routing::Backend` trait to control call routing
//! and authentication. A built-in `StaticBackend` (TOML dialplan) is included.

#![feature(portable_simd)]
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod audio;
pub mod call;
pub mod config;
pub mod error;
pub mod fax;
pub mod routing;
pub mod services;
pub mod transport;

pub use error::BridgeError;
