//! Typed error types for the SIP transport layer.

use std::ffi::NulError;

/// Umbrella error for everything the SIP transport layer can return.
#[derive(thiserror::Error, Debug)]
pub enum SipError {
    #[error(transparent)]
    Init(#[from] SipInitError),

    #[error(transparent)]
    Response(#[from] SipResponseError),

    #[error(transparent)]
    Audio(#[from] SipAudioError),

    #[error(transparent)]
    Call(#[from] SipCallError),
}

/// Errors raised by outbound-call setup (`make_outbound_call`).
#[derive(thiserror::Error, Debug)]
pub enum SipCallError {
    #[error("invalid {field} for outbound call: {source}")]
    InvalidString {
        field: &'static str,
        #[source]
        source: NulError,
    },

    #[error("pjsua_call_make_call failed (status {0})")]
    MakeCall(i32),
}

/// Errors raised by `init_pjsua`, `create_tls_transport`, `reload_tls_transport`,
/// `process_pjsua_events`, and friends.
#[derive(thiserror::Error, Debug)]
pub enum SipInitError {
    /// A pjsua API returned a non-success status code; `operation` names the
    /// specific call (e.g. `"pjsua_init"`, `"pjsua_acc_add"`).
    #[error("pjsua {operation} failed (status {status})")]
    Pjsua {
        operation: &'static str,
        status: i32,
    },

    /// `pjsua_transport_create` failed; `kind` is `"UDP"`, `"TCP"`, or `"TLS"`.
    #[error("transport create ({kind}) failed (status {status})")]
    TransportCreate {
        kind: &'static str,
        status: i32,
    },

    #[error("invalid {field} string for FFI: {source}")]
    InvalidString {
        field: &'static str,
        #[source]
        source: NulError,
    },

    #[error("{field} path is not valid UTF-8")]
    NonUtf8Path { field: &'static str },
}

/// Errors raised by audio-port plumbing (players, conf port hookup).
#[derive(thiserror::Error, Debug)]
pub enum SipAudioError {
    /// Media negotiation hasn't produced a conference port yet (or the call
    /// has just ended). Caller may retry or drop the audio.
    #[error("no conference port for call {call_id} (media not ready yet)")]
    NoConfPort { call_id: super::ffi::types::CallId },

    #[error("invalid port name: {0}")]
    InvalidPortName(#[from] NulError),

    #[error("pjsua conf {operation} failed (status {status})")]
    Pjsua {
        operation: &'static str,
        status: i32,
    },

    #[error("frame mismatch: {0}")]
    FrameMismatch(String),

    #[error(transparent)]
    Streaming(#[from] crate::services::sound::StreamingError),
}

/// Errors raised while building or sending a SIP response from inside an
/// FFI callback. The typical caller logs and continues.
#[derive(thiserror::Error, Debug)]
pub enum SipResponseError {
    #[error("CString conversion failed (interior NUL)")]
    CStringNul(#[from] NulError),

    #[error("pjsua pool allocation failed")]
    PoolAlloc,

    #[error("pjsip header creation failed")]
    HeaderCreate,

    #[error("pjsip endpoint is null (pjsua not initialised)")]
    EndpointNull,

    #[error("pjsip stateless send failed (status {0})")]
    StatelessSend(i32),

    #[error("pjsip UAS transaction creation failed (status {0})")]
    TsxCreate(i32),

    #[error("pjsip response build failed (status {0})")]
    ResponseBuild(i32),

    #[error("pjsua_call_answer failed (status {0})")]
    CallAnswer(i32),
}
