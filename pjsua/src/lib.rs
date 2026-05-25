//! Rust bindings for pjsua (pjproject SIP library)
//!
//! This crate provides low-level FFI bindings to pjsua, generated via bindgen.
//! The pjproject library is built from source automatically if not found on the system.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]
#![allow(clippy::all)]

mod pjsua {
    #![allow(unnecessary_transmutes)]
    #![allow(unsafe_op_in_unsafe_fn)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub use pjsua::*;
