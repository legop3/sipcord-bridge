//! Helpers around the pjsua/pjsip C-string and header-building idioms that
//! recur across the SIP transport layer.

use crate::transport::sip::error::SipResponseError;
use pjsua::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

/// Convert a [`CStr`] (typically a `c"..."` literal) into a [`pj_str_t`].
///
/// Caller must keep `s` alive for the resulting `pj_str_t`'s usage window.
#[inline]
pub unsafe fn pj_str_from_cstr(s: &CStr) -> pj_str_t {
    unsafe { pj_str(s.as_ptr() as *mut c_char) }
}

/// Initialise a `pjsip_hdr` as an empty list head.
#[inline]
pub unsafe fn pj_list_init_hdr(hdr: *mut pjsip_hdr) {
    unsafe {
        (*hdr).next = hdr as *mut _;
        (*hdr).prev = hdr as *mut _;
    }
}

/// Create a generic string header in `pool`.
///
/// pjsip duplicates `name` and `value` into the pool, so the temporary
/// `CString` for `value` is dropped before this returns.
pub unsafe fn make_string_hdr(
    pool: *mut pj_pool_t,
    name: &CStr,
    value: &str,
) -> Result<*mut pjsip_generic_string_hdr, SipResponseError> {
    unsafe {
        let value_c = CString::new(value)?;
        let name_pj = pj_str_from_cstr(name);
        let value_pj = pj_str(value_c.as_ptr() as *mut c_char);
        let hdr = pjsip_generic_string_hdr_create(pool, &name_pj, &value_pj);
        if hdr.is_null() {
            return Err(SipResponseError::HeaderCreate);
        }
        Ok(hdr)
    }
}

/// Append a generic string header onto the message buffer in `tdata`,
/// allocating from the tdata's own pool.
pub unsafe fn append_tdata_hdr(
    tdata: *mut pjsip_tx_data,
    name: &CStr,
    value: &str,
) -> Result<(), SipResponseError> {
    unsafe {
        let hdr = make_string_hdr((*tdata).pool, name, value)?;
        pj_list_insert_before(
            &mut (*(*tdata).msg).hdr as *mut pjsip_hdr as *mut pj_list_type,
            hdr as *mut pj_list_type,
        );
        Ok(())
    }
}

/// Answer a pjsua call with N custom headers attached to the response.
///
/// The pool is intentionally NOT released — pjsua continues referencing the
/// header data after `pjsua_call_answer` returns, so releasing here triggers
/// use-after-free. Each call leaks ~512 bytes, reclaimed when pjsua shuts down.
///
/// The caller is responsible for `pjsua_call_hangup` on Err.
pub unsafe fn answer_call_with_headers(
    call_id: i32,
    status_code: u32,
    reason: &CStr,
    pool_name: &CStr,
    headers: &[(&CStr, &str)],
) -> Result<(), SipResponseError> {
    unsafe {
        let mut msg_data = std::mem::MaybeUninit::<pjsua_msg_data>::uninit();
        pjsua_msg_data_init(msg_data.as_mut_ptr());
        let msg_data_ptr = msg_data.assume_init_mut();

        let pool = pjsua_pool_create(pool_name.as_ptr(), 512, 512);
        if pool.is_null() {
            return Err(SipResponseError::PoolAlloc);
        }

        for (name, value) in headers {
            let hdr = make_string_hdr(pool, name, value)?;
            pj_list_insert_before(
                &mut msg_data_ptr.hdr_list as *mut _ as *mut pj_list_type,
                hdr as *mut pj_list_type,
            );
        }

        let reason_pj = pj_str_from_cstr(reason);
        let status = pjsua_call_answer(call_id, status_code, &reason_pj, msg_data_ptr);
        if status != pj_constants__PJ_SUCCESS as i32 {
            return Err(SipResponseError::CallAnswer(status));
        }
        Ok(())
    }
}

/// Send a stateless SIP response with N string headers.
///
/// `reason` is the SIP reason phrase (e.g. `Some(c"Unauthorized")`); pass
/// `None` to let pjsip pick the default for `status_code`.
pub unsafe fn respond_stateless_with_headers(
    rdata: *mut pjsip_rx_data,
    status_code: u16,
    reason: Option<&CStr>,
    headers: &[(&CStr, &str)],
) -> Result<(), SipResponseError> {
    unsafe {
        let endpt = pjsua_get_pjsip_endpt();
        if endpt.is_null() {
            return Err(SipResponseError::EndpointNull);
        }

        let pool = pjsua_pool_create(c"sip_resp".as_ptr(), 1024, 1024);
        if pool.is_null() {
            return Err(SipResponseError::PoolAlloc);
        }

        // Closure so the pool gets released even if a step `?`-returns.
        let result =
            (|| -> Result<i32, SipResponseError> {
                let hdr_list =
                    pj_pool_alloc(pool, std::mem::size_of::<pjsip_hdr>()) as *mut pjsip_hdr;
                if hdr_list.is_null() {
                    return Err(SipResponseError::PoolAlloc);
                }
                pj_list_init_hdr(hdr_list);

                for (name, value) in headers {
                    let hdr = make_string_hdr(pool, name, value)?;
                    pj_list_insert_before(
                        hdr_list as *mut pj_list_type,
                        hdr as *mut pj_list_type,
                    );
                }

                let reason_pj = reason.map(|r| pj_str_from_cstr(r));
                let reason_ptr = reason_pj
                    .as_ref()
                    .map(|r| r as *const pj_str_t)
                    .unwrap_or(ptr::null());

                Ok(pjsip_endpt_respond_stateless(
                    endpt,
                    rdata,
                    status_code.into(),
                    reason_ptr,
                    hdr_list,
                    ptr::null(),
                ))
            })();

        pj_pool_release(pool);

        match result {
            Ok(status) if status == pj_constants__PJ_SUCCESS as i32 => Ok(()),
            Ok(status) => Err(SipResponseError::StatelessSend(status)),
            Err(e) => Err(e),
        }
    }
}
