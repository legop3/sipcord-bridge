//! Utility functions for pjsua wrapper

use pjsua::pj_str_t;

/// Convert a pj_str_t to a Rust String
///
/// # Safety
/// The pj_str_t must point to valid memory for `slen` bytes.
pub unsafe fn pj_str_to_string(s: &pj_str_t) -> String {
    if s.ptr.is_null() || s.slen <= 0 {
        return String::new();
    }

    let slice = unsafe { std::slice::from_raw_parts(s.ptr as *const u8, s.slen as usize) };
    String::from_utf8_lossy(slice).to_string()
}

/// Extract username from SIP URI (e.g., "<sip:username@domain>" -> "username")
pub fn extract_sip_username(uri: &str) -> String {
    // Remove angle brackets if present
    let uri = uri.trim_start_matches('<').trim_end_matches('>');

    // Remove "sip:" prefix
    let uri = uri.strip_prefix("sip:").unwrap_or(uri);

    // Take everything before @ as username
    if let Some(at_pos) = uri.find('@') {
        uri[..at_pos].to_string()
    } else {
        uri.to_string()
    }
}

/// Extract display name from SIP URI if present (e.g., "Alice Smith" <sip:alice@example.com> -> "Alice Smith")
pub fn extract_display_name(uri: &str) -> String {
    let uri = uri.trim();

    // Check if there's a quoted display name before the angle bracket
    if let Some(angle_pos) = uri.find('<') {
        let display_part = uri[..angle_pos].trim();

        // Remove surrounding quotes if present
        let display_name = display_part
            .trim_start_matches('"')
            .trim_end_matches('"')
            .trim();

        return display_name.to_string();
    }

    // No angle brackets found - no display name
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_sip_username_full_uri() {
        assert_eq!(extract_sip_username("<sip:alice@example.com>"), "alice");
    }

    #[test]
    fn test_extract_sip_username_no_brackets() {
        assert_eq!(extract_sip_username("sip:bob@domain"), "bob");
    }

    #[test]
    fn test_extract_sip_username_no_sip_prefix() {
        assert_eq!(extract_sip_username("charlie@host"), "charlie");
    }

    #[test]
    fn test_extract_sip_username_no_at() {
        assert_eq!(extract_sip_username("sip:dave"), "dave");
    }

    #[test]
    fn test_extract_sip_username_with_port() {
        assert_eq!(extract_sip_username("<sip:eve@host:5060>"), "eve");
    }

    #[test]
    fn test_extract_sip_username_empty() {
        assert_eq!(extract_sip_username(""), "");
    }

    #[test]
    fn test_extract_display_name_with_quotes_and_uri() {
        assert_eq!(
            extract_display_name("\"Alice Smith\" <sip:alice@example.com>"),
            "Alice Smith"
        );
    }

    #[test]
    fn test_extract_display_name_without_quotes() {
        assert_eq!(
            extract_display_name("Bob Jones <sip:bob@example.com>"),
            "Bob Jones"
        );
    }

    #[test]
    fn test_extract_display_name_no_display_name() {
        assert_eq!(extract_display_name("<sip:charlie@example.com>"), "");
    }

    #[test]
    fn test_extract_display_name_uri_only() {
        assert_eq!(extract_display_name("sip:dave@example.com"), "");
    }

    #[test]
    fn test_extract_display_name_with_extra_spaces() {
        assert_eq!(
            extract_display_name("  \"Eve Adams\"  <sip:eve@example.com>  "),
            "Eve Adams"
        );
    }

    #[test]
    fn test_extract_display_name_empty() {
        assert_eq!(extract_display_name(""), "");
    }
}
