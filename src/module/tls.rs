//! Provider and trust-root primitives shared by the TLS transports.
//!
//! Configurations use explicit providers and never install a process default.
//! HTTP preserves the host's provider choice; WebSocket keeps its independent
//! AWS-LC policy. Each transport owns its protocol and session settings.

use rustls::crypto::CryptoProvider;
use std::sync::Arc;

pub(crate) fn local_provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

#[cfg(feature = "http-client")]
pub(crate) fn http_provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(local_provider)
}

pub(crate) fn root_store(include_builtin: bool) -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    if include_builtin {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_store_only_includes_builtin_roots_when_requested() {
        assert!(root_store(false).is_empty());
        assert_eq!(root_store(true).len(), webpki_roots::TLS_SERVER_ROOTS.len());
    }

    #[test]
    fn local_provider_does_not_install_a_process_default() {
        let before = CryptoProvider::get_default().cloned();
        let local = local_provider();
        assert!(!local.cipher_suites.is_empty());
        match (before, CryptoProvider::get_default()) {
            (None, None) => {}
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(&before, after)),
            _ => panic!("constructing a local provider changed the process default"),
        }
    }

    #[cfg(feature = "http-client")]
    #[test]
    fn http_provider_preserves_the_process_default() {
        let before = CryptoProvider::get_default().cloned();
        let selected = http_provider();
        match (before, CryptoProvider::get_default()) {
            (None, None) => {
                let aws_lc = local_provider();
                assert_eq!(
                    selected
                        .cipher_suites
                        .iter()
                        .map(|suite| suite.suite())
                        .collect::<Vec<_>>(),
                    aws_lc
                        .cipher_suites
                        .iter()
                        .map(|suite| suite.suite())
                        .collect::<Vec<_>>()
                );
            }
            (Some(before), Some(after)) => {
                assert!(Arc::ptr_eq(&selected, &before));
                assert!(Arc::ptr_eq(&before, after));
            }
            _ => panic!("selecting an HTTP provider changed the process default"),
        }
    }
}
