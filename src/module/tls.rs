//! Provider and trust-root primitives shared by the TLS transports.
//!
//! Explicit client providers take precedence over the host's process default.
//! Applications own all cryptography; no path installs a process default.

use crate::NetError;
use rustls::crypto::CryptoProvider;
use std::sync::Arc;

pub(crate) fn select_provider(
    explicit: Option<&Arc<CryptoProvider>>,
) -> Result<Arc<CryptoProvider>, NetError> {
    if let Some(provider) = explicit.or_else(|| CryptoProvider::get_default()) {
        return Ok(Arc::clone(provider));
    }
    Err(NetError::config(
        "tls.provider",
        "configure a rustls CryptoProvider or install an application process default",
    ))
}

/// Explicit provider for test peers; never affects production feature selection.
#[cfg(test)]
pub(crate) fn local_provider() -> Arc<CryptoProvider> {
    Arc::new(rustls_openssl::default_provider())
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
    fn explicit_provider_is_preserved_without_changing_the_process_default() {
        let before = CryptoProvider::get_default().cloned();
        let custom = local_provider();
        let selected = select_provider(Some(&custom)).expect("explicit provider");
        assert!(Arc::ptr_eq(&custom, &selected));
        match (before, CryptoProvider::get_default()) {
            (None, None) => {}
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(&before, after)),
            _ => panic!("selecting a provider changed the process default"),
        }
    }

    #[test]
    fn default_selection_does_not_install_a_provider() {
        let before = CryptoProvider::get_default().cloned();
        let selected = select_provider(None);
        match (before, CryptoProvider::get_default()) {
            (None, None) => {
                assert!(
                    matches!(selected, Err(error) if matches!(error.kind(), crate::error::ErrorKind::InvalidConfig))
                );
            }
            (Some(before), Some(after)) => {
                assert!(Arc::ptr_eq(&selected.expect("process default"), &before));
                assert!(Arc::ptr_eq(&before, after));
            }
            _ => panic!("selecting a provider changed the process default"),
        }
    }
}
