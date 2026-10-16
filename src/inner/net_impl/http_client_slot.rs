#![cfg(feature = "http-client")]

use super::managed_http_client::ManagedHttpClientInner;
use crate::api::http::http_client::HttpClient;
use std::sync::Arc;

/// Registration state for an HTTP client owned by an [`OpenNetInner`].
///
/// The HTTP registry deliberately has its own slot type.  WebSocket clients
/// have worker-thread ownership and shutdown semantics that are different from
/// the HTTP lanes, so sharing the WebSocket slot would couple the two
/// protocols' lifecycles.
pub(super) enum HttpClientSlot {
    Creating,
    Ready(HttpClient),
    Closing(HttpClient),
    Managed(Arc<ManagedHttpClientInner>),
}

impl HttpClientSlot {
    pub(super) fn into_client(self) -> Option<HttpClient> {
        match self {
            Self::Ready(client) | Self::Closing(client) => Some(client),
            Self::Creating | Self::Managed(_) => None,
        }
    }
}
