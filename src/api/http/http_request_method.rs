use crate::api::error::NetError;
use http::Method;

/// The HTTP methods supported by the request API.
///
/// This type keeps the public request contract independent from the `http`
/// crate. Convert it to [`http::Method`] at the transport boundary with
/// [`TryFrom::try_from`]. Extension methods are retained losslessly in `Extension`,
/// and conversion rejects invalid HTTP method tokens with
/// [`crate::error::ErrorKind::InvalidInput`] instead of panicking or changing the method.
/// Use `Method::try_from(method)` or `method.try_into()` for this fallible direction;
/// conversion from an already validated [`http::Method`] remains infallible.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum HttpRequestMethod {
    GET,
    POST,
    PUT,
    DELETE,
    HEAD,
    OPTIONS,
    CONNECT,
    PATCH,
    TRACE,
    QUERY,
    Extension(String),
}

impl TryFrom<HttpRequestMethod> for Method {
    type Error = NetError;

    fn try_from(method: HttpRequestMethod) -> Result<Self, Self::Error> {
        Self::try_from(&method)
    }
}

impl TryFrom<&HttpRequestMethod> for Method {
    type Error = NetError;

    fn try_from(method: &HttpRequestMethod) -> Result<Self, Self::Error> {
        Ok(match method {
            HttpRequestMethod::GET => Method::GET,
            HttpRequestMethod::POST => Method::POST,
            HttpRequestMethod::PUT => Method::PUT,
            HttpRequestMethod::DELETE => Method::DELETE,
            HttpRequestMethod::HEAD => Method::HEAD,
            HttpRequestMethod::OPTIONS => Method::OPTIONS,
            HttpRequestMethod::CONNECT => Method::CONNECT,
            HttpRequestMethod::PATCH => Method::PATCH,
            HttpRequestMethod::TRACE => Method::TRACE,
            HttpRequestMethod::QUERY => Method::QUERY,
            HttpRequestMethod::Extension(method) => Method::from_bytes(method.as_bytes())
                .map_err(|_| NetError::input("method", "invalid HTTP method token"))?,
        })
    }
}

impl From<Method> for HttpRequestMethod {
    fn from(method: Method) -> Self {
        match method.as_str() {
            "GET" => Self::GET,
            "POST" => Self::POST,
            "PUT" => Self::PUT,
            "DELETE" => Self::DELETE,
            "HEAD" => Self::HEAD,
            "OPTIONS" => Self::OPTIONS,
            "CONNECT" => Self::CONNECT,
            "PATCH" => Self::PATCH,
            "TRACE" => Self::TRACE,
            "QUERY" => Self::QUERY,
            method => Self::Extension(method.to_owned()),
        }
    }
}

impl From<&Method> for HttpRequestMethod {
    fn from(method: &Method) -> Self {
        Self::from(method.clone())
    }
}

impl PartialEq<Method> for HttpRequestMethod {
    fn eq(&self, method: &Method) -> bool {
        self.as_str() == method.as_str()
    }
}

impl PartialEq<HttpRequestMethod> for Method {
    fn eq(&self, method: &HttpRequestMethod) -> bool {
        method == self
    }
}

impl HttpRequestMethod {
    /// Return the standard method name.
    pub fn as_str(&self) -> &str {
        match self {
            Self::GET => "GET",
            Self::POST => "POST",
            Self::PUT => "PUT",
            Self::DELETE => "DELETE",
            Self::HEAD => "HEAD",
            Self::OPTIONS => "OPTIONS",
            Self::CONNECT => "CONNECT",
            Self::PATCH => "PATCH",
            Self::TRACE => "TRACE",
            Self::QUERY => "QUERY",
            Self::Extension(method) => method,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::error::{ErrorKind, ErrorStage};

    #[test]
    fn converts_all_standard_methods_in_both_directions() -> Result<(), NetError> {
        let methods = [
            (HttpRequestMethod::GET, Method::GET),
            (HttpRequestMethod::POST, Method::POST),
            (HttpRequestMethod::PUT, Method::PUT),
            (HttpRequestMethod::DELETE, Method::DELETE),
            (HttpRequestMethod::HEAD, Method::HEAD),
            (HttpRequestMethod::OPTIONS, Method::OPTIONS),
            (HttpRequestMethod::CONNECT, Method::CONNECT),
            (HttpRequestMethod::PATCH, Method::PATCH),
            (HttpRequestMethod::TRACE, Method::TRACE),
            (HttpRequestMethod::QUERY, Method::QUERY),
        ];

        for (request_method, http_method) in methods {
            assert_eq!(Method::try_from(&request_method)?, http_method);
            assert_eq!(Method::try_from(request_method.clone())?, http_method);
            assert_eq!(HttpRequestMethod::from(http_method), request_method);
        }
        Ok(())
    }

    #[test]
    fn preserves_extension_methods() -> Result<(), NetError> {
        for token in ["CUSTOM", "custom", "CuStOm", "X!#$%&'*+-.^_`|~"] {
            let request_method = HttpRequestMethod::Extension(token.to_owned());
            let method = Method::try_from(&request_method)?;
            assert_eq!(method.as_str(), token);
            assert_eq!(Method::try_from(request_method.clone())?, method);
            assert_eq!(HttpRequestMethod::from(method), request_method);
        }
        Ok(())
    }

    #[test]
    fn rejects_invalid_extension_methods_without_exposing_input() {
        for token in ["", "BAD METHOD", "GET\r\nsecret", "GÉT", "GET/", "GET\0"] {
            let request_method = HttpRequestMethod::Extension(token.to_owned());
            for error in [
                Method::try_from(&request_method).expect_err("invalid borrowed method"),
                Method::try_from(request_method).expect_err("invalid owned method"),
            ] {
                assert_eq!(error.kind(), ErrorKind::InvalidInput);
                assert_eq!(error.context().stage, Some(ErrorStage::RequestBuild));
                let detail = error.config_error().expect("method error details");
                assert_eq!(detail.field(), "method");
                assert_eq!(detail.reason(), "invalid HTTP method token");
                assert!(!format!("{error} {error:?}").contains("secret"));
            }
        }
    }
}
