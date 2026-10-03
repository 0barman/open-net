use http::Method;

/// The HTTP methods supported by the request API.
///
/// This type keeps the public request contract independent from the `http`
/// crate. Convert it to [`http::Method`] at the transport boundary with
/// [`From::from`]. Extension methods are retained losslessly in `Extension`.
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

impl From<HttpRequestMethod> for Method {
    fn from(method: HttpRequestMethod) -> Self {
        match method {
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
                .expect("HttpRequestMethod::Extension must contain a valid method"),
        }
    }
}

impl From<&HttpRequestMethod> for Method {
    fn from(method: &HttpRequestMethod) -> Self {
        method.clone().into()
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

    #[test]
    fn converts_all_standard_methods_in_both_directions() {
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
            assert_eq!(Method::from(request_method.clone()), http_method);
            assert_eq!(HttpRequestMethod::try_from(http_method), Ok(request_method));
        }
    }

    #[test]
    fn preserves_extension_methods() {
        let method = Method::from_bytes(b"CUSTOM").expect("valid extension method");
        let request_method = HttpRequestMethod::try_from(method.clone()).unwrap();
        assert_eq!(request_method.as_str(), "CUSTOM");
        assert_eq!(Method::from(request_method), method);
    }
}
