use super::http_error::HttpError;
use crate::common::log::log_def::LogType;
use crate::common::log::summary;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Error, Response, StatusCode};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct HttpWrapper {
    client: Client,
    time_out: Duration,
}

impl HttpWrapper {
    /// 根据传入的参数发送 get 请求，返回结果
    pub async fn get(&self, url: &str) -> Result<(u16, String), HttpError> {
        crate::log_t!(LogType::HTTP; "get", "url|timeout_ms", summary::url(url), self.time_out.as_millis());
        let request = self.client.get(url).timeout(self.time_out);
        let ret = request.send().await;
        match ret.as_ref() {
            Ok(_) => unwrap_resp(ret, url).await,
            Err(err) => print_error(url, err),
        }
    }

    /// 根据传入的参数发送 post 请求，返回结果
    pub async fn post(
        &self,
        url: &str,
        header: HashMap<&'static str, String>,
        body: &str,
    ) -> Result<(u16, String), HttpError> {
        crate::log_t!(LogType::HTTP; "post", "url|header_count|body_bytes|timeout_ms", summary::url(url), header.len(), body.len(), self.time_out.as_millis());
        let mut header_map = HeaderMap::new();
        for (k, v) in header {
            let v = HeaderValue::try_from(v).map_err(|error| {
                crate::log_e!(LogType::HTTP; "post", "stage|error", "parse_header", summary::error(&error));
                HttpError::RequestFailed
            })?;
            header_map.append(k, v);
        }
        let request = self
            .client
            .post(url)
            .headers(header_map)
            .body(body.to_string())
            .timeout(self.time_out);
        let ret = request.send().await;
        match ret.as_ref() {
            Ok(_) => unwrap_resp(ret, url).await,
            Err(err) => print_error(url, err),
        }
    }
}

fn print_error(url: &str, err: &Error) -> Result<(u16, String), HttpError> {
    crate::log_t!(LogType::HTTP; "print_error", "url", summary::url(url));
    crate::log_e!(LogType::HTTP; "print_error", "url|error|timeout", summary::url(url), summary::error(err), err.is_timeout());
    if err.is_timeout() {
        Err(HttpError::RequestTimeout)
    } else {
        Err(HttpError::RequestFailed)
    }
}

async fn unwrap_resp(
    resp: Result<Response, reqwest::Error>,
    url: &str,
) -> Result<(u16, String), HttpError> {
    crate::log_t!(LogType::HTTP; "unwrap_resp", "url|response_ok", summary::url(url), resp.is_ok());
    match resp {
        Ok(resp) => {
            let code = resp.status();
            let text = resp.text().await.unwrap_or_else(|error| {
                crate::log_e!(LogType::HTTP; "unwrap_resp", "stage|error", "read_body", summary::error(&error));
                // Retain the existing fallback and HTTP return semantics.
                "no response!".to_string()
            });
            crate::log_s!(LogType::HTTP; "unwrap_resp", "http_code|body_bytes", code.as_u16(), text.len());
            if code != StatusCode::OK {
                crate::log_e!(LogType::HTTP; "unwrap_resp", "http_code|url", code.as_u16(), summary::url(url));
                Err(HttpError::RequestFailed)
            } else {
                Ok((code.as_u16(), text))
            }
        }
        Err(e) => {
            crate::log_e!(LogType::HTTP; "unwrap_resp", "url|error", summary::url(url), summary::error(&e));
            Err(HttpError::RequestFailed)
        }
    }
}
