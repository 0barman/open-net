use crate::api::http::{HttpRequest, HttpRequestTrait, HttpResponseResult};
use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method};
use tokio::sync::oneshot;

pub struct ResultRequest {
    request: HttpRequest,
    sender: Option<oneshot::Sender<HttpResponseResult>>,
}

impl ResultRequest {
    pub fn new(request: HttpRequest, sender: oneshot::Sender<HttpResponseResult>) -> Self {
        Self {
            request,
            sender: Some(sender),
        }
    }
}

#[async_trait]
impl HttpRequestTrait for ResultRequest {
    fn get_path(&self) -> String {
        self.request.path.clone()
    }

    fn get_method(&self) -> Method {
        self.request.method.clone()
    }

    fn get_req_body(&self) -> Bytes {
        self.request.body.clone()
    }

    fn headers(&self) -> HeaderMap {
        self.request.headers.clone()
    }

    fn retry_policy(&self) -> crate::api::http::RetryPolicy {
        self.request.retry_policy.clone()
    }

    async fn deal_with_response(mut self: Box<Self>, result: HttpResponseResult) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(result);
        }
    }
}
