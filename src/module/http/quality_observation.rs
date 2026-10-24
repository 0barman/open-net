//! Passive HTTP observations. No diagnostic operation is awaited by transport.
use super::*;
use crate::api::quality::QualityReportRef;

tokio::task_local! {
    static EXECUTING_HTTP: (QualityProbe, u64, u64);
}

impl HttpClientInner {
    pub(crate) fn quality_doctor(&self) -> Option<crate::api::quality::QualityDoctor> {
        self.quality.doctor()
    }

    pub(super) fn begin_quality(&self, streaming: bool) -> Option<QualityProbe> {
        self.quality.begin(
            QualityProtocol::Http,
            if streaming {
                QualityOperationKind::HttpStream
            } else {
                QualityOperationKind::HttpRequest
            },
            self.quality_client_id,
        )
    }
}

impl CancellationSignal {
    pub(crate) fn quality_ref(&self) -> Option<QualityReportRef> {
        self.quality.get().map(QualityProbe::reference)
    }

    pub(super) fn quality_phase(&self, phase: PhaseKind) -> Option<QualityPhaseGuard> {
        self.quality.get().map(|probe| probe.phase(phase))
    }

    pub(super) fn quality_mark(&self, phase: PhaseKind) {
        if let Some(probe) = self.quality.get() {
            probe.mark(phase);
        }
    }

    pub(super) fn quality_error(&self, error: NetError) -> NetError {
        match self.quality.get() {
            Some(probe) => {
                probe.error(&error);
                error.with_quality_ref(probe.reference())
            }
            None => error,
        }
    }

    pub(super) fn quality_annotate(&self, error: NetError) -> NetError {
        match self.quality_ref() {
            Some(reference) => error.with_quality_ref(reference),
            None => error,
        }
    }

    pub(super) fn quality_response(&self, response: HttpResponse) -> HttpResponse {
        match self.quality.get() {
            Some(probe) => response.with_quality_ref(probe.reference()),
            None => response,
        }
    }

    pub(super) fn quality_result(&self, result: Result<http::StatusCode, &NetError>) {
        if let Some(probe) = self.quality.get() {
            match result {
                Ok(status) => probe.http_result(status.as_u16()),
                Err(error) => probe.error(error),
            }
        }
    }

    pub(super) fn quality_chunk(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        if let Some(probe) = self.quality.get() {
            if !self.quality_first_chunk.swap(true, Ordering::Relaxed) {
                probe.mark(PhaseKind::FirstBodyChunk);
            }
            probe.bytes(bytes as u64);
        }
    }

    pub(super) fn quality_finish(&self) {
        if let Some(probe) = self.quality.get() {
            probe.finish();
        }
    }
}

/// The original execute future is polled in place; task-local data identifies
/// only the initiating operation, never the connection ultimately selected by a pool.
pub(super) async fn execute(
    client: &Client,
    request: reqwest::Request,
    control: &CancellationSignal,
    hop: u64,
) -> Result<Response, NetError> {
    let Some(probe) = control.quality.get() else {
        return client.execute(request).await.map_err(map_execute_error);
    };
    let phase = probe.phase(PhaseKind::ExecuteToHeaders);
    let result = EXECUTING_HTTP
        .scope(
            (
                probe.clone(),
                u64::from(control.attempt.load(Ordering::Relaxed)),
                hop,
            ),
            client.execute(request),
        )
        .await;
    drop(phase);
    if let Ok(response) = &result {
        probe.mark(PhaseKind::ResponseHeaders);
        // Record the dependency's observed protocol after every successful
        // response head, including redirects. This does not infer pool reuse.
        probe.http_version(response.version());
    }
    result.map_err(map_execute_error)
}

#[cfg(test)]
#[path = "quality_observation_tests.rs"]
mod tests;

#[derive(Clone)]
pub(super) struct ConnectorLayer {
    context: QualityContext,
    client_id: u64,
}

impl ConnectorLayer {
    pub(super) fn new(context: QualityContext, client_id: u64) -> Self {
        Self { context, client_id }
    }
}

impl<S> tower_layer::Layer<S> for ConnectorLayer {
    type Service = ConnectorService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        ConnectorService {
            inner,
            context: self.context.clone(),
            client_id: self.client_id,
        }
    }
}

#[derive(Clone)]
pub(super) struct ConnectorService<S> {
    inner: S,
    context: QualityContext,
    client_id: u64,
}

struct ConnectionObservation {
    probe: QualityProbe,
    phase: Option<QualityPhaseGuard>,
    outcome: QualityOutcome,
}

impl Drop for ConnectionObservation {
    fn drop(&mut self) {
        drop(self.phase.take());
        self.probe.result(self.outcome);
        self.probe.finish();
    }
}

impl<S, Request> tower_service::Service<Request> for ConnectorService<S>
where
    S: tower_service::Service<Request>,
    S::Future: Send + 'static,
    S::Response: 'static,
    S::Error: 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures_util::future::Either<
        S::Future,
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<S::Response, S::Error>> + Send>>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let probe = self.context.begin(
            QualityProtocol::Http,
            QualityOperationKind::Connection,
            self.client_id,
        );
        let Some(probe) = probe else {
            return futures_util::future::Either::Left(self.inner.call(request));
        };
        if let Ok((parent, attempt, hop)) =
            EXECUTING_HTTP.try_with(|(probe, attempt, hop)| (probe.reference(), *attempt, *hop))
        {
            probe.parent(parent);
            probe.attempt(attempt);
            probe.hop(hop);
        }
        let phase = Some(probe.phase(PhaseKind::Connector));
        let mut observation = ConnectionObservation {
            probe,
            phase,
            outcome: QualityOutcome::Abandoned,
        };
        // Call the same service instance whose readiness was checked, exactly once.
        let connection = self.inner.call(request);
        futures_util::future::Either::Right(Box::pin(async move {
            observation.probe.mark(PhaseKind::ConnectorPolled);
            let result = connection.await;
            observation.outcome = if result.is_ok() {
                QualityOutcome::Success
            } else {
                QualityOutcome::Failed
            };
            drop(observation);
            result
        }))
    }
}
