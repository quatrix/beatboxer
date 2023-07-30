use anyhow::{anyhow, Result};
use axum::{extract::MatchedPath, middleware::Next, response::IntoResponse};
use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use std::time::Instant;

pub fn setup_metrics_recorder() -> Result<PrometheusHandle> {
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    ];

    let builder = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("http_requests_duration_seconds".to_string()),
            EXPONENTIAL_SECONDS,
        )?
        .set_buckets_for_metric(
            Matcher::Full("message_sync_latency_seconds".to_string()),
            [0.001, 0.005, 0.05, 0.1, 0.2, 0.5, 1.0, 5.0, 10.0, 30.0].as_ref(),
        )?;

    match builder.install_recorder() {
        Ok(recorder) => Ok(recorder),
        Err(e) => Err(anyhow!("Failed to install recorder: {}", e)),
    }
}

pub async fn track_metrics<B>(req: axum::http::Request<B>, next: Next<B>) -> impl IntoResponse {
    let start = Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().clone();

    let response = next.run(req).await;

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [
        ("method", method.to_string()),
        ("path", path),
        ("status", status),
    ];

    metrics::increment_counter!("http_requests_total", &labels);
    metrics::histogram!("http_requests_duration_seconds", latency, &labels);

    response
}
