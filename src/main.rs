use anyhow::Result;
use std::{future::ready, sync::Arc};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::get,
    Router,
};
use clap::Parser;
use keep_alive::KeepAliveTrait;
use tracing::info;
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

use crate::{
    keep_alive::KeepAlive,
    metrics::{setup_metrics_recorder, track_metrics},
};

mod keep_alive;
mod metrics;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    http_host: String,

    #[arg(long, default_value = "8080")]
    http_port: u16,

    #[arg(short, long)]
    ka_sync_addr: String,

    #[arg(short, long)]
    nodes: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "example_error_handling_and_dependency_injection=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let keep_alive = Arc::new(KeepAlive::new(
        args.ka_sync_addr.clone(),
        args.nodes.clone(),
    ));
    keep_alive.connect_to_nodes();

    let cloned_keep_alive = Arc::clone(&keep_alive);

    let recorder_handle = setup_metrics_recorder()?;

    let app = Router::new()
        .route("/pulse/:id", get(pulse_handler))
        .route("/ka/:id", get(get_ka_handler))
        .layer(middleware::from_fn(track_metrics))
        .route("/metrics", get(move || ready(recorder_handle.render())))
        .with_state(cloned_keep_alive);

    let addr = format!("{}:{}", args.http_host, args.http_port)
        .parse()
        .unwrap();

    let http_server = axum::Server::bind(&addr).serve(app.into_make_service());

    info!("Server running on http://{}", addr);

    tokio::select! {
        _ = keep_alive.listen() => { }
        _ = http_server => { }
    };

    Ok(())
}

async fn pulse_handler(
    State(keep_alive): State<Arc<dyn KeepAliveTrait + Send + Sync>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    keep_alive.pulse(&id).await;
    (StatusCode::OK, "OK")
}

async fn get_ka_handler(
    State(keep_alive): State<Arc<dyn KeepAliveTrait + Send + Sync>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match keep_alive.get(&id).await {
        Some(ts) => (StatusCode::OK, ts.to_string()),
        None => (StatusCode::NOT_FOUND, "Not found".to_string()),
    }
}
