use anyhow::Result;
use beatboxer::{
    keep_alive::{
        constants::is_dead,
        types::{Event, EventType},
        KeepAlive, KeepAliveTrait,
    },
    metrics::{setup_metrics_recorder, track_metrics},
    storage::{memory::InMemoryStorage, persistent::PersistentStorage, Storage},
};
use serde::Deserialize;
use std::{future::ready, sync::Arc};
use tokio::sync::mpsc::Receiver;

use axum::{
    extract::{ws::WebSocket, Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

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

    #[arg(short, long)]
    use_rocksdb: bool,
}

fn get_storage(use_rocksdb: bool, http_port: u16) -> Arc<dyn Storage + Sync + Send> {
    if use_rocksdb {
        Arc::new(PersistentStorage::new(&format!(
            "/tmp/beatboxer_{}.db",
            http_port
        )))
    } else {
        Arc::new(InMemoryStorage::default())
    }
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

    let storage = get_storage(args.use_rocksdb, args.http_port);

    storage.start_background_tasks();

    let keep_alive = Arc::new(KeepAlive::new(
        args.ka_sync_addr.clone(),
        args.nodes.clone(),
        storage,
    ));
    keep_alive.connect_to_nodes();

    let cloned_keep_alive = Arc::clone(&keep_alive);
    let recorder_handle = setup_metrics_recorder()?;

    // FIXME: if it's a notifying node, shouldn't
    // expose the pulse and ka end points.
    let app = Router::new()
        .route("/pulse/:id", post(pulse_handler))
        .route("/ka/:id", get(get_ka_handler))
        .layer(middleware::from_fn(track_metrics))
        .route("/updates", get(ws_handler))
        .route("/metrics", get(move || ready(recorder_handle.render())))
        .route("/ping", get(ping_handler))
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

async fn ping_handler() -> &'static str {
    "PONG"
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

#[derive(Debug, Deserialize)]
struct WsParams {
    offset: Option<i64>,
}

async fn ws_handler(
    State(keep_alive): State<Arc<dyn KeepAliveTrait + Send + Sync>>,
    Query(params): Query<WsParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if let Some(rx) = keep_alive.subscribe(params.offset).await {
        ws.on_upgrade(move |socket| handle_socket(Arc::clone(&keep_alive), socket, rx))
    } else {
        panic!("can't get rx for updates")
    }
}

async fn handle_socket(
    keep_alive: Arc<dyn KeepAliveTrait + Send + Sync>,
    mut socket: WebSocket,
    mut rx: Receiver<Event>,
) {
    loop {
        if let Some(event) = rx.recv().await {
            let current_state = match keep_alive.get(&event.id).await {
                Some(ts) if is_dead(ts) => EventType::Dead,
                Some(_) => EventType::Connected,
                None => EventType::Unknown,
            };

            let msg = axum::extract::ws::Message::Text(format!(
                "{},{},{},{}",
                event.ts, event.id, event.typ, current_state
            ));

            if let Err(e) = socket.send(msg).await {
                error!("error sending ws message: {:?}", e);
                break;
            }
        }
    }
}
