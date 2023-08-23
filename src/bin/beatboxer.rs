use anyhow::Result;
use beatboxer::{
    keep_alive::{
        constants::is_dead,
        types::{Event, EventType},
        KeepAlive, KeepAliveTrait,
    },
    metrics::{setup_metrics_recorder, track_metrics},
    storage::{memory::InMemoryStorage, Storage},
};

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "rocksdb")]
use beatboxer::storage::persistent::PersistentStorage;

use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{future::ready, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::Receiver;

use axum::{
    extract::{ws::WebSocket, ConnectInfo, Path, Query, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
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
    listen_addr: String,

    #[arg(short, long)]
    listen_port: u16,

    #[arg(short, long)]
    nodes: Vec<String>,

    #[arg(short, long)]
    use_rocksdb: bool,
}

#[cfg(feature = "rocksdb")]
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

#[cfg(not(feature = "rocksdb"))]
fn get_storage(use_rocksdb: bool, _http_port: u16) -> Arc<dyn Storage + Sync + Send> {
    if use_rocksdb {
        panic!("build with --features=rocksdb to use this feature")
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
        &args.listen_addr,
        args.listen_port,
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
        .route("/ready", get(ready_handler))
        .route("/cluster_status", get(cluster_status_handler))
        .with_state(cloned_keep_alive);

    let addr = format!("{}:{}", args.http_host, args.http_port)
        .parse()
        .unwrap();

    let http_server =
        axum::Server::bind(&addr).serve(app.into_make_service_with_connect_info::<SocketAddr>());

    info!("Server running on http://{}", addr);

    tokio::select! {
        Err(e) = keep_alive.listen() => { error!("got error while listen(): {:?}", e) }
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
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    info!("ws_connect! {}", addr);

    match keep_alive.subscribe(params.offset).await {
        Ok(rx) => {
            ws.on_upgrade(move |socket| handle_socket(Arc::clone(&keep_alive), socket, rx, addr))
        }
        Err(e) => panic!("can't get rx for updates: {:?}", e),
    }
}

async fn handle_socket(
    keep_alive: Arc<dyn KeepAliveTrait + Send + Sync>,
    socket: WebSocket,
    mut rx: Receiver<Event>,
    who: SocketAddr,
) {
    let (mut sender, mut receiver) = socket.split();

    let mut send_task = tokio::spawn(async move {
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

                if let Err(e) = sender.send(msg).await {
                    error!("error sending ws message to {}: {:?}", who, e);
                    break;
                }
            }
        }
    });

    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let axum::extract::ws::Message::Close(_) = msg {
                info!("client {} closed socket", who)
            }
        }
    });

    tokio::select! {
        rv_a = (&mut send_task) => {
            match rv_a {
                Ok(_) => {}
                Err(a) => error!("Error sending messages {:?}", a)
            }
            recv_task.abort();
        },
        rv_b = (&mut recv_task) => {
            match rv_b {
                Ok(_) => {}
                Err(b) => error!("Error receiving messages {:?}", b)
            }
            send_task.abort();
        }
    }

    info!("Websocket context {} destroyed", who);
}

async fn ready_handler(
    State(keep_alive): State<Arc<dyn KeepAliveTrait + Send + Sync>>,
) -> impl IntoResponse {
    if keep_alive.is_ready().await {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Service Not Ready Yet")
    }
}

async fn cluster_status_handler(
    State(keep_alive): State<Arc<dyn KeepAliveTrait + Send + Sync>>,
) -> impl IntoResponse {
    let cs = keep_alive.cluster_status().await;
    let r = serde_json::to_string(&cs).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    (headers, r)
}
