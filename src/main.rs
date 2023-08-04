use anyhow::Result;
use beatboxer::{
    keep_alive::{KeepAlive, KeepAliveTrait},
    metrics::{setup_metrics_recorder, track_metrics},
    storage::{
        memory::InMemoryStorage, persistent::PersistentStorage, sorted_set::NotifyingStorage,
        Storage,
    },
};
use std::{future::ready, sync::Arc};

use axum::{
    extract::{Path, State},
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

    #[arg(short, long)]
    notifying_node: bool,
}

fn get_storage(
    use_rocksdb: bool,
    http_port: u16,
    notfying_node: bool,
) -> Arc<dyn Storage + Sync + Send> {
    if notfying_node {
        Arc::new(NotifyingStorage::new())
    } else if use_rocksdb {
        Arc::new(PersistentStorage::new(&format!(
            "/tmp/beatboxer_{}.db",
            http_port
        )))
    } else {
        Arc::new(InMemoryStorage::new())
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

    let storage = get_storage(args.use_rocksdb, args.http_port, args.notifying_node);

    if args.notifying_node {
        let notifying_storage: &NotifyingStorage =
            match storage.as_any().downcast_ref::<NotifyingStorage>() {
                Some(b) => b,
                None => panic!("&a isn't a B!"),
            };

        let rxc = notifying_storage.subscribe();

        tokio::spawn(async move {
            let mut rxc = rxc.write().await;
            loop {
                match rxc.recv().await {
                    Some(msg) => info!("got notification: {}", msg),
                    None => {
                        error!("error getting notification");
                        break;
                    }
                }
            }
        });
    }

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
