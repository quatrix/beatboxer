use std::sync::Arc;
use tonic::{transport::Server, Request, Response, Status};

use axum::{
    async_trait,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use clap::Parser;
use keep_alive::KeepAliveTrait;
use tracing::{error, info};
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};

use crate::keep_alive::KeepAlive;
use keep_alive_sync::keep_alive_sync_server::{KeepAliveSync, KeepAliveSyncServer};
use keep_alive_sync::{ForwardRequest, ForwardResponse};

pub mod keep_alive_sync {
    tonic::include_proto!("keep_alive_sync");
}

mod keep_alive;

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
struct KaSyncServer {
    keep_alive: Arc<KeepAlive>,
}

#[async_trait]
impl KeepAliveSync for KaSyncServer {
    async fn forward_ka(
        &self,
        request: Request<ForwardRequest>,
    ) -> Result<Response<ForwardResponse>, Status> {
        self.keep_alive
            .update(request.get_ref().id.clone(), request.get_ref().ts)
            .await;

        let response = ForwardResponse {
            status: "OK".to_string(),
        };
        Ok(Response::new(response))
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

    let keep_alive = Arc::new(KeepAlive::new(args.nodes.clone()));
    keep_alive.connect_to_nodes();

    let ka_sync_addr = args.ka_sync_addr.parse()?;
    let ka_sync_server = KaSyncServer {
        keep_alive: Arc::clone(&keep_alive),
    };

    let grpc = Server::builder()
        .tcp_keepalive(Some(std::time::Duration::from_secs(5)))
        .add_service(KeepAliveSyncServer::new(ka_sync_server))
        .serve(ka_sync_addr);

    let cloned_keep_alive = Arc::clone(&keep_alive);

    let app = Router::new()
        .route("/pulse/:id", get(pulse_handler))
        .route("/ka/:id", get(get_ka_handler))
        .with_state(cloned_keep_alive);

    let addr = format!("{}:{}", args.http_host, args.http_port)
        .parse()
        .unwrap();

    let http_server = axum::Server::bind(&addr).serve(app.into_make_service());

    info!("Server running on http://{}", addr);

    tokio::select! {
        _ = grpc => { }
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
