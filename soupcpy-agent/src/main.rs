use std::{
    collections::HashMap,
    env, fmt, fs,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{Html, IntoResponse, Json},
    routing::{get, post, put},
    Router,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    SqlitePool,
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch, RwLock},
    task::JoinHandle,
    time::{interval, sleep, Duration, MissedTickBehavior},
};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};
use tracing::{info, warn};

#[derive(Clone)]
struct AppConfig {
    index_html: String,
    channels_html: String,
    controller_endpoint: String,
    agent_udid: String,
    bridge_remote_url: String,
    publisher_endpoint: String,
    heartbeat_seconds: u64,
    database_path: PathBuf,
}

#[derive(Deserialize)]
struct RawAppConfig {
    index_html_path: PathBuf,
    channels_html_path: PathBuf,
    controller_endpoint: String,
    agent_udid: String,
    bridge_remote_url: String,
    publisher_endpoint: String,
    heartbeat_seconds: u64,
    database_path: PathBuf,
}

fn load_config() -> Result<AppConfig, Box<dyn std::error::Error + Send + Sync>> {
    let contents = fs::read_to_string("config/config.yml")?;
    let raw_config: RawAppConfig = serde_yaml::from_str(&contents)?;
    let index_html = fs::read_to_string(&raw_config.index_html_path)?;
    let channels_html = fs::read_to_string(&raw_config.channels_html_path)?;

    Ok(AppConfig {
        index_html,
        channels_html,
        controller_endpoint: raw_config.controller_endpoint,
        agent_udid: raw_config.agent_udid,
        bridge_remote_url: raw_config.bridge_remote_url,
        publisher_endpoint: raw_config.publisher_endpoint,
        heartbeat_seconds: raw_config.heartbeat_seconds,
        database_path: raw_config.database_path,
    })
}

#[derive(Clone)]
struct AppState {
    broadcaster: broadcast::Sender<String>,
    started_at: Instant,
    active_bridges: Arc<RwLock<HashMap<String, BridgeSession>>>,
    bridge_counter: Arc<AtomicU64>,
    config: Arc<AppConfig>,
    db: SqlitePool,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    uptime_seconds: u64,
    heartbeat_seconds: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("server failed: {err}");
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();

    let config = load_config()?;
    if let Some(parent) = config.database_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&config.database_path)
                .create_if_missing(true),
        )
        .await?;
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            udid TEXT NOT NULL,
            group_name TEXT NOT NULL,
            code TEXT NOT NULL,
            gw INTEGER NOT NULL
        )
        "#,
    )
    .execute(&db)
    .await?;
    let config = Arc::new(config);
    let bridge_counter = Arc::new(AtomicU64::new(0));
    let (tx, _rx) = broadcast::channel(128);
    let state = AppState {
        broadcaster: tx,
        started_at: Instant::now(),
        active_bridges: Arc::new(RwLock::new(HashMap::new())),
        bridge_counter: bridge_counter.clone(),
        config: config.clone(),
        db: db.clone(),
    };

    spawn_controller_link(
        state.broadcaster.clone(),
        state.active_bridges.clone(),
        config.clone(),
        state.db.clone(),
        bridge_counter.clone(),
    );

    let api_router = Router::new()
        .route("/channels", get(list_channels).post(create_channel))
        .route("/channels/:id", put(update_channel).delete(delete_channel))
        .route("/channels/:id/duplicate", post(duplicate_channel))
        .route("/channels/export", get(export_channels))
        .route("/channels/import", post(import_channels));

    let app = Router::new()
        .route("/", get(index))
        .route("/channels", get(channel_admin))
        .route("/health", get(health))
        .route("/ws", get(ws_handler))
        .route("/bridge/start", get(start_bridge_handler))
        .nest("/api", api_router)
        .with_state(state);

    let addr: SocketAddr = ([0, 0, 0, 0], 3001).into();
    let listener = TcpListener::bind(addr).await?;
    let actual_addr = listener.local_addr()?;
    info!("HTTP and WebSocket server listening on http://{actual_addr}");

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,axum=info".into());
    let _ = fmt().with_env_filter(filter).with_target(false).try_init();
}

async fn index(State(state): State<AppState>) -> Html<String> {
    Html(state.config.index_html.clone())
}

async fn channel_admin(State(state): State<AppState>) -> Html<String> {
    Html(state.config.channels_html.clone())
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let uptime = state.started_at.elapsed().as_secs();
    Json(HealthResponse {
        status: "ok",
        uptime_seconds: uptime,
        heartbeat_seconds: state.config.heartbeat_seconds,
    })
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| async move {
        if let Err(err) = websocket_loop(socket, state).await {
            warn!("websocket session ended: {err}");
        }
    })
}

async fn websocket_loop(mut socket: WebSocket, state: AppState) -> Result<(), axum::Error> {
    let mut rx = state.broadcaster.subscribe();

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let trimmed = text.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let payload = format!("{} (handled by thread {:?})", trimmed, std::thread::current().id());
                        let _ = state.broadcaster.send(payload);
                    }
                    Some(Ok(Message::Binary(_))) => {
                        // Ignore binary messages to keep the protocol text-only.
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        socket.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {
                        // No-op; axum handles heartbeats for us.
                    }
                    Some(Ok(Message::Close(frame))) => {
                        socket.send(Message::Close(frame)).await?;
                        break;
                    }
                    Some(Err(err)) => {
                        warn!("error receiving ws message: {err}");
                        break;
                    }
                    None => break,
                }
            }
            broadcast_msg = rx.recv() => {
                match broadcast_msg {
                    Ok(msg) => {
                        if socket.send(Message::Text(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!("websocket lagged; skipped {skipped} messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }

    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        signal(SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, draining connections");
}

fn spawn_controller_link(
    broadcaster: broadcast::Sender<String>,
    active_registry: Arc<RwLock<HashMap<String, BridgeSession>>>,
    config: Arc<AppConfig>,
    db: SqlitePool,
    bridge_counter: Arc<AtomicU64>,
) {
    tokio::spawn(async move {
        if let Err(err) =
            controller_link_loop(broadcaster, active_registry, config, db, bridge_counter).await
        {
            warn!("controller link loop exited: {err}");
        }
    });
}

async fn controller_link_loop(
    broadcaster: broadcast::Sender<String>,
    active_registry: Arc<RwLock<HashMap<String, BridgeSession>>>,
    config: Arc<AppConfig>,
    db: SqlitePool,
    bridge_counter: Arc<AtomicU64>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut endpoint =
        env::var("CONTROLLER_ENDPOINT").unwrap_or_else(|_| config.controller_endpoint.clone());
    let udid = env::var("AGENT_UDID").unwrap_or_else(|_| config.agent_udid.clone());

    if !endpoint.contains("udid=") {
        if endpoint.contains('?') {
            endpoint.push('&');
        } else {
            endpoint.push('?');
        }
        endpoint.push_str("udid=");
        endpoint.push_str(&udid);
    }

    loop {
        match connect_async(endpoint.as_str()).await {
            Ok((ws_stream, _)) => {
                info!("connected to controller endpoint {}", endpoint);

                let (mut write, mut read) = ws_stream.split();
                let mut heartbeat = interval(Duration::from_secs(config.heartbeat_seconds));
                heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

                if let Err(err) = write
                    .send(WsMessage::Text(format!(
                        "agent {0} connected",
                        udid.clone()
                    )))
                    .await
                {
                    warn!("failed to send registration message: {err}");
                }

                loop {
                    tokio::select! {
                        _ = heartbeat.tick() => {
                            match build_channel_snapshot(&db).await {
                                Ok(snapshot) => {
                                    if let Err(err) = write.send(WsMessage::Text(snapshot)).await {
                                        warn!("failed to push channel snapshot: {err}");
                                        break;
                                    }
                                }
                                Err(err) => {
                                    warn!("failed to build channel snapshot: {err}");
                                }
                            }
                            if let Err(err) = write.send(WsMessage::Ping(Vec::new())).await {
                                warn!("failed to send heartbeat ping: {err}");
                                break;
                            }
                        }
                        message = read.next() => {
                            match message {
                                Some(Ok(WsMessage::Text(text))) => {
                                    info!("controller message: {text}");
                                    let _ = broadcaster.send(format!("[controller] {text}"));
                                    if let Some(command) = parse_controller_command(&text) {
                                        match command {
                                            ControllerCommand::Connect {
                                                udid: requested_udid,
                                                publisher_udid,
                                                remote_url,
                                                publisher_url,
                                            } => {
                                                let target_udid = requested_udid
                                                    .unwrap_or_else(|| config.agent_udid.clone());
                                                let publisher_udid = publisher_udid.unwrap_or_else(|| config.agent_udid.clone());
                                                let remote_url = remote_url.unwrap_or_else(|| build_remote_url(config.as_ref(), &target_udid));
                                                let publisher_url = publisher_url.unwrap_or_else(|| build_publisher_url(config.as_ref(), &publisher_udid));
                                                let started = start_bridge_session(
                                                    &active_registry,
                                                    &bridge_counter,
                                                    &broadcaster,
                                                    remote_url,
                                                    publisher_url,
                                                    publisher_udid.clone(),
                                                    config.heartbeat_seconds,
                                                )
                                                .await;
                                                let status = if started {
                                                    "bridge_started"
                                                } else {
                                                    "bridge_running"
                                                };
                                                let ack = serde_json::json!({
                                                    "event": status,
                                                    "udid": target_udid,
                                                })
                                                .to_string();
                                                if let Err(err) = write.send(WsMessage::Text(ack)).await {
                                                    warn!("failed to send controller ack: {err}");
                                                }
                                            }
                                            ControllerCommand::Disconnect { publisher_udid, udid } => {
                                                info!("controller disconnect message: {text}");
                                                let target_udid =
                                                    udid.unwrap_or_else(|| config.agent_udid.clone());
                                                let publisher_udid = publisher_udid.unwrap_or_else(|| config.agent_udid.clone());
                                                let stopped =
                                                    stop_bridge_session(&active_registry, &publisher_udid)
                                                        .await;
                                                let status = if stopped {
                                                    "bridge_stopped"
                                                } else {
                                                    "bridge_missing"
                                                };
                                                let ack = serde_json::json!({
                                                    "event": status,
                                                    "udid": target_udid,
                                                })
                                                .to_string();
                                                if let Err(err) = write.send(WsMessage::Text(ack)).await {
                                                    warn!("failed to send controller ack: {err}");
                                                }
                                            }
                                            ControllerCommand::Snapshot => {
                                                match build_channel_snapshot(&db).await {
                                                    Ok(snapshot) => {
                                                        if let Err(err) =
                                                            write.send(WsMessage::Text(snapshot)).await
                                                        {
                                                            warn!("failed to push snapshot on demand: {err}");
                                                        }
                                                    }
                                                    Err(err) => {
                                                        warn!("failed to build snapshot on request: {err}");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                Some(Ok(WsMessage::Ping(payload))) => {
                                    if let Err(err) = write.send(WsMessage::Pong(payload)).await {
                                        warn!("failed to respond to ping: {err}");
                                        break;
                                    }
                                }
                                Some(Ok(WsMessage::Pong(_))) => { /* heartbeat acknowledged */ }
                                Some(Ok(WsMessage::Binary(_))) => { /* ignore binary */ }
                                Some(Ok(WsMessage::Frame(_))) => { /* ignore low-level */ }
                                Some(Ok(WsMessage::Close(frame))) => {
                                    info!("controller requested close: {:?}", frame);
                                    if let Err(err) = write.send(WsMessage::Close(frame)).await {
                                        warn!("failed to echo controller close frame: {err}");
                                    }
                                    break;
                                }
                                Some(Err(err)) => {
                                    warn!("controller stream error: {err}");
                                    break;
                                }
                                None => {
                                    info!("controller connection closed");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                warn!("failed to connect to controller endpoint {endpoint}: {err}");
            }
        }

        sleep(Duration::from_secs(5)).await;
        info!("reconnecting to controller endpoint {endpoint}");
    }
}

#[derive(Deserialize)]
struct BridgeParams {
    remote_url: Option<String>,
    udid: Option<String>,
    publisher_url: Option<String>,
}

#[derive(Serialize)]
struct BridgeResponse {
    status: &'static str,
    message: String,
}

struct BridgeSession {
    cancel: watch::Sender<bool>,
    handle: JoinHandle<()>,
    id: u64,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
enum ControllerCommandPayload {
    Connect {
        udid: Option<String>,
        publisher_udid: Option<String>,
        remote_url: Option<String>,
        publisher_url: Option<String>,
    },
    Disconnect {
        publisher_udid: Option<String>,
        udid: Option<String>,
    },
    Snapshot,
}

enum ControllerCommand {
    Connect {
        udid: Option<String>,
        publisher_udid: Option<String>,
        remote_url: Option<String>,
        publisher_url: Option<String>,
    },
    Disconnect {
        publisher_udid: Option<String>,
        udid: Option<String>,
    },
    Snapshot,
}

impl From<ControllerCommandPayload> for ControllerCommand {
    fn from(value: ControllerCommandPayload) -> Self {
        match value {
            ControllerCommandPayload::Connect {
                udid,
                publisher_udid,
                remote_url,
                publisher_url,
            } => ControllerCommand::Connect {
                udid,
                publisher_udid,
                remote_url,
                publisher_url,
            },
            ControllerCommandPayload::Disconnect { publisher_udid, udid } => ControllerCommand::Disconnect { publisher_udid, udid },
            ControllerCommandPayload::Snapshot => ControllerCommand::Snapshot,
        }
    }
}

fn parse_controller_command(text: &str) -> Option<ControllerCommand> {
    if let Ok(payload) = serde_json::from_str::<ControllerCommandPayload>(text) {
        return Some(payload.into());
    }

    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut parts = trimmed.splitn(2, char::is_whitespace);
    let head = parts.next().unwrap();
    let rest = parts.next().unwrap_or("").trim();

    match head.to_ascii_lowercase().as_str() {
        "connect" => {
            if rest.is_empty() {
                Some(ControllerCommand::Connect {
                    udid: None,
                    publisher_udid: None,
                    remote_url: None,
                    publisher_url: None,
                })
            } else {
                let mut udid = None;
                let mut remote_url = None;
                let mut publisher_url = None;
                let mut publisher_udid = None;
                for token in rest.split_whitespace() {
                    if let Some((key, value)) = token.split_once('=') {
                        match key.to_ascii_lowercase().as_str() {
                            "udid" => udid = Some(value.to_string()),
                            "publisher_udid" => publisher_udid = Some(value.to_string()),
                            "remote" | "remote_url" => remote_url = Some(value.to_string()),
                            "publisher" | "publisher_url" => publisher_url = Some(value.to_string()),
                            _ => return None,
                        }
                    }
                }
                Some(ControllerCommand::Connect {
                    udid,
                    publisher_udid,
                    remote_url,
                    publisher_url,
                })
            }
        }
        "disconnect" => {
            if rest.is_empty() {
                Some(ControllerCommand::Disconnect { publisher_udid: None, udid: None })
            } else {
                let mut udid = None;
                let mut publisher_udid = None;
                for token in rest.split_whitespace() {
                    if let Some((key, value)) = token.split_once('=') {
                        match key.to_ascii_lowercase().as_str() {
                            "udid" => udid = Some(value.to_string()),
                            "publisher_udid" => publisher_udid = Some(value.to_string()),
                            _ => return None,
                        }
                    }
                }
                Some(ControllerCommand::Disconnect {
                    publisher_udid,
                    udid
               
                })
            }
            
            
            // if let Some((key, value)) = rest.split_once('=') {
            //     if key.eq_ignore_ascii_case("udid") {
            //         Some(ControllerCommand::Disconnect {
            //             publisher_udid: None,
            //             udid: Some(value.to_string()),
            //         })
            //     } else 
            //     if key.eq_ignore_ascii_case("publisher_udid") {
            //         Some(ControllerCommand::Disconnect {
            //             publisher_udid: Some(value.to_string()),
            //             udid: None,
            //         })
            //     } else
            //     {
            //         Some(ControllerCommand::Disconnect {
            //             publisher_udid: None,
            //             udid: Some(rest.to_string()),
            //         })
            //     }
            // } else {
            //     Some(ControllerCommand::Disconnect {
            //         udid: Some(rest.to_string()),
            //     })
            // }
        }
        "snapshot" => Some(ControllerCommand::Snapshot),
        _ => None,
    }
}

#[derive(Deserialize)]
struct ChannelPayload {
    name: String,
    udid: String,
    #[serde(rename = "group")]
    group_name: String,
    code: String,
    gw: i64,
}

impl ChannelPayload {
    fn sanitize(mut self) -> Result<Self, (StatusCode, String)> {
        self.name = self.name.trim().to_string();
        self.udid = self.udid.trim().to_string();
        self.group_name = self.group_name.trim().to_string();
        self.code = self.code.trim().to_string();
        if self.name.is_empty()
            || self.udid.is_empty()
            || self.group_name.is_empty()
            || self.code.is_empty()
        {
            return Err((
                StatusCode::BAD_REQUEST,
                "name, udid, group, and code must be provided".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Serialize)]
struct ChannelResponse {
    id: i64,
    name: String,
    udid: String,
    #[serde(rename = "group")]
    group_name: String,
    code: String,
    gw: i64,
}

#[derive(Serialize)]
struct ChannelsSnapshot {
    event: &'static str,
    count: usize,
    channels: Vec<ChannelResponse>,
}

#[derive(Deserialize)]
struct ImportChannelsRequest {
    csv: String,
}

#[derive(Deserialize)]
struct ChannelCsvRow {
    name: String,
    udid: String,
    #[serde(rename = "group")]
    group_name: String,
    code: String,
    gw: i64,
}

#[derive(sqlx::FromRow)]
struct ChannelRecord {
    id: i64,
    name: String,
    udid: String,
    group_name: String,
    code: String,
    gw: i64,
}

impl From<ChannelRecord> for ChannelResponse {
    fn from(value: ChannelRecord) -> Self {
        Self {
            id: value.id,
            name: value.name,
            udid: value.udid,
            group_name: value.group_name,
            code: value.code,
            gw: value.gw,
        }
    }
}

async fn load_all_channels(pool: &SqlitePool) -> Result<Vec<ChannelResponse>, sqlx::Error> {
    let records = sqlx::query_as::<_, ChannelRecord>(
        r#"SELECT id, name, udid, group_name, code, gw FROM channels ORDER BY name"#,
    )
    .fetch_all(pool)
    .await?;

    Ok(records
        .into_iter()
        .map(ChannelResponse::from)
        .collect::<Vec<_>>())
}

async fn list_channels(
    State(state): State<AppState>,
) -> Result<Json<Vec<ChannelResponse>>, (StatusCode, String)> {
    let channels = load_all_channels(&state.db).await.map_err(internal_error)?;

    Ok(Json(channels))
}

async fn create_channel(
    State(state): State<AppState>,
    Json(payload): Json<ChannelPayload>,
) -> Result<(StatusCode, Json<ChannelResponse>), (StatusCode, String)> {
    let payload = payload.sanitize()?;

    let result = sqlx::query(
        r#"INSERT INTO channels (name, udid, group_name, code, gw) VALUES (?1, ?2, ?3, ?4, ?5)"#,
    )
    .bind(&payload.name)
    .bind(&payload.udid)
    .bind(&payload.group_name)
    .bind(&payload.code)
    .bind(payload.gw)
    .execute(&state.db)
    .await;

    let channel = match result {
        Ok(res) => {
            let id = res.last_insert_rowid();
            fetch_channel(&state.db, id).await?
        }
        Err(err) => return Err(map_database_error(err)),
    };

    Ok((StatusCode::CREATED, Json(channel)))
}

async fn update_channel(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<ChannelPayload>,
) -> Result<Json<ChannelResponse>, (StatusCode, String)> {
    let payload = payload.sanitize()?;
    let result = sqlx::query(
        r#"UPDATE channels SET name = ?1, udid = ?2, group_name = ?3, code = ?4, gw = ?5 WHERE id = ?6"#,
    )
    .bind(&payload.name)
    .bind(&payload.udid)
    .bind(&payload.group_name)
    .bind(&payload.code)
    .bind(payload.gw)
    .bind(id)
    .execute(&state.db)
    .await
    .map_err(internal_error)?;

    if result.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, format!("channel {id} not found")));
    }

    let channel = fetch_channel(&state.db, id).await?;
    Ok(Json(channel))
}

async fn delete_channel(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<StatusCode, (StatusCode, String)> {
    let result = sqlx::query(r#"DELETE FROM channels WHERE id = ?1"#)
        .bind(id)
        .execute(&state.db)
        .await
        .map_err(internal_error)?;

    if result.rows_affected() == 0 {
        return Err((StatusCode::NOT_FOUND, format!("channel {id} not found")));
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn duplicate_channel(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<(StatusCode, Json<ChannelResponse>), (StatusCode, String)> {
    let original = fetch_channel(&state.db, id).await?;
    let duplicate_name = generate_duplicate_name(&state.db, &original.name).await?;
    let payload = ChannelPayload {
        name: duplicate_name,
        udid: original.udid.clone(),
        group_name: original.group_name.clone(),
        code: original.code.clone(),
        gw: original.gw,
    }
    .sanitize()?;

    let result = sqlx::query(
        r#"INSERT INTO channels (name, udid, group_name, code, gw) VALUES (?1, ?2, ?3, ?4, ?5)"#,
    )
    .bind(&payload.name)
    .bind(&payload.udid)
    .bind(&payload.group_name)
    .bind(&payload.code)
    .bind(payload.gw)
    .execute(&state.db)
    .await;

    let channel = match result {
        Ok(res) => {
            let new_id = res.last_insert_rowid();
            fetch_channel(&state.db, new_id).await?
        }
        Err(err) => return Err(map_database_error(err)),
    };

    Ok((StatusCode::CREATED, Json(channel)))
}

async fn export_channels(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let records = sqlx::query_as::<_, ChannelRecord>(
        r#"SELECT id, name, udid, group_name, code, gw FROM channels ORDER BY name"#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(internal_error)?;

    let mut wtr = csv::Writer::from_writer(Vec::new());
    wtr.write_record(["name", "udid", "group", "code", "gw"])
        .map_err(|err| internal_error(err))?;
    for record in records {
        wtr.write_record([
            record.name,
            record.udid,
            record.group_name,
            record.code,
            record.gw.to_string(),
        ])
        .map_err(|err| internal_error(err))?;
    }
    let bytes = wtr
        .into_inner()
        .map_err(|err| internal_error(err.error()))?;
    let csv_text = String::from_utf8(bytes).map_err(|err| internal_error(err))?;

    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    headers.insert(
        axum::http::header::CONTENT_DISPOSITION,
        axum::http::HeaderValue::from_static("attachment; filename=\"channels.csv\""),
    );
    Ok((headers, csv_text))
}

async fn import_channels(
    State(state): State<AppState>,
    Json(body): Json<ImportChannelsRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(body.csv.as_bytes());

    let mut tx = state.db.begin().await.map_err(internal_error)?;
    let mut processed = 0usize;
    for result in reader.deserialize::<ChannelCsvRow>() {
        let row = result.map_err(|err| (StatusCode::BAD_REQUEST, err.to_string()))?;
        let payload = ChannelPayload {
            name: row.name,
            udid: row.udid,
            group_name: row.group_name,
            code: row.code,
            gw: row.gw,
        }
        .sanitize()?;

        sqlx::query(
            r#"
                INSERT INTO channels (name, udid, group_name, code, gw)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ON CONFLICT(name) DO UPDATE SET
                    udid=excluded.udid,
                    group_name=excluded.group_name,
                    code=excluded.code,
                    gw=excluded.gw
            "#,
        )
        .bind(&payload.name)
        .bind(&payload.udid)
        .bind(&payload.group_name)
        .bind(&payload.code)
        .bind(payload.gw)
        .execute(&mut *tx)
        .await
        .map_err(internal_error)?;
        processed += 1;
    }

    tx.commit().await.map_err(internal_error)?;

    Ok(Json(serde_json::json!({ "imported": processed })))
}

async fn fetch_channel(
    pool: &SqlitePool,
    id: i64,
) -> Result<ChannelResponse, (StatusCode, String)> {
    let record = sqlx::query_as::<_, ChannelRecord>(
        r#"SELECT id, name, udid, group_name, code, gw FROM channels WHERE id = ?1"#,
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .map_err(|err| match err {
        sqlx::Error::RowNotFound => (StatusCode::NOT_FOUND, format!("channel {id} not found")),
        other => internal_error(other),
    })?;

    Ok(ChannelResponse::from(record))
}

async fn generate_duplicate_name(
    pool: &SqlitePool,
    original_name: &str,
) -> Result<String, (StatusCode, String)> {
    let base = original_name.trim();
    let base = if base.is_empty() { "Channel" } else { base };
    let mut attempt = 1usize;
    loop {
        let candidate = if attempt == 1 {
            format!("{base} Copy")
        } else {
            format!("{base} Copy {attempt}")
        };
        if !channel_name_exists(pool, &candidate).await? {
            return Ok(candidate);
        }
        attempt += 1;
    }
}

async fn channel_name_exists(pool: &SqlitePool, name: &str) -> Result<bool, (StatusCode, String)> {
    let (count,): (i64,) = sqlx::query_as(r#"SELECT COUNT(1) FROM channels WHERE name = ?1"#)
        .bind(name)
        .fetch_one(pool)
        .await
        .map_err(internal_error)?;
    Ok(count > 0)
}

async fn build_channel_snapshot(
    pool: &SqlitePool,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let channels = load_all_channels(pool).await?;
    let snapshot = ChannelsSnapshot {
        event: "channels_snapshot",
        count: channels.len(),
        channels,
    };
    Ok(serde_json::to_string(&snapshot)?)
}

async fn start_bridge_session(
    registry: &Arc<RwLock<HashMap<String, BridgeSession>>>,
    counter: &Arc<AtomicU64>,
    broadcaster: &broadcast::Sender<String>,
    remote_url: String,
    publisher_url: String,
    udid: String,
    heartbeat_seconds: u64,
) -> bool {
    let mut guard = registry.write().await;
    if guard.contains_key(&udid) {
        return false;
    }

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let session_id = counter.fetch_add(1, Ordering::Relaxed);
    let registry_clone = Arc::clone(registry);
    let udid_task = udid.clone();
    let broadcaster_clone = broadcaster.clone();
    let remote_clone = remote_url.clone();
    let publisher_clone = publisher_url.clone();
    let handle = tokio::spawn(async move {
        run_bridge(
            remote_clone,
            publisher_clone,
            udid_task.clone(),
            broadcaster_clone,
            heartbeat_seconds,
            cancel_rx,
        )
        .await;
        let mut map = registry_clone.write().await;
        if let Some(existing) = map.get(&udid_task) {
            if existing.id == session_id {
                map.remove(&udid_task);
            }
        }
    });

    guard.insert(
        udid,
        BridgeSession {
            cancel: cancel_tx,
            handle,
            id: session_id,
        },
    );

    true
}

async fn stop_bridge_session(
    registry: &Arc<RwLock<HashMap<String, BridgeSession>>>,
    udid: &str,
) -> bool {
    let entry = {
        let mut guard = registry.write().await;
        guard.remove(udid)
    };

    if let Some(entry) = entry {
        if let Err(err) = entry.cancel.send(true) {
            warn!("bridge stop signal failed for {udid}: {err}");
        }
        let udid_string = udid.to_string();
        tokio::spawn(async move {
            if let Err(err) = entry.handle.await {
                warn!("bridge task join error for {udid_string}: {err}");
            }
        });
        true
    } else {
        false
    }
}

fn map_database_error(err: sqlx::Error) -> (StatusCode, String) {
    match &err {
        sqlx::Error::Database(db_err) if db_err.message().contains("UNIQUE") => {
            (StatusCode::CONFLICT, "channel name must be unique".into())
        }
        _ => internal_error(err),
    }
}

fn internal_error<E: std::fmt::Display>(err: E) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("internal error: {err}"),
    )
}

async fn start_bridge_handler(
    State(state): State<AppState>,
    Query(params): Query<BridgeParams>,
) -> Json<BridgeResponse> {
    let config = state.config.clone();
    let udid = params
        .udid
        .clone()
        .unwrap_or_else(|| config.agent_udid.clone());
    let remote_url = params
        .remote_url
        .clone()
        .unwrap_or_else(|| config.bridge_remote_url.clone());
    let publisher_url = params
        .publisher_url
        .clone()
        .unwrap_or_else(|| build_publisher_url(config.as_ref(), &udid));
    let heartbeat_seconds = config.heartbeat_seconds;

    let started = start_bridge_session(
        &state.active_bridges,
        &state.bridge_counter,
        &state.broadcaster,
        remote_url,
        publisher_url,
        udid.clone(),
        heartbeat_seconds,
    )
    .await;

    if !started {
        return Json(BridgeResponse {
            status: "running",
            message: format!("bridge already running for udid {udid}"),
        });
    }

    Json(BridgeResponse {
        status: "started",
        message: format!("bridge launched for udid {udid}"),
    })
}

fn build_remote_url(config: &AppConfig, udid: &str) -> String {
    if config.bridge_remote_url.contains('?') {
        format!("{}&udid={udid}", config.bridge_remote_url)
    } else {
        format!("{}?udid={udid}", config.bridge_remote_url)
    }
}


fn build_publisher_url(config: &AppConfig, udid: &str) -> String {
    if config.publisher_endpoint.contains('?') {
        format!("{}&udid={udid}", config.publisher_endpoint)
    } else {
        format!("{}?udid={udid}", config.publisher_endpoint)
    }
}

async fn run_bridge(
    remote_url: String,
    publisher_url: String,
    udid: String,
    broadcaster: broadcast::Sender<String>,
    heartbeat_seconds: u64,
    shutdown: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_secs(2);
    loop {
        let outcome = tokio::select! {
            _ = wait_for_shutdown(shutdown.clone()) => {
                info!("bridge shutdown requested for {udid}");
                break;
            }
            result = bridge_once(
                &remote_url,
                &publisher_url,
                &udid,
                &broadcaster,
                heartbeat_seconds,
            ) => result,
        };

        match outcome {
            Ok(()) => {
                info!("bridge session completed for {udid}, restarting");
                backoff = Duration::from_secs(2);
            }
            Err(err) => {
                warn!("bridge error for {udid}: {err}");
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }

        let should_stop = tokio::select! {
            _ = wait_for_shutdown(shutdown.clone()) => true,
            _ = sleep(backoff) => false,
        };

        if should_stop {
            info!("bridge shutdown requested during backoff for {udid}");
            break;
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            break;
        }
    }
}

async fn bridge_once(
    remote_url: &str,
    publisher_url: &str,
    udid: &str,
    broadcaster: &broadcast::Sender<String>,
    heartbeat_seconds: u64,
) -> Result<(), BridgeError> {
    let (remote_stream, _) = connect_async(remote_url)
        .await
        .map_err(BridgeError::RemoteConnect)?;
    let (publisher_stream, _) = connect_async(publisher_url)
        .await
        .map_err(BridgeError::PublisherConnect)?;

    info!("bridge connected: remote={remote_url}, publisher={publisher_url}, udid={udid}");

    let (mut remote_write, mut remote_read) = remote_stream.split();
    let (mut publisher_write, mut publisher_read) = publisher_stream.split();
    let mut heartbeat = interval(Duration::from_secs(heartbeat_seconds));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if let Err(err) = remote_write.send(WsMessage::Ping(Vec::new())).await {
                    return Err(BridgeError::RemoteSend(err));
                }
                if let Err(err) = publisher_write.send(WsMessage::Ping(Vec::new())).await {
                    warn!("failed to send heartbeat ping to publisher: {err}");
                }
            }
            remote_msg = remote_read.next() => {
                match remote_msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        let _ = broadcaster.send(format!("[bridge:{udid}] remote -> publisher: {text}"));
                        publisher_write
                            .send(WsMessage::Text(text))
                            .await
                            .map_err(BridgeError::PublisherSend)?;
                    }
                    Some(Ok(WsMessage::Binary(data))) => {
                   
                        publisher_write
                            .send(WsMessage::Binary(data))
                            .await
                            .map_err(BridgeError::PublisherSend)?;
                    }
                    Some(Ok(WsMessage::Ping(payload))) => {
                        remote_write
                            .send(WsMessage::Pong(payload))
                            .await
                            .map_err(BridgeError::RemoteSend)?;
                    }
                    Some(Ok(WsMessage::Pong(_))) => {
                        // Heartbeat acknowledged.
                    }
                    Some(Ok(WsMessage::Close(frame))) => {
                        info!("remote closed bridge for {udid}");
                        publisher_write
                            .send(WsMessage::Close(frame.clone()))
                            .await
                            .map_err(BridgeError::PublisherSend)?;
                        return Ok(());
                    }
                    Some(Ok(WsMessage::Frame(_))) => {
                        // Ignore low-level frames.
                    }
                    Some(Err(err)) => return Err(BridgeError::RemoteReceive(err)),
                    None => return Err(BridgeError::RemoteClosed),
                }
            }
            publisher_msg = publisher_read.next() => {
                match publisher_msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        let _ = broadcaster.send(format!("[bridge:{udid}] publisher -> remote: {text}"));
                        remote_write
                            .send(WsMessage::Text(text))
                            .await
                            .map_err(BridgeError::RemoteSend)?;
                    }
                    Some(Ok(WsMessage::Binary(data))) => {
                info!("publich r");
                        remote_write
                            .send(WsMessage::Binary(data))
                            .await
                            .map_err(BridgeError::RemoteSend)?;
                    }
                    Some(Ok(WsMessage::Ping(payload))) => {

                        publisher_write
                            .send(WsMessage::Pong(payload))
                            .await
                            .map_err(BridgeError::PublisherSend)?;
                    }
                    Some(Ok(WsMessage::Pong(_))) => {
                        // No-op.
                    }
                    Some(Ok(WsMessage::Close(frame))) => {
                        info!("publisher closed bridge for {udid}");
                        remote_write
                            .send(WsMessage::Close(frame.clone()))
                            .await
                            .map_err(BridgeError::RemoteSend)?;
                        return Ok(());
                    }
                    Some(Ok(WsMessage::Frame(_))) => {
                        // Ignore low-level frames.
                    }
                    Some(Err(err)) => return Err(BridgeError::PublisherReceive(err)),
                    None => return Err(BridgeError::PublisherClosed),
                }
            }
        }
    }
}

#[derive(Debug)]
enum BridgeError {
    RemoteConnect(tokio_tungstenite::tungstenite::Error),
    PublisherConnect(tokio_tungstenite::tungstenite::Error),
    RemoteSend(tokio_tungstenite::tungstenite::Error),
    PublisherSend(tokio_tungstenite::tungstenite::Error),
    RemoteReceive(tokio_tungstenite::tungstenite::Error),
    PublisherReceive(tokio_tungstenite::tungstenite::Error),
    RemoteClosed,
    PublisherClosed,
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BridgeError::RemoteConnect(err) => write!(f, "remote connect error: {err}"),
            BridgeError::PublisherConnect(err) => write!(f, "publisher connect error: {err}"),
            BridgeError::RemoteSend(err) => write!(f, "remote send error: {err}"),
            BridgeError::PublisherSend(err) => write!(f, "publisher send error: {err}"),
            BridgeError::RemoteReceive(err) => write!(f, "remote receive error: {err}"),
            BridgeError::PublisherReceive(err) => write!(f, "publisher receive error: {err}"),
            BridgeError::RemoteClosed => write!(f, "remote websocket closed"),
            BridgeError::PublisherClosed => write!(f, "publisher websocket closed"),
        }
    }
}

impl std::error::Error for BridgeError {}
