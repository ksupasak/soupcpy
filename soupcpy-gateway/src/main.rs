use std::{
    collections::HashMap,
    env, fmt, fs,
    io::ErrorKind,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::{Html, IntoResponse, Json},
    routing::{get, get_service},
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{broadcast, broadcast::error::RecvError, mpsc, Notify, RwLock},
};
use tower_http::services::ServeDir;
use tracing::{info, warn};

type ClientTx = mpsc::UnboundedSender<Message>;

/// Registry entry for one live socket. `session_id` lets a session prove it
/// still owns the slot before removing it, so a session that was replaced
/// cannot evict the connection that replaced it.
#[derive(Clone)]
struct ClientHandle {
    session_id: u64,
    tx: ClientTx,
    shutdown: Arc<Notify>,
}

#[derive(Clone)]
struct AppState {
    broadcaster: broadcast::Sender<String>,
    started_at: Instant,
    connections: ConnectionRegistry,
    channels: ChannelStore,
    settings: AppSettings,
    session_counter: Arc<AtomicU64>,
}

impl AppState {
    /// Monotonic id identifying one socket for the lifetime of the process.
    fn next_session_id(&self) -> u64 {
        self.session_counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Clone, Default)]
struct ConnectionRegistry {
    receivers: Arc<RwLock<HashMap<String, ClientHandle>>>,
    controllers: Arc<RwLock<HashMap<String, ClientHandle>>>,
    publishers: Arc<RwLock<HashMap<String, ClientHandle>>>,
    dashboards: Arc<RwLock<HashMap<String, ClientHandle>>>,
}

#[derive(Clone, Default)]
struct ChannelStore {
    channels: Arc<RwLock<HashMap<String, Vec<ChannelDescriptor>>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChannelDescriptor {
    id: u64,
    name: String,
    udid: String,
    #[serde(default)]
    group: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    gw: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct IncomingChannelsSnapshot {
    event: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    count: usize,
    #[serde(default)]
    channels: Vec<ChannelDescriptor>,
}

#[derive(Debug, Clone, Serialize)]
struct ChannelsOverview {
    total_channels: usize,
    controllers: Vec<ControllerChannels>,
    merged_channels: Vec<MergedChannel>,
}

#[derive(Debug, Clone, Serialize)]
struct ControllerChannels {
    udid: String,
    count: usize,
    last_updated_epoch: u64,
    channels: Vec<ChannelDescriptor>,
}

#[derive(Debug, Clone, Serialize)]
struct MergedChannel {
    controller_udid: String,
    channel: ChannelDescriptor,
}

#[derive(Serialize)]
struct ChannelsUpdateMessage {
    event: &'static str,
    controller_udid: String,
    overview: ChannelsOverview,
}

#[derive(Serialize)]
struct SettingsResponse {
    http_base: String,
    ws_base: String,
}

#[derive(Clone)]
struct AppSettings {
    http_base: String,
    ws_base: String,
}

impl AppSettings {
    fn from_config(config: &ServerConfig) -> Self {
        Self {
            http_base: config.http_base(),
            ws_base: config.ws_base(),
        }
    }
}

const DEFAULT_CONFIG_PATH: &str = "config/server.toml";

#[derive(Debug, Clone, Deserialize)]
struct ServerConfig {
    #[serde(default = "default_host")]
    host: String,
    #[serde(default = "default_port")]
    port: u16,
    #[serde(default)]
    public_host: Option<String>,
    #[serde(default)]
    public_port: Option<u16>,
}

impl ServerConfig {
    fn load() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let path = env::var("GATEWAY_CONFIG").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_owned());
        match fs::read_to_string(&path) {
            Ok(contents) => {
                let config: ServerConfig = toml::from_str(&contents)?;
                info!("loaded server config from {path}");
                Ok(config)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                info!(
                    "config file {path} not found; using defaults host={} port={}",
                    default_host(),
                    default_port()
                );
                Ok(ServerConfig::default())
            }
            Err(err) => Err(Box::new(err)),
        }
    }

    fn bind_target(&self) -> (&str, u16) {
        (self.host.as_str(), self.port)
    }

    fn http_base(&self) -> String {
        format!("http://{}:{}", self.public_host(), self.public_port())
    }

    fn ws_base(&self) -> String {
        format!("ws://{}:{}", self.public_host(), self.public_port())
    }

    fn public_host(&self) -> &str {
        self.public_host.as_deref().unwrap_or(self.host.as_str())
    }

    fn public_port(&self) -> u16 {
        self.public_port.unwrap_or(self.port)
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            public_host: None,
            public_port: None,
        }
    }
}

const fn default_port() -> u16 {
    3000
}

fn default_host() -> String {
    "0.0.0.0".to_owned()
}

impl ChannelStore {
    async fn update_channels(
        &self,
        name: &str,
        snapshot: IncomingChannelsSnapshot,
    ) -> ChannelsOverview {
        let reported_count = snapshot.count;
        let mut map = self.channels.write().await;
        map.insert(name.to_owned(), snapshot.channels);
        drop(map);
        let mut overview = self.overview().await;
        if reported_count > 0 {
            if let Some(controller) = overview.controllers.iter_mut().find(|c| c.udid == name) {
                controller.count = reported_count;
            }
            overview.total_channels = overview.controllers.iter().map(|c| c.count).sum();
        }
        overview
    }

    async fn find_channel(&self, name: &str) -> Option<ChannelDescriptor> {
        let map = self.channels.read().await;
        for channels in map.values() {
            if let Some(found) = channels
                .iter()
                .find(|channel| channel.udid == name || channel.name == name)
            {
                return Some(found.clone());
            }
        }
        None
    }

    async fn overview(&self) -> ChannelsOverview {
        let map = self.channels.read().await;
        let mut controllers = Vec::with_capacity(map.len());
        let mut total_channels = 0;
        let mut merged_channels = Vec::new();

        for (name, channels) in map.iter() {
            total_channels += channels.len();
            let channel_count = channels.len();
            controllers.push(ControllerChannels {
                udid: name.clone(),
                count: channel_count,
                last_updated_epoch: 0,
                channels: channels.clone(),
            });
            for channel in channels {
                merged_channels.push(MergedChannel {
                    controller_udid: name.clone(),
                    channel: channel.clone(),
                });
            }
        }

        controllers.sort_by(|a, b| a.udid.cmp(&b.udid));
        merged_channels.sort_by(|a, b| {
            let group_a = a.channel.group.as_deref().unwrap_or_default();
            let group_b = b.channel.group.as_deref().unwrap_or_default();
            match group_a.cmp(group_b) {
                std::cmp::Ordering::Equal => a.channel.name.cmp(&b.channel.name),
                other => other,
            }
        });

        ChannelsOverview {
            total_channels,
            controllers,
            merged_channels,
        }
    }
}

#[derive(Clone, Copy)]
enum EndpointKind {
    Receiver,
    Controller,
    Publisher,
    /// Browser dashboard: observes the log/snapshot broadcast and routes nowhere.
    Dashboard,
}

impl EndpointKind {
    const fn label(self) -> &'static str {
        match self {
            EndpointKind::Receiver => "receiver",
            EndpointKind::Controller => "controller",
            EndpointKind::Publisher => "publisher",
            EndpointKind::Dashboard => "dashboard",
        }
    }
}

impl fmt::Display for EndpointKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

impl ConnectionRegistry {
    async fn insert(
        &self,
        kind: EndpointKind,
        udid: String,
        handle: ClientHandle,
    ) -> Option<ClientHandle> {
        self.map(kind).write().await.insert(udid, handle)
    }

    /// Remove `udid` only while it is still held by `session_id`, returning
    /// whether this session owned the slot. A session that was already replaced
    /// gets `false` and leaves the newer connection registered.
    async fn remove_if_current(&self, kind: EndpointKind, udid: &str, session_id: u64) -> bool {
        let mut map = self.map(kind).write().await;
        match map.get(udid) {
            Some(handle) if handle.session_id == session_id => {
                map.remove(udid);
                true
            }
            _ => false,
        }
    }

    async fn send_text(&self, kind: EndpointKind, udid: &str, payload: &str) {
        let map = self.map(kind).read().await;
        if let Some(handle) = map.get(udid) {
            info!("send_text {kind} {udid} {payload}");
            let _ = handle.tx.send(Message::Text(payload.to_owned()));
        }
    }

    async fn send_binary(&self, kind: EndpointKind, udid: &str, payload: Vec<u8>) {
        let map = self.map(kind).read().await;
        if let Some(handle) = map.get(udid) {
            let _ = handle.tx.send(Message::Binary(payload));
        }
    }

    async fn broadcast_text(&self, kind: EndpointKind, payload: &str) {
        let map = self.map(kind).read().await;
        for handle in map.values() {
            let _ = handle.tx.send(Message::Text(payload.to_owned()));
        }
    }

    fn map(&self, kind: EndpointKind) -> &Arc<RwLock<HashMap<String, ClientHandle>>> {
        match kind {
            EndpointKind::Receiver => &self.receivers,
            EndpointKind::Controller => &self.controllers,
            EndpointKind::Publisher => &self.publishers,
            EndpointKind::Dashboard => &self.dashboards,
        }
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    uptime_seconds: u64,
}

#[derive(Deserialize)]
struct EndpointQuery {
    udid: Option<String>,
}

const INDEX_HTML: &str = include_str!("../templates/index.html");
const MULTIVIEW_HTML: &str = include_str!("../templates/multiview.html");
const STREAM_HTML: &str = include_str!("../templates/stream.html");

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("server failed: {err}");
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();

    let config = match ServerConfig::load() {
        Ok(cfg) => cfg,
        Err(err) => {
            warn!("failed to load server config; using defaults: {err}");
            ServerConfig::default()
        }
    };
    let settings = AppSettings::from_config(&config);

    let (tx, _rx) = broadcast::channel(128);
    let state = AppState {
        broadcaster: tx,
        started_at: Instant::now(),
        connections: ConnectionRegistry::default(),
        channels: ChannelStore::default(),
        settings: settings.clone(),
        session_counter: Arc::new(AtomicU64::new(1)),
    };

    let static_assets = get_service(ServeDir::new("public"));

    let app = Router::new()
        .route("/", get(index))
        .route("/multiview", get(multiview))
        .route("/stream", get(stream))
        .route("/health", get(health))
        .route("/ws", get(ws_receiver_handler))
        .route("/ws/publisher", get(ws_publisher_handler))
        .route("/ws/controller", get(ws_controller_handler))
        .route("/ws/dashboard", get(ws_dashboard_handler))
        .route("/api/settings", get(runtime_settings))
        .route("/api/channels", get(list_channels))
        .nest_service("/public", static_assets)
        .with_state(state);

    let listener = TcpListener::bind(config.bind_target()).await?;
    let actual_addr = listener.local_addr()?;
    info!(
        "HTTP and WebSocket server listening on http://{actual_addr} (public base {})",
        settings.http_base
    );

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

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn multiview() -> Html<&'static str> {
    Html(MULTIVIEW_HTML)
}

async fn stream() -> Html<&'static str> {
    Html(STREAM_HTML)
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let uptime = state.started_at.elapsed().as_secs();
    Json(HealthResponse {
        status: "ok",
        uptime_seconds: uptime,
    })
}

async fn runtime_settings(State(state): State<AppState>) -> Json<SettingsResponse> {
    Json(SettingsResponse {
        http_base: state.settings.http_base.clone(),
        ws_base: state.settings.ws_base.clone(),
    })
}

async fn list_channels(State(state): State<AppState>) -> Json<ChannelsOverview> {
    let overview = state.channels.overview().await;
    Json(overview)
}

async fn ws_receiver_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<EndpointQuery>,
) -> impl IntoResponse {
    let session_id = state.next_session_id();
    let udid = routed_udid(query, EndpointKind::Receiver);
    upgrade_endpoint(ws, state, udid, EndpointKind::Receiver, session_id)
}

async fn ws_publisher_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<EndpointQuery>,
) -> impl IntoResponse {
    let session_id = state.next_session_id();
    let udid = routed_udid(query, EndpointKind::Publisher);
    upgrade_endpoint(ws, state, udid, EndpointKind::Publisher, session_id)
}

async fn ws_controller_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<EndpointQuery>,
) -> impl IntoResponse {
    let session_id = state.next_session_id();
    let udid = routed_udid(query, EndpointKind::Controller);
    upgrade_endpoint(ws, state, udid, EndpointKind::Controller, session_id)
}

async fn ws_dashboard_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(query): Query<EndpointQuery>,
) -> impl IntoResponse {
    let session_id = state.next_session_id();
    // Dashboards observe rather than route, so each socket gets its own key:
    // several open tabs must not evict one another, and they must not land in
    // the receiver namespace where a real stream could share the slot.
    let udid = query
        .udid
        .unwrap_or_else(|| format!("dashboard-{session_id}"));
    upgrade_endpoint(ws, state, udid, EndpointKind::Dashboard, session_id)
}

/// Routing key for an endpoint that is addressed by udid. A missing `udid`
/// still falls back to "default" for compatibility, but it is a misconfigured
/// client and worth saying so in the log.
fn routed_udid(query: EndpointQuery, kind: EndpointKind) -> String {
    query.udid.unwrap_or_else(|| {
        warn!("{kind} connected without a udid; routing it as 'default'");
        "default".to_owned()
    })
}

fn upgrade_endpoint(
    ws: WebSocketUpgrade,
    state: AppState,
    udid: String,
    kind: EndpointKind,
    session_id: u64,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| {
        let state = state.clone();
        let udid = udid.clone();
        async move {
            if let Err(err) = websocket_session(socket, kind, udid, session_id, state).await {
                warn!("{kind} session ended with error: {err}");
            }
        }
    })
}

async fn websocket_session(
    mut socket: WebSocket,
    kind: EndpointKind,
    udid: String,
    session_id: u64,
    state: AppState,
) -> Result<(), axum::Error> {
    let label = kind.label();
    let connections = state.connections.clone();
    // Only dashboards tail the log/snapshot broadcast. Receiver and publisher
    // sockets carry the scrcpy protocol, and injecting log text into them
    // corrupts the stream.
    let mut broadcast_rx =
        matches!(kind, EndpointKind::Dashboard).then(|| state.broadcaster.subscribe());
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Message>();
    let shutdown = Arc::new(Notify::new());

    if let Some(previous) = connections
        .insert(
            kind,
            udid.clone(),
            ClientHandle {
                session_id,
                tx: outbound_tx.clone(),
                shutdown: shutdown.clone(),
            },
        )
        .await
    {
        warn!("replaced existing {label} connection for udid '{udid}', closing prior socket");
        previous.shutdown.notify_one();
    }

    info!("{label} connected: {udid}");

    if let EndpointKind::Receiver = kind {
        // &udid can be name
        let s = state.channels.find_channel(&udid).await;
        if let Some(channel) = s {
            let gateway = channel.gw.unwrap_or(0);
            info!("sending connect to controller {gateway} for channel {udid}");

            connections
                .send_text(
                    EndpointKind::Controller,
                    gateway.to_string().as_str(),
                    format!(
                        "connect publisher_udid={} udid={}",
                        channel.name, channel.udid
                    )
                    .as_str(),
                )
                .await;
        }
    }

    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                info!("{label} {udid} superseded by a newer connection, closing");
                let _ = socket.send(Message::Close(None)).await;
                break;
            }
            Some(message) = outbound_rx.recv() => {
                if let Err(err) = socket.send(message).await {
                    warn!("error sending to {label} {udid}: {err}");
                    break;
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let trimmed = text.trim();
                        if trimmed.is_empty() {
                            continue;
                        }

                        if matches!(kind, EndpointKind::Controller) {
                            if let Ok(snapshot) =
                                serde_json::from_str::<IncomingChannelsSnapshot>(trimmed)
                            {
                                if snapshot.event == "channels_snapshot" {
                                    let name = snapshot.name.clone();
                                    let overview = state
                                        .channels
                                        .update_channels(&name, snapshot)
                                        .await;
                                    if let Ok(json) = serde_json::to_string(&ChannelsUpdateMessage {
                                        event: "channels_snapshot_update",
                                        controller_udid: udid.clone(),
                                        overview,
                                    }) {
                                        let _ = state.broadcaster.send(json);
                                    }
                                }
                            }
                        }

                        let log_line = format!(
                            "[{label}:{udid}] {trimmed} (handled by thread {:?})",
                            std::thread::current().id()
                        );
                        let _ = state.broadcaster.send(log_line);

                        forward_direct(kind, &connections, &udid, trimmed).await;
                    }
                    Some(Ok(Message::Binary(data))) => {
                        // Ignore binary payloads for now.

                        match kind {
                            EndpointKind::Receiver => {
                                connections
                                .send_binary(EndpointKind::Publisher, &udid, data)
                                .await;
                            }
                            EndpointKind::Controller => {
                                // connections
                                // .send_text(EndpointKind::Receiver, &udid, "Welcome to the controller!")
                                // .await;
                            }
                            EndpointKind::Publisher => {
                                connections
                                .send_binary(EndpointKind::Receiver, &udid, data)
                                .await;
                            }
                            EndpointKind::Dashboard => {
                                // Dashboards never carry stream payloads.
                            }
                        }


                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if outbound_tx.send(Message::Pong(payload)).is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        // No-op; heartbeat acknowledged.
                    }
                    Some(Ok(Message::Close(frame))) => {
                        if let Err(err) = socket.send(Message::Close(frame)).await {
                            warn!("error echoing close to {label} {udid}: {err}");
                        }
                        break;
                    }
                    Some(Err(err)) => {
                        warn!("error receiving ws message for {label} {udid}: {err}");
                        break;
                    }
                    None => break,
                }
            }
            broadcast_msg = async {
                match broadcast_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    // Not a dashboard: this arm must never become ready.
                    None => std::future::pending().await,
                }
            } => {
                match broadcast_msg {
                    Ok(msg) => {
                        if let Err(err) = socket.send(Message::Text(msg)).await {
                            warn!("error broadcasting to {label} {udid}: {err}");
                            break;
                        }
                    }
                    Err(RecvError::Lagged(skipped)) => {
                        warn!("{label} {udid} lagged; skipped {skipped} broadcast messages");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        }
    }

    let was_current = connections.remove_if_current(kind, &udid, session_id).await;
    info!("{label} disconnected: {udid}");

    // Only the session still holding the slot announces the disconnect. A
    // superseded session's replacement is live, so tearing down the publisher
    // here would kill the stream that just reconnected.
    if was_current {
        if let EndpointKind::Receiver = kind {
            // &udid can be name
            let s = state.channels.find_channel(&udid).await;
            if let Some(channel) = s {
                let gateway = channel.gw.unwrap_or(0);
                info!("sending disconnect to controller {gateway} for channel {udid}");

                connections
                    .send_text(
                        EndpointKind::Controller,
                        gateway.to_string().as_str(),
                        format!(
                            "disconnect publisher_udid={} udid={}",
                            channel.name, channel.udid
                        )
                        .as_str(),
                    )
                    .await;
            }
        }
    }

    Ok(())
}

async fn forward_direct(
    kind: EndpointKind,
    connections: &ConnectionRegistry,
    udid: &str,
    payload: &str,
) {
    match kind {
        EndpointKind::Receiver => {
            connections
                .send_text(EndpointKind::Controller, udid, payload)
                .await;
        }
        EndpointKind::Controller => {
            connections
                .send_text(EndpointKind::Receiver, udid, payload)
                .await;
        }
        EndpointKind::Publisher => {
            connections
                .send_text(EndpointKind::Controller, udid, payload)
                .await;
            connections
                .broadcast_text(EndpointKind::Receiver, payload)
                .await;
        }
        EndpointKind::Dashboard => {
            // The dashboard is a monitor: whatever it sends is already echoed
            // back through the log broadcast, so it routes nowhere.
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(session_id: u64) -> (ClientHandle, mpsc::UnboundedReceiver<Message>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            ClientHandle {
                session_id,
                tx,
                shutdown: Arc::new(Notify::new()),
            },
            rx,
        )
    }

    fn expect_text(message: Message) -> String {
        match message {
            Message::Text(text) => text,
            other => panic!("expected a text frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn remove_if_current_releases_the_owning_session() {
        let registry = ConnectionRegistry::default();
        let (only, _rx) = handle(7);
        registry
            .insert(EndpointKind::Receiver, "dev-1".to_owned(), only)
            .await;

        assert!(
            registry
                .remove_if_current(EndpointKind::Receiver, "dev-1", 7)
                .await
        );
        assert!(registry.map(EndpointKind::Receiver).read().await.is_empty());
    }

    #[tokio::test]
    async fn superseded_session_cannot_evict_its_replacement() {
        let registry = ConnectionRegistry::default();
        let (first, _first_rx) = handle(1);
        let (second, mut second_rx) = handle(2);

        registry
            .insert(EndpointKind::Receiver, "dev-1".to_owned(), first)
            .await;
        let previous = registry
            .insert(EndpointKind::Receiver, "dev-1".to_owned(), second)
            .await;
        assert!(
            previous.is_some(),
            "the second insert should displace the first"
        );

        // The stale session tears down after its replacement registered.
        assert!(
            !registry
                .remove_if_current(EndpointKind::Receiver, "dev-1", 1)
                .await,
            "a stale session must not report ownership"
        );

        // The replacement has to stay routable.
        registry
            .send_text(EndpointKind::Receiver, "dev-1", "still here")
            .await;
        let delivered = second_rx
            .try_recv()
            .expect("replacement should be reachable");
        assert_eq!(expect_text(delivered), "still here");
    }

    #[tokio::test]
    async fn replaced_session_is_told_to_shut_down() {
        let registry = ConnectionRegistry::default();
        let (first, _first_rx) = handle(1);
        let (second, _second_rx) = handle(2);

        registry
            .insert(EndpointKind::Controller, "gw-1".to_owned(), first)
            .await;
        let previous = registry
            .insert(EndpointKind::Controller, "gw-1".to_owned(), second)
            .await
            .expect("previous handle");

        // `notify_one` leaves a permit, so the old session wakes even if it is
        // not parked on `notified()` yet.
        previous.shutdown.notify_one();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                previous.shutdown.notified(),
            )
            .await
            .is_ok(),
            "the replaced session should observe the shutdown signal"
        );
    }

    #[tokio::test]
    async fn receiver_traffic_never_reaches_a_dashboard() {
        let registry = ConnectionRegistry::default();
        let (dash, mut dash_rx) = handle(1);
        registry
            .insert(EndpointKind::Dashboard, "dashboard-1".to_owned(), dash)
            .await;

        registry
            .broadcast_text(EndpointKind::Receiver, "stream chatter")
            .await;

        assert!(
            dash_rx.try_recv().is_err(),
            "dashboards sit in their own namespace"
        );
    }

    #[tokio::test]
    async fn dashboards_with_distinct_ids_coexist() {
        let registry = ConnectionRegistry::default();
        let (first, mut first_rx) = handle(1);
        let (second, mut second_rx) = handle(2);

        // Mirrors two open browser tabs, each keyed by its own session id.
        registry
            .insert(EndpointKind::Dashboard, "dashboard-1".to_owned(), first)
            .await;
        registry
            .insert(EndpointKind::Dashboard, "dashboard-2".to_owned(), second)
            .await;

        registry
            .broadcast_text(EndpointKind::Dashboard, "log line")
            .await;

        assert_eq!(
            expect_text(first_rx.try_recv().expect("first tab")),
            "log line"
        );
        assert_eq!(
            expect_text(second_rx.try_recv().expect("second tab")),
            "log line"
        );
    }
}
