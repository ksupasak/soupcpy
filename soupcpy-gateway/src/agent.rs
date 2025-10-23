use std::fmt;

use futures::{SinkExt, StreamExt};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{protocol::Message, Error as WsError},
    WebSocketStream,
};
use url::Url;

/// Reported errors from the controller WebSocket client.
#[derive(Debug)]
pub enum AgentError {
    InvalidUrl(url::ParseError),
    UnsupportedScheme(String),
    SchemeAdjust,
    Connect(WsError),
    Send(WsError),
    Receive(WsError),
    ChannelClosed,
    DriverJoin(tokio::task::JoinError),
    Serialize(serde_json::Error),
}

impl fmt::Display for AgentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AgentError::InvalidUrl(err) => write!(f, "invalid controller url: {err}"),
            AgentError::UnsupportedScheme(scheme) => {
                write!(f, "unsupported url scheme '{scheme}' for controller channel")
            }
            AgentError::SchemeAdjust => write!(f, "failed to adjust url scheme"),
            AgentError::Connect(err) => write!(f, "websocket connect error: {err}"),
            AgentError::Send(err) => write!(f, "failed to send websocket message: {err}"),
            AgentError::Receive(err) => write!(f, "failed while receiving websocket message: {err}"),
            AgentError::ChannelClosed => write!(f, "controller channel closed"),
            AgentError::DriverJoin(err) => write!(f, "controller task exited unexpectedly: {err}"),
            AgentError::Serialize(err) => write!(f, "failed to serialize payload: {err}"),
        }
    }
}

impl std::error::Error for AgentError {}

/// Message stream items yielded by the controller agent.
pub type ControllerEvent = Result<Message, AgentError>;

/// Async client that connects to `/ws/controller` and forwards messages via channels.
pub struct ControllerAgent {
    endpoint: Url,
    outbound: mpsc::UnboundedSender<Message>,
    inbound: mpsc::UnboundedReceiver<ControllerEvent>,
    shutdown: Option<oneshot::Sender<()>>,
    driver: JoinHandle<()>,
}

impl ControllerAgent {
    /// Establish a controller connection for the given `udid`.
    pub async fn connect(
        server_origin: impl AsRef<str>,
        udid: impl AsRef<str>,
    ) -> Result<Self, AgentError> {
        let endpoint = build_controller_url(server_origin.as_ref(), udid.as_ref())?;
        let (ws_stream, _) = connect_async(endpoint.clone())
            .await
            .map_err(AgentError::Connect)?;

        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Drive send/receive work on a dedicated task so tests and binaries can poll `next_event`.
        let driver = tokio::spawn(async move {
            if let Err(err) =
                run_connection(ws_stream, outbound_rx, inbound_tx.clone(), shutdown_rx).await
            {
                let _ = inbound_tx.send(Err(err));
            }
        });

        Ok(Self {
            endpoint,
            outbound: outbound_tx,
            inbound: inbound_rx,
            shutdown: Some(shutdown_tx),
            driver,
        })
    }

    /// URL used for the active controller connection.
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Queue a raw WebSocket message for delivery to the controller channel.
    pub fn send(&self, message: Message) -> Result<(), AgentError> {
        self.outbound
            .send(message)
            .map_err(|_| AgentError::ChannelClosed)
    }

    /// Convenience helper to send text frames.
    pub fn send_text(&self, text: impl Into<String>) -> Result<(), AgentError> {
        self.send(Message::Text(text.into()))
    }

    /// Convenience helper to send JSON payloads, serializing with `serde_json`.
    pub fn send_json<T>(&self, payload: &T) -> Result<(), AgentError>
    where
        T: serde::Serialize,
    {
        let text = serde_json::to_string(payload).map_err(AgentError::Serialize)?;
        self.send_text(text)
    }

    /// Await the next inbound message or error from the controller connection.
    pub async fn next_event(&mut self) -> Option<ControllerEvent> {
        self.inbound.recv().await
    }

    /// Gracefully close the WebSocket and await the driver task.
    pub async fn close(mut self) -> Result<(), AgentError> {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.driver.await.map_err(AgentError::DriverJoin)?;
        Ok(())
    }
}

async fn run_connection(
    ws_stream: WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    mut outbound_rx: mpsc::UnboundedReceiver<Message>,
    inbound_tx: mpsc::UnboundedSender<ControllerEvent>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), AgentError> {
    let (mut writer, mut reader) = ws_stream.split();

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => {
                if let Err(err) = writer.send(Message::Close(None)).await {
                    let _ = inbound_tx.send(Err(AgentError::Send(err)));
                } else {
                    let _ = inbound_tx.send(Ok(Message::Close(None)));
                }
                break;
            }
            Some(to_send) = outbound_rx.recv() => {
                if let Err(err) = writer.send(to_send).await {
                    return Err(AgentError::Send(err));
                }
            }
            incoming = reader.next() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        // Respond to ping frames immediately to keep the connection alive.
                        if let Err(err) = writer.send(Message::Pong(payload)).await {
                            return Err(AgentError::Send(err));
                        }
                    }
                    Some(Ok(message)) => {
                        let should_break = matches!(message, Message::Close(_));
                        let _ = inbound_tx.send(Ok(message));
                        if should_break {
                            break;
                        }
                    }
                    Some(Err(err)) => return Err(AgentError::Receive(err)),
                    None => {
                        let _ = inbound_tx.send(Ok(Message::Close(None)));
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn build_controller_url(base: &str, udid: &str) -> Result<Url, AgentError> {
    let mut origin = Url::parse(base).map_err(AgentError::InvalidUrl)?;
    match origin.scheme() {
        "http" => origin.set_scheme("ws").map_err(|_| AgentError::SchemeAdjust)?,
        "https" => origin
            .set_scheme("wss")
            .map_err(|_| AgentError::SchemeAdjust)?,
        "ws" | "wss" => {}
        other => return Err(AgentError::UnsupportedScheme(other.to_owned())),
    }

    let mut url = origin
        .join("ws/controller")
        .map_err(AgentError::InvalidUrl)?;
    url.query_pairs_mut().append_pair("udid", udid);
    Ok(url)
}
