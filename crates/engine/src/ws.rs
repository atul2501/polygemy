//! WebSocket connection manager shared by every collector: connect, subscribe, read, heartbeat,
//! detect silence, and reconnect with exponential backoff + full jitter. Source-specific logic lives
//! in a `Session`; its handlers are synchronous so the read loop never awaits on anything but I/O.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::clock::Received;
use crate::health::FeedHealth;

pub trait Session: Send + 'static {
    fn url(&self) -> String;
    /// Frames to send right after connecting (auth, subscriptions).
    fn on_connect(&mut self) -> Vec<String>;
    /// Handle one text frame; frames pushed to `out` are sent right away.
    /// An `Err` forces a reconnect (e.g. an unrecoverable sequence gap).
    fn on_text(&mut self, text: &str, rx: Received, out: &mut Vec<String>) -> Result<()>;
    /// Called once per second while connected (periodic snapshots, readiness checks).
    fn on_tick(&mut self, _out: &mut Vec<String>) {}
    fn on_disconnect(&mut self) {}
    /// Application-level heartbeat (interval, frame) for sources that require one.
    fn app_ping(&self) -> Option<(Duration, String)> {
        None
    }
    /// Reconnect if nothing at all arrives for this long.
    fn idle_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }
}

const BACKOFF_BASE: Duration = Duration::from_secs(1);
const BACKOFF_CAP: Duration = Duration::from_secs(30);
/// Close codes meaning "fix the request first" (PolyBolt auth failure / policy violation).
const FATAL_CLOSE_BACKOFF: Duration = Duration::from_secs(60);

/// Runs a session until `shutdown` flips to true. `commands` carries extra outbound frames
/// (e.g. Polymarket subscribe/unsubscribe) without reconnecting.
pub async fn run<S: Session>(
    mut session: S,
    health: Arc<FeedHealth>,
    mut commands: Option<mpsc::UnboundedReceiver<String>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let name = health.name;
    let mut attempt: u32 = 0;
    let mut down_since: Option<Instant> = None;
    while !*shutdown.borrow() {
        let started = Instant::now();
        let result = tokio::select! {
            r = connection(&mut session, &health, commands.as_mut(), &mut down_since) => r,
            _ = shutdown.changed() => break,
        };
        session.on_disconnect();
        let (err, min_wait) = match result {
            Ok(()) => (None, Duration::ZERO),
            Err(Disconnect::Fatal(e)) => (Some(e), FATAL_CLOSE_BACKOFF),
            Err(Disconnect::Retry(e)) => (Some(e), Duration::ZERO),
        };
        let msg = err.as_ref().map(|e| format!("{e:#}"));
        warn!(
            feed = name,
            error = msg.as_deref().unwrap_or("closed"),
            "disconnected"
        );
        health.on_disconnected(msg);
        down_since.get_or_insert_with(Instant::now);
        // A connection that stayed up a while resets the backoff.
        attempt = if started.elapsed() > Duration::from_secs(60) {
            0
        } else {
            attempt + 1
        };
        let ceiling = BACKOFF_BASE
            .saturating_mul(1 << attempt.min(5))
            .min(BACKOFF_CAP);
        let wait = min_wait.max(Duration::from_millis(fastrand::u64(
            0..=ceiling.as_millis() as u64,
        )));
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.changed() => break,
        }
    }
    health.on_stopped();
    info!(feed = name, "collector stopped");
}

enum Disconnect {
    Retry(anyhow::Error),
    Fatal(anyhow::Error),
}

impl<E: Into<anyhow::Error>> From<E> for Disconnect {
    fn from(e: E) -> Self {
        Disconnect::Retry(e.into())
    }
}

async fn connection<S: Session>(
    session: &mut S,
    health: &FeedHealth,
    mut commands: Option<&mut mpsc::UnboundedReceiver<String>>,
    down_since: &mut Option<Instant>,
) -> Result<(), Disconnect> {
    let url = session.url();
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .map_err(|_| anyhow!("connect timeout"))??;
    let (mut write, mut read) = ws.split();
    health.on_connected();
    match down_since.take() {
        Some(t) => info!(
            feed = health.name,
            reconnect_ms = t.elapsed().as_millis() as u64,
            "reconnected"
        ),
        None => info!(feed = health.name, "connected"),
    }
    // Frames queued while disconnected are superseded by on_connect's full subscription set.
    if let Some(c) = commands.as_deref_mut() {
        while c.try_recv().is_ok() {}
    }
    for frame in session.on_connect() {
        write.send(Message::text(frame)).await?;
    }

    let ping = session.app_ping();
    let mut ping_timer = tokio::time::interval(
        ping.as_ref()
            .map(|p| p.0)
            .unwrap_or(Duration::from_secs(3600)),
    );
    ping_timer.tick().await;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let idle = session.idle_timeout();
    let mut last_rx = Instant::now();
    let mut out = Vec::new();

    loop {
        tokio::select! {
            msg = read.next() => {
                let Some(msg) = msg else { return Err(anyhow!("stream ended").into()) };
                let msg = msg?;
                last_rx = Instant::now();
                match msg {
                    Message::Text(t) => {
                        let rx = Received::now();
                        session.on_text(t.as_str(), rx, &mut out)?;
                    }
                    Message::Binary(b) => {
                        let rx = Received::now();
                        let t = std::str::from_utf8(&b).map_err(|e| anyhow!("non-utf8 binary frame: {e}"))?;
                        session.on_text(t, rx, &mut out)?;
                    }
                    Message::Ping(p) => write.send(Message::Pong(p)).await?,
                    Message::Close(frame) => {
                        let code = frame.as_ref().map(|f| f.code);
                        let reason = frame.as_ref().map(|f| f.reason.to_string()).unwrap_or_default();
                        let e = anyhow!("closed by server: {code:?} {reason}");
                        return Err(match code {
                            Some(CloseCode::Library(4001)) | Some(CloseCode::Library(4008)) => Disconnect::Fatal(e),
                            _ => Disconnect::Retry(e),
                        });
                    }
                    _ => {}
                }
            }
            Some(frame) = async { match commands.as_deref_mut() { Some(c) => c.recv().await, None => std::future::pending().await } } => {
                out.push(frame);
            }
            _ = ping_timer.tick(), if ping.is_some() => {
                out.push(ping.as_ref().map(|p| p.1.clone()).unwrap_or_default());
            }
            _ = tick.tick() => {
                if last_rx.elapsed() > idle {
                    return Err(anyhow!("no data for {:?}", idle).into());
                }
                session.on_tick(&mut out);
            }
        }
        for frame in out.drain(..) {
            write.send(Message::text(frame)).await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    struct Echo {
        url: String,
        texts: Arc<AtomicUsize>,
    }

    impl Session for Echo {
        fn url(&self) -> String {
            self.url.clone()
        }
        fn on_connect(&mut self) -> Vec<String> {
            vec!["subscribe".into()]
        }
        fn on_text(&mut self, _: &str, _: Received, _: &mut Vec<String>) -> Result<()> {
            self.texts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// The server sends one message per connection and then drops the socket. The manager must
    /// reconnect, resubscribe and keep counting.
    #[tokio::test]
    async fn reconnects_after_server_drops() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let subscribes = Arc::new(AtomicUsize::new(0));
        let subs = subscribes.clone();
        tokio::spawn(async move {
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                if let Some(Ok(Message::Text(t))) = ws.next().await {
                    assert_eq!(t.as_str(), "subscribe");
                    subs.fetch_add(1, Ordering::SeqCst);
                }
                ws.send(Message::text("hello")).await.unwrap();
                drop(ws);
            }
        });

        let texts = Arc::new(AtomicUsize::new(0));
        let health = Arc::new(FeedHealth::new("echo", false, Duration::from_secs(5)));
        let (stop_tx, stop_rx) = watch::channel(false);
        let session = Echo {
            url: format!("ws://{addr}"),
            texts: texts.clone(),
        };
        let task = tokio::spawn(run(session, health.clone(), None, stop_rx));

        tokio::time::timeout(Duration::from_secs(20), async {
            while texts.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("did not reconnect 3 times");
        stop_tx.send(true).unwrap();
        task.await.unwrap();
        assert!(subscribes.load(Ordering::SeqCst) >= 3);
        assert!(health.snapshot().reconnects >= 2);
        assert_eq!(health.status(), crate::health::FeedStatus::Disconnected);
    }
}
