//! The Watch API provides an event-based interface for asynchronously monitoring changes to keys.
//!
//! # Examples
//!
//! Watch key `foo` changes
//!
//! ```no_run
//! use futures::StreamExt;
//!
//! use etcd_rs::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = Client::connect(ClientConfig {
//!         endpoints: vec!["http://127.0.0.1:2379".to_owned()],
//!         auth: None,
//!         tls: None,
//!     }).await?;
//!
//!     // print out all received watch responses
//!     let mut inbound = client.watch(KeyRange::key("foo")).await.unwrap();
//!     tokio::spawn(async move {
//!         while let Some(resp) = inbound.next().await {
//!             println!("watch response: {:?}", resp);
//!         }
//!     });
//!
//!     let key = "foo";
//!     client.kv().put(PutRequest::new(key, "bar")).await?;
//!     client.kv().put(PutRequest::new(key, "baz")).await?;
//!     client
//!         .kv()
//!         .delete(DeleteRequest::new(KeyRange::key(key)))
//!         .await?;
//!
//!     // not necessary, but will cleanly shut down the long-running tasks
//!     // spawned by the client
//!     client.shutdown().await;
//!
//!     Ok(())
//! }
//!
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::FutureExt;
use futures::Stream;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Channel;

pub use watch::{WatchCancelRequest, WatchCreateRequest, WatchResponse};

use crate::lazy::{Lazy, Shutdown};
use crate::proto::etcdserverpb;
use crate::proto::etcdserverpb::watch_client::WatchClient;
use crate::proto::mvccpb;
use crate::Error;
use crate::KeyValue;
use crate::Result;

mod watch;

/// WatchTunnel is a reusable connection for `Watch` operation
/// The underlying gRPC method is Bi-directional streaming
struct WatchTunnel {
    req_sender: Option<UnboundedSender<etcdserverpb::WatchRequest>>,
    resp_receiver: Option<UnboundedReceiver<Result<Option<WatchResponse>>>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl WatchTunnel {
    fn new(mut client: WatchClient<Channel>) -> Self {
        let (req_sender, req_receiver) = unbounded_channel::<etcdserverpb::WatchRequest>();
        let (resp_sender, resp_receiver) = unbounded_channel::<Result<Option<WatchResponse>>>();

        let request = tonic::Request::new(UnboundedReceiverStream::new(req_receiver));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // monitor inbound watch response and transfer to the receiver
        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx.fuse();
            let mut inbound = futures::select! {
                res = client.watch(request).fuse() => {
                    match res {
                        Err(e) => {
                            let _ = resp_sender.send(Err(From::from(e)));
                            return;
                        },
                        Ok(i) => i.into_inner(),
                    }
                },
                _ = shutdown_rx => {
                    let _ = resp_sender.send(Ok(None));
                    return;
                },
            };

            loop {
                let resp = futures::select! {
                    resp = inbound.message().fuse() => resp,
                    _ = shutdown_rx => { break; },
                };
                match resp {
                    Ok(Some(resp)) => {
                        if resp_sender.send(Ok(Some(resp.into()))).is_err() {
                            return;
                        }
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        let _ = resp_sender.send(Err(e.into()));
                        return;
                    }
                };
            }
            let _ = resp_sender.send(Ok(None));
        });

        Self {
            req_sender: Some(req_sender),
            resp_receiver: Some(resp_receiver),
            shutdown: Some(shutdown_tx),
        }
    }
}

#[async_trait]
impl Shutdown for WatchTunnel {
    async fn shutdown(&mut self) -> Result<()> {
        self.req_sender.take().ok_or(Error::ChannelClosed)?;
        self.shutdown.take().ok_or(Error::ChannelClosed)?;
        Ok(())
    }
}

/// Watch client.
#[derive(Clone)]
pub struct Watch {
    client: WatchClient<Channel>,
    tunnel: Arc<Lazy<WatchTunnel>>,
}

impl Watch {
    pub(crate) fn new(client: WatchClient<Channel>) -> Self {
        let tunnel = {
            let client = client.clone();
            Arc::new(Lazy::new(move || WatchTunnel::new(client.clone())))
        };

        Self { client, tunnel }
    }

    /// Performs a watch operation.
    pub async fn watch<C>(&mut self, req: C) -> Result<()>
    where
        C: Into<WatchCreateRequest>,
    {
        self.tunnel
            .write()
            .await
            .req_sender
            .as_mut()
            .ok_or(Error::ChannelClosed)?
            .send(req.into().into())
            .map_err(|_| Error::ChannelClosed)?;
        Ok(())
    }

    pub async fn take_receiver(&mut self) -> impl Stream<Item = Result<Option<WatchResponse>>> {
        let mut tunnel = self.tunnel.write().await;
        UnboundedReceiverStream::new(tunnel.resp_receiver.take().unwrap())
    }

    /// Shut down the running watch task, if any.
    pub async fn shutdown(&mut self) -> Result<()> {
        // If we implemented `Shutdown` for this, callers would need it in scope in
        // order to call this method.
        self.tunnel.evict().await
    }
}

/// The kind of event.
pub enum EventType {
    Put,
    Delete,
}

impl From<mvccpb::event::EventType> for EventType {
    fn from(event_type: mvccpb::event::EventType) -> Self {
        use mvccpb::event::EventType;
        match event_type {
            EventType::Put => Self::Put,
            EventType::Delete => Self::Delete,
        }
    }
}

/// Every change to every key is represented with Event messages.
pub struct Event {
    proto: mvccpb::Event,
}

impl Event {
    /// Gets the kind of event.
    pub fn event_type(&self) -> EventType {
        match self.proto.r#type {
            0 => EventType::Put,
            _ => EventType::Delete, // FIXME: assert valid event type
        }
    }

    /// Takes the key-value pair out of response, leaving a `None` in its place.
    pub fn take_kvs(&mut self) -> Option<KeyValue> {
        match self.proto.kv.take() {
            Some(kv) => Some(From::from(kv)),
            _ => None,
        }
    }
}

impl From<mvccpb::Event> for Event {
    fn from(event: mvccpb::Event) -> Self {
        Self { proto: event }
    }
}
