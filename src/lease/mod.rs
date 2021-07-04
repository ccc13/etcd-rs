//! Leases are a mechanism for detecting client liveness. The cluster grants leases with a time-to-live. A lease expires if the etcd cluster does not receive a keepAlive within a given TTL period.
//!
//! # Examples
//!
//! Grant lease and keep lease alive
//!
//! ```no_run
//! use std::time::Duration;
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
//!     let key = "foo";
//!
//!     // grant lease
//!     let lease = client
//!         .lease()
//!         .grant(LeaseGrantRequest::new(Duration::from_secs(3)))
//!         .await?;
//!
//!     let lease_id = lease.id();
//!
//!     // set key with lease
//!     client
//!         .kv()
//!         .put({
//!             let mut req = PutRequest::new(key, "bar");
//!             req.set_lease(lease_id);
//!
//!             req
//!         })
//!         .await?;
//!
//!     {
//!         // keep alive the lease every 1 second
//!         let client = client.clone();
//!
//!         let mut interval = tokio::time::interval(Duration::from_secs(1));
//!
//!         loop {
//!             interval.tick().await;
//!             client
//!                 .lease()
//!                 .keep_alive(LeaseKeepAliveRequest::new(lease_id))
//!                 .await;
//!         }
//!     }
//!
//!     // not necessary, but will cleanly shut down the long-running tasks
//!     // spawned by the client
//!     client.shutdown().await;
//!
//!     Ok(())
//! }
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

pub use grant::{LeaseGrantRequest, LeaseGrantResponse};
pub use keep_alive::{LeaseKeepAliveRequest, LeaseKeepAliveResponse};
pub use revoke::{LeaseRevokeRequest, LeaseRevokeResponse};

use crate::lazy::{Lazy, Shutdown};
use crate::proto::etcdserverpb;
use crate::proto::etcdserverpb::lease_client::LeaseClient;
use crate::{Error, Result};

mod grant;
mod keep_alive;
mod revoke;

/// LeaseKeepAliveTunnel is a reusable connection for `Lease Keep Alive` operation.
/// The underlying gRPC method is Bi-directional streaming.
struct LeaseKeepAliveTunnel {
    req_sender: Option<UnboundedSender<etcdserverpb::LeaseKeepAliveRequest>>,
    resp_receiver: Option<UnboundedReceiver<Result<LeaseKeepAliveResponse>>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl LeaseKeepAliveTunnel {
    fn new(mut client: LeaseClient<Channel>) -> Self {
        let (req_sender, req_receiver) = unbounded_channel::<etcdserverpb::LeaseKeepAliveRequest>();
        let (resp_sender, resp_receiver) = unbounded_channel::<Result<LeaseKeepAliveResponse>>();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let request = tonic::Request::new(UnboundedReceiverStream::new(req_receiver));

        // monitor inbound watch response and transfer to the receiver
        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx.fuse();
            let mut inbound = futures::select! {
                res = client.lease_keep_alive(request).fuse() => {
                    match res {
                        Ok(_res) => _res.into_inner(),
                        Err(e) => {
                            let _ = resp_sender.send(Err(e.into()));
                            return;
                        }
                    }
                },
                _ = shutdown_rx => { return; }
            };

            loop {
                let resp = futures::select! {
                    resp = inbound.message().fuse() => resp,
                    _ = shutdown_rx => { return; }
                };
                match resp {
                    Ok(Some(resp)) => {
                        if resp_sender.send(Ok(From::from(resp))).is_err() {
                            return;
                        }
                    }
                    Ok(None) => {
                        return;
                    }
                    Err(e) => {
                        resp_sender.send(Err(From::from(e))).unwrap();
                    }
                };
            }
        });

        Self {
            req_sender: Some(req_sender),
            resp_receiver: Some(resp_receiver),
            shutdown: Some(shutdown_tx),
        }
    }
}

#[async_trait]
impl Shutdown for LeaseKeepAliveTunnel {
    async fn shutdown(&mut self) -> Result<()> {
        self.req_sender.take().ok_or(Error::ChannelClosed)?;
        self.shutdown.take().ok_or(Error::ChannelClosed)?;
        Ok(())
    }
}

/// Lease client.
#[derive(Clone)]
pub struct Lease {
    client: LeaseClient<Channel>,
    keep_alive_tunnel: Arc<Lazy<LeaseKeepAliveTunnel>>,
}

impl Lease {
    pub(crate) fn new(client: LeaseClient<Channel>) -> Self {
        let keep_alive_tunnel = {
            let client = client.clone();
            Arc::new(Lazy::new(move || LeaseKeepAliveTunnel::new(client.clone())))
        };
        Self {
            client,
            keep_alive_tunnel,
        }
    }

    /// Performs a lease granting operation.
    pub async fn grant(&mut self, req: LeaseGrantRequest) -> Result<LeaseGrantResponse> {
        let resp = self
            .client
            .lease_grant(tonic::Request::new(req.into()))
            .await?;

        Ok(resp.into_inner().into())
    }

    /// Performs a lease revoking operation.
    pub async fn revoke(&mut self, req: LeaseRevokeRequest) -> Result<LeaseRevokeResponse> {
        let resp = self
            .client
            .lease_revoke(tonic::Request::new(req.into()))
            .await?;

        Ok(resp.into_inner().into())
    }

    /// Fetch keep alive response stream.
    pub async fn keep_alive_responses(
        &mut self,
    ) -> Result<impl Stream<Item = Result<LeaseKeepAliveResponse>>> {
        self.keep_alive_tunnel
            .write()
            .await
            .resp_receiver
            .take()
            .ok_or(Error::ChannelClosed)
            .map(UnboundedReceiverStream::new)
    }

    /// Performs a lease refreshing operation.
    pub async fn keep_alive(&mut self, req: LeaseKeepAliveRequest) -> Result<()> {
        self.keep_alive_tunnel
            .write()
            .await
            .req_sender
            .as_mut()
            .ok_or(Error::ChannelClosed)?
            .send(req.into())
            .map_err(|_| Error::ChannelClosed)
    }

    /// Shut down the running lease task, if any.
    pub async fn shutdown(&mut self) -> Result<()> {
        // If we implemented `Shutdown` for this, callers would need it in scope in
        // order to call this method.
        self.keep_alive_tunnel.evict().await
    }
}
