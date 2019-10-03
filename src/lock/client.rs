use std::sync::Arc;

use futures::Future;

use super::{LockRequest, LockResponse, UnlockRequest, UnlockResponse};
use crate::client::Inner;
use crate::Error;

#[derive(Clone)]
pub struct LockClient {
    inner: Arc<Inner>,
}

impl LockClient {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    pub fn lock(&self, req: LockRequest) -> impl Future<Item = LockResponse, Error = Error> {
        self.inner
            .lock
            .lock_async(&req.into())
            .unwrap()
            .map(From::from)
            .map_err(Error::GrpcFailure)
    }

    pub fn unlock(&self, req: UnlockRequest) -> impl Future<Item = UnlockResponse, Error = Error> {
        self.inner
            .lock
            .unlock_async(&req.into())
            .unwrap()
            .map(From::from)
            .map_err(Error::GrpcFailure)
    }
}
