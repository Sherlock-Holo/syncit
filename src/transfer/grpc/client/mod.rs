use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream, Stream, TryStreamExt};
use tap::TapFallible;
use tonic::body::BoxBody;
use tonic::codegen::{Body, StdError};
use tonic::Status;
use tower::Service;
use tracing::{error, info, instrument};

use super::super::{DownloadBlock, DownloadBlockRequest, DownloadTransfer};
use super::pb::{self, download_transfer_service_client::DownloadTransferServiceClient};

#[derive(Debug)]
pub struct GrpcClient<T> {
    client: DownloadTransferServiceClient<T>,
}

impl<T, RespBody> GrpcClient<T>
where
    T: Service<http::Request<BoxBody>, Response = http::Response<RespBody>> + Send + Sync,
    T::Error: Into<StdError>,
    T::Future: Send,
    RespBody: Body<Data = Bytes> + Send + 'static,
    RespBody::Error: Into<StdError> + Send,
{
    pub fn new(grpc_channel: T) -> Self {
        Self {
            client: DownloadTransferServiceClient::new(grpc_channel),
        }
    }
}

#[async_trait]
impl<T, RespBody> DownloadTransfer for GrpcClient<T>
where
    T: Service<http::Request<BoxBody>, Response = http::Response<RespBody>> + Send + Sync,
    T::Error: Into<StdError>,
    T::Future: Send,
    T: Clone,
    RespBody: Body<Data = Bytes> + Send + 'static,
    RespBody::Error: Into<StdError> + Send,
{
    type Error = Status;
    type BlockStream<'a> = impl Stream<Item=Result<Option<DownloadBlock>, Self::Error>> where Self: 'a;

    #[instrument(err, skip(self))]
    async fn download<'a>(
        &'a self,
        block_offset: &'a [DownloadBlockRequest],
    ) -> Result<Self::BlockStream<'a>, Self::Error> {
        let reqs = block_offset
            .iter()
            .map(|req| pb::DownloadBlockRequest {
                dir_id: req.dir_id.as_hyphenated().to_string(),
                filename: req.filename.clone(),
                offset: req.offset,
                len: req.len,
                hash_sum: hex::encode(req.hash_sum),
            })
            .collect::<Vec<_>>();
        let reqs = stream::iter(reqs);

        let resp = self
            .client
            .clone()
            .download(reqs)
            .await
            .tap_err(|err| error!(%err, "download block failed"))?;

        info!("send download request done");

        let resp = resp.into_inner();

        Ok(resp.map_ok(|block| {
            block.inner.map(|block| DownloadBlock {
                offset: block.offset,
                data: block.data,
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::Bytes;
    use futures_util::future::Either;
    use http::Uri;
    use tokio::io::DuplexStream;
    use tonic::transport::{Channel, Endpoint, Server};
    use tonic::{Request, Response, Streaming};
    use uuid::Uuid;

    use super::*;
    use crate::ext::hash_file;
    use crate::transfer::grpc::pb::download_transfer_service_server::{
        DownloadTransferService, DownloadTransferServiceServer,
    };

    struct MockServer(Vec<pb::DownloadBlockRequest>);

    #[async_trait]
    impl DownloadTransferService for MockServer {
        type DownloadStream =
            impl Stream<Item = Result<pb::DownloadBlock, Status>> + Send + 'static;

        async fn download(
            &self,
            request: Request<Streaming<pb::DownloadBlockRequest>>,
        ) -> Result<Response<Self::DownloadStream>, Status> {
            let got_reqs = request.into_inner().try_collect::<Vec<_>>().await.unwrap();

            if self.0 != got_reqs {
                return Ok(Response::new(Either::Left(stream::iter([Ok(
                    pb::DownloadBlock { inner: None },
                )]))));
            }

            Ok(Response::new(Either::Right(stream::iter(
                got_reqs.into_iter().map(|req| {
                    Ok(pb::DownloadBlock {
                        inner: Some(pb::DownloadBlockInner {
                            offset: req.offset,
                            data: Bytes::from_static(b"test"),
                        }),
                    })
                }),
            ))))
        }
    }

    #[tokio::test]
    async fn download_block() {
        let dir_id = Uuid::new_v4();
        let (client, server) = tokio::io::duplex(4096);
        let client = Some(client);
        let (hash_sum, _) = hash_file(Cursor::new(b"test")).await.unwrap();

        tokio::spawn(async move {
            let hash_sum = hex::encode(hash_sum);

            Server::builder()
                .add_service(DownloadTransferServiceServer::new(MockServer(vec![
                    pb::DownloadBlockRequest {
                        dir_id: dir_id.as_hyphenated().to_string(),
                        filename: "test.txt".to_string(),
                        offset: 0,
                        len: 4,
                        hash_sum: hash_sum.clone(),
                    },
                ])))
                .serve_with_incoming(stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });

        let grpc_client = GrpcClient::new(build_channel(client).await);

        let resp = grpc_client
            .download(&[DownloadBlockRequest {
                dir_id,
                filename: "test.txt".to_string(),
                offset: 0,
                len: 4,
                hash_sum,
            }])
            .await
            .unwrap();

        let resp = resp.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            resp,
            vec![Some(DownloadBlock {
                offset: 0,
                data: Bytes::from_static(b"test"),
            })]
        );
    }

    #[tokio::test]
    async fn not_found() {
        let dir_id = Uuid::new_v4();
        let (client, server) = tokio::io::duplex(4096);
        let client = Some(client);
        let (hash_sum, _) = hash_file(Cursor::new(b"test")).await.unwrap();

        tokio::spawn(async move {
            let hash_sum = hex::encode(hash_sum);

            Server::builder()
                .add_service(DownloadTransferServiceServer::new(MockServer(vec![
                    pb::DownloadBlockRequest {
                        dir_id: dir_id.as_hyphenated().to_string(),
                        filename: "test.txt".to_string(),
                        offset: 0,
                        len: 4,
                        hash_sum: hash_sum.clone(),
                    },
                ])))
                .serve_with_incoming(stream::iter(vec![Ok::<_, std::io::Error>(server)]))
                .await
        });

        let grpc_client = GrpcClient::new(build_channel(client).await);

        let (hash_sum, _) = hash_file(Cursor::new(b"wrong")).await.unwrap();

        let resp = grpc_client
            .download(&[DownloadBlockRequest {
                dir_id,
                filename: "test.txt".to_string(),
                offset: 0,
                len: 4,
                hash_sum,
            }])
            .await
            .unwrap();

        let resp = resp.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(resp, vec![None]);
    }

    async fn build_channel(mut client: Option<DuplexStream>) -> Channel {
        Endpoint::try_from("http://127.0.0.1:80")
            .unwrap()
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let client = client.take();

                async move {
                    match client {
                        None => Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Client already taken",
                        )),
                        Some(client) => Ok(client),
                    }
                }
            }))
            .await
            .unwrap()
    }
}
