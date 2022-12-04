use std::io;

use bytes::BytesMut;
use sha2::{Digest, Sha256};
use tap::TapFallible;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::error;

use crate::index::{Block, BlockChain, Sha256sum, BLOCK_SIZE};

pub async fn hash_file<R: AsyncRead + Unpin>(
    mut reader: R,
) -> anyhow::Result<(Sha256sum, BlockChain)> {
    let mut hasher = Sha256::new();
    let mut block_hasher = Sha256::new();

    let mut buf = BytesMut::zeroed(BLOCK_SIZE);
    let mut offset = 0;
    let mut blocks = vec![];
    loop {
        let n = read_fill(&mut reader, &mut buf)
            .await
            .tap_err(|err| error!(%err, "read file block failed"))?;

        hasher.update(&buf);

        block_hasher.update(&buf);
        let block_hash_sum = block_hasher.finalize_reset();

        blocks.push(Block {
            offset,
            len: n as _,
            hash_sum: block_hash_sum.into(),
        });

        offset += n as u64;

        if n < buf.len() {
            break;
        }
    }

    let hash_sum = hasher.finalize();

    Ok((
        hash_sum.into(),
        BlockChain {
            block_size: BLOCK_SIZE as _,
            blocks,
        },
    ))
}

async fn read_fill<R: AsyncRead + Unpin>(reader: &mut R, mut buf: &mut [u8]) -> io::Result<usize> {
    let mut sum = 0;
    while !buf.is_empty() {
        let n = reader.read(buf).await?;
        if n == 0 {
            return Ok(sum);
        }

        sum += n;

        buf = &mut buf[n..];
    }

    Ok(sum)
}
