use crate::coding::{Decode, Encode};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug)]
pub struct AnnounceError {
	// Echo back the namespace that was announced.
	// TODO Propose using an ID to save bytes.
	pub track_namespace: String,

	// An error code.
	pub code: u64,

	// An optional, human-readable reason.
	pub reason: String,
}

#[async_trait(?Send)]
impl Decode for AnnounceError {
	async fn decode<R: AsyncRead + Unpin>(r: &mut R) -> anyhow::Result<Self> {
		let track_namespace = String::decode(r).await?;
		let code = u64::decode(r).await?;
		let reason = String::decode(r).await?;

		Ok(Self {
			track_namespace,
			code,
			reason,
		})
	}
}

#[async_trait(?Send)]
impl Encode for AnnounceError {
	async fn encode<W: AsyncWrite + Unpin>(&self, w: &mut W) -> anyhow::Result<()> {
		self.track_namespace.encode(w).await?;
		self.code.encode(w).await?;
		self.reason.encode(w).await?;

		Ok(())
	}
}
