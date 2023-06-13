use super::{Role, Version};
use crate::coding::{Decode, Encode, Params};

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

use anyhow::Context;

// Sent by the client to setup up the session.
#[derive(Debug)]
pub struct Client {
	// NOTE: This is not a message type, but rather the control stream header.
	// Proposal: https://github.com/moq-wg/moq-transport/issues/138

	// The list of supported versions in preferred order.
	pub versions: Vec<Version>,

	// param: 0x0: Indicate if the client is a publisher, a subscriber, or both.
	// Proposal: moq-wg/moq-transport#151
	pub role: Role,

	// param 0x1: The path, sent ONLY when not using WebTransport.
	pub path: Option<String>,

	// A generic list of paramters.
	pub unknown: Params,
}

#[async_trait(?Send)]
impl Decode for Client {
	async fn decode<R: AsyncRead + Unpin>(r: &mut R) -> anyhow::Result<Self> {
		let versions = Vec::decode(r).await.context("failed to read supported versions")?;
		anyhow::ensure!(!versions.is_empty(), "client must support at least one version");

		let mut role = None;
		let mut path = None;
		let mut unknown = Params::new();

		while let Ok(id) = u64::decode(r).await {
			match id {
				0 => {
					let v = Role::decode(r).await.context("failed to decode role")?;
					anyhow::ensure!(role.replace(v).is_none(), "duplicate role");
				}
				1 => {
					let v = String::decode(r).await.context("failed to read path")?;
					anyhow::ensure!(path.replace(v).is_none(), "duplicate path");
				}
				_ => {
					unknown
						.decode_one(id, r)
						.await
						.context("failed to decode unknown param")?;
				}
			};
		}

		let role = role.context("missing role")?;

		Ok(Self {
			versions,
			role,
			path,
			unknown,
		})
	}
}

#[async_trait(?Send)]
impl Encode for Client {
	async fn encode<W: AsyncWrite + Unpin>(&self, w: &mut W) -> anyhow::Result<()> {
		anyhow::ensure!(!self.versions.is_empty(), "client must support at least one version");
		self.versions.encode(w).await?;

		0u64.encode(w).await?;
		self.role.encode(w).await?;

		if let Some(path) = &self.path {
			1u64.encode(w).await?;
			path.encode(w).await?;
		}

		self.unknown.encode(w).await?;

		Ok(())
	}
}
