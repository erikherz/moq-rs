use super::{Role, Versions};
use crate::{
	coding::{decode_string, encode_string, DecodeError, EncodeError},
	VarInt,
};

use crate::coding::{AsyncRead, AsyncWrite};

// Sent by the client to setup up the session.
#[derive(Debug)]
pub struct Client {
	// NOTE: This is not a message type, but rather the control stream header.
	// Proposal: https://github.com/moq-wg/moq-transport/issues/138

	// The list of supported versions in preferred order.
	pub versions: Versions,

	// Indicate if the client is a publisher, a subscriber, or both.
	// Proposal: moq-wg/moq-transport#151
	pub role: Role,

	// The path, non-empty ONLY when not using WebTransport.
	pub path: String,
}

impl Client {
	pub async fn decode<R: AsyncRead>(r: &mut R) -> Result<Self, DecodeError> {
		let typ = VarInt::decode(r).await?;
		if typ.into_inner() != 1 {
			return Err(DecodeError::InvalidType(typ));
		}

		let versions = Versions::decode(r).await?;
		let role = Role::decode(r).await?;
		let path = decode_string(r).await?;

		Ok(Self { versions, role, path })
	}

	pub async fn encode<W: AsyncWrite>(&self, w: &mut W) -> Result<(), EncodeError> {
		VarInt::from_u32(1).encode(w).await?;
		self.versions.encode(w).await?;
		self.role.encode(w).await?;
		encode_string(&self.path, w).await?;

		Ok(())
	}
}
