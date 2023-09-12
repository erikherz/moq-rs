use crate::coding::{DecodeError, EncodeError, VarInt};

use crate::coding::{AsyncRead, AsyncWrite};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Debug)]
pub struct Object {
	// An ID for this track.
	// Proposal: https://github.com/moq-wg/moq-transport/issues/209
	pub track: VarInt,

	// The group sequence number.
	pub group: VarInt,

	// The object sequence number.
	pub sequence: VarInt,

	// The priority/send order.
	// Proposal: int32 instead of a varint.
	pub send_order: i32,
}

impl Object {
	pub async fn decode<R: AsyncRead>(r: &mut R) -> Result<Self, DecodeError> {
		let typ = VarInt::decode(r).await?;
		if typ.into_inner() != 0 {
			return Err(DecodeError::InvalidType(typ));
		}

		// NOTE: size has been omitted

		let track = VarInt::decode(r).await?;
		let group = VarInt::decode(r).await?;
		let sequence = VarInt::decode(r).await?;
		let send_order = r.read_i32().await?; // big-endian

		Ok(Self {
			track,
			group,
			sequence,
			send_order,
		})
	}

	pub async fn encode<W: AsyncWrite>(&self, w: &mut W) -> Result<(), EncodeError> {
		VarInt::from_u32(0).encode(w).await?;
		self.track.encode(w).await?;
		self.group.encode(w).await?;
		self.sequence.encode(w).await?;
		w.write_i32(self.send_order).await?;

		Ok(())
	}
}
