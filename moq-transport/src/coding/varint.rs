// Derived from quinn-proto
// https://github.com/quinn-rs/quinn/blob/main/quinn-proto/src/varint.rs
// Licensed via Apache 2.0 and MIT

use std::convert::{TryFrom, TryInto};
use std::fmt;

use super::{Decode, Encode};

use thiserror::Error;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Error)]
#[error("value too large for varint encoding")]
pub struct BoundsExceeded;

/// An integer less than 2^62
///
/// Values of this type are suitable for encoding as QUIC variable-length integer.
// It would be neat if we could express to Rust that the top two bits are available for use as enum
// discriminants
#[derive(Default, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VarInt(pub u64);

impl VarInt {
	/// The largest representable value
	pub const MAX: Self = Self((1 << 62) - 1);

	/// The largest encoded value length
	pub const MAX_SIZE: usize = 8;

	/// Construct a `VarInt` infallibly
	pub const fn from_u32(x: u32) -> Self {
		Self(x as u64)
	}

	/// Succeeds iff `x` < 2^62
	pub fn from_u64(x: u64) -> Result<Self, BoundsExceeded> {
		if x < 2u64.pow(62) {
			Ok(Self(x))
		} else {
			Err(BoundsExceeded)
		}
	}

	/// Create a VarInt without ensuring it's in range
	///
	/// # Safety
	///
	/// `x` must be less than 2^62.
	pub const unsafe fn from_u64_unchecked(x: u64) -> Self {
		Self(x)
	}

	/// Compute the number of bytes needed to encode this value
	pub(crate) fn size(&self) -> usize {
		let x = self.0;

		if x < 2u64.pow(6) {
			1
		} else if x < 2u64.pow(14) {
			2
		} else if x < 2u64.pow(30) {
			4
		} else if x < 2u64.pow(62) {
			8
		} else {
			unreachable!("malformed VarInt");
		}
	}
}

impl From<VarInt> for u64 {
	fn from(x: VarInt) -> Self {
		x.0
	}
}

impl From<VarInt> for usize {
	fn from(x: VarInt) -> Self {
		x.0 as usize
	}
}

impl From<u8> for VarInt {
	fn from(x: u8) -> Self {
		Self(x.into())
	}
}

impl From<u16> for VarInt {
	fn from(x: u16) -> Self {
		Self(x.into())
	}
}

impl From<u32> for VarInt {
	fn from(x: u32) -> Self {
		Self(x.into())
	}
}

impl TryFrom<u64> for VarInt {
	type Error = BoundsExceeded;
	/// Succeeds iff `x` < 2^62
	fn try_from(x: u64) -> Result<Self, BoundsExceeded> {
		Self::from_u64(x)
	}
}

impl TryFrom<u128> for VarInt {
	type Error = BoundsExceeded;
	/// Succeeds iff `x` < 2^62
	fn try_from(x: u128) -> Result<Self, BoundsExceeded> {
		Self::from_u64(x.try_into().map_err(|_| BoundsExceeded)?)
	}
}

impl TryFrom<usize> for VarInt {
	type Error = BoundsExceeded;
	/// Succeeds iff `x` < 2^62
	fn try_from(x: usize) -> Result<Self, BoundsExceeded> {
		Self::try_from(x as u64)
	}
}

impl fmt::Debug for VarInt {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

impl fmt::Display for VarInt {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}

use async_trait::async_trait;

#[async_trait(?Send)]
impl Decode for VarInt {
	async fn decode<R: AsyncRead + Unpin>(r: &mut R) -> anyhow::Result<Self> {
		let mut buf = [0; 8];
		r.read_exact(buf[0..1].as_mut()).await?;

		let tag = buf[0] >> 6;
		buf[0] &= 0b0011_1111;

		let x = match tag {
			0b00 => u64::from(buf[0]),
			0b01 => {
				r.read_exact(buf[1..2].as_mut()).await?;
				u64::from(u16::from_be_bytes(buf[..2].try_into().unwrap()))
			}
			0b10 => {
				r.read_exact(buf[1..4].as_mut()).await?;
				u64::from(u32::from_be_bytes(buf[..4].try_into().unwrap()))
			}
			0b11 => {
				r.read_exact(buf[1..8].as_mut()).await?;
				u64::from_be_bytes(buf)
			}
			_ => unreachable!(),
		};

		Ok(Self(x))
	}
}

#[async_trait(?Send)]
impl Encode for VarInt {
	async fn encode<W: AsyncWrite + Unpin>(&self, w: &mut W) -> anyhow::Result<()> {
		let x = self.0;
		if x < 2u64.pow(6) {
			w.write_u8(x as u8).await?;
		} else if x < 2u64.pow(14) {
			w.write_u16(0b01 << 14 | x as u16).await?;
		} else if x < 2u64.pow(30) {
			w.write_u32(0b10 << 30 | x as u32).await?;
		} else if x < 2u64.pow(62) {
			w.write_u64(0b11 << 62 | x).await?;
		} else {
			anyhow::bail!("malformed VarInt");
		}

		Ok(())
	}
}
