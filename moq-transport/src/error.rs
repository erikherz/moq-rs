use thiserror::Error;

use crate::VarInt;

/// A MoQTransport error with an associated error code.
#[derive(Copy, Clone, Debug, Error)]
pub enum Error {
	/// A clean termination, represented as error code 0.
	/// This error is automatically used when publishers or subscribers are dropped without calling close.
	#[error("closed")]
	Closed,

	/// An ANNOUNCE_RESET or SUBSCRIBE_RESET was sent by the publisher.
	#[error("reset code={0:?}")]
	Reset(u32),

	/// An ANNOUNCE_STOP or SUBSCRIBE_STOP was sent by the subscriber.
	#[error("stop code={0:?}")]
	Stop(u32),

	/// The requested resource was not found.
	#[error("not found")]
	NotFound,

	/// A resource already exists with that ID.
	#[error("duplicate")]
	Duplicate,

	/// The role negiotiated in the handshake was violated. For example, a publisher sent a SUBSCRIBE, or a subscriber sent an OBJECT.
	#[error("role violation: msg={0}")]
	Role(VarInt),

	/// An error occured while reading from the QUIC stream.
	#[error("failed to read from stream")]
	Read,

	/// An error occured while writing to the QUIC stream.
	#[error("failed to write to stream")]
	Write,

	/// An unclassified error because I'm lazy. TODO classify these errors
	#[error("unknown error")]
	Unknown,
}

impl Error {
	/// An integer code that is sent over the wire.
	pub fn code(&self) -> u32 {
		match self {
			Self::Closed => 0,
			Self::Reset(code) => *code,
			Self::Stop(code) => *code,
			Self::NotFound => 404,
			Self::Role(_) => 405,
			Self::Duplicate => 409,
			Self::Unknown => 500,
			Self::Write => 501,
			Self::Read => 502,
		}
	}

	/// A reason that is sent over the wire.
	pub fn reason(&self) -> &str {
		match self {
			Self::Closed => "closed",
			Self::Reset(_) => "reset",
			Self::Stop(_) => "stop",
			Self::NotFound => "not found",
			Self::Duplicate => "duplicate",
			Self::Role(_msg) => "role violation",
			Self::Unknown => "unknown",
			Self::Read => "read error",
			Self::Write => "write error",
		}
	}
}
