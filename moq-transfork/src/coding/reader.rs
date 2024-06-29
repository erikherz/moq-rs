use std::{cmp, io};

use bytes::{Buf, Bytes, BytesMut};

use crate::coding::{Decode, DecodeError};

pub struct Reader {
	stream: web_transport::RecvStream,
	buffer: BytesMut,
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum ReadError {
	#[error("decode error: {0}")]
	Decode(#[from] DecodeError),

	#[error("webtransport error: {0}")]
	Transport(#[from] web_transport::ReadError),
}

impl Reader {
	pub fn new(stream: web_transport::RecvStream) -> Self {
		Self {
			stream,
			buffer: Default::default(),
		}
	}

	pub async fn decode<T: Decode>(&mut self) -> Result<T, ReadError> {
		loop {
			let mut cursor = io::Cursor::new(&self.buffer);

			// Try to decode with the current buffer.
			let required = match T::decode(&mut cursor) {
				Ok(msg) => {
					self.buffer.advance(cursor.position() as usize);
					return Ok(msg);
				}
				Err(DecodeError::More(required)) => self.buffer.len() + required, // Try again with more data
				Err(err) => return Err(err.into()),
			};

			// Read in more data until we reach the requested amount.
			// We always read at least once to avoid an infinite loop if some dingus puts remain=0
			loop {
				if !self.stream.read_buf(&mut self.buffer).await? {
					return Err(DecodeError::More(required - self.buffer.len()).into());
				};

				if self.buffer.len() >= required {
					break;
				}
			}
		}
	}

	// Decode optional messages at the end of a stream
	// The weird order of Option<Result is for tokio::select!
	pub async fn decode_maybe<T: Decode>(&mut self) -> Result<Option<T>, ReadError> {
		match self.finished().await {
			Ok(()) => Ok(None),
			Err(ReadError::Decode(DecodeError::ExpectedData)) => Ok(Some(self.decode().await?)),
			Err(e) => Err(e),
		}
	}

	pub async fn read_chunk(&mut self, max: usize) -> Result<Option<Bytes>, ReadError> {
		if !self.buffer.is_empty() {
			let size = cmp::min(max, self.buffer.len());
			let data = self.buffer.split_to(size).freeze();
			return Ok(Some(data));
		}

		Ok(self.stream.read_chunk(max).await?)
	}

	pub fn stop(&mut self, code: u32) {
		self.stream.stop(code)
	}

	/// Wait until the stream is closed, ensuring there are no additional bytes
	pub async fn finished(&mut self) -> Result<(), ReadError> {
		if self.buffer.is_empty() && !self.stream.read_buf(&mut self.buffer).await? {
			return Ok(());
		}

		Err(DecodeError::ExpectedEnd.into())
	}

	/// Wait until the stream is closed, ignoring any unread bytes
	pub async fn closed(&mut self) -> Result<(), ReadError> {
		while self.stream.read_buf(&mut self.buffer).await? {}
		Ok(())
	}

	pub fn id(&self) -> u64 {
		self.stream.id()
	}
}
