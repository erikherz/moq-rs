//! A fragment is a stream of bytes with a header, split into a [Publisher] and [Subscriber] handle.
//!
//! A [Publisher] writes an ordered stream of bytes in chunks.
//! There's no framing, so these chunks can be of any size or position, and won't be maintained over the network.
//!
//! A [Subscriber] reads an ordered stream of bytes in chunks.
//! These chunks are returned directly from the QUIC connection, so they may be of any size or position.
//! You can clone the [Subscriber] and each will read a copy of of all future chunks. (fanout)
//!
//! The fragment is closed with [CacheError::Closed] when all publishers or subscribers are dropped.
use core::fmt;
use std::{ops::Deref, sync::Arc};

use crate::{error::CacheError, util::Watch};
use bytes::Bytes;

/// Create a new segment with the given info.
pub fn new(info: Info) -> (Publisher, Subscriber) {
	let state = Watch::new(State::default());
	let info = Arc::new(info);

	let publisher = Publisher::new(state.clone(), info.clone());
	let subscriber = Subscriber::new(state, info);

	(publisher, subscriber)
}

/// Static information about the segment.
#[derive(Debug)]
pub struct Info {
	// The sequence number of the group within the track.
	pub group_id: u64,

	// The sequence number of the object within the group.
	pub object_id: u64,

	// The priority of the stream.
	pub send_order: u64,

	// The size of the object.
	pub size: usize,
}

/// Same as below but with a fully known payload.
pub struct Full {
	// The sequence number of the group within the track.
	pub group_id: u64,

	// The sequence number of the object within the group.
	pub object_id: u64,

	// The priority of the stream.
	pub send_order: u64,

	// The payload.
	pub payload: Bytes,
}

impl From<Full> for Info {
	fn from(info: Full) -> Self {
		Self {
			group_id: info.group_id,
			object_id: info.object_id,
			send_order: info.send_order,
			size: info.payload.len(),
		}
	}
}

struct State {
	// The data that has been received thus far.
	chunks: Vec<Bytes>,

	// Set when the publisher is dropped.
	closed: Result<(), CacheError>,
}

impl State {
	pub fn close(&mut self, err: CacheError) -> Result<(), CacheError> {
		self.closed.clone()?;
		self.closed = Err(err);
		Ok(())
	}
}

impl Default for State {
	fn default() -> Self {
		Self {
			chunks: Vec::new(),
			closed: Ok(()),
		}
	}
}

impl fmt::Debug for State {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		// We don't want to print out the contents, so summarize.
		f.debug_struct("State").field("closed", &self.closed).finish()
	}
}

/// Used to write data to a segment and notify subscribers.
pub struct Publisher {
	// Mutable segment state.
	state: Watch<State>,

	// Immutable segment state.
	info: Arc<Info>,

	// The amount of promised data that has yet to be written.
	remain: usize,

	// Closes the segment when all Publishers are dropped.
	_dropped: Arc<Dropped>,
}

impl Publisher {
	fn new(state: Watch<State>, info: Arc<Info>) -> Self {
		let _dropped = Arc::new(Dropped::new(state.clone()));
		let remain = info.size;

		Self {
			state,
			info,
			remain,
			_dropped,
		}
	}

	/// Write a new chunk of bytes.
	pub fn chunk(&mut self, chunk: Bytes) -> Result<(), CacheError> {
		if chunk.len() > self.remain {
			return Err(CacheError::WrongSize);
		}
		self.remain -= chunk.len();

		let mut state = self.state.lock_mut();
		state.closed.clone()?;
		state.chunks.push(chunk);

		Ok(())
	}

	/// Close the segment with an error.
	pub fn close(self, mut err: CacheError) -> Result<(), CacheError> {
		if err == CacheError::Done && self.remain != 0 {
			err = CacheError::WrongSize;
		}

		self.state.lock_mut().close(err)?;
		Ok(())
	}
}

impl Deref for Publisher {
	type Target = Info;

	fn deref(&self) -> &Self::Target {
		&self.info
	}
}

impl fmt::Debug for Publisher {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Publisher")
			.field("state", &self.state)
			.field("info", &self.info)
			.finish()
	}
}

/// Notified when a segment has new data available.
#[derive(Clone)]
pub struct Subscriber {
	// Modify the segment state.
	state: Watch<State>,

	// Immutable segment state.
	info: Arc<Info>,

	// The number of chunks that we've read.
	// NOTE: Cloned subscribers inherit this index, but then run in parallel.
	index: usize,

	// Dropped when all Subscribers are dropped.
	_dropped: Arc<Dropped>,
}

impl Subscriber {
	fn new(state: Watch<State>, info: Arc<Info>) -> Self {
		let _dropped = Arc::new(Dropped::new(state.clone()));

		Self {
			state,
			info,
			index: 0,
			_dropped,
		}
	}

	/// Block until the next chunk of bytes is available.
	pub async fn chunk(&mut self) -> Result<Option<Bytes>, CacheError> {
		loop {
			let notify = {
				let state = self.state.lock();
				if self.index < state.chunks.len() {
					let chunk = state.chunks[self.index].clone();
					self.index += 1;
					return Ok(Some(chunk));
				}

				match &state.closed {
					Err(CacheError::Done) => return Ok(None),
					Err(err) => return Err(err.clone()),
					Ok(()) => state.changed(),
				}
			};

			notify.await; // Try again when the state changes
		}
	}
}

impl Deref for Subscriber {
	type Target = Info;

	fn deref(&self) -> &Self::Target {
		&self.info
	}
}

impl fmt::Debug for Subscriber {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Subscriber")
			.field("state", &self.state)
			.field("info", &self.info)
			.field("index", &self.index)
			.finish()
	}
}

struct Dropped {
	// Modify the segment state.
	state: Watch<State>,
}

impl Dropped {
	fn new(state: Watch<State>) -> Self {
		Self { state }
	}
}

impl Drop for Dropped {
	fn drop(&mut self) {
		self.state.lock_mut().close(CacheError::Done).ok();
	}
}
