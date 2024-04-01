//! A fragment is a stream of bytes with a header, split into a [Writer] and [Reader] handle.
//!
//! A [Writer] writes an ordered stream of bytes in chunks.
//! There's no framing, so these chunks can be of any size or position, and won't be maintained over the network.
//!
//! A [Reader] reads an ordered stream of bytes in chunks.
//! These chunks are returned directly from the QUIC connection, so they may be of any size or position.
//! You can clone the [Reader] and each will read a copy of of all future chunks. (fanout)
//!
//! The fragment is closed with [ServeError::Closed] when all writers or readers are dropped.
use std::{cmp, fmt, ops::Deref, sync::Arc};

use super::{ServeError, Track};
use crate::util::State;
use bytes::Bytes;

pub struct Objects {
	pub track: Arc<Track>,
}

impl Objects {
	pub fn produce(self) -> (ObjectsWriter, ObjectsReader) {
		let (writer, reader) = State::default();

		let writer = ObjectsWriter {
			state: writer,
			track: self.track.clone(),
		};
		let reader = ObjectsReader::new(reader, self.track);

		(writer, reader)
	}
}

#[derive(Debug)]
struct ObjectsState {
	// The latest group.
	objects: Vec<ObjectReader>,

	// Increased each time objects changes.
	epoch: usize,

	// Can be sent by the writer with an explicit error code.
	closed: Result<(), ServeError>,
}

impl Default for ObjectsState {
	fn default() -> Self {
		Self {
			objects: Vec::new(),
			epoch: 0,
			closed: Ok(()),
		}
	}
}

#[derive(Debug)]
pub struct ObjectsWriter {
	state: State<ObjectsState>,
	pub track: Arc<Track>,
}

impl ObjectsWriter {
	pub fn write(&mut self, object: Object, payload: Bytes) -> Result<(), ServeError> {
		let mut writer = self.create(object)?;
		writer.write(payload)?;
		Ok(())
	}

	pub fn create(&mut self, object: Object) -> Result<ObjectWriter, ServeError> {
		let object = ObjectInfo {
			track: self.track.clone(),
			group_id: object.group_id,
			object_id: object.object_id,
			priority: object.priority,
		};

		let (writer, reader) = object.produce();

		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;

		if let Some(first) = state.objects.first() {
			match writer.group_id.cmp(&first.group_id) {
				// Drop this old group
				cmp::Ordering::Less => return Ok(writer),
				cmp::Ordering::Greater => state.objects.clear(),
				cmp::Ordering::Equal => {}
			}
		}

		state.objects.push(reader);
		state.epoch += 1;

		Ok(writer)
	}

	pub fn close(self, err: ServeError) -> Result<(), ServeError> {
		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
		state.closed = Err(err);

		Ok(())
	}
}

impl Deref for ObjectsWriter {
	type Target = Track;

	fn deref(&self) -> &Self::Target {
		&self.track
	}
}

#[derive(Clone, Debug)]
pub struct ObjectsReader {
	state: State<ObjectsState>,
	pub track: Arc<Track>,
	epoch: usize,
}

impl ObjectsReader {
	fn new(state: State<ObjectsState>, track: Arc<Track>) -> Self {
		Self { state, track, epoch: 0 }
	}

	pub async fn next(&mut self) -> Result<Option<ObjectReader>, ServeError> {
		loop {
			let notify = {
				let state = self.state.lock();
				if self.epoch < state.epoch {
					let index = state.objects.len().saturating_sub(state.epoch - self.epoch);
					self.epoch = state.epoch - state.objects.len() + index + 1;
					return Ok(Some(state.objects[index].clone()));
				}

				state.closed.clone()?;
				match state.modified() {
					Some(notify) => notify,
					None => return Ok(None), // No more updates will come
				}
			};

			notify.await;
		}
	}

	// Returns the largest group/sequence
	pub fn latest(&self) -> Option<(u64, u64)> {
		let state = self.state.lock();
		state
			.objects
			.iter()
			.max_by_key(|a| (a.group_id, a.object_id))
			.map(|a| (a.group_id, a.object_id))
	}
}

impl Deref for ObjectsReader {
	type Target = Track;

	fn deref(&self) -> &Self::Target {
		&self.track
	}
}

/// Static information about the segment.
#[derive(Clone, PartialEq, Debug)]
pub struct ObjectInfo {
	pub track: Arc<Track>,

	// The sequence number of the group within the track.
	pub group_id: u64,

	// The sequence number of the object within the group.
	pub object_id: u64,

	// The priority of the stream.
	pub priority: u64,
}

impl Deref for ObjectInfo {
	type Target = Track;

	fn deref(&self) -> &Self::Target {
		&self.track
	}
}

impl ObjectInfo {
	pub fn produce(self) -> (ObjectWriter, ObjectReader) {
		let (writer, reader) = State::default();
		let info = Arc::new(self);

		let writer = ObjectWriter::new(writer, info.clone());
		let reader = ObjectReader::new(reader, info);

		(writer, reader)
	}
}

pub struct Object {
	// The sequence number of the group within the track.
	pub group_id: u64,

	// The sequence number of the object within the group.
	pub object_id: u64,

	// The priority of the stream.
	pub priority: u64,
}

struct ObjectState {
	// The data that has been received thus far.
	chunks: Vec<Bytes>,

	// Set when the writer is dropped.
	closed: Result<(), ServeError>,
}

impl fmt::Debug for ObjectState {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ObjectState")
			.field("chunks", &self.chunks.len())
			.field("size", &self.chunks.iter().map(|c| c.len()).sum::<usize>())
			.field("closed", &self.closed)
			.finish()
	}
}

impl Default for ObjectState {
	fn default() -> Self {
		Self {
			chunks: Vec::new(),
			closed: Ok(()),
		}
	}
}

/// Used to write data to a segment and notify readers.
#[derive(Debug)]
pub struct ObjectWriter {
	// Mutable segment state.
	state: State<ObjectState>,

	// Immutable segment state.
	pub info: Arc<ObjectInfo>,
}

impl ObjectWriter {
	/// Create a new segment with the given info.
	fn new(state: State<ObjectState>, object: Arc<ObjectInfo>) -> Self {
		Self { state, info: object }
	}

	/// Write a new chunk of bytes.
	pub fn write(&mut self, chunk: Bytes) -> Result<(), ServeError> {
		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
		state.chunks.push(chunk);

		Ok(())
	}

	/// Close the segment with an error.
	pub fn close(self, err: ServeError) -> Result<(), ServeError> {
		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
		state.closed = Err(err);

		Ok(())
	}
}

impl Deref for ObjectWriter {
	type Target = ObjectInfo;

	fn deref(&self) -> &Self::Target {
		&self.info
	}
}

/// Notified when a segment has new data available.
#[derive(Clone, Debug)]
pub struct ObjectReader {
	// Modify the segment state.
	state: State<ObjectState>,

	// Immutable segment state.
	pub info: Arc<ObjectInfo>,

	// The number of chunks that we've read.
	// NOTE: Cloned readers inherit this index, but then run in parallel.
	index: usize,
}

impl ObjectReader {
	fn new(state: State<ObjectState>, object: Arc<ObjectInfo>) -> Self {
		Self {
			state,
			info: object,
			index: 0,
		}
	}

	/// Block until the next chunk of bytes is available.
	pub async fn read(&mut self) -> Result<Option<Bytes>, ServeError> {
		loop {
			let notify = {
				let state = self.state.lock();

				if self.index < state.chunks.len() {
					let chunk = state.chunks[self.index].clone();
					self.index += 1;
					return Ok(Some(chunk));
				}

				state.closed.clone()?;
				match state.modified() {
					Some(notify) => notify,
					None => return Ok(None), // No more updates will come
				}
			};

			notify.await; // Try again when the state changes
		}
	}

	pub async fn read_all(&mut self) -> Result<Bytes, ServeError> {
		let mut chunks = Vec::new();
		while let Some(chunk) = self.read().await? {
			chunks.push(chunk);
		}

		Ok(Bytes::from(chunks.concat()))
	}
}

impl Deref for ObjectReader {
	type Target = ObjectInfo;

	fn deref(&self) -> &Self::Target {
		&self.info
	}
}
