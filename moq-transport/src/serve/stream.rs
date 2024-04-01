use bytes::Bytes;
use std::{fmt, ops::Deref, sync::Arc};

use crate::util::State;

use super::{ServeError, Track};

#[derive(Debug, PartialEq, Clone)]
pub struct Stream {
	pub track: Arc<Track>,
	pub priority: u64,
}

impl Stream {
	pub fn produce(self) -> (StreamWriter, StreamReader) {
		let (writer, reader) = State::default();
		let info = Arc::new(self);

		let writer = StreamWriter::new(writer, info.clone());
		let reader = StreamReader::new(reader, info);

		(writer, reader)
	}
}

impl Deref for Stream {
	type Target = Track;

	fn deref(&self) -> &Self::Target {
		&self.track
	}
}

#[derive(Debug)]
struct StreamState {
	// The latest group.
	latest: Option<StreamGroupReader>,

	// Updated each time objects changes.
	epoch: usize,

	// Set when the writer is dropped.
	closed: Result<(), ServeError>,
}

impl Default for StreamState {
	fn default() -> Self {
		Self {
			latest: None,
			epoch: 0,
			closed: Ok(()),
		}
	}
}

/// Used to write data to a stream and notify readers.
///
/// This is Clone as a work-around, but be very careful because it's meant to be sequential.
#[derive(Debug, Clone)]
pub struct StreamWriter {
	// Mutable stream state.
	state: State<StreamState>,

	// Immutable stream state.
	pub stream: Arc<Stream>,
}

impl StreamWriter {
	fn new(state: State<StreamState>, stream: Arc<Stream>) -> Self {
		Self { state, stream }
	}

	pub fn create(&mut self, group_id: u64) -> Result<StreamGroupWriter, ServeError> {
		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;

		if let Some(latest) = &state.latest {
			if latest.group_id > group_id {
				return Err(ServeError::Duplicate);
			}
		}

		let group = Arc::new(StreamGroup {
			stream: self.stream.clone(),
			group_id,
		});

		let (writer, reader) = State::default();

		let reader = StreamGroupReader::new(reader, group.clone());
		let writer = StreamGroupWriter::new(writer, group);

		state.latest = Some(reader);
		state.epoch += 1;

		Ok(writer)
	}

	pub fn append(&mut self) -> Result<StreamGroupWriter, ServeError> {
		let next = self
			.state
			.lock()
			.latest
			.as_ref()
			.map(|g| g.group_id + 1)
			.unwrap_or_default();
		self.create(next)
	}

	/// Close the stream with an error.
	pub fn close(self, err: ServeError) -> Result<(), ServeError> {
		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
		state.closed = Err(err);

		Ok(())
	}
}

impl Deref for StreamWriter {
	type Target = Stream;

	fn deref(&self) -> &Self::Target {
		&self.stream
	}
}

/// Notified when a stream has new data available.
#[derive(Clone, Debug)]
pub struct StreamReader {
	// Modify the stream state.
	state: State<StreamState>,

	// Immutable stream state.
	pub stream: Arc<Stream>,

	// The number of chunks that we've read.
	// NOTE: Cloned readers inherit this index, but then run in parallel.
	epoch: usize,
}

impl StreamReader {
	fn new(state: State<StreamState>, stream: Arc<Stream>) -> Self {
		Self {
			state,
			stream,
			epoch: 0,
		}
	}

	/// Block until the next group is available.
	pub async fn next(&mut self) -> Result<Option<StreamGroupReader>, ServeError> {
		loop {
			let notify = {
				let state = self.state.lock();
				if self.epoch != state.epoch {
					self.epoch = state.epoch;
					let latest = state.latest.clone().unwrap();
					return Ok(Some(latest));
				}

				state.closed.clone()?;
				match state.modified() {
					Some(notify) => notify,
					None => return Ok(None),
				}
			};

			notify.await; // Try again when the state changes
		}
	}

	// Returns the largest group/sequence
	pub fn latest(&self) -> Option<(u64, u64)> {
		let state = self.state.lock();
		state.latest.as_ref().map(|group| (group.group_id, group.latest()))
	}
}

impl Deref for StreamReader {
	type Target = Stream;

	fn deref(&self) -> &Self::Target {
		&self.stream
	}
}

#[derive(Clone, PartialEq, Debug)]
pub struct StreamGroup {
	pub stream: Arc<Stream>,
	pub group_id: u64,
}

impl Deref for StreamGroup {
	type Target = Stream;

	fn deref(&self) -> &Self::Target {
		&self.stream
	}
}

#[derive(Debug)]
struct StreamGroupState {
	// The objects that have been received thus far.
	objects: Vec<StreamObjectReader>,
	closed: Result<(), ServeError>,
}

impl Default for StreamGroupState {
	fn default() -> Self {
		Self {
			objects: Vec::new(),
			closed: Ok(()),
		}
	}
}

#[derive(Debug)]
pub struct StreamGroupWriter {
	state: State<StreamGroupState>,
	pub group: Arc<StreamGroup>,
	next: u64,
}

impl StreamGroupWriter {
	fn new(state: State<StreamGroupState>, group: Arc<StreamGroup>) -> Self {
		Self { state, group, next: 0 }
	}

	/// Add a new object to the group.
	pub fn write(&mut self, payload: Bytes) -> Result<(), ServeError> {
		let mut writer = self.create(payload.len())?;
		writer.write(payload)?;
		Ok(())
	}

	pub fn create(&mut self, size: usize) -> Result<StreamObjectWriter, ServeError> {
		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;

		let (writer, reader) = StreamObject {
			group: self.group.clone(),
			object_id: self.next,
			size,
		}
		.produce();

		state.objects.push(reader);

		Ok(writer)
	}

	/// Close the stream with an error.
	pub fn close(self, err: ServeError) -> Result<(), ServeError> {
		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
		state.closed = Err(err);

		Ok(())
	}
}

impl Deref for StreamGroupWriter {
	type Target = StreamGroup;

	fn deref(&self) -> &Self::Target {
		&self.group
	}
}

#[derive(Debug, Clone)]
pub struct StreamGroupReader {
	pub group: Arc<StreamGroup>,
	state: State<StreamGroupState>,
	index: usize,
}

impl StreamGroupReader {
	fn new(state: State<StreamGroupState>, group: Arc<StreamGroup>) -> Self {
		Self { state, group, index: 0 }
	}

	pub async fn read_next(&mut self) -> Result<Option<Bytes>, ServeError> {
		if let Some(mut reader) = self.next().await? {
			Ok(Some(reader.read_all().await?))
		} else {
			Ok(None)
		}
	}

	pub async fn next(&mut self) -> Result<Option<StreamObjectReader>, ServeError> {
		loop {
			let notify = {
				let state = self.state.lock();
				if self.index < state.objects.len() {
					self.index += 1;
					return Ok(Some(state.objects[self.index].clone()));
				}

				state.closed.clone()?;
				match state.modified() {
					Some(notify) => notify,
					None => return Ok(None),
				}
			};

			notify.await
		}
	}

	pub fn latest(&self) -> u64 {
		let state = self.state.lock();
		state.objects.last().map(|o| o.object_id).unwrap_or_default()
	}
}

impl Deref for StreamGroupReader {
	type Target = StreamGroup;

	fn deref(&self) -> &Self::Target {
		&self.group
	}
}

/// A subset of Object, since we use the group's info.
#[derive(Clone, PartialEq, Debug)]
pub struct StreamObject {
	// The group this belongs to.
	pub group: Arc<StreamGroup>,

	pub object_id: u64,

	// The size of the object.
	pub size: usize,
}

impl StreamObject {
	pub fn produce(self) -> (StreamObjectWriter, StreamObjectReader) {
		let (writer, reader) = State::default();
		let info = Arc::new(self);

		let writer = StreamObjectWriter::new(writer, info.clone());
		let reader = StreamObjectReader::new(reader, info);

		(writer, reader)
	}
}

impl Deref for StreamObject {
	type Target = StreamGroup;

	fn deref(&self) -> &Self::Target {
		&self.group
	}
}

struct StreamObjectState {
	// The data that has been received thus far.
	chunks: Vec<Bytes>,

	closed: Result<(), ServeError>,
}

impl Default for StreamObjectState {
	fn default() -> Self {
		Self {
			chunks: Vec::new(),
			closed: Ok(()),
		}
	}
}

impl fmt::Debug for StreamObjectState {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("StreamObjectState")
			.field("chunks", &self.chunks.len())
			.field("size", &self.chunks.iter().map(|c| c.len()).sum::<usize>())
			.finish()
	}
}

/// Used to write data to a segment and notify readers.
#[derive(Debug)]
pub struct StreamObjectWriter {
	// Mutable segment state.
	state: State<StreamObjectState>,

	// Immutable segment state.
	pub object: Arc<StreamObject>,

	// The amount of promised data that has yet to be written.
	remain: usize,
}

impl StreamObjectWriter {
	/// Create a new segment with the given info.
	fn new(state: State<StreamObjectState>, object: Arc<StreamObject>) -> Self {
		Self {
			state,
			remain: object.size,
			object,
		}
	}

	/// Write a new chunk of bytes.
	pub fn write(&mut self, chunk: Bytes) -> Result<(), ServeError> {
		if chunk.len() > self.remain {
			return Err(ServeError::Size);
		}
		self.remain -= chunk.len();

		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
		state.chunks.push(chunk);

		Ok(())
	}

	/// Close the stream with an error.
	pub fn close(self, err: ServeError) -> Result<(), ServeError> {
		let mut state = self.state.lock_mut().ok_or(ServeError::Done)?;
		state.closed = Err(err);

		Ok(())
	}
}

impl Drop for StreamObjectWriter {
	// Make sure we fully write the segment, otherwise close it with an error.
	fn drop(&mut self) {
		if self.remain == 0 {
			return;
		}

		let state = self.state.lock();
		if state.closed.is_err() {
			return;
		}

		if let Some(mut state) = state.into_mut() {
			state.closed = Err(ServeError::Size);
		}
	}
}

impl Deref for StreamObjectWriter {
	type Target = StreamObject;

	fn deref(&self) -> &Self::Target {
		&self.object
	}
}

/// Notified when a segment has new data available.
#[derive(Clone, Debug)]
pub struct StreamObjectReader {
	// Modify the segment state.
	state: State<StreamObjectState>,

	// Immutable segment state.
	pub object: Arc<StreamObject>,

	// The number of chunks that we've read.
	// NOTE: Cloned readers inherit this index, but then run in parallel.
	index: usize,
}

impl StreamObjectReader {
	fn new(state: State<StreamObjectState>, object: Arc<StreamObject>) -> Self {
		Self {
			state,
			object,
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
					None => return Ok(None),
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

impl Deref for StreamObjectReader {
	type Target = StreamObject;

	fn deref(&self) -> &Self::Target {
		&self.object
	}
}
