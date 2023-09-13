//! A broadcast is a collection of tracks, split into two handles: [Publisher] and [Subscriber].
//!
//! The [Publisher] can create tracks, either manually or on request.
//! It receives all requests by a [Subscriber] for a tracks that don't exist.
//! The simplest implementation is to close every unknown track with [Error::NotFound].
//!
//! A [Subscriber] can request tracks by name.
//! If the track already exists, it will be returned.
//! If the track doesn't exist, it will be sent to [Unknown] to be handled.
//! A [Subscriber] can be cloned to create multiple subscriptions.
//!
//! The broadcast is automatically closed with [Error::Closed] when [Publisher] is dropped, or all [Subscriber]s are dropped.
use std::{
	collections::{hash_map, HashMap, VecDeque},
	fmt,
	ops::Deref,
	sync::Arc,
};

use crate::Error;

use super::{track, Watch};

/// Create a new broadcast with the given namespace.
pub fn new(name: &str) -> (Publisher, Subscriber) {
	let state = Watch::new(State::default());
	let info = Arc::new(Info { name: name.to_string() });

	let publisher = Publisher::new(state.clone(), info.clone());
	let subscriber = Subscriber::new(state.clone(), info.clone());

	(publisher, subscriber)
}

/// Static information about the broadcast.
#[derive(Debug)]
pub struct Info {
	pub name: String,
}

/// Dynamic information about the broadcast.
#[derive(Debug)]
struct State {
	tracks: HashMap<String, track::Subscriber>,
	requested: VecDeque<track::Publisher>,
	closed: Result<(), Error>,
}

impl State {
	pub fn get(&self, name: &str) -> Result<Option<track::Subscriber>, Error> {
		// Don't check closed, so we can return from cache.
		Ok(self.tracks.get(name).cloned())
	}

	pub fn insert(&mut self, track: track::Subscriber) -> Result<(), Error> {
		self.closed?;

		match self.tracks.entry(track.name.clone()) {
			hash_map::Entry::Occupied(_) => return Err(Error::Duplicate),
			hash_map::Entry::Vacant(v) => v.insert(track),
		};

		Ok(())
	}

	pub fn request(&mut self, name: &str) -> Result<track::Subscriber, Error> {
		self.closed?;

		// Create a new track.
		let (publisher, subscriber) = track::new(name);

		// Insert the track into our Map so we deduplicate future requests.
		self.tracks.insert(name.to_string(), subscriber.clone());

		// Send the track to the Publisher to handle.
		self.requested.push_back(publisher);

		Ok(subscriber)
	}

	pub fn has_next(&self) -> Result<bool, Error> {
		// Check if there's any elements in the queue before checking closed.
		if !self.requested.is_empty() {
			return Ok(true);
		}

		self.closed?;
		Ok(false)
	}

	pub fn next(&mut self) -> track::Publisher {
		// We panic instead of erroring to avoid a nasty wakeup loop if you don't call has_next first.
		self.requested.pop_front().expect("no entry in queue")
	}

	pub fn close(&mut self, err: Error) -> Result<(), Error> {
		self.closed?;
		self.closed = Err(err);
		Ok(())
	}
}

impl Default for State {
	fn default() -> Self {
		Self {
			tracks: HashMap::new(),
			closed: Ok(()),
			requested: VecDeque::new(),
		}
	}
}

/// Publish new tracks for a broadcast by name.
// TODO remove Clone
#[derive(Clone)]
pub struct Publisher {
	state: Watch<State>,
	info: Arc<Info>,

	_dropped: Arc<Dropped>,
}

impl Publisher {
	fn new(state: Watch<State>, info: Arc<Info>) -> Self {
		let _dropped = Arc::new(Dropped::new(state.clone()));
		Self { state, info, _dropped }
	}

	/// Create a new track with the given name, inserting it into the broadcast.
	pub fn create_track(&mut self, name: &str) -> Result<track::Publisher, Error> {
		let (publisher, subscriber) = track::new(name);
		self.state.lock_mut().insert(subscriber)?;
		Ok(publisher)
	}

	/// Insert a track into the broadcast.
	pub fn insert_track(&mut self, track: track::Subscriber) -> Result<(), Error> {
		self.state.lock_mut().insert(track)
	}

	/// Block until the next track requested by a subscriber.
	pub async fn next_track(&mut self) -> Result<Option<track::Publisher>, Error> {
		loop {
			let notify = {
				let state = self.state.lock();
				if state.has_next()? {
					return Ok(Some(state.into_mut().next()));
				}

				state.changed()
			};

			notify.await;
		}
	}

	/// Close the broadcast with an error.
	pub fn close(self, err: Error) -> Result<(), Error> {
		self.state.lock_mut().close(err)
	}
}

impl fmt::Debug for Publisher {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Publisher")
			.field("name", &self.name)
			.field("state", &self.state)
			.finish()
	}
}

impl Deref for Publisher {
	type Target = Info;

	fn deref(&self) -> &Self::Target {
		&self.info
	}
}

/// Subscribe to a broadcast by requesting tracks.
///
/// This can be cloned to create handles.
#[derive(Clone)]
pub struct Subscriber {
	state: Watch<State>,
	info: Arc<Info>,

	_dropped: Arc<Dropped>,
}

impl Subscriber {
	fn new(state: Watch<State>, info: Arc<Info>) -> Self {
		let _dropped = Arc::new(Dropped::new(state.clone()));
		Self { state, info, _dropped }
	}

	/// Get a track from the broadcast by name.
	/// If the track does not exist, it will be created and potentially fufilled by the publisher (via Unknown).
	/// Otherwise, it will return [Error::NotFound].
	pub fn get_track(&self, name: &str) -> Result<track::Subscriber, Error> {
		let state = self.state.lock();
		if let Some(track) = state.get(name)? {
			return Ok(track);
		}

		// Request a new track if it does not exist.
		state.into_mut().request(name)
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
			.field("name", &self.name)
			.field("state", &self.state)
			.finish()
	}
}

// A handle that closes the broadcast when dropped:
// - when all Subscribers are dropped or
// - when Publisher and Unknown are dropped.
struct Dropped {
	state: Watch<State>,
}

impl Dropped {
	fn new(state: Watch<State>) -> Self {
		Self { state }
	}
}

impl Drop for Dropped {
	fn drop(&mut self) {
		self.state.lock_mut().close(Error::Closed).ok();
	}
}
