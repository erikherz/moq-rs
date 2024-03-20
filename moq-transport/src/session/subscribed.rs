use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use crate::serve::ServeError;
use crate::util::{Watch, WatchWeak};
use crate::{control, data, serve};

use super::{Publisher, SessionError};

#[derive(Clone)]
pub struct Subscribed {
	session: Publisher,
	state: Watch<State>,
	msg: control::Subscribe,
}

impl Subscribed {
	pub(super) fn new(session: Publisher, msg: control::Subscribe) -> (Subscribed, SubscribedRecv) {
		let state = Watch::new(State::new(session.clone(), msg.id));
		let recv = SubscribedRecv {
			state: state.downgrade(),
		};

		let subscribed = Self { session, state, msg };
		(subscribed, recv)
	}

	pub fn namespace(&self) -> &str {
		self.msg.track_namespace.as_str()
	}

	pub fn name(&self) -> &str {
		self.msg.track_name.as_str()
	}

	pub async fn serve(mut self, mut track: serve::TrackSubscriber) -> Result<(), SessionError> {
		log::debug!(
			"serving subscription: namespace={} name={}",
			self.namespace(),
			self.name()
		);

		let mut tasks = FuturesUnordered::new();

		self.state.lock_mut().ok(track.latest())?;
		let mut done = false;

		loop {
			tokio::select! {
				next = track.next(), if !done => {
					let next = match next? {
						Some(next) => next,
						None => { done = true; continue },
					};

					match next {
						serve::TrackMode::Stream(stream) => return self.serve_track(stream).await,
						serve::TrackMode::Group(group) => tasks.push(Self::serve_group(self.clone(), group).boxed()),
						serve::TrackMode::Object(object) => tasks.push(Self::serve_object(self.clone(), object).boxed()),
						serve::TrackMode::Datagram(datagram) => self.serve_datagram(datagram).await?,
					}
				},
				task = tasks.next(), if !tasks.is_empty() => task.unwrap()?,
				else => return Ok(()),
			};
		}
	}

	async fn serve_track(mut self, mut track: serve::StreamSubscriber) -> Result<(), SessionError> {
		let mut stream = self.session.webtransport().open_uni().await?;

		let header = data::TrackHeader {
			subscribe_id: self.msg.id,
			track_alias: self.msg.track_alias,
			send_order: track.send_order,
		};
		header.encode(&mut stream).await?;

		loop {
			// TODO support streaming chunks
			// TODO check if closed
			let object = track.object().await?;

			let chunk = data::TrackChunk {
				group_id: object.group_id,
				object_id: object.object_id,
				size: object.payload.len(),
			};

			self.state.lock_mut().update_max(object.group_id, object.object_id)?;

			chunk.encode(&mut stream).await?;
			stream.write_all(&object.payload).await?;
		}
	}

	pub async fn serve_group(mut self, mut group: serve::GroupSubscriber) -> Result<(), SessionError> {
		let mut stream = self.session.webtransport().open_uni().await?;

		let header = data::GroupHeader {
			subscribe_id: self.msg.id,
			track_alias: self.msg.track_alias,
			group_id: group.id,
			send_order: group.send_order,
		};
		header.encode(&mut stream).await?;

		while let Some(mut object) = group.next().await? {
			let chunk = data::GroupChunk {
				object_id: object.object_id,
				size: object.size,
			};

			self.state.lock_mut().update_max(group.id, object.object_id)?;

			chunk.encode(&mut stream).await?;

			while let Some(chunk) = object.chunk().await? {
				stream.write_all(&chunk).await?;
			}
		}

		Ok(())
	}

	pub async fn serve_object(mut self, mut object: serve::ObjectSubscriber) -> Result<(), SessionError> {
		let mut stream = self.session.webtransport().open_uni().await?;

		let header = data::ObjectHeader {
			subscribe_id: self.msg.id,
			track_alias: self.msg.track_alias,
			group_id: object.group_id,
			object_id: object.object_id,
			send_order: object.send_order,
		};
		header.encode(&mut stream).await?;

		self.state.lock_mut().update_max(object.group_id, object.object_id)?;

		while let Some(chunk) = object.chunk().await? {
			stream.write_all(&chunk).await?;
		}

		Ok(())
	}

	pub async fn serve_datagram(&mut self, datagram: serve::Datagram) -> Result<(), SessionError> {
		let datagram = data::Datagram {
			subscribe_id: self.msg.id,
			track_alias: self.msg.track_alias,
			group_id: datagram.group_id,
			object_id: datagram.object_id,
			send_order: datagram.send_order,
			payload: datagram.payload,
		};

		let mut buffer = Vec::with_capacity(datagram.payload.len() + 100);
		datagram.encode(&mut buffer).await?; // TODO Not actually async

		// TODO send the datagram
		//self.session.webtransport().send_datagram(&buffer)?;

		self.state
			.lock_mut()
			.update_max(datagram.group_id, datagram.object_id)?;

		Ok(())
	}

	pub fn close(&mut self, err: ServeError) -> Result<(), ServeError> {
		self.state.lock_mut().close(err)
	}

	pub async fn closed(&self) -> Result<(), ServeError> {
		loop {
			let notify = {
				let state = self.state.lock();
				state.closed.clone()?;
				state.changed()
			};

			notify.await
		}
	}
}

pub(super) struct SubscribedRecv {
	state: WatchWeak<State>,
}

impl SubscribedRecv {
	pub fn recv_unsubscribe(&mut self) -> Result<(), ServeError> {
		if let Some(state) = self.state.upgrade() {
			state.lock_mut().close(ServeError::Done)?;
		}
		Ok(())
	}
}

struct State {
	session: Publisher,
	id: u64,

	ok: bool,
	max: Option<(u64, u64)>,
	closed: Result<(), ServeError>,
}

impl State {
	fn new(session: Publisher, id: u64) -> Self {
		Self {
			session,
			id,
			ok: false,
			max: None,
			closed: Ok(()),
		}
	}
}

impl State {
	fn ok(&mut self, latest: Option<(u64, u64)>) -> Result<(), ServeError> {
		self.ok = true;
		self.max = latest;

		self.session
			.send_message(control::SubscribeOk {
				id: self.id,
				expires: None,
				latest,
			})
			.ok();

		Ok(())
	}

	fn close(&mut self, err: ServeError) -> Result<(), ServeError> {
		self.closed.clone()?;
		self.closed = Err(err.clone());

		if self.ok {
			self.session
				.send_message(control::SubscribeDone {
					id: self.id,
					last: self.max,
					code: err.code(),
					reason: err.to_string(),
				})
				.ok();
		} else {
			self.session
				.send_message(control::SubscribeError {
					id: self.id,
					alias: 0,
					code: err.code(),
					reason: err.to_string(),
				})
				.ok();
		}

		Ok(())
	}

	fn update_max(&mut self, group_id: u64, object_id: u64) -> Result<(), ServeError> {
		self.closed.clone()?;

		if let Some((max_group, max_object)) = self.max {
			if group_id >= max_group && object_id >= max_object {
				self.max = Some((group_id, object_id));
			}
		}

		Ok(())
	}
}

impl Drop for State {
	fn drop(&mut self) {
		self.close(ServeError::Done).ok();
		self.session.drop_subscribe(self.id);
	}
}
