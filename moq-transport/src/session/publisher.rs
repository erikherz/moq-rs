use std::{
	collections::{hash_map, HashMap},
	sync::{Arc, Mutex},
};

use tokio::task::AbortHandle;
use webtransport_quinn::{RecvStream, SendStream, Session};

use crate::{
	message,
	message::Message,
	model::{broadcast, segment, track},
	Error, VarInt,
};

use super::Control;

/// Serves broadcasts over the network, automatically handling subscriptions and caching.
// TODO Clone specific fields when a task actually needs it.
#[derive(Clone, Debug)]
pub struct Publisher {
	// A map of active subscriptions, containing an abort handle to cancel them.
	subscribes: Arc<Mutex<HashMap<VarInt, AbortHandle>>>,
	webtransport: Session,
	control: Control,
	source: broadcast::Subscriber,
}

impl Publisher {
	pub(crate) fn new(webtransport: Session, control: (SendStream, RecvStream), source: broadcast::Subscriber) -> Self {
		let control = Control::new(control.0, control.1);

		Self {
			webtransport,
			subscribes: Default::default(),
			control,
			source,
		}
	}

	// TODO Serve a broadcast without sending an ANNOUNCE.
	// fn serve(&mut self, broadcast: broadcast::Subscriber) -> Result<(), Error> {

	// TODO Wait until the next subscribe that doesn't route to an ANNOUNCE.
	// pub async fn subscribed(&mut self) -> Result<track::Producer, Error> {

	pub async fn run(mut self) -> Result<(), Error> {
		loop {
			tokio::select! {
				_stream = self.webtransport.accept_uni() => {
					return Err(Error::Role(VarInt::ZERO));
				}
				// NOTE: this is not cancel safe, but it's fine since the other branch is a fatal error.
				msg = self.control.recv() => {
					let msg = msg.map_err(|_x| Error::Read)?;

					log::info!("message received: {:?}", msg);
					if let Err(err) = self.recv_message(&msg).await {
						log::warn!("message error: {:?} {:?}", err, msg);
					}
				}
			}
		}
	}

	async fn recv_message(&mut self, msg: &Message) -> Result<(), Error> {
		match msg {
			Message::AnnounceOk(msg) => self.recv_announce_ok(msg).await,
			Message::AnnounceStop(msg) => self.recv_announce_stop(msg).await,
			Message::Subscribe(msg) => self.recv_subscribe(msg).await,
			Message::SubscribeStop(msg) => self.recv_subscribe_stop(msg).await,
			_ => Err(Error::Role(msg.id())),
		}
	}

	async fn recv_announce_ok(&mut self, _msg: &message::AnnounceOk) -> Result<(), Error> {
		// We didn't send an announce.
		Err(Error::NotFound)
	}

	async fn recv_announce_stop(&mut self, _msg: &message::AnnounceStop) -> Result<(), Error> {
		// We didn't send an announce.
		Err(Error::NotFound)
	}

	async fn recv_subscribe(&mut self, msg: &message::Subscribe) -> Result<(), Error> {
		// Assume that the subscribe ID is unique for now.
		let abort = match self.start_subscribe(msg.clone()) {
			Ok(abort) => abort,
			Err(err) => return self.reset_subscribe(msg.id, err).await,
		};

		// Insert the abort handle into the lookup table.
		match self.subscribes.lock().unwrap().entry(msg.id) {
			hash_map::Entry::Occupied(_) => return Err(Error::Duplicate), // TODO fatal, because we already started the task
			hash_map::Entry::Vacant(entry) => entry.insert(abort),
		};

		self.control.send(message::SubscribeOk { id: msg.id }).await
	}

	async fn reset_subscribe(&mut self, id: VarInt, err: Error) -> Result<(), Error> {
		self.control
			.send(message::SubscribeReset {
				id,
				code: err.code(),
				reason: err.reason().to_string(),
			})
			.await
	}

	fn start_subscribe(&mut self, msg: message::Subscribe) -> Result<AbortHandle, Error> {
		// We currently don't use the namespace field in SUBSCRIBE
		if !msg.namespace.is_empty() {
			return Err(Error::NotFound);
		}

		let mut track = self.source.get_track(&msg.name)?;

		// TODO only clone the fields we need
		let this = self.clone();

		let handle = tokio::spawn(async move {
			log::info!("serving track: name={}", track.name);

			let res = this.run_subscribe(msg.id, &mut track).await;
			if let Err(err) = &res {
				log::warn!("failed to serve track: name={} err={:?}", track.name, err);
			}

			let err = res.err().unwrap_or(Error::Closed);
			let msg = message::SubscribeReset {
				id: msg.id,
				code: err.code(),
				reason: err.reason().to_string(),
			};

			this.control.send(msg).await.ok();
		});

		Ok(handle.abort_handle())
	}

	async fn run_subscribe(&self, id: VarInt, track: &mut track::Subscriber) -> Result<(), Error> {
		// TODO add an Ok method to track::Publisher so we can send SUBSCRIBE_OK

		while let Some(mut segment) = track.next_segment().await? {
			// TODO only clone the fields we need
			let this = self.clone();

			tokio::spawn(async move {
				if let Err(err) = this.run_segment(id, &mut segment).await {
					log::warn!("failed to serve segment: {:?}", err)
				}
			});
		}

		Ok(())
	}

	async fn run_segment(&self, id: VarInt, segment: &mut segment::Subscriber) -> Result<(), Error> {
		let object = message::Object {
			track: id,
			sequence: segment.sequence,
			priority: segment.priority,
			expires: segment.expires,
		};

		log::debug!("serving object: {:?}", object);

		let mut stream = self.webtransport.open_uni().await.map_err(|_e| Error::Write)?;

		stream.set_priority(object.priority).ok();

		// TODO better handle the error.
		object.encode(&mut stream).await.map_err(|_e| Error::Write)?;

		while let Some(data) = segment.read_chunk().await? {
			stream.write_chunk(data).await.map_err(|_e| Error::Write)?;
		}

		Ok(())
	}

	async fn recv_subscribe_stop(&mut self, msg: &message::SubscribeStop) -> Result<(), Error> {
		let abort = self.subscribes.lock().unwrap().remove(&msg.id).ok_or(Error::NotFound)?;
		abort.abort();

		self.reset_subscribe(msg.id, Error::Stop).await
	}
}
