use anyhow::Context;

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task::JoinSet; // allows locking across await

use std::collections::HashMap;
use std::sync::Arc;

use moq_transport::coding::VarInt;
use moq_transport::{control, object};

use crate::{broadcasts, track, Segment};

pub struct Distribute {
	// Objects are sent to the client using this transport.
	transport: Arc<object::Transport>,

	// Use a tokio mutex so we can hold the lock while trying to write a control message.
	control: Arc<Mutex<control::SendStream>>,

	// Globally announced namespaces, which can be subscribed to.
	broadcasts: broadcasts::Shared,

	// Active subscriptions based on the track ID.
	tracks: HashMap<VarInt, track::Subscriber>,

	// A list of tasks that are currently running.
	tasks: JoinSet<anyhow::Result<()>>,
}

impl Distribute {
	pub fn new(
		transport: Arc<object::Transport>,
		control: Arc<Mutex<control::SendStream>>,
		broadcasts: broadcasts::Shared,
	) -> Self {
		Self {
			transport,
			control,
			broadcasts,
			tracks: HashMap::new(),
			tasks: JoinSet::new(),
		}
	}

	pub async fn run(&mut self) -> anyhow::Result<()> {
		let mut broadcasts = self.broadcasts.lock().await.updates();

		loop {
			tokio::select! {
				res = self.tasks.join_next(), if !self.tasks.is_empty() => {
					let res = res.expect("no tasks").expect("task aborted");
					if let Err(err) = res {
						log::error!("failed to serve subscription: {:?}", err);
					}
				},
				delta = broadcasts.next() => {
					let delta = delta.expect("no more broadcasts");
					self.on_broadcast(delta).await?;
				},
			}
		}
	}

	// Called by the session when it receives a control message.
	pub async fn receive_message(&mut self, msg: control::Message) -> anyhow::Result<()> {
		log::info!("received message: {:?}", msg);

		match msg {
			control::Message::AnnounceOk(msg) => self.receive_announce_ok(msg),
			control::Message::AnnounceError(msg) => self.receive_announce_error(msg),
			control::Message::Subscribe(msg) => self.receive_subscribe(msg).await,
			// TODO make this type safe
			_ => anyhow::bail!("invalid message for distrubution: {:?}", msg),
		}
	}

	fn receive_announce_ok(&mut self, _msg: control::AnnounceOk) -> anyhow::Result<()> {
		Ok(())
	}

	fn receive_announce_error(&mut self, msg: control::AnnounceError) -> anyhow::Result<()> {
		// TODO make sure we sent this announce
		anyhow::bail!("received ANNOUNCE_ERROR({:?}): {}", msg.code, msg.reason)
	}

	async fn receive_subscribe(&mut self, msg: control::Subscribe) -> anyhow::Result<()> {
		match self.receive_subscribe_inner(&msg).await {
			Ok(()) => {
				self.send_message(control::SubscribeOk {
					track_id: msg.track_id,
					expires: None,
				})
				.await
			}
			Err(e) => {
				self.send_message(control::SubscribeError {
					track_id: msg.track_id,
					code: VarInt::from_u32(1),
					reason: e.to_string(),
				})
				.await
			}
		}
	}

	async fn receive_subscribe_inner(&mut self, msg: &control::Subscribe) -> anyhow::Result<()> {
		let broadcasts = self.broadcasts.lock().await;

		let broadcast = broadcasts
			.get(&msg.track_namespace)
			.context("unknown track namespace")?;

		let track = broadcast.subscribe(msg.track_name.clone()).await?;

		let track_id = msg.track_id;
		let transport = self.transport.clone();

		self.tasks
			.spawn(async move { Self::serve_track(transport, track_id, track).await });

		Ok(())
	}

	async fn serve_track(
		transport: Arc<object::Transport>,
		track_id: VarInt,
		mut track: track::Subscriber,
	) -> anyhow::Result<()> {
		let mut tasks = JoinSet::new();
		let mut done = false;

		loop {
			tokio::select! {
				// Accept new segments added to the track.
				segment = track.segments.next(), if !done => {
					match segment {
						Some(segment) => {
							let transport = transport.clone();
							//let track_id = track_id;

							tasks.spawn(async move { Self::serve_group(transport, track_id, segment).await });
						},
						None => done = true, // no more segments in the track
					}
				},
				// Poll any pending segments until they exit.
				res = tasks.join_next(), if !tasks.is_empty() => {
					let res = res.expect("no tasks").expect("task aborted");
					res.context("failed serve segment")?
				},
				else => return Ok(()), // all segments received and finished serving
			}
		}
	}

	async fn serve_group(
		transport: Arc<object::Transport>,
		track_id: VarInt,
		mut segment: Segment,
	) -> anyhow::Result<()> {
		let header = object::Header {
			track_id,
			group_sequence: segment.sequence,
			object_sequence: VarInt::from_u32(0), // Always zero since we send an entire group as an object
			send_order: segment.send_order,
		};

		let mut stream = transport.send(header).await?;

		// Write each fragment as they are available.
		while let Some(fragment) = segment.fragments.next().await {
			stream.write_all(fragment.as_slice()).await?;
		}

		// NOTE: stream is automatically closed when dropped

		Ok(())
	}

	async fn on_broadcast(&mut self, delta: broadcasts::Delta) -> anyhow::Result<()> {
		match delta {
			broadcasts::Delta::Insert(name, _broadcast) => {
				self.send_message(control::Announce {
					track_namespace: name.clone(),
				})
				.await
			}
			broadcasts::Delta::Remove(name) => {
				self.send_message(control::AnnounceError {
					track_namespace: name,
					code: VarInt::from_u32(0),
					reason: "broadcast closed".to_string(),
				})
				.await
			}
		}
	}

	async fn send_message<T: Into<control::Message>>(&mut self, msg: T) -> anyhow::Result<()> {
		// We use a tokio mutex so we can hold the lock across await (when control stream is full).
		let mut control = self.control.lock().await;

		let msg = msg.into();
		log::info!("sending message: {:?}", msg);
		control.send(msg).await
	}
}