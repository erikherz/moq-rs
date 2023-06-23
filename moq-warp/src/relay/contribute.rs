use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::{time};

use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet; // lock across await boundaries

use moq_transport::coding::VarInt;
use moq_transport::{control, object};

use anyhow::Context;

use super::broker;
use crate::model::{segment, track};
use crate::source::Source;

pub struct Contribute {
	// Used to receive objects.
	// TODO split into send/receive halves.
	transport: Arc<object::Transport>,

	// Used to send control messages.
	control: control::SendShared,

	// Globally announced namespaces, which we can add ourselves to.
	broker: broker::Broadcasts,

	// The names of active broadcasts being produced.
	broadcasts: HashMap<String, Arc<Broadcast>>,

	//  Active tracks being produced by this session.
	publishers: Publishers,

	// Tasks we are currently serving.
	run_segments: JoinSet<anyhow::Result<()>>, // receiving objects
}

impl Contribute {
	pub fn new(transport: Arc<object::Transport>, control: control::SendShared, broker: broker::Broadcasts) -> Self {
		Self {
			transport,
			control,
			broker,
			broadcasts: HashMap::new(),
			publishers: Publishers::new(),
			run_segments: JoinSet::new(),
		}
	}

	pub async fn run(&mut self) -> anyhow::Result<()> {
		loop {
			tokio::select! {
				res = self.run_segments.join_next(), if !self.run_segments.is_empty() => {
					let res = res.expect("no tasks").expect("task aborted");
					if let Err(err) = res {
						log::error!("failed to produce segment: {:?}", err);
					}
				},
				object = self.transport.recv() => {
					let (header, stream )= object.context("failed to receive object")?;
					let res = self.receive_object(header, stream).await;
					if let Err(err) = res {
						log::error!("failed to receive object: {:?}", err);
					}
				},
				subscribe = self.publishers.incoming() => {
					let msg = subscribe.context("failed to receive subscription")?;
					self.control.send(msg).await?;
				},
			}
		}
	}

	pub async fn receive_message(&mut self, msg: control::Message) -> anyhow::Result<()> {
		match msg {
			control::Message::Announce(msg) => self.receive_announce(msg).await,
			control::Message::SubscribeOk(msg) => self.receive_subscribe_ok(msg),
			control::Message::SubscribeError(msg) => self.receive_subscribe_error(msg),
			_ => Ok(()), // ignore unknown messages
		}
	}

	async fn receive_object(&mut self, header: object::Header, stream: object::RecvStream) -> anyhow::Result<()> {
		let id = header.track_id;

		let segment = segment::Info {
			sequence: header.object_sequence,
			send_order: header.send_order,
			expires: Some(time::Instant::now() + time::Duration::from_secs(10)),
		};

		let segment = segment::Publisher::new(segment);

		self.publishers
			.push_segment(id, segment.subscribe())
			.context("failed to publish segment")?;

		// TODO implement a timeout

		self.run_segments
			.spawn(async move { Self::run_segment(segment, stream).await });

		Ok(())
	}

	async fn run_segment(mut segment: segment::Publisher, mut stream: object::RecvStream) -> anyhow::Result<()> {
		let mut buf = [0u8; 32 * 1024];
		loop {
			let size = stream.read(&mut buf).await.context("failed to read from stream")?;
			if size == 0 {
				return Ok(());
			}

			let chunk = buf[..size].to_vec();
			segment.fragments.push(chunk.into())
		}
	}

	async fn receive_announce(&mut self, msg: control::Announce) -> anyhow::Result<()> {
		match self.receive_announce_inner(&msg).await {
			Ok(()) => {
				let msg = control::AnnounceOk {
					track_namespace: msg.track_namespace,
				};
				self.control.send(msg).await
			}
			Err(e) => {
				let msg = control::AnnounceError {
					track_namespace: msg.track_namespace,
					code: VarInt::from_u32(1),
					reason: e.to_string(),
				};
				self.control.send(msg).await
			}
		}
	}

	async fn receive_announce_inner(&mut self, msg: &control::Announce) -> anyhow::Result<()> {
		let broadcast = Arc::new(Broadcast::new(&msg.track_namespace, &self.publishers));
		self.broker.announce(&msg.track_namespace, broadcast.clone())?;

		self.broadcasts.insert(msg.track_namespace.clone(), broadcast);

		Ok(())
	}

	fn receive_subscribe_ok(&mut self, _msg: control::SubscribeOk) -> anyhow::Result<()> {
		// TODO make sure this is for a track we are subscribed to
		Ok(())
	}

	fn receive_subscribe_error(&mut self, msg: control::SubscribeError) -> anyhow::Result<()> {
		let error = track::Error {
			code: msg.code,
			reason: format!("upstream error: {}", msg.reason),
		};

		self.publishers
			.close(msg.track_id, error)
			.context("failed to close track")?;

		Ok(())
	}
}

impl Drop for Contribute {
	fn drop(&mut self) {
		// Unannounce all broadcasts we have announced.
		// TODO make this automatic so we can't screw up?
		for broadcast in self.broadcasts.values() {
			self.broker.unannounce(&broadcast.namespace).unwrap();
		}
	}
}

// A list of subscriptions for a broadcast.
#[derive(Clone)]
pub struct Broadcast {
	// Our namespace
	namespace: String,

	// A lookup from name to a subscription (duplicate subscribers)
	subscriptions: Arc<Mutex<HashMap<String, track::Subscriber>>>,

	// Issue a SUBSCRIBE message for a new subscription (new subscriber)
	queue: mpsc::UnboundedSender<(String, track::Publisher)>,
}

impl Broadcast {
	pub fn new(namespace: &str, publishers: &Publishers) -> Self {
		Self {
			namespace: namespace.to_string(),
			subscriptions: Default::default(),
			queue: publishers.sender.clone(),
		}
	}
}

impl Source for Broadcast {
	fn subscribe(&self, name: &str) -> Option<track::Subscriber> {
		let mut subscriptions = self.subscriptions.lock().unwrap();

		// Check if there's an existing subscription.
		if let Some(subscriber) = subscriptions.get(name).cloned() {
			return Some(subscriber);
		}

		// Otherwise, make a new track and tell the publisher to fufill it.
		let track = track::Publisher::new(name);
		let subscriber = track.subscribe();

		// Save the subscriber for duplication.
		subscriptions.insert(name.to_string(), subscriber.clone());

		// Send the publisher to another thread to actually subscribe.
		self.queue.send((self.namespace.clone(), track)).unwrap();

		// Return the subscriber we created.
		Some(subscriber)
	}
}

pub struct Publishers {
	// A lookup from subscription ID to a track being produced, or none if it's been closed.
	tracks: HashMap<VarInt, Option<track::Publisher>>,

	// The next subscription ID
	next: u64,

	// A queue of subscriptions that we need to fulfill
	receiver: mpsc::UnboundedReceiver<(String, track::Publisher)>,

	// A clonable queue, so other threads can issue subscriptions.
	sender: mpsc::UnboundedSender<(String, track::Publisher)>,
}

impl Publishers {
	pub fn new() -> Self {
		let (sender, receiver) = mpsc::unbounded_channel();

		Self {
			tracks: Default::default(),
			next: 0,
			sender,
			receiver,
		}
	}

	pub fn push_segment(&mut self, id: VarInt, segment: segment::Subscriber) -> anyhow::Result<()> {
		let track = self.tracks.get_mut(&id).context("no track with that ID")?;
		let track = track.as_mut().context("track closed")?; // TODO don't make fatal

		track.push_segment(segment);

		Ok(())
	}

	pub fn close(&mut self, id: VarInt, err: track::Error) -> anyhow::Result<()> {
		let track = self.tracks.get_mut(&id).context("no track with that ID")?;
		let track = track.take().context("track closed")?;
		track.close(err);

		Ok(())
	}

	// Returns the next subscribe message we need to issue.
	pub async fn incoming(&mut self) -> anyhow::Result<control::Subscribe> {
		let (namespace, track) = self.receiver.recv().await.context("no more subscriptions")?;

		let msg = control::Subscribe {
			track_id: VarInt::try_from(self.next)?,
			track_namespace: namespace,
			track_name: track.name,
		};

		self.next += 1;

		Ok(msg)
	}
}
