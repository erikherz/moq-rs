use anyhow::Context;

use std::sync::Arc;

use moq_transport::{control, object, server, setup};

use tokio::sync::Mutex;

use super::{Contribute, Distribute};
use crate::broadcasts;

pub struct Session {
	// Used to send/receive data streams.
	transport: Arc<object::Transport>,

	// Used to receive control messages.
	control: control::RecvStream,

	// Split logic into contribution/distribution to reduce the problem space.
	contribute: Contribute,
	distribute: Distribute,
}

impl Session {
	pub async fn accept(session: server::Accept, broadcasts: broadcasts::Shared) -> anyhow::Result<Session> {
		// Accep the WebTransport session.
		// OPTIONAL validate the conn.uri() otherwise call conn.reject()
		let session = session
			.accept()
			.await
			.context("failed to accept WebTransport session")?;

		session
			.setup()
			.versions
			.iter()
			.find(|v| **v == setup::Version::DRAFT_00)
			.context("failed to find supported version")?;

		match session.setup().role {
			setup::Role::Subscriber => {}
			_ => anyhow::bail!("TODO publishing not yet supported"),
		}

		let setup = setup::Server {
			version: setup::Version::DRAFT_00,
			role: setup::Role::Publisher,
		};

		let (transport, control) = session.accept(setup).await?;
		let transport = Arc::new(transport);

		let (control_sender, control_receiver) = control.split();
		let control_sender = Arc::new(Mutex::new(control_sender));

		let contribute = Contribute::new(transport.clone(), control_sender.clone(), broadcasts.clone());
		let distribute = Distribute::new(transport.clone(), control_sender, broadcasts);

		let session = Self {
			transport,
			control: control_receiver,
			contribute,
			distribute,
		};

		Ok(session)
	}

	pub async fn run(&mut self) -> anyhow::Result<()> {
		loop {
			tokio::select! {
				msg = self.control.recv() => {
					let msg = msg.context("failed to receive control message")?;
					self.receive_message(msg).await?;
				},
				res = self.contribute.run() => {
					res.context("failed to run contribution")?;
				},
				res = self.distribute.run() => {
					res.context("failed to run contribution")?;
				},
			}
		}
	}

	async fn receive_message(&mut self, msg: control::Message) -> anyhow::Result<()> {
		// TODO split messages into contribution/distribution types to make this safer.
		match msg {
			control::Message::Announce(_) | control::Message::SubscribeOk(_) | control::Message::SubscribeError(_) => {
				self.contribute.receive_message(msg).await
			}
			control::Message::AnnounceOk(_) | control::Message::AnnounceError(_) | control::Message::Subscribe(_) => {
				self.distribute.receive_message(msg).await
			}
			control::Message::GoAway(_) => anyhow::bail!("client can't send GOAWAY u nerd"),
		}
	}
}
