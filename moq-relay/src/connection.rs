use anyhow::Context;

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use moq_transport::session::{Publisher, SessionError, Subscriber};

use crate::{Origin, OriginPublisher};

#[derive(Clone)]
pub struct Connection {
	origin: Origin,
}

impl Connection {
	pub fn new(origin: Origin) -> Self {
		Self { origin }
	}

	pub async fn run(self, mut conn: quinn::Connecting) -> anyhow::Result<()> {
		let handshake = conn
			.handshake_data()
			.await?
			.downcast::<quinn::crypto::rustls::HandshakeData>()
			.unwrap();

		let alpn = handshake.protocol.context("missing ALPN")?;

		log::debug!(
			"received QUIC handshake: ip={} alpn={:?} server={:?}",
			conn.remote_address(),
			alpn,
			handshake.server_name
		);

		// Wait for the QUIC connection to be established.
		let conn = conn.await.context("failed to establish QUIC connection")?;

		log::debug!(
			"established QUIC connection: id={} ip={} alpn={:?} server={:?}",
			conn.stable_id(),
			conn.remote_address(),
			alpn,
			handshake.server_name
		);

		if alpn.as_slice() == webtransport_quinn::ALPN {
			self.serve_webtransport(conn).await?;
		} else {
			self.serve_quic(conn).await?;
		}

		Ok(())
	}

	async fn serve_webtransport(self, conn: quinn::Connection) -> anyhow::Result<()> {
		// Wait for the CONNECT request.
		let request = webtransport_quinn::accept(conn)
			.await
			.context("failed to receive WebTransport request")?;

		// Accept the CONNECT request.
		let session = request
			.ok()
			.await
			.context("failed to respond to WebTransport request")?;

		let (session, publisher, subscriber) = moq_transport::Session::accept(session).await?;

		let mut tasks = FuturesUnordered::new();
		tasks.push(session.run().boxed());

		if let Some(publisher) = publisher {
			tasks.push(Self::serve_publisher(publisher, self.origin.clone()).boxed());
		}

		if let Some(subscriber) = subscriber {
			tasks.push(Self::serve_subscriber(subscriber, self.origin).boxed());
		}

		// Return the first error
		tasks.next().await.unwrap()?;

		Ok(())
	}

	async fn serve_quic(self, conn: quinn::Connection) -> anyhow::Result<()> {
		let session: quictransport_quinn::Session = conn.into();

		let (session, publisher, subscriber) = moq_transport::Session::accept(session).await?;

		let mut tasks = FuturesUnordered::new();
		tasks.push(session.run().boxed());

		if let Some(publisher) = publisher {
			tasks.push(Self::serve_publisher(publisher, self.origin.clone()).boxed());
		}

		if let Some(subscriber) = subscriber {
			tasks.push(Self::serve_subscriber(subscriber, self.origin).boxed());
		}

		// Return the first error
		tasks.next().await.unwrap()?;

		Ok(())
	}

	async fn serve_publisher<S: webtransport_generic::Session>(
		mut publisher: Publisher<S>,
		origin: Origin,
	) -> Result<(), SessionError> {
		let mut tasks = FuturesUnordered::new();

		loop {
			tokio::select! {
				res = tasks.next(), if !tasks.is_empty() => {
					if let Err(err) = res.unwrap() {
						log::info!("failed serving subscribe: err={}", err)
					}
				},
				res = publisher.subscribed() => {
					let subscribe = res?;
					log::info!("serving subscribe: namespace={} name={}", subscribe.namespace(), subscribe.name());

					let track = origin.subscribe(subscribe.namespace(), subscribe.name())?;
					tasks.push(subscribe.serve(track).boxed());
				}
			};
		}
	}

	async fn serve_subscriber<S: webtransport_generic::Session>(
		mut subscriber: Subscriber<S>,
		origin: Origin,
	) -> Result<(), SessionError> {
		let mut tasks = FuturesUnordered::new();

		loop {
			tokio::select! {
				res = tasks.next(), if !tasks.is_empty() => {
					if let Err(err) = res.unwrap() {
						log::info!("failed serving announce: err={}", err)
					}
				},
				res = subscriber.announced() => {
					let announce = res?;
					log::info!("serving announce: namespace={}", announce.namespace());

					let publisher = origin.announce(announce.namespace())?;
					tasks.push(Self::serve_announce(subscriber.clone(), publisher));
				}
			};
		}
	}

	async fn serve_announce<S: webtransport_generic::Session>(
		mut subscriber: Subscriber<S>,
		mut publisher: OriginPublisher,
	) -> Result<(), SessionError> {
		loop {
			let track = publisher.requested().await?;
			subscriber.subscribe(track)?;
		}
	}
}
