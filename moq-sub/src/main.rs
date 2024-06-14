use std::{net, time};

use anyhow::Context;
use clap::Parser;
use url::Url;

use moq_native::quic;
use moq_sub::media::Media;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	env_logger::init();

	// Disable tracing so we don't get a bunch of Quinn spam.
	let tracer = tracing_subscriber::FmtSubscriber::builder()
		.with_max_level(tracing::Level::WARN)
		.finish();
	tracing::subscriber::set_global_default(tracer).unwrap();

	let config = Config::parse();

	let out = tokio::io::stdout();

	let tls = config.tls.load()?;

	let quic = quic::Endpoint::new(quic::Config { bind: config.bind, tls })?;

	let session = quic.client.connect(&config.url).await?;

	let (session, subscriber) = moq_transport::session::Subscriber::connect(session)
		.await
		.context("failed to create MoQ Transport session")?;

	let mut media = Media::new(subscriber, out).await?;

	tokio::select! {
		res = session.run() => res.context("session error")?,
		res = media.run() => res.context("media error")?,
	}

	Ok(())
}

pub struct NoCertificateVerification {}

impl rustls::client::ServerCertVerifier for NoCertificateVerification {
	fn verify_server_cert(
		&self,
		_end_entity: &rustls::Certificate,
		_intermediates: &[rustls::Certificate],
		_server_name: &rustls::ServerName,
		_scts: &mut dyn Iterator<Item = &[u8]>,
		_ocsp_response: &[u8],
		_now: time::SystemTime,
	) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
		Ok(rustls::client::ServerCertVerified::assertion())
	}
}

#[derive(Parser, Clone)]
pub struct Config {
	/// Listen for UDP packets on the given address.
	#[arg(long, default_value = "[::]:0")]
	pub bind: net::SocketAddr,

	/// Connect to the given URL starting with https://
	#[arg(value_parser = moq_url)]
	pub url: Url,

	/// The TLS configuration.
	#[command(flatten)]
	pub tls: moq_native::tls::Args,
}

fn moq_url(s: &str) -> Result<Url, String> {
	let url = Url::try_from(s).map_err(|e| e.to_string())?;

	// Make sure the scheme is moq
	if url.scheme() != "https" {
		return Err("url scheme must be https:// for WebTransport".to_string());
	}

	Ok(url)
}
