use std::net;
use anyhow::{self, Context};
use clap::Parser;
use url::Url;
use moq_native::quic;
use moq_transport::serve::Tracks;
use log::{debug, warn};

mod media;
use media::Media;

#[derive(Parser, Clone)]
pub struct Config {
    /// Listen for UDP packets on the given address for subscribing.
    #[arg(long, default_value = "[::]:0")]
    pub sub_bind: net::SocketAddr,

    /// Listen for UDP packets on the given address for publishing.
    #[arg(long, default_value = "[::]:0")]
    pub pub_bind: net::SocketAddr,

    /// Subscribe from the given URL starting with https://
    #[arg(long = "sub-url", value_parser = moq_url)]
    pub sub_url: Url,

    /// The namespace for the subscription (usually "watch")
    #[arg(long = "sub-namespace", default_value = "watch")]
    pub sub_namespace: String,

    /// The name of the subscription broadcast
    #[arg(long = "sub-name")]
    pub sub_name: String,

    /// Publish to the given URL starting with https://
    #[arg(long = "pub-url", value_parser = moq_url)]
    pub pub_url: Url,

    /// The namespace for the publication (usually "watch")
    #[arg(long = "pub-namespace", default_value = "watch")]
    pub pub_namespace: String,

    /// The name of the publication broadcast
    #[arg(long = "pub-name")]
    pub pub_name: String,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native::tls::Args,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let tracer = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::set_global_default(tracer).unwrap();

    let config = Config::parse();
    let tls = config.tls.load()?;

    let sub_quic = quic::Endpoint::new(quic::Config { bind: config.sub_bind, tls: tls.clone() })?;
    let pub_quic = quic::Endpoint::new(quic::Config { bind: config.pub_bind, tls })?;

    debug!("Connecting to subscription URL: {}", config.sub_url);
    let sub_session = match sub_quic.client.connect(&config.sub_url).await {
        Ok(session) => {
            debug!("Successfully connected to subscription URL");
            session
        },
        Err(e) => {
            warn!("Failed to connect to subscription URL: {:?}", e);
            return Err(anyhow::anyhow!("Failed to connect to subscription URL: {:?}", e));
        }
    };

    debug!("Connecting to publication URL: {}", config.pub_url);
    let pub_session = match pub_quic.client.connect(&config.pub_url).await {
        Ok(session) => {
            debug!("Successfully connected to publication URL");
            session
        },
        Err(e) => {
            warn!("Failed to connect to publication URL: {:?}", e);
            return Err(anyhow::anyhow!("Failed to connect to publication URL: {:?}", e));
        }
    };

    debug!("Creating MoQ Transport subscriber session");
    let (sub_session, subscriber) = match moq_transport::session::Subscriber::connect(sub_session).await {
        Ok((session, sub)) => {
            debug!("Successfully created MoQ Transport subscriber session");
            (session, sub)
        },
        Err(e) => {
            warn!("Failed to create MoQ Transport subscriber session: {:?}", e);
            return Err(anyhow::anyhow!("Failed to create MoQ Transport subscriber session: {:?}", e));
        }
    };

    debug!("Creating MoQ Transport publisher session");
    let (pub_session, publisher) = match moq_transport::session::Publisher::connect(pub_session).await {
        Ok((session, pub_instance)) => {
            debug!("Successfully created MoQ Transport publisher session");
            (session, pub_instance)
        },
        Err(e) => {
            warn!("Failed to create MoQ Transport publisher session: {:?}", e);
            return Err(anyhow::anyhow!("Failed to create MoQ Transport publisher session: {:?}", e));
        }
    };

    debug!("Creating tracks with name: {}", config.pub_name);
    let tracks = Tracks::new(config.pub_namespace.clone());

    debug!("Creating Media instance");
    let mut media = Media::new(
        subscriber,
        publisher,
        tracks,
        config.sub_namespace,
        config.sub_name,
        config.pub_namespace,
        config.pub_name
    ).await?;
    debug!("Media instance created");

    tokio::select! {
        res = sub_session.run() => res.map_err(|e| anyhow::anyhow!("subscriber session error: {:?}", e))?,
        res = pub_session.run() => res.map_err(|e| anyhow::anyhow!("publisher session error: {:?}", e))?,
        res = media.run() => res?,
    }

    Ok(())
}

fn moq_url(s: &str) -> Result<Url, String> {
    let url = Url::try_from(s).map_err(|e| e.to_string())?;
    if url.scheme() != "https" {
        return Err("url scheme must be https:// for WebTransport".to_string());
    }
    Ok(url)
}
