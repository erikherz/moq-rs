use anyhow::Context;

use chrono::prelude::*;
use moq_transfork::prelude::*;

pub struct Publisher {
	track: TrackWriter,
}

impl Publisher {
	pub fn new(track: TrackWriter) -> Self {
		Self { track }
	}

	pub async fn run(mut self) -> anyhow::Result<()> {
		let start = Utc::now();
		let mut now = start;

		// Just for fun, don't start at zero.
		let mut sequence = start.minute();

		loop {
			let segment = self
				.track
				.create(sequence as u64)
				.context("failed to create minute segment")?;

			sequence += 1;

			tokio::spawn(async move {
				if let Err(err) = Self::send_segment(segment, now).await {
					log::warn!("failed to send minute: {:?}", err);
				}
			});

			let next = now + chrono::Duration::try_minutes(1).unwrap();
			let next = next.with_second(0).unwrap().with_nanosecond(0).unwrap();

			let delay = (next - now).to_std().unwrap();
			tokio::time::sleep(delay).await;

			now = next; // just assume we didn't undersleep
		}
	}

	async fn send_segment(mut segment: GroupWriter, mut now: DateTime<Utc>) -> anyhow::Result<()> {
		// Everything but the second.
		let base = now.format("%Y-%m-%d %H:%M:").to_string();

		segment.write(base.clone().into()).context("failed to write base")?;

		loop {
			let delta = now.format("%S").to_string();
			segment.write(delta.clone().into()).context("failed to write delta")?;

			println!("{}{}", base, delta);

			let next = now + chrono::Duration::try_seconds(1).unwrap();
			let next = next.with_nanosecond(0).unwrap();

			let delay = (next - now).to_std().unwrap();
			tokio::time::sleep(delay).await;

			// Get the current time again to check if we overslept
			let next = Utc::now();
			if next.minute() != now.minute() {
				return Ok(());
			}

			now = next;
		}
	}
}
pub struct Subscriber {
	track: TrackReader,
}

impl Subscriber {
	pub fn new(track: TrackReader) -> Self {
		Self { track }
	}

	pub async fn run(mut self) -> anyhow::Result<()> {
		while let Some(mut group) = self.track.next().await? {
			let base = group
				.read()
				.await
				.context("failed to get first object")?
				.context("empty group")?;

			let base = String::from_utf8_lossy(&base);

			while let Some(object) = group.read().await? {
				let str = String::from_utf8_lossy(&object);
				println!("{}{}", base, str);
			}
		}

		Ok(())
	}
}
