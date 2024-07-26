use std::io::Cursor;
use anyhow;
use log::{debug, warn};
use moq_transport::serve::{
    TrackReaderMode, Tracks, TracksReader, TracksWriter,
    TrackReader, TrackWriter, GroupReader, GroupObjectReader, GroupsWriter,
};
use moq_transport::session::{Subscriber, Publisher};
use mp4::ReadBox;
use tokio::io::AsyncReadExt;
use std::time::Duration;

#[allow(dead_code)]
pub struct Media {
    subscriber: Subscriber,
    publisher: Publisher,
    sub_broadcast: TracksReader,
    pub_broadcast: TracksWriter,
    sub_namespace: String,
    sub_name: String,
    pub_namespace: String,
    pub_name: String,
}

impl Media {
    pub async fn new(
        subscriber: Subscriber,
        publisher: Publisher,
        tracks: Tracks,
        sub_namespace: String,
        sub_name: String,
        pub_namespace: String,
        pub_name: String
    ) -> anyhow::Result<Self> {
        let (pub_broadcast, _, sub_broadcast) = tracks.produce();
        debug!("Media::new: Created new Media instance");
        Ok(Self {
            subscriber,
            publisher,
            sub_broadcast,
            pub_broadcast,
            sub_namespace,
            sub_name,
            pub_namespace,
            pub_name,
        })
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        debug!("Media::run: Starting media processing");
        let moov = self.handle_init().await?;
        debug!("Media::run: Init handled successfully");
        self.handle_media_tracks(&moov).await?;
        debug!("Media::run: Media tracks handled successfully");
        Ok(())
    }

    async fn handle_init(&mut self) -> anyhow::Result<mp4::MoovBox> {
        debug!("Media::handle_init: Starting init handling");
        let init_track_name = format!("{}/0.mp4", self.sub_name);
        debug!("Media::handle_init: Init track name: {}", init_track_name);
        
        let full_track_name = format!("{}/{}", self.sub_namespace, init_track_name);
        let track = match self.pub_broadcast.create(&full_track_name) {
            Some(t) => {
                debug!("Media::handle_init: Created init track: {}", full_track_name);
                t
            },
            None => {
                warn!("Media::handle_init: Failed to create init track");
                return Err(anyhow::anyhow!("Failed to create init track"));
            }
        };

        let mut subscriber = self.subscriber.clone();
        debug!("Media::handle_init: Attempting to subscribe to init track");
        debug!("Media::handle_init: Full track name: {}", full_track_name);
        
        let subscribe_result = tokio::time::timeout(
            Duration::from_secs(30),
            subscriber.subscribe(track)
        ).await;
        match subscribe_result {
            Ok(Ok(_)) => debug!("Media::handle_init: Successfully subscribed to init track"),
            Ok(Err(err)) => {
                warn!("Media::handle_init: Failed to subscribe to init track: {:?}", err);
                return Err(anyhow::anyhow!("Failed to subscribe to init track: {:?}", err));
            },
            Err(_) => {
                warn!("Media::handle_init: Subscription attempt timed out after 30 seconds");
                return Err(anyhow::anyhow!("Subscription attempt timed out"));
            }
        }

        debug!("Media::handle_init: Subscribing to init track from sub_broadcast");
        let track = match self.sub_broadcast.subscribe(&full_track_name) {
            Some(t) => {
                debug!("Media::handle_init: Got track from sub_broadcast");
                t
            },
            None => {
                warn!("Media::handle_init: Failed to subscribe to init track from sub_broadcast");
                return Err(anyhow::anyhow!("Failed to subscribe to init track from sub_broadcast"));
            }
        };

        let mut group = match track.mode().await {
            Ok(TrackReaderMode::Groups(mut groups)) => {
                debug!("Media::handle_init: Track mode is Groups");
                match groups.next().await {
                    Ok(Some(g)) => g,
                    Ok(None) => {
                        warn!("Media::handle_init: No init group available");
                        return Err(anyhow::anyhow!("No init group available"));
                    },
                    Err(e) => {
                        warn!("Media::handle_init: Failed to get next group: {:?}", e);
                        return Err(anyhow::anyhow!("Failed to get next group: {:?}", e));
                    }
                }
            },
            Ok(_) => {
                warn!("Media::handle_init: Unexpected track mode");
                return Err(anyhow::anyhow!("Unexpected track mode"));
            },
            Err(e) => {
                warn!("Media::handle_init: Failed to get track mode: {:?}", e);
                return Err(anyhow::anyhow!("Failed to get track mode: {:?}", e));
            }
        };
        debug!("Media::handle_init: Got group from track");

        let object = group.next().await?.ok_or_else(|| anyhow::anyhow!("no init fragment"))?;
        let buf = recv_object(object).await?;
        
        // Republish the init segment
        let init_writer = self.pub_broadcast.create(&full_track_name).ok_or_else(|| anyhow::anyhow!("failed to create init writer"))?;
        let mut groups_writer = init_writer.groups()?;
        groups_writer.append(0)?.write(buf.clone().into())?;

        let mut reader = Cursor::new(&buf);
        let ftyp = read_atom(&mut reader).await?;
        anyhow::ensure!(&ftyp[4..8] == b"ftyp", "expected ftyp atom");

        let moov = read_atom(&mut reader).await?;
        anyhow::ensure!(&moov[4..8] == b"moov", "expected moov atom");
        let mut moov_reader = Cursor::new(&moov);
        let moov_header = mp4::BoxHeader::read(&mut moov_reader)?;

        Ok(mp4::MoovBox::read_box(&mut moov_reader, moov_header.size)?)
    }

    async fn handle_media_tracks(&mut self, moov: &mp4::MoovBox) -> anyhow::Result<()> {
        debug!("Media::handle_media_tracks: Starting media tracks handling");
        for trak in &moov.traks {
            let id = trak.tkhd.track_id;
            let name = format!("{}/{}.m4s", self.sub_name, id);
            let full_track_name = format!("{}/{}", self.sub_namespace, name);
            warn!("found track {full_track_name}");

            let track = self.pub_broadcast.create(&full_track_name).ok_or_else(|| anyhow::anyhow!("failed to create track"))?;
            let mut subscriber = self.subscriber.clone();
            tokio::task::spawn(async move {
                subscriber.subscribe(track).await.unwrap_or_else(|err| {
                    warn!("failed to subscribe to track: {err:?}");
                });
            });

            let sub_track = self.sub_broadcast.subscribe(&full_track_name).ok_or_else(|| anyhow::anyhow!("no track"))?;
            let pub_track = self.pub_broadcast.create(&full_track_name).ok_or_else(|| anyhow::anyhow!("failed to create publish track"))?;

            tokio::task::spawn(async move {
                if let Err(err) = handle_track(sub_track, pub_track).await {
                    warn!("failed to handle track {full_track_name}: {err:?}");
                }
            });
        }

        Ok(())
    }
}

async fn handle_track(track: TrackReader, pub_track: TrackWriter) -> anyhow::Result<()> {
    let name = track.name.clone();
    debug!("track {name}: start");
    if let TrackReaderMode::Groups(mut groups) = track.mode().await? {
        let mut groups_writer = pub_track.groups()?;
        while let Some(group) = groups.next().await? {
            handle_group(group, &mut groups_writer).await?;
        }
    }
    debug!("track {name}: finish");
    Ok(())
}

async fn handle_group(mut group: GroupReader, groups_writer: &mut GroupsWriter) -> anyhow::Result<()> {
    debug!("group={} start", group.group_id);
    let mut pub_group = groups_writer.append(group.group_id)?;
    while let Some(object) = group.next().await? {
        debug!("group={} fragment={} start", group.group_id, object.object_id);
        let buf = recv_object(object).await?;
        pub_group.write(buf.into())?;
    }
    Ok(())
}

async fn recv_object(mut object: GroupObjectReader) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(object.size);
    while let Some(chunk) = object.read().await? {
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

async fn read_atom<R: AsyncReadExt + Unpin>(reader: &mut R) -> anyhow::Result<Vec<u8>> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;

    let size = u32::from_be_bytes(buf[0..4].try_into()?) as u64;
    let mut raw = buf.to_vec();

    let mut limit = match size {
        0 => reader.take(u64::MAX),
        1 => {
            reader.read_exact(&mut buf).await?;
            let size_large = u64::from_be_bytes(buf);
            anyhow::ensure!(size_large >= 16, "impossible extended box size: {}", size_large);
            reader.take(size_large - 16)
        }
        2..=7 => anyhow::bail!("impossible box size: {}", size),
        size => reader.take(size - 8),
    };

    tokio::io::copy(&mut limit, &mut raw).await?;
    Ok(raw)
}
