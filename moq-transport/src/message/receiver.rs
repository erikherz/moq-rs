use crate::{coding::DecodeError, message::Message};

use webtransport_generic::RecvStream;

pub struct Receiver<R: RecvStream> {
	stream: R,
}

impl<R: RecvStream> Receiver<R> {
	pub fn new(stream: R) -> Self {
		Self { stream }
	}

	// Read the next full message from the stream.
	pub async fn recv(&mut self) -> Result<Message, DecodeError> {
		Message::decode(&mut self.stream).await
	}
}
