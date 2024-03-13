use std::sync::{Arc, Mutex, Weak};

use crate::{control, error::AnnounceError};

use super::Session;

#[derive(Clone)]
pub struct Announce {
	namespace: String,
	state: Arc<Mutex<AnnounceState>>,
}

impl Announce {
	pub(super) fn new(session: Session, namespace: String) -> Self {
		let state = Arc::new(Mutex::new(AnnounceState::new(session, namespace.clone())));
		Self { namespace, state }
	}

	pub fn namespace(&self) -> &str {
		&self.namespace
	}

	pub async fn closed(&self) -> Result<(), AnnounceError> {
		self.state.lock().unwrap().closed.clone()
	}

	pub(super) fn downgrade(&self) -> AnnounceWeak {
		AnnounceWeak {
			state: Arc::downgrade(&self.state),
		}
	}
}

pub(super) struct AnnounceWeak {
	state: Weak<Mutex<AnnounceState>>,
}

impl AnnounceWeak {
	pub fn close(&mut self, err: AnnounceError) -> Result<(), AnnounceError> {
		if let Some(state) = self.state.upgrade() {
			state.lock().unwrap().close(err)
		} else {
			Err(AnnounceError::Dropped)
		}
	}
}

pub(super) struct AnnounceState {
	session: Session,
	namespace: String,
	closed: Result<(), AnnounceError>,
}

impl AnnounceState {
	pub fn new(session: Session, namespace: String) -> Self {
		Self {
			session,
			namespace,
			closed: Ok(()),
		}
	}

	pub fn close(&mut self, err: AnnounceError) -> Result<(), AnnounceError> {
		self.closed.clone()?;
		self.closed = Err(err.clone());

		self.session.send_message(control::Unannounce {
			namespace: self.namespace.clone(),
		})?;

		self.session.remove_announce(self.namespace.clone());

		Ok(())
	}
}

impl Drop for AnnounceState {
	fn drop(&mut self) {
		self.close(AnnounceError::Done).unwrap();
	}
}
