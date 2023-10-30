//! Messages used for the MoQ Transport handshake.
//!
//! After establishing the WebTransport session, the client creates a bidirectional QUIC stream.
//! The client sends the [Client] message and the server responds with the [Server] message.
//! Both sides negotate the [Version] and [Role].

mod client;
mod params;
mod role;
mod server;
mod version;

pub use client::*;
pub use params::*;
pub use role::*;
pub use server::*;
pub use version::*;
