#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod client;
pub mod network;

pub use client::{BloxrouteClient, BloxrouteClientError, BloxrouteOptions};
pub use network::BloxrouteNetwork;
