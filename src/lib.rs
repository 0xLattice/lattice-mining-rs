//! Node-facing mining coordination. No network operation happens at construction.
#![forbid(unsafe_code)]
pub mod coordinator;
pub mod core;
pub mod http;
pub mod worker;
pub use coordinator::*;
pub use core::*;
pub use http::*;
pub use worker::*;
