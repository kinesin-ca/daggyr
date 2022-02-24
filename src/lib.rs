#![allow(dead_code)]
#![warn(clippy::all, clippy::pedantic)]

pub use anyhow::Result;

pub mod dag;
pub mod executors;
pub mod state_trackers;
pub mod structs;
