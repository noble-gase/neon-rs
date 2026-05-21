use std::panic::Location;

use rand::distr::{Alphanumeric, SampleString};

#[track_caller]
pub fn make_ctx(msg: impl Into<String>) -> String {
    let loc = Location::caller();
    format!("{} ({}:{})", msg.into(), loc.file(), loc.line())
}

pub fn nonce(size: usize) -> String {
    let mut rng = rand::rng();
    Alphanumeric.sample_string(&mut rng, size)
}
