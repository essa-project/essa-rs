//! Triggers a run for the given WASM module.
//!
//! This executable sends a run request for the specified WASM file to the
//! essa-rs scheduler and then exits. It does not wait for the results of the
//! WASM module.

use anyhow::Context;
use essa_common::{essa_default_zenoh_prefix, scheduler_run_module_topic};
use std::{fs, path::PathBuf};
use zenoh::prelude::sync::SyncResolve;

#[derive(argh::FromArgs)]
/// Triggers a run of the given WASM file in essa-rs.
struct Args {
    /// the WASM file that should be executed in essa-rs
    #[argh(positional)]
    wasm_file: PathBuf,
}

fn main() -> anyhow::Result<()> {
    // read the specified WASM file
    let args: Args = argh::from_env();
    let wasm_bytes = fs::read(&args.wasm_file).context("failed to read given wasm file")?;

    let zenoh = zenoh::open(zenoh::config::Config::default())
        .res()
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to connect to zenoh")?;
    let zenoh_prefix = essa_default_zenoh_prefix();

    // pass the WASM file to the scheduler
    zenoh
        .put(scheduler_run_module_topic(zenoh_prefix), wasm_bytes)
        .res()
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to start module")?;

    zenoh
        .close()
        .res()
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to close zenoh")?;

    Ok(())
}
