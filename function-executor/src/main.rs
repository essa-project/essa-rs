//! Essa function execution nodes are responsible for running given WASM
//! modules/functions.
//!
//! In an essa-rs system, there are typically multiple nodes, each running the
//! essa function executor binary. The essa scheduler dispatches incoming run
//! requests evenly across the available function executor nodes.

#![warn(missing_docs)]

use anna::{
    anna_default_zenoh_prefix,
    nodes::{request_cluster_info, ClientNode},
    store::LatticeValue,
    topics::RoutingThread,
    ClientKey,
};
use anyhow::Context;
use essa_common::{
    essa_default_zenoh_prefix, executor_run_function_subscribe_topic, executor_run_module_topic,
};
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;
use zenoh::prelude::{Receiver, SplitBuffer, ZFuture};

#[cfg(not(feature = "wasmtime_executor"))]
mod wasmedge;
#[cfg(feature = "wasmtime_executor")]
mod wasmtime;

/// Starts a essa function executor.
///
/// The given ID must be an unique identifier for this executor instance, i.e.
/// there must not be another active executor instance with the same id.
#[derive(argh::FromArgs, Debug, Clone)]
struct Args {
    #[argh(positional)]
    id: u32,
}

fn main() -> anyhow::Result<()> {
    if let Err(err) = set_up_logger() {
        eprintln!(
            "ERROR: {:?}",
            anyhow::anyhow!(err).context("Failed to set up logger")
        );
    }

    let args: Args = argh::from_env();

    let zenoh = Arc::new(
        zenoh::open(zenoh::config::Config::default())
            .wait()
            .map_err(|e| anyhow::anyhow!(e))
            .context("failed to connect to zenoh")?,
    );
    let zenoh_prefix = essa_default_zenoh_prefix().to_owned();

    // listen for function call requests in a separate thread
    {
        let zenoh = zenoh.clone();
        let zenoh_prefix = zenoh_prefix.clone();
        let args = args.clone();

        thread::spawn(move || {
            if let Err(err) = function_call_receive_loop(args, zenoh, zenoh_prefix) {
                log::error!("{:?}", err)
            }
        });
    }

    // listen for module run requests
    module_receive_loop(args, zenoh, zenoh_prefix)?;

    Ok(())
}

/// Creates a new client connected to the `anna-rs` key-value store.
fn new_anna_client(zenoh: Arc<zenoh::Session>) -> Result<ClientNode, anyhow::Error> {
    let anna_zenoh_prefix = anna_default_zenoh_prefix().to_owned();

    // request the available routing nodes from an anna seed node
    let cluster_info = smol::block_on(request_cluster_info(&zenoh, &anna_zenoh_prefix))
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to request cluster info from seed node")?;

    let routing_threads: Vec<_> = cluster_info
        .routing_node_ids
        .into_iter()
        .map(|node_id| RoutingThread {
            node_id,
            // TODO: use anna config file to get number of threads per
            // routing node
            thread_id: 0,
        })
        .collect();

    // connect to anna as a new client node
    let mut anna = ClientNode::new(
        Uuid::new_v4().to_string(),
        0,
        routing_threads.clone(),
        Duration::from_secs(10),
        zenoh,
        anna_zenoh_prefix,
    )
    .map_err(eyre_to_anyhow)
    .context("failed to connect to anna")?;
    smol::block_on(anna.init_tcp_connections())
        .map_err(eyre_to_anyhow)
        .context("failed to init TCP connections in anna client")?;
    Ok(anna)
}

/// Listens for incoming module run requests from the scheduler.
fn module_receive_loop(
    args: Args,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
) -> anyhow::Result<()> {
    let mut new_modules = zenoh
        .subscribe(executor_run_module_topic(args.id, &zenoh_prefix))
        .wait()
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to subscribe to new modules")?;

    loop {
        match new_modules.receiver().recv() {
            Ok(change) => {
                let wasm_bytes = change.value.payload.contiguous().into_owned();

                // start a new function executor instance to compile the module
                // and run its main function
                let executor = FunctionExecutor {
                    zenoh: zenoh.clone(),
                    zenoh_prefix: zenoh_prefix.clone(),
                    anna: new_anna_client(zenoh.clone())?,
                };
                std::thread::spawn(move || {
                    let start = Instant::now();
                    if let Err(err) = executor
                        .run_module(wasm_bytes)
                        .context("failed to run module")
                    {
                        log::error!("{:?}", err);
                    }
                    log::info!("Module run finished in {:?}", Instant::now() - start);
                });
            }
            Err(zenoh::sync::channel::RecvError::Disconnected) => break,
        }
    }
    Ok(())
}

/// Listens for incoming function run requests.
fn function_call_receive_loop(
    args: Args,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
) -> anyhow::Result<()> {
    let mut function_calls = zenoh
        .queryable(executor_run_function_subscribe_topic(
            args.id,
            &zenoh_prefix,
        ))
        .wait()
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to subscribe to new modules")?;

    loop {
        match function_calls.receiver().recv() {
            Ok(query) => {
                // start a new function executor instance to run the requested
                // function
                let executor = FunctionExecutor {
                    zenoh: zenoh.clone(),
                    zenoh_prefix: zenoh_prefix.clone(),
                    anna: new_anna_client(zenoh.clone())?,
                };
                std::thread::spawn(move || {
                    if let Err(err) = executor
                        .handle_function_call(query)
                        .context("failed to run function call")
                    {
                        log::error!("{:?}", err);
                    }
                });
            }
            Err(zenoh::sync::channel::RecvError::Disconnected) => break,
        }
    }
    Ok(())
}

/// Responsible for handling module/function run requests.
struct FunctionExecutor {
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
    anna: ClientNode,
}

/// Store the given value in the key-value store.
fn kvs_put(key: ClientKey, value: LatticeValue, anna: &mut ClientNode) -> anyhow::Result<()> {
    let start = Instant::now();

    smol::block_on(anna.put(key, value))
        .map_err(eyre_to_anyhow)
        .context("put failed")?;

    let put_latency = (Instant::now() - start).as_millis();
    if put_latency >= 100 {
        log::trace!("high kvs_put latency: {}ms", put_latency);
    }

    Ok(())
}

/// Get the given value in the key-value store.
///
/// Returns an error if the requested key does not exist in the KVS.
fn kvs_get(key: ClientKey, anna: &mut ClientNode) -> anyhow::Result<LatticeValue> {
    smol::block_on(anna.get(key))
        .map_err(eyre_to_anyhow)
        .context("get failed")
}

/// Copy of `essa-api::EssaResult`, must be kept in sync.
///
/// TODO: move to a separate crate to remove the duplication
#[derive(Debug)]
#[repr(i32)]
#[allow(missing_docs)]
pub enum EssaResult {
    Ok = 0,
    UnknownError = -1,
    NoSuchFunction = -2,
    BufferTooSmall = -3,
    NotFound = -4,
    InvalidFunctionSignature = -5,
    NoResult = -6,
    InvalidResult = -8,
}

/// Transforms an [`eyre::Report`] to an [`anyhow::Error`].
fn eyre_to_anyhow(err: eyre::Report) -> anyhow::Error {
    let err = Box::<dyn std::error::Error + 'static + Send + Sync>::from(err);
    anyhow::anyhow!(err)
}

/// Set up the `log` crate.
fn set_up_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .level_for("zenoh", log::LevelFilter::Warn)
        .level_for("essa_function_executor", log::LevelFilter::Trace)
        .chain(std::io::stdout())
        .chain(fern::log_file("function-executor.log")?)
        .apply()?;
    Ok(())
}
