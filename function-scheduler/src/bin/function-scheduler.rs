//! Schedules function call requests across available executor nodes.
//!
//! Handles both module run requests sent by the user (through `run-function`)
//! and remote function call requests invoked by WASM functions.
//!
//! (Right now this is only a sample implementation that always chooses
//! the executor node with ID 0 for all requests.)

use std::sync::Arc;

use anyhow::Context;
use essa_common::{
    essa_default_zenoh_prefix, executor_run_function_topic, executor_run_module_topic,
    scheduler_function_call_subscribe_topic, scheduler_function_call_topic,
    scheduler_run_module_topic,
};
use futures::{select, StreamExt};
use zenoh::{prelude::r#async::*, queryable::Query};

fn main() -> anyhow::Result<()> {
    smol::block_on(run())
}

async fn run() -> anyhow::Result<()> {
    let zenoh = zenoh::open(zenoh::config::Config::default())
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to connect to zenoh")?
        .into_arc();
    let zenoh_prefix = essa_default_zenoh_prefix();

    // subscribe to module run requests issued through `run-function`
    let new_modules_sub = zenoh
        .declare_subscriber(scheduler_run_module_topic(zenoh_prefix))
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to subscribe to new modules")?;
    let mut new_modules = new_modules_sub.clone().into_stream().fuse();

    // subscribe to remote function call requests issued by WASM functions
    let function_calls_sub = zenoh
        .declare_queryable(scheduler_function_call_subscribe_topic(zenoh_prefix))
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to subscribe to function calls")?;

    let mut function_calls = function_calls_sub.clone().into_stream().fuse();

    loop {
        select! {
            change = new_modules.select_next_some() => {
                run_module(change.payload.contiguous().into_owned(), &zenoh, &zenoh_prefix)
                .await
                .context("failed to run module")?
            }
            query = function_calls.select_next_some() => {
                call_function(query, zenoh.clone(), zenoh_prefix)
                    .await
                    .context("failed to run module")?
            }
            complete => break,
        }
    }

    Ok(())
}

async fn run_module(
    wasm_bytes: Vec<u8>,
    zenoh: &zenoh::Session,
    zenoh_prefix: &str,
) -> anyhow::Result<()> {
    // TODO: implement an actual scheduling policy instead of always choosing
    // executor node 0
    let executor_id = 0;

    // forward the request to the selected executor
    zenoh
        .put(
            executor_run_module_topic(executor_id, zenoh_prefix),
            wasm_bytes,
        )
        .res()
        .await
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to send module to executor")?;

    Ok(())
}

async fn call_function(
    query: Query,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: &'static str,
) -> anyhow::Result<()> {
    // TODO: implement an actual scheduling policy instead of always choosing
    // executor node 0
    let executor_id = 0;

    let mut topic_split = query.key_expr().as_str().split('/');
    let args = topic_split
        .next_back()
        .context("no args key in topic")?
        .to_owned();
    let function = topic_split
        .next_back()
        .context("no function key in topic")?
        .to_owned();
    let module = topic_split
        .next_back()
        .context("no module key in topic")?
        .to_owned();

    // forward the request to the selected executor
    let task = async move {
        let topic =
            executor_run_function_topic(executor_id, &zenoh_prefix, &module, &function, &args);

        let reply = zenoh
            .get(topic)
            .res()
            .await
            .expect("failed to forward function call to executor")
            .recv_async()
            .await
            .expect("failed to receive reply");

        // TODO: this may be wrong. This is here to "fix" the problem:
        // Attempted to reply on `essa/executor/<id>/run-function/<module>/<func>/<args>`, which does not intersect
        // with query `essa/call/<module>/<func>/<args>`,
        // despite query only allowing replies on matching key expressions
        // at ../index.crates.io-6f17d22bba15001f/zenoh-0.7.0-rc/src/queryable.rs:186.',
        // function-scheduler/src/bin/function-scheduler.rs:136:47
        //
        // Newer version don't accept disjoint querying disjoint keys:
        // https://github.com/eclipse-zenoh/roadmap/discussions/55?sort=top
        // This creates a `key` like the `query`.
        let key = scheduler_function_call_topic(&zenoh_prefix, &module, &function, &args);
        match reply.sample {
            Err(e) => eprintln!("`{key}` reply.sample is Err. Error = {e}"),
            Ok(value) => {
                let sample = Sample::try_from(key, value).expect("Cannot create a `Sample`");
                query
                    .reply(Ok(sample))
                    .res()
                    .await
                    .expect("Failed to reply to executor_run_function_topic");
            }
        };
    };
    // TODO: this detach may be a problem??
    smol::spawn(task).detach();

    Ok(())
}
