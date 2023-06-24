//! Provides common functionality used by both the `essa-function-executor`
//! and `essa-function-scheduler` crates.

#![warn(missing_docs)]

/// R-Arguments representation.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Rargs {
    // In the future this should not be retricted to Vec<f64>.
    // XXX: should be &str instead of String??
    /// Vector with labels and Vec<f64>.
    pub args: Option<Vec<(String, Vec<f64>)>>,
}

/// R-Return representation.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Rreturn {
    // In the future this should not be retricted to Vec<f64>.
    // XXX: should be &str instead of String??
    /// Vector with labels and Vec<f64>.
    pub result: Option<Vec<f64>>,
}

/// The default topic prefix for zenoh, used by all essa code.
pub fn essa_default_zenoh_prefix() -> &'static str {
    "essa"
}

/// The subtopic for instructing the essa function scheduler to start the given
/// WASM module.
pub fn scheduler_run_module_topic(zenoh_prefix: &str) -> String {
    format!("{}/run-module", zenoh_prefix)
}

/// The subtopic for instructing the essa function scheduler to invoke the
/// WASM function with the given name on a remote node.
pub fn scheduler_function_call_subscribe_topic(zenoh_prefix: &str) -> String {
    format!("{}/call/**", zenoh_prefix)
}

/// The subtopic for invoking a specific function of a specific module.
pub fn scheduler_function_call_topic(
    zenoh_prefix: &str,
    module: &str,
    function: &str,
    args: &str,
) -> String {
    format!("{zenoh_prefix}/call/{module}/{function}/{args}",)
}

/// Subtopic for instructing a specific essa function executor to run the
/// give WASM module.
pub fn executor_run_module_topic(executor_id: u32, zenoh_prefix: &str) -> String {
    format!("{}/executor/{}/run-module", zenoh_prefix, executor_id)
}

/// Subtopic for instructing a specific essa function executor to run a
/// function.
pub fn executor_run_function_subscribe_topic(executor_id: u32, zenoh_prefix: &str) -> String {
    format!("{}/executor/{}/run-function/**", zenoh_prefix, executor_id)
}

/// Subtopic for instructing a specific essa function executor to run the
/// WASM function with the given name.
pub fn executor_run_function_topic(
    executor_id: u32,
    zenoh_prefix: &str,
    module: &str,
    function: &str,
    args: &str,
) -> String {
    format!("{zenoh_prefix}/executor/{executor_id}/run-function/{module}/{function}/{args}")
}
