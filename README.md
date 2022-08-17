# essa-rs

Essa is an experimental stateful serverless programming framework based on WebAssembly.

This project was heavily insprired by **[`cloudburst`](https://github.com/hydro-project/cloudburst)**, which provides a "low-latency, stateful serverless programming framework". Essa differs from the original project in multiple ways:

- **WebAssembly functions:** Instead of requiring that the user-provided functions are written in Python, we use the [_WebAssembly_](https://webassembly.org/) format. WebAssembly — or _"Wasm"_ for short — is a portable instruction format that aims to be as fast as native code, while being completely sandboxed. Multiple programming languages such as C/C++, Rust, Go, or C# can be compiled to it already. These properties make Wasm very suitable for running untrusted user functions in a serverless framework.
- **Written in Rust:** While the original `cloudburst` is written in Python, we rely on the [Rust](https://www.rust-lang.org/) programming language. In addition to Rust's memory safety guarantees and high performance, this choice has the advantage that we can easily use the [`wasmedge-sdk`](https://github.com/WasmEdge/WasmEdge/tree/master/bindings/rust/wasmedge-sdk) or the [`wasmtime`](https://github.com/bytecodealliance/wasmtime) crate for running [_WASI_](https://wasi.dev/)-compatible WebAssembly functions.
- **Based on our `anna-rs` project:** The original `cloudburst` project uses the [`anna` key-value store](https://github.com/hydro-project/anna) for storing function state. While we follow that design, we instead use [our `anna-rs` port](https://github.com/essa-project/anna-rs), which is written in Rust instead of C++ and communicates using [`zenoh`](https://zenoh.io/) instead of [`ZeroMQ`](https://zeromq.org/).

**Note:** This project is still in a prototype state, so don't use it in production!

## Overview

This project contains several executables:

- **`essa-function-executor`**: Allows to compile and execute a WebAssembly module and its functions. We use [`WasmEdge`](https://github.com/WasmEdge/WasmEdge) as the default executor. By enabling the feature `wasmtime_executor`, we can use [`wasmtime`](https://github.com/bytecodealliance/wasmtime) as the executor.
- **`essa-function-scheduler`**: Schedules function execution requests to function executors.
- **`essa-test-function`:** A WebAssembly module that can be run in `essa`. It shows how to perform function calls across nodes and how to share state between them.
- **`run-function`**: Helper executable to start a given WebAssembly module by passing it to a function scheduler.

The following libraries are provided to interact with the Essa framework in WebAssembly crates:


- **`essa-api`**: Provides an interface to `essa` functions, which can be used from WebAssembly modules written in Rust.
- **`essa-macros`**: Provides macros to automatically make Rust functions in WebAssembly modules compatible with the `essa` function call ABI. Re-exported from `essa-api`.

## Build

You need the latest version of [Rust](https://www.rust-lang.org/) for building.

The build commands are:

- `cargo build --workspace --exclude essa-test-function` for a debug build
- `cargo build --workspace --exclude essa-test-function --release` for an optimized release build

After building, you can find the resulting executables under `../target/debug` (for debug builds) or `../target/release` (for release builds).

To run the **test suite**, execute `cargo test`. The **API documentation** can be generated through `cargo doc --open`.

## Run

In order to run `essa-rs`, you need to first initialize the [`anna-rs`](https://github.com/essa-project/anna-rs) submodule through `git submodule update --init`.

For a **quick demo**, run `cargo run -p essa-test-function --example local`. This command will compile and start all necessary runtime nodes, compile the `essa-test-function` to WASM, and then run the compiled WASM module on the started essa-rs nodes. It is worth noting that in this demo, [`WasmEdge`](https://github.com/WasmEdge/WasmEdge) will be used as the default executor.

For a **manual run**, execute the following commands in different terminal windows:

- Start an `anna-rs` routing node:

  ```
  cargo run --release --manifest-path anna-rs/Cargo.toml --bin routing -- anna-rs/example-config.yml
  ```
- Start an `anna-rs` KVS node:

  ```
  cargo run --release --manifest-path anna-rs/Cargo.toml --bin kvs -- anna-rs/example-config.yml
  ```
- Start the `essa-rs` function scheduler:

  ```
  cargo run --release -p essa-function-scheduler --bin function-scheduler
  ```
- Start an `essa-rs` function executor node with ID `0`:

  - Use [`WasmEdge`](https://github.com/WasmEdge/WasmEdge) as the executor:

  ```
  cargo run --release -p essa-function-executor -- 0
  ```
  - Use [`wasmtime`](https://github.com/bytecodealliance/wasmtime) as the executor:

  ```
  cargo run --no-default-features --features wasmtime_executor --release -p essa-function-executor -- 0
  ```
- Compile the `essa-test-function` to WASM and start it through the `run-function` executable:

  ```
  cargo build --release -p essa-test-function --target wasm32-wasi
  cargo run -p essa-function-scheduler --bin run-function -- target/wasm32-wasi/release/essa-test-function.wasm
  ```
  The output of the WebAssembly function is then shown in the console window of the `essa-function-executor` process.

The above commands should result in the same output as the demo command mentioned at the beginning of this section.

Instead of using `cargo run --release`, it is of course also possible to first compile the executables as described in the [_Build_](#build) section and then run them from the `target/release` folder.

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>). Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any additional terms or conditions.
