//! Starts up a minimal essa-rs system and runs the `essa-test-function` in it.

use std::{
    io::{self, stdin},
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

fn main() {
    // build essa-rs executables and the essa-test-function
    run_command(
        "cargo build --release --workspace --exclude essa-test-function",
        true,
    )
    .unwrap()
    .wait()
    .unwrap();
    run_command(
        "cargo build --release -p essa-test-function --target wasm32-wasi",
        true,
    )
    .unwrap()
    .wait()
    .unwrap();

    // start anna and essa nodes
    let mut routing = run_command("cargo run --release --manifest-path anna-rs/Cargo.toml --bin routing -- anna-rs/example-config.yml", false).unwrap();
    let mut kvs = run_command("cargo run --release --manifest-path anna-rs/Cargo.toml --bin kvs -- anna-rs/example-config.yml", false).unwrap();
    let mut scheduler = run_command(
        "cargo run --release -p essa-function-scheduler --bin function-scheduler",
        true,
    )
    .unwrap();
    let mut executor =
        run_command("cargo run --release -p essa-function-executor -- 0", true).unwrap();

    // wait a bit, then trigger test function run
    thread::sleep(Duration::from_secs(2));
    run_command("cargo run -p essa-function-scheduler --bin run-function -- target/wasm32-wasi/release/essa-test-function.wasm", true).unwrap().wait().unwrap();

    // wait for user input
    println!("Press enter to exit");
    let mut buffer = String::new();
    stdin().read_line(&mut buffer).unwrap();

    // kill all the essa-rs executables again
    executor.kill().unwrap();
    scheduler.kill().unwrap();
    kvs.kill().unwrap();
    routing.kill().unwrap();
}

/// Runs the given command as a subprocess.
///
/// If `stdout` is true, the subprocess is started with an inherited stdout
/// handle, i.e. the same stdout as this main process. If `stdout` is set
/// to false, the `stdout` of the subprocess is set to `null`, i.e. all
/// output is thrown away.
fn run_command(command: &str, stdout: bool) -> io::Result<Child> {
    let mut split = command.split_ascii_whitespace();
    let mut cmd = Command::new(split.next().unwrap());
    cmd.args(split);
    if !stdout {
        cmd.stdout(Stdio::null());
    }
    cmd.spawn()
}
