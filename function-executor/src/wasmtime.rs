//! Use wasmtime as executor's runtime.

#![warn(missing_docs)]

use crate::{get_args, get_module, kvs_get, kvs_put, EssaResult, FunctionExecutor};
use anna::{lattice::LastWriterWinsLattice, nodes::ClientNode, ClientKey};
use anyhow::Context;
use essa_common::scheduler_function_call_topic;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;
use wasmtime::{Caller, Engine, Extern, Linker, Module, Store, Trap, ValType};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder};
use zenoh::{
    prelude::{Receiver, Sample, SplitBuffer, ZFuture},
    query::ReplyReceiver,
    queryable::Query,
};

impl FunctionExecutor {
    /// Runs the given WASM module.
    pub fn run_module(mut self, wasm_bytes: Vec<u8>) -> anyhow::Result<()> {
        log::info!("Start running WASM module");

        // compile the WASM module
        let engine = Engine::default();
        let module = Module::new(&engine, &wasm_bytes).context("failed to load wasm module")?;

        // store the compiled module in the key-value store under an
        // unique key (to avoid recompiling the module on future function
        // calls)
        log::info!("Storing compiled wasm module in anna KVS");
        let module_key: ClientKey = Uuid::new_v4().to_string().into();
        let value = LastWriterWinsLattice::new_now(
            module.serialize().context("failed to serialize module")?,
        )
        .into();
        kvs_put(module_key.clone(), value, &mut self.anna)?;

        let (mut store, linker) = set_up_module(
            engine,
            &module,
            module_key,
            vec![],
            self.zenoh.clone(),
            self.zenoh_prefix.clone(),
            self.anna,
        )?;

        // get the default function (i.e. the `main` function)
        let default_func = linker
            .get_default(&mut store, "")
            .context("module has no default function")?
            .typed::<(), (), _>(&store)
            .context("default function has invalid type")?;

        log::info!("Starting default function of wasm module");

        // call the function
        //
        // TODO: wasmtime allows fine-grained control of the execution, e.g.
        // periodic interrupts. We might want to use these features to guard
        // against malicious or buggy user programs. For example, an infinite
        // loop should not waste CPU time forever.
        default_func
            .call(&mut store, ())
            .context("default function failed")?;

        Ok(())
    }

    /// Runs the given function of a already compiled WASM module.
    pub fn handle_function_call(mut self, query: Query) -> anyhow::Result<()> {
        let mut topic_split = query.key_selector().as_str().split('/');
        let args_key = ClientKey::from(topic_split.next_back().context("no args key in topic")?);
        let function_name = topic_split
            .next_back()
            .context("no function key in topic")?;
        let module_key =
            ClientKey::from(topic_split.next_back().context("no module key in topic")?);

        let module = get_module(module_key.clone(), &mut self.anna)?;

        let args = get_args(args_key, &mut self.anna)?;
        let args_len = u32::try_from(args.len()).unwrap();

        // deserialize and set up WASM module
        let engine = Engine::default();
        let module =
            unsafe { Module::deserialize(&engine, module).expect("failed to deserialize module") };
        let (mut store, linker) = set_up_module(
            engine,
            &module,
            module_key,
            args,
            self.zenoh,
            self.zenoh_prefix.clone(),
            self.anna,
        )?;

        // get the function that we've been requested to call
        let func = linker
            .get(&mut store, "", function_name)
            .and_then(|e| e.into_func())
            .with_context(|| format!("module has no function `{}`", function_name))?
            .typed::<(i32,), (), _>(&store)
            .context("default function has invalid type")?;

        func.call(&mut store, (args_len as i32,))
            .context("function trapped")?;

        // store the function's result into the key value store under
        // the requested key
        //
        // TODO: The result is needed only once, so it might make more sense to
        // send it as a message instead of storing it in the KVS. This would
        // also improve performance since the receiver would no longer need to
        // busy-wait on the result key in the KVS anymore.
        let mut host_state = store.into_data();
        if let Some(result_value) = host_state.function_result.take() {
            let selector = query.key_selector().to_string();
            query.reply(Sample::new(selector, result_value));

            Ok(())
        } else {
            Err(anyhow::anyhow!("no result"))
        }
    }
}

/// Link the host functions and WASI abstractions into the WASM module.
fn set_up_module(
    engine: Engine,
    module: &Module,
    module_key: ClientKey,
    args: Vec<u8>,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
    anna: ClientNode,
) -> Result<(Store<HostState>, Linker<HostState>), anyhow::Error> {
    let wasi = WasiCtxBuilder::new()
        // TODO: we probably want to write to a log file or an anna key instead
        .inherit_stdout()
        .inherit_stderr()
        .build();
    let mut store = Store::new(
        &engine,
        HostState {
            wasi,
            module: module.clone(),
            module_key,
            function_result: None,
            next_result_handle: 1,
            results: HashMap::new(),
            result_receivers: HashMap::new(),
            zenoh,
            zenoh_prefix,
            anna,
        },
    );
    let mut linker = Linker::new(&engine);

    // link in the essa host functions
    linker
        .func_wrap(
            "host",
            "essa_get_args",
            move |mut caller: Caller<'_, HostState>, buf_ptr: u32, buf_len: u32| {
                // the given buffer must be large enough to hold `args`
                if buf_len < u32::try_from(args.len()).unwrap() {
                    return Ok(EssaResult::BufferTooSmall as i32);
                }

                // write `args` to the given memory region in the sandbox
                let mem = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return Err(Trap::new("failed to find host memory")),
                };
                mem.write(&mut caller, buf_ptr as usize, args.as_slice())
                    .context("val ptr/len out of bounds")?;

                Ok(EssaResult::Ok as i32)
            },
        )
        .context("failed to create essa_get_args")?;
    linker
        .func_wrap(
            "host",
            "essa_set_result",
            |mut caller: Caller<'_, HostState>, buf_ptr: u32, buf_len: u32| {
                // copy the given memory region out of the sandbox
                let mem = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return Err(Trap::new("failed to find host memory")),
                };
                let mut buf = vec![0; buf_len as usize];
                mem.read(&mut caller, buf_ptr as usize, &mut buf)
                    .context("ptr/len out of bounds")?;

                // set the function result in the host state
                caller.data_mut().function_result = Some(buf);

                Ok(EssaResult::Ok as i32)
            },
        )
        .context("failed to create essa_set_result")?;
    linker
        .func_wrap(
            "host",
            "essa_call",
            |caller: Caller<'_, HostState>,
             function_name_ptr: u32,
             function_name_len: u32,
             serialized_args_ptr: u32,
             serialized_arg_len: u32,
             result_handle_ptr: u32| {
                essa_call_wrapper(
                    caller,
                    function_name_ptr,
                    function_name_len,
                    serialized_args_ptr,
                    serialized_arg_len,
                    result_handle_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_call host function")?;
    linker
        .func_wrap(
            "host",
            "essa_get_result_len",
            |caller: Caller<'_, HostState>, handle: u32, value_len_ptr: u32| {
                essa_get_result_len_wrapper(caller, handle, value_len_ptr).map(|r| r as i32)
            },
        )
        .context("failed to create essa_get_result_len host function")?;
    linker
        .func_wrap(
            "host",
            "essa_get_result",
            |caller: Caller<'_, HostState>,
             handle: u32,
             value_ptr: u32,
             value_capacity: u32,
             value_len_ptr: u32| {
                essa_get_result_wrapper(caller, handle, value_ptr, value_capacity, value_len_ptr)
                    .map(|r| r as i32)
            },
        )
        .context("failed to create essa_get_result host function")?;
    linker
        .func_wrap(
            "host",
            "essa_put_lattice",
            |caller: Caller<'_, HostState>,
             key_ptr: u32,
             key_len: u32,
             value_ptr: u32,
             value_len: u32| {
                essa_put_lattice_wrapper(caller, key_ptr, key_len, value_ptr, value_len)
                    .map(|r| r as i32)
            },
        )
        .context("failed to create essa_put_lattice host function")?;
    linker
        .func_wrap(
            "host",
            "essa_get_lattice_len",
            |caller: Caller<'_, HostState>, key_ptr: u32, key_len: u32, value_len_ptr: u32| {
                essa_get_lattice_len_wrapper(caller, key_ptr, key_len, value_len_ptr)
                    .map(|r| r as i32)
            },
        )
        .context("failed to create essa_get_lattice host function")?;
    linker
        .func_wrap(
            "host",
            "essa_get_lattice_data",
            |caller: Caller<'_, HostState>,
             key_ptr: u32,
             key_len: u32,
             value_ptr: u32,
             value_capacity: u32,
             value_len_ptr: u32| {
                essa_get_lattice_data_wrapper(
                    caller,
                    key_ptr,
                    key_len,
                    value_ptr,
                    value_capacity,
                    value_len_ptr,
                )
                .map(|r| r as i32)
            },
        )
        .context("failed to create essa_get_lattice_data host function")?;

    // add WASI functionality (e.g. stdin/stdout access)
    wasmtime_wasi::add_to_linker(&mut linker, |state: &mut HostState| &mut state.wasi)
        .context("failed to add wasi functionality to linker")?;

    // link the host and WASI functions with the user-supplied WASM module
    linker
        .module(&mut store, "", module)
        .context("failed to add module to linker")?;

    Ok((store, linker))
}

/// Host function for calling the specified function on a remote node.
fn essa_call_wrapper(
    mut caller: Caller<HostState>,
    function_name_ptr: u32,
    function_name_len: u32,
    serialized_args_ptr: u32,
    serialized_args_len: u32,
    result_handle_ptr: u32,
) -> Result<EssaResult, Trap> {
    // Use our `caller` context to get the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("failed to find host memory")),
    };

    // read the function name from the WASM sandbox
    let function_name = {
        let mut data = vec![0u8; function_name_len as usize];
        mem.read(&caller, function_name_ptr as usize, &mut data)
            .context("function name ptr/len out of bounds")?;
        String::from_utf8(data).context("function name not valid utf8")?
    };
    // read the serialized function arguments from the WASM sandbox
    let args = {
        let mut data = vec![0u8; serialized_args_len as usize];
        mem.read(&caller, serialized_args_ptr as usize, &mut data)
            .context("function name ptr/len out of bounds")?;
        data
    };

    // trigger the external function call
    match caller.data_mut().essa_call(function_name, args) {
        Ok(reply) => {
            let host_state = caller.data_mut();
            let handle = host_state.next_result_handle;
            host_state.next_result_handle += 1;
            host_state.result_receivers.insert(handle, reply);

            // write handle
            mem.write(
                &mut caller,
                result_handle_ptr as usize,
                &handle.to_le_bytes(),
            )
            .context("result_handle_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

fn essa_get_result_len_wrapper(
    mut caller: Caller<HostState>,
    handle: u32,
    val_len_ptr: u32,
) -> Result<EssaResult, Trap> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("failed to find host memory")),
    };

    // get the corresponding value from the KVS
    match caller.data_mut().get_result(handle) {
        Ok(value) => {
            let len = value.len();
            // write the length of the value into the sandbox
            //
            // We cannot write the value directly because the WASM module
            // needs to allocate some space for the (dynamically-sized) value
            // first.
            mem.write(
                &mut caller,
                val_len_ptr as usize,
                &u32::try_from(len).unwrap().to_le_bytes(),
            )
            .context("val_len_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

fn essa_get_result_wrapper(
    mut caller: Caller<HostState>,
    handle: u32,
    val_ptr: u32,
    val_capacity: u32,
    val_len_ptr: u32,
) -> Result<EssaResult, Trap> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("failed to find host memory")),
    };

    // get the corresponding value from the KVS
    match caller.data_mut().get_result(handle) {
        Ok(value) => {
            if value.len() > val_capacity as usize {
                Ok(EssaResult::BufferTooSmall)
            } else {
                // write the value into the sandbox
                mem.write(&mut caller, val_ptr as usize, &value)
                    .context("val ptr/len out of bounds")?;
                // write the length of the value
                mem.write(
                    &mut caller,
                    val_len_ptr as usize,
                    &u32::try_from(value.len()).unwrap().to_le_bytes(),
                )
                .context("val_len_ptr out of bounds")?;

                caller.data_mut().remove_result(handle);

                Ok(EssaResult::Ok)
            }
        }
        Err(err) => Ok(err),
    }
}

/// Host function for storing a given lattice value into the KVS.
fn essa_put_lattice_wrapper(
    mut caller: Caller<HostState>,
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> Result<EssaResult, Trap> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("failed to find host memory")),
    };
    // read out and parse the KVS key
    let key = {
        let mut data = vec![0u8; key_len as usize];
        mem.read(&caller, key_ptr as usize, &mut data)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };
    // read out the value that should be stored
    let value = {
        let mut data = vec![0u8; value_len as usize];
        mem.read(&caller, value_ptr as usize, &mut data)
            .context("value ptr/len out of bounds")?;
        data
    };

    match caller.data_mut().put_lattice(&key, &value) {
        Ok(()) => Ok(EssaResult::Ok),
        Err(other) => Ok(other),
    }
}

/// Host function for reading the length of value stored under a specific key
/// in the KVS.
fn essa_get_lattice_len_wrapper(
    mut caller: Caller<HostState>,
    key_ptr: u32,
    key_len: u32,
    val_len_ptr: u32,
) -> Result<EssaResult, Trap> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("failed to find host memory")),
    };
    // read out and parse the KVS key
    let key = {
        let mut data = vec![0u8; key_len as usize];
        mem.read(&caller, key_ptr as usize, &mut data)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };
    // get the corresponding value from the KVS
    match caller.data_mut().get_lattice(&key) {
        Ok(value) => {
            // write the length of the value into the sandbox
            //
            // We cannot write the value directly because the WASM module
            // needs to allocate some space for the (dynamically-sized) value
            // first.
            mem.write(
                &mut caller,
                val_len_ptr as usize,
                &u32::try_from(value.len()).unwrap().to_le_bytes(),
            )
            .context("val_len_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

/// Host function for reading a specific value from the KVS.
fn essa_get_lattice_data_wrapper(
    mut caller: Caller<HostState>,
    key_ptr: u32,
    key_len: u32,
    val_ptr: u32,
    val_capacity: u32,
    val_len_ptr: u32,
) -> Result<EssaResult, Trap> {
    // Use our `caller` context to learn about the memory export of the
    // module which called this host function.
    let mem = match caller.get_export("memory") {
        Some(Extern::Memory(mem)) => mem,
        _ => return Err(Trap::new("failed to find host memory")),
    };
    // read out and parse the KVS key
    let key = {
        let mut data = vec![0u8; key_len as usize];
        mem.read(&caller, key_ptr as usize, &mut data)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };
    // get the corresponding value from the KVS
    match caller.data_mut().get_lattice(&key) {
        Ok(value) => {
            if value.len() > val_capacity as usize {
                Ok(EssaResult::BufferTooSmall)
            } else {
                // write the value into the sandbox
                mem.write(&mut caller, val_ptr as usize, value.as_slice())
                    .context("val ptr/len out of bounds")?;
                // write the length of the value
                mem.write(
                    &mut caller,
                    val_len_ptr as usize,
                    &u32::try_from(value.len()).unwrap().to_le_bytes(),
                )
                .context("val_len_ptr out of bounds")?;

                Ok(EssaResult::Ok)
            }
        }
        Err(err) => Ok(err),
    }
}

/// Stores all the information needed during execution.
///
/// The `wasmtime` crate gives this struct as an additional argument to all
/// host functions, which makes it possible to keep state between across
/// host function invocations.
struct HostState {
    wasi: WasiCtx,
    /// The compiled WASM module.
    module: Module,
    /// The KVS key under which a serialized version of the compiled WASM
    /// module is stored.
    module_key: ClientKey,
    /// The result value of this function, set through the `essa_set_result`
    /// host function.
    function_result: Option<Vec<u8>>,

    next_result_handle: u32,
    result_receivers: HashMap<u32, ReplyReceiver>,
    results: HashMap<u32, Arc<Vec<u8>>>,

    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: String,
    anna: ClientNode,
}

impl HostState {
    /// Calls the given function on a node and returns the reply receiver for
    /// the corresponding result.
    fn essa_call(
        &mut self,
        function_name: String,
        args: Vec<u8>,
    ) -> Result<ReplyReceiver, EssaResult> {
        // get the requested function and check its signature
        let func = self
            .module
            .get_export(&function_name)
            .and_then(|e| e.func().cloned())
            .ok_or(EssaResult::NoSuchFunction)?;
        if func.params().collect::<Vec<_>>().as_slice() != [ValType::I32]
            || !func.results().collect::<Vec<_>>().as_slice().is_empty()
        {
            return Err(EssaResult::InvalidFunctionSignature);
        }

        // store args in kvs
        let args_key: ClientKey = Uuid::new_v4().to_string().into();
        kvs_put(
            args_key.clone(),
            LastWriterWinsLattice::new_now(args).into(),
            &mut self.anna,
        )
        .map_err(|_| EssaResult::UnknownError)?;

        // trigger the function call on a remote node
        let reply = call_function_extern(
            self.module_key.clone(),
            function_name,
            args_key,
            self.zenoh.clone(),
            &self.zenoh_prefix,
        )
        .unwrap();

        Ok(reply)
    }

    /// Stores the given serialized `LattiveValue` in the KVS.
    fn put_lattice(&mut self, key: &ClientKey, value: &[u8]) -> Result<(), EssaResult> {
        let value = bincode::deserialize(value).map_err(|_| EssaResult::UnknownError)?;
        kvs_put(self.with_prefix(key), value, &mut self.anna).map_err(|_| EssaResult::UnknownError)
    }

    /// Reads the `LattiveValue` at the specified key from the KVS serializes it.
    fn get_lattice(&mut self, key: &ClientKey) -> Result<Vec<u8>, EssaResult> {
        kvs_get(self.with_prefix(key), &mut self.anna)
            .map_err(|_| EssaResult::NotFound)
            .and_then(|v| bincode::serialize(&v).map_err(|_| EssaResult::UnknownError))
    }

    fn with_prefix(&self, key: &ClientKey) -> ClientKey {
        format!("{}/data/{}", self.module_key, key).into()
    }

    fn get_result(&mut self, handle: u32) -> Result<Arc<Vec<u8>>, EssaResult> {
        match self.results.entry(handle) {
            std::collections::hash_map::Entry::Occupied(entry) => Ok(entry.get().clone()),
            std::collections::hash_map::Entry::Vacant(entry) => {
                if let Some(result) = self.result_receivers.remove(&handle) {
                    let reply = result.recv().map_err(|_| EssaResult::UnknownError)?;
                    let value = reply.sample.value.payload.contiguous().into_owned();
                    let value = entry.insert(Arc::new(value));
                    Ok(value.clone())
                } else {
                    Err(EssaResult::NotFound)
                }
            }
        }
    }

    fn remove_result(&mut self, handle: u32) {
        self.results.remove(&handle);
    }
}

/// Call the specfied function on a remote node.
fn call_function_extern(
    module_key: ClientKey,
    function_name: String,
    args_key: ClientKey,
    zenoh: Arc<zenoh::Session>,
    zenoh_prefix: &str,
) -> anyhow::Result<ReplyReceiver> {
    let topic = scheduler_function_call_topic(zenoh_prefix, &module_key, &function_name, &args_key);

    // send the request to the scheduler node
    let reply = zenoh
        .get(topic)
        .wait()
        .map_err(|e| anyhow::anyhow!(e))
        .context("failed to send function call request to scheduler")?;

    Ok(reply)
}
