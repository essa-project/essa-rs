//! Use WasmEdge as executor's runtime.

#![warn(missing_docs)]

use crate::{get_args, get_module, kvs_get, kvs_put, EssaResult, FunctionExecutor};
use anna::{lattice::LastWriterWinsLattice, nodes::ClientNode, ClientKey};
use anyhow::Context;
use essa_common::scheduler_function_call_topic;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use uuid::Uuid;
use wasmedge_sdk::{
    error::HostFuncError, CallingFrame, Executor, Func, ImportObject, ImportObjectBuilder,
    Instance, Memory, Module, Store,
};
use wasmedge_sys::types::WasmValue;
use wasmedge_types::{ExternalInstanceType, ValType};
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
        let module =
            Arc::new(Module::from_bytes(None, &wasm_bytes).context("failed to load wasm module")?);

        // store the compiled module in the key-value store under an
        // unique key (to avoid recompiling the module on future function
        // calls)
        log::info!("Storing compiled wasm module in anna KVS");
        let module_key: ClientKey = Uuid::new_v4().to_string().into();
        let value = LastWriterWinsLattice::new_now(
            // TODO: serialize the WasmEdge module into bytes, require WasmEdge core support.
            wasm_bytes,
        )
        .into();
        kvs_put(module_key.clone(), value, &mut self.anna)?;

        let mut host_state = HostState {
            module: module.clone(),
            module_key,
            function_result: None,
            next_result_handle: 1,
            results: HashMap::new(),
            result_receivers: HashMap::new(),
            zenoh: self.zenoh.clone(),
            zenoh_prefix: self.zenoh_prefix.clone(),
            anna: self.anna,
        };

        let mut instance_wrapper = InstanceWrapper::new()?;
        instance_wrapper.register(&module, &mut host_state, vec![])?;

        instance_wrapper.call_default()?;
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
        // TODO: deserialize the bytes into WasmEdge module, require WasmEdge core support.
        let module =
            Arc::new(Module::from_bytes(None, &module).context("failed to load wasm module")?);

        let mut host_state = HostState {
            module: module.clone(),
            module_key,
            function_result: None,
            next_result_handle: 1,
            results: HashMap::new(),
            result_receivers: HashMap::new(),
            zenoh: self.zenoh.clone(),
            zenoh_prefix: self.zenoh_prefix.clone(),
            anna: self.anna,
        };

        let mut instance_wrapper = InstanceWrapper::new()?;
        instance_wrapper.register(&module, &mut host_state, args)?;

        instance_wrapper.call(function_name, args_len as i32)?;

        // store the function's result into the key value store under
        // the requested key
        //
        // TODO: The result is needed only once, so it might make more sense to
        // send it as a message instead of storing it in the KVS. This would
        // also improve performance since the receiver would no longer need to
        // busy-wait on the result key in the KVS anymore.
        if let Some(result_value) = host_state.function_result.take() {
            let selector = query.key_selector().to_string();
            query.reply(Sample::new(selector, result_value));

            Ok(())
        } else {
            Err(anyhow::anyhow!("no result"))
        }
    }
}

// TODO: in order to use external variables in closure, it is currently necessary to use
// TODO: some tricks (in an unsafe way) to avoid the current shortcomings of WasmEdge.
struct HostContext {
    memory: *mut Option<Memory>,
    host_state: *mut HostState,
}
unsafe impl Send for HostContext {}

/// A wrapper that stores some important data structures of WasmEdge runtime.
struct InstanceWrapper {
    executor: Executor,
    store: Store,
    instance: Option<Instance>,
    memory: Option<Memory>,
    import: Option<ImportObject>,
    wasi: Option<ImportObject>,
}

impl InstanceWrapper {
    fn new() -> Result<Self, anyhow::Error> {
        let executor = Executor::new(None, None).context("failed to create wasmedge executor")?;
        let store = Store::new().context("failed to create wasmedge store")?;

        Ok(InstanceWrapper {
            executor,
            store,
            instance: None,
            memory: None,
            import: None,
            wasi: None,
        })
    }

    /// Register the import module, wasi module, active module to the WasmEdge Store.
    fn register(
        &mut self,
        module: &Module,
        host_state: &mut HostState,
        args: Vec<u8>,
    ) -> Result<(), anyhow::Error> {
        // essa_get_args
        let host_context = Arc::new(Mutex::new(HostContext {
            memory: &mut self.memory as *mut Option<Memory>,
            host_state: host_state as *mut HostState,
        }));
        let essa_get_args = move |_: &CallingFrame,
                                  inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            let buf_ptr = inputs[0].to_i32() as u32;
            let buf_len = inputs[1].to_i32() as u32;

            // the given buffer must be large enough to hold `args`
            if buf_len < u32::try_from(args.len()).unwrap() {
                return Ok(vec![WasmValue::from_i32(EssaResult::BufferTooSmall as i32)]);
            }

            // write `args` to the given memory region in the sandbox
            let mem = unsafe { &mut *(host_context.lock().unwrap().memory) }
                .as_mut()
                .unwrap();
            mem.write(args.clone(), buf_ptr).unwrap();

            Ok(vec![WasmValue::from_i32(EssaResult::Ok as i32)])
        };

        // essa_set_result
        let host_context = Arc::new(Mutex::new(HostContext {
            memory: &mut self.memory as *mut Option<Memory>,
            host_state: host_state as *mut HostState,
        }));
        let essa_set_result = move |_: &CallingFrame,
                                    inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            let buf_ptr = inputs[0].to_i32() as u32;
            let buf_len = inputs[1].to_i32() as u32;

            // copy the given memory region out of the sandbox
            let mem = unsafe { &mut *(host_context.lock().unwrap().memory) }
                .as_mut()
                .unwrap();
            let buf = mem.read(buf_ptr, buf_len).unwrap();

            let host_state = unsafe { &mut *(host_context.lock().unwrap().host_state) };
            host_state.function_result = Some(buf);

            Ok(vec![WasmValue::from_i32(EssaResult::Ok as i32)])
        };

        // essa_call
        let host_context = Arc::new(Mutex::new(HostContext {
            memory: &mut self.memory as *mut Option<Memory>,
            host_state: host_state as *mut HostState,
        }));
        let essa_call = move |_: &CallingFrame,
                              inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            let function_name_ptr = inputs[0].to_i32() as u32;
            let function_name_len = inputs[1].to_i32() as u32;
            let serialized_args_ptr = inputs[2].to_i32() as u32;
            let serialized_arg_len = inputs[3].to_i32() as u32;
            let result_handle_ptr = inputs[4].to_i32() as u32;

            let memory = unsafe { &mut *(host_context.lock().unwrap().memory) }
                .as_mut()
                .unwrap();
            let host_state = unsafe { &mut *(host_context.lock().unwrap().host_state) };

            let res = essa_call_wrapper(
                memory,
                host_state,
                function_name_ptr,
                function_name_len,
                serialized_args_ptr,
                serialized_arg_len,
                result_handle_ptr,
            )
            .unwrap();

            Ok(vec![WasmValue::from_i32(res as i32)])
        };

        // essa_get_result_len
        let host_context = Arc::new(Mutex::new(HostContext {
            memory: &mut self.memory as *mut Option<Memory>,
            host_state: host_state as *mut HostState,
        }));
        let essa_get_result_len = move |_: &CallingFrame,
                                        inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            let handle = inputs[0].to_i32() as u32;
            let value_len_ptr = inputs[1].to_i32() as u32;

            let memory = unsafe { &mut *(host_context.lock().unwrap().memory) }
                .as_mut()
                .unwrap();
            let host_state = unsafe { &mut *(host_context.lock().unwrap().host_state) };

            let res =
                essa_get_result_len_wrapper(memory, host_state, handle, value_len_ptr).unwrap();

            Ok(vec![WasmValue::from_i32(res as i32)])
        };

        // essa_get_result
        let host_context = Arc::new(Mutex::new(HostContext {
            memory: &mut self.memory as *mut Option<Memory>,
            host_state: host_state as *mut HostState,
        }));
        let essa_get_result = move |_: &CallingFrame,
                                    inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            let handle = inputs[0].to_i32() as u32;
            let value_ptr = inputs[1].to_i32() as u32;
            let value_capacity = inputs[2].to_i32() as u32;
            let value_len_ptr = inputs[3].to_i32() as u32;

            let memory = unsafe { &mut *(host_context.lock().unwrap().memory) }
                .as_mut()
                .unwrap();
            let host_state = unsafe { &mut *(host_context.lock().unwrap().host_state) };

            let res = essa_get_result_wrapper(
                memory,
                host_state,
                handle,
                value_ptr,
                value_capacity,
                value_len_ptr,
            )
            .unwrap();

            Ok(vec![WasmValue::from_i32(res as i32)])
        };

        // essa_put_lattice
        let host_context = Arc::new(Mutex::new(HostContext {
            memory: &mut self.memory as *mut Option<Memory>,
            host_state: host_state as *mut HostState,
        }));
        let essa_put_lattice = move |_: &CallingFrame,
                                     inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            let key_ptr = inputs[0].to_i32() as u32;
            let key_len = inputs[1].to_i32() as u32;
            let value_ptr = inputs[2].to_i32() as u32;
            let value_len = inputs[3].to_i32() as u32;

            let memory = unsafe { &mut *(host_context.lock().unwrap().memory) }
                .as_mut()
                .unwrap();
            let host_state = unsafe { &mut *(host_context.lock().unwrap().host_state) };

            let res = essa_put_lattice_wrapper(
                memory, host_state, key_ptr, key_len, value_ptr, value_len,
            )
            .unwrap();

            Ok(vec![WasmValue::from_i32(res as i32)])
        };

        // essa_get_lattice_len
        let host_context = Arc::new(Mutex::new(HostContext {
            memory: &mut self.memory as *mut Option<Memory>,
            host_state: host_state as *mut HostState,
        }));
        let essa_get_lattice_len = move |_: &CallingFrame,
                                         inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            let key_ptr = inputs[0].to_i32() as u32;
            let key_len = inputs[1].to_i32() as u32;
            let value_len_ptr = inputs[2].to_i32() as u32;

            let memory = unsafe { &mut *(host_context.lock().unwrap().memory) }
                .as_mut()
                .unwrap();
            let host_state = unsafe { &mut *(host_context.lock().unwrap().host_state) };

            let res =
                essa_get_lattice_len_wrapper(memory, host_state, key_ptr, key_len, value_len_ptr)
                    .unwrap();

            Ok(vec![WasmValue::from_i32(res as i32)])
        };

        // essa_get_lattice_data
        let host_context = Arc::new(Mutex::new(HostContext {
            memory: &mut self.memory as *mut Option<Memory>,
            host_state: host_state as *mut HostState,
        }));
        let essa_get_lattice_data = move |_: &CallingFrame,
                                          inputs: Vec<WasmValue>|
              -> Result<Vec<WasmValue>, HostFuncError> {
            let key_ptr = inputs[0].to_i32() as u32;
            let key_len = inputs[1].to_i32() as u32;
            let value_ptr = inputs[2].to_i32() as u32;
            let value_capacity = inputs[3].to_i32() as u32;
            let value_len_ptr = inputs[4].to_i32() as u32;

            let memory = unsafe { &mut *(host_context.lock().unwrap().memory) }
                .as_mut()
                .unwrap();
            let host_state = unsafe { &mut *(host_context.lock().unwrap().host_state) };

            let res = essa_get_lattice_data_wrapper(
                memory,
                host_state,
                key_ptr,
                key_len,
                value_ptr,
                value_capacity,
                value_len_ptr,
            )
            .unwrap();

            Ok(vec![WasmValue::from_i32(res as i32)])
        };

        // Register import module.
        let import = ImportObjectBuilder::new()
            .with_func_single_thread::<(i32, i32), i32>("essa_get_args", essa_get_args)?
            .with_func_single_thread::<(i32, i32), i32>("essa_set_result", essa_set_result)?
            .with_func_single_thread::<(i32, i32, i32, i32, i32), i32>("essa_call", essa_call)?
            .with_func_single_thread::<(i32, i32), i32>("essa_get_result_len", essa_get_result_len)?
            .with_func_single_thread::<(i32, i32, i32, i32), i32>(
                "essa_get_result",
                essa_get_result,
            )?
            .with_func_single_thread::<(i32, i32, i32, i32), i32>(
                "essa_put_lattice",
                essa_put_lattice,
            )?
            .with_func_single_thread::<(i32, i32, i32), i32>(
                "essa_get_lattice_len",
                essa_get_lattice_len,
            )?
            .with_func_single_thread::<(i32, i32, i32, i32, i32), i32>(
                "essa_get_lattice_data",
                essa_get_lattice_data,
            )?
            .build("host")
            .context("failed to create a ImportObject")?;
        self.import = Some(import);
        self.store
            .register_import_module(&mut self.executor, self.import.as_ref().unwrap())
            .context("failed to register and instantiate a wasmedge import object into a store")?;

        // Register wasi module.
        let wasi = ImportObjectBuilder::new()
            .build_as_wasi(None, None, None)
            .context("failed to create a ImportObject")?;
        self.wasi = Some(wasi);
        self.store
            .register_import_module(&mut self.executor, self.wasi.as_ref().unwrap())
            .context("failed to register and instantiate a wasmedge import object into a store")?;

        // Register active module and get the instance.
        self.instance = Some(
            self.store
                .register_active_module(&mut self.executor, module)
                .context("failed to register and instantiate a wasmedge module into a store as an anonymous module")?
        );

        self.memory = Some(
            self.get_instance()?
                .memory("memory")
                .context("failed to find host memory")?,
        );

        Ok(())
    }

    // Call the “default function” of a module.
    fn call_default(&mut self) -> Result<(), anyhow::Error> {
        let func = get_default(self.get_instance()?).context("module has no default function")?;

        let func_ty = func.ty().context("failed to get the function type")?;

        // Check the signature of the default function.
        if func_ty.args_len() != 0 || func_ty.returns_len() != 0 {
            return Err(anyhow::anyhow!("the default function has invalid type"));
        }

        log::info!("Starting default function of wasm module");

        func.call(&mut self.executor, [])
            .context("default function failed")?;

        Ok(())
    }

    // Call the function with the `name`.
    fn call(&mut self, name: &str, args: i32) -> Result<(), anyhow::Error> {
        // get the function that we've been requested to call
        let func = self
            .get_instance()?
            .func(name)
            .with_context(|| format!("module has no function `{}`", name))?;

        let func_ty = func.ty().context("failed to get the function type")?;

        // Check the signature of the function
        if func_ty
            .args()
            .context("failed to return the type of the arguments")?
            != [ValType::I32]
            || func_ty.returns_len() != 0
        {
            return Err(anyhow::anyhow!(format!(
                "the function `{}` has invalid type",
                name
            )));
        }

        func.call(&mut self.executor, vec![WasmValue::from_i32(args)])
            .context("function trapped")?;

        Ok(())
    }

    fn get_instance(&self) -> Result<&Instance, anyhow::Error> {
        self.instance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("module has not been instantiated"))
    }
}

/// Returns the "default export" of a WASM instance.
fn get_default(instance: &Instance) -> Result<Func, anyhow::Error> {
    if let Some(func) = instance.func("") {
        return Ok(func);
    }

    // For compatibility, also recognize "_start".
    if let Some(func) = instance.func("_start") {
        return Ok(func);
    }

    // Otherwise return a no-op function.
    let func = |_: &CallingFrame, _: Vec<WasmValue>| -> Result<Vec<WasmValue>, HostFuncError> {
        Ok(vec![])
    };
    Func::wrap_single_thread::<(), ()>(func).context("failed to create wasmedge function")
}

/// Host function for calling the specified function on a remote node.
fn essa_call_wrapper(
    memory: &mut Memory,
    host_state: &mut HostState,
    function_name_ptr: u32,
    function_name_len: u32,
    serialized_args_ptr: u32,
    serialized_args_len: u32,
    result_handle_ptr: u32,
) -> Result<EssaResult, anyhow::Error> {
    // read the function name from the WASM sandbox
    let function_name = {
        let data = memory
            .read(function_name_ptr, function_name_len)
            .context("function name ptr/len out of bounds")?;
        String::from_utf8(data).context("function name not valid utf8")?
    };
    // read the serialized function arguments from the WASM sandbox
    let args = memory
        .read(serialized_args_ptr, serialized_args_len)
        .context("function name ptr/len out of bounds")?;

    // trigger the external function call
    match host_state.essa_call(function_name, args) {
        Ok(reply) => {
            let handle = host_state.next_result_handle;
            host_state.next_result_handle += 1;
            host_state.result_receivers.insert(handle, reply);

            // write handle
            memory
                .write(handle.to_le_bytes(), result_handle_ptr)
                .context("result_handle_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

fn essa_get_result_len_wrapper(
    memory: &mut Memory,
    host_state: &mut HostState,
    handle: u32,
    val_len_ptr: u32,
) -> Result<EssaResult, anyhow::Error> {
    // get the corresponding value from the KVS
    match host_state.get_result(handle) {
        Ok(value) => {
            let len = value.len();
            // write the length of the value into the sandbox
            //
            // We cannot write the value directly because the WASM module
            // needs to allocate some space for the (dynamically-sized) value
            // first.
            memory
                .write(u32::try_from(len).unwrap().to_le_bytes(), val_len_ptr)
                .context("val_len_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

fn essa_get_result_wrapper(
    memory: &mut Memory,
    host_state: &mut HostState,
    handle: u32,
    val_ptr: u32,
    val_capacity: u32,
    val_len_ptr: u32,
) -> Result<EssaResult, anyhow::Error> {
    // get the corresponding value from the KVS
    match host_state.get_result(handle) {
        Ok(value) => {
            if value.len() > val_capacity as usize {
                Ok(EssaResult::BufferTooSmall)
            } else {
                // write the value into the sandbox
                memory
                    .write(unsafe { &*Arc::into_raw(value.clone()) }.clone(), val_ptr)
                    .context("val ptr/len out of bounds")?;
                // write the length of the value
                memory
                    .write(
                        u32::try_from(value.len()).unwrap().to_le_bytes(),
                        val_len_ptr,
                    )
                    .context("val_len_ptr out of bounds")?;

                host_state.remove_result(handle);

                Ok(EssaResult::Ok)
            }
        }
        Err(err) => Ok(err),
    }
}

/// Host function for storing a given lattice value into the KVS.
fn essa_put_lattice_wrapper(
    memory: &mut Memory,
    host_state: &mut HostState,
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> Result<EssaResult, anyhow::Error> {
    // read out and parse the KVS key
    let key = {
        let data = memory
            .read(key_ptr, key_len)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };
    // read out the value that should be stored
    let value = memory
        .read(value_ptr, value_len)
        .context("value ptr/len out of bounds")?;

    match host_state.put_lattice(&key, &value) {
        Ok(()) => Ok(EssaResult::Ok),
        Err(other) => Ok(other),
    }
}

/// Host function for reading the length of value stored under a specific key
/// in the KVS.
fn essa_get_lattice_len_wrapper(
    memory: &mut Memory,
    host_state: &mut HostState,
    key_ptr: u32,
    key_len: u32,
    val_len_ptr: u32,
) -> Result<EssaResult, anyhow::Error> {
    // read out and parse the KVS key
    let key = {
        let data = memory
            .read(key_ptr, key_len)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };
    // get the corresponding value from the KVS
    match host_state.get_lattice(&key) {
        Ok(value) => {
            // write the length of the value into the sandbox
            //
            // We cannot write the value directly because the WASM module
            // needs to allocate some space for the (dynamically-sized) value
            // first.
            memory
                .write(
                    u32::try_from(value.len()).unwrap().to_le_bytes(),
                    val_len_ptr,
                )
                .context("val_len_ptr out of bounds")?;

            Ok(EssaResult::Ok)
        }
        Err(err) => Ok(err),
    }
}

/// Host function for reading a specific value from the KVS.
fn essa_get_lattice_data_wrapper(
    memory: &mut Memory,
    host_state: &mut HostState,
    key_ptr: u32,
    key_len: u32,
    val_ptr: u32,
    val_capacity: u32,
    val_len_ptr: u32,
) -> Result<EssaResult, anyhow::Error> {
    // read out and parse the KVS key
    let key = {
        let data = memory
            .read(key_ptr, key_len)
            .context("key ptr/len out of bounds")?;
        String::from_utf8(data)
            .context("key is not valid utf8")?
            .into()
    };
    // get the corresponding value from the KVS
    match host_state.get_lattice(&key) {
        Ok(value) => {
            if value.len() > val_capacity as usize {
                Ok(EssaResult::BufferTooSmall)
            } else {
                // write the value into the sandbox
                memory
                    .write(value.clone(), val_ptr)
                    .context("val ptr/len out of bounds")?;
                // write the length of the value
                memory
                    .write(
                        u32::try_from(value.len()).unwrap().to_le_bytes(),
                        val_len_ptr,
                    )
                    .context("val_len_ptr out of bounds")?;

                Ok(EssaResult::Ok)
            }
        }
        Err(err) => Ok(err),
    }
}

/// Stores all the information needed during execution.
struct HostState {
    /// The compiled WASM module.
    module: Arc<Module>,
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
            .exports()
            .into_iter()
            .find(|x| x.name() == function_name)
            .ok_or(EssaResult::NoSuchFunction)?
            .ty()
            .map_err(|_| EssaResult::NoSuchFunction)?;
        if let ExternalInstanceType::Func(func_type) = func {
            if func_type.args() != Some(&[ValType::I32]) || func_type.returns_len() != 0 {
                return Err(EssaResult::InvalidFunctionSignature);
            };
        } else {
            return Err(EssaResult::NoSuchFunction);
        };

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
