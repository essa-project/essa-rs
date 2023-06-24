//! Specifies the API and ABI for communication between a WASM module and the runtime.
//!
//! Many different programming languages can be compiled to WebAssembly (WASM),
//! so we need to define a stable ABI that is supported in all potential
//! languages. For this reason, we define the interface as C-compatible
//! functions. This makes the interface very low-level and cumbersome to use
//! directly, so we create more high-level wrapper functions on top of it.
//!
//! ## Memory Management
//!
//! WASM and the wasmtime runtime have native support for passing primitive
//! types as arguments and return values between the runtime and a WASM module
//! (through host-defined functions). For passing larger data, pointers need to
//! be used, which leads to some challenges:
//!
//! - WASM modules are sandboxed, so they cannot access any memory outside of
//!   their assigned memory region. So pointers to runtime memory are not
//!   possible.
//! - The WASM runtime can read and write the sandboxed memory of any WASM
//!   module it executes without restrictions. However, it does not know
//!   which memory areas of the WASM module are unused and can be safely
//!   used for writing data.
//! - The WASM modules manage their own memory and have their own custom
//!   memory allocator, which is not usable by the runtime.
//!
//! We decided on the following design around these challenges:
//!
//! - Pass data from WASM module to runtime: The WASM module passes the
//!   pointer (i.e. the offset) and the length of the data as two integer
//!   arguments to the host function defined by the runtime. Using these
//!   arguments, the runtime can read the data out of the sandbox. However,
//!   it must not access the data anymore after returning from the host
//!   function because the WASM module might deallocate that memory again.
//! - Passing data from runtime to WASM module is a three-step process. First,
//!   the runtime tells the WASM module the size of the data it wants to pass
//!   (e.g. the length of a byte array). The WASM module then allocates a
//!   a suitable location in its sandboxed memory and passes the corresponding
//!   pointer to the runtime. The runtime then writes the data into the sandbox
//!   at the given location.
//!
//! An alternative approach for passing data from the runtime to WASM modules
//! is to pre-allocate a larger amount of buffer memory that is then "owned"
//! by the runtime. This way, the runtime can write data directly to WASM
//! accessible-memory, which should be more performant. The drawbacks are
//! the additional memory consumption of the buffer memory and that the buffer
//! size limits the data size.
//!
//! ## Future Possibilities
//!
//! There is ongoing work on
//! [WASM interface types](https://github.com/WebAssembly/interface-types/blob/main/proposals/interface-types/Explainer.md),
//! which would allow us to define the API in a higher-level way. The most
//! advanced project in this area seems to be
//! [`wit-bindgen`](https://github.com/bytecodealliance/wit-bindgen),
//! which generates bindings for different languages based on a `.wit`
//! descriptor file. The ABI details are abstracted away this way.

// Essa core functions: getting function arguments, setting the function
// result, and calling a remote function.
#[link(wasm_import_module = "host")]
extern "C" {
    /// Requests the function arguments from the runtime as a serialized byte
    /// array.
    ///
    /// The runtime cannot write the arguments into the memory of the WASM
    /// module directly since this requires a prior memory allocation. For this
    /// reason, the runtime only passes the number of needed bytes as WASM
    /// argument when invoking a function. The function should then allocate
    /// the requested amount of memory and then call `essa_get_args` to copy
    /// the actual arguments into that allocated memory.
    ///
    /// The `args_len` must be identical to the raw argument byte length
    /// passed as function argument.
    ///
    /// The return value is an [`EssaResult`](crate::EssaResult).
    ///
    /// The [`crate::get_args_raw`] function provides a higher-level abstraction
    /// for this function.
    pub fn essa_get_args(args_ptr: *mut u8, args_len: usize) -> i32;

    /// Sets the given byte array as the function's result.
    ///
    /// We cannot use normal WASM return values for this since only primitive
    /// types are supported as return values. For this reason, functions
    /// should serialize their return value and then call `essa_set_result`
    /// to register the return value with the runtime. Only a single return
    /// value must be set for each function.
    ///
    /// The return value is an [`EssaResult`](crate::EssaResult).
    ///
    /// The [`crate::set_result`] function provides a higher-level abstraction
    /// for this function.
    pub fn essa_set_result(ptr: *const u8, len: usize) -> i32;

    /// Invokes the specified function on a different node.
    ///
    /// Since WASM has no native support for strings, we need to pass the
    /// function as a pointer and length pair. The function name must be valid
    /// UTF-8 and refer to a valid function registered with the runtime.
    ///
    /// The `args` argument specifies a byte array that should be passed to
    /// the external function as arguments. This is typically a serialized
    /// struct.
    ///
    /// The `return_handle` is filled with an unique handle that can be passed
    /// to `essa_get_result` to get the function's result value.
    ///
    /// The return value of this function is an [`EssaResult`](crate::EssaResult).
    ///
    /// The [`crate::call_function`] function provides a higher-level abstraction
    /// for this function.
    pub fn essa_call(
        function_name_ptr: *const u8,
        function_name_len: usize,
        args_ptr: *const u8,
        args_len: usize,
        result_handle: *mut usize,
    ) -> i32;

    /// Waits for the function result associated with the given handle and
    /// returns its length.
    ///
    /// The caller should use the returned length to allocate a large-enough
    /// memory region and then call [`essa_get_result`] to fill in the result.
    ///
    /// The return value is an [`EssaResult`](crate::EssaResult).
    pub fn essa_get_result_len(result_handle: usize, value_len_ptr: *mut usize) -> i32;

    /// Waits for the function result associated with the given handle and
    /// writes it to the given memory location.
    ///
    /// This function can only be used once for each handle.
    ///
    /// The `value_capacity` must be at least
    /// as large as the value length returned by [`essa_get_result_len`]. The
    /// `value_ptr` must point to a memory allocation of that size. The
    /// runtime will write the value to that pointer and return the actual
    /// value length through `value_len_ptr`.
    ///
    /// The return value is an [`EssaResult`](crate::EssaResult).
    pub fn essa_get_result(
        result_handle: usize,
        value_ptr: *mut u8,
        value_capacity: usize,
        value_len_ptr: *mut usize,
    ) -> i32;
}

// Functions for reading and writing data to a key-value store.
//
// (Right now this is based on `anna-rs` and lattice values, but other KVS
// stores should work in a similar way.)
#[link(wasm_import_module = "host")]
extern "C" {
    /// Stores the given byte array under the given key in the key-value-store.
    ///
    /// The key must be valid UTF-8. The value must be a serialized _lattice_
    /// type.
    ///
    /// The return value is an [`EssaResult`](crate::EssaResult).
    ///
    /// The [`crate::kvs_put`] function provides a safe, higher-level
    /// abstraction for this function.
    pub fn essa_put_lattice(
        key_ptr: *const u8,
        key_len: usize,
        value_ptr: *const u8,
        value_len: usize,
    ) -> i32;

    /// Reads the given key from the KVS and returns the size of the
    /// corresponding value.
    ///
    /// The key must be valid UTF-8. The value size is written to the given
    /// `value_len_ptr`. To read the actual value, allocate a large-enough
    /// memory region and then call `essa_get_lattice_data`.
    ///
    /// The return value is an [`EssaResult`](crate::EssaResult).
    pub fn essa_get_lattice_len(
        key_ptr: *const u8,
        key_len: usize,
        value_len_ptr: *mut usize,
    ) -> i32;

    /// Reads the lattice value stored for given key from the KVS.
    ///
    /// The key must be valid UTF-8. The `value_capacity` must be at least
    /// as large as the value length returned by [`essa_get_lattice_len`]. The
    /// `value_ptr` must point to a memory allocation of that size. The
    /// runtime will write the value to that pointer and return the actual
    /// value length through `value_len_ptr`.
    ///
    /// The return value is an [`EssaResult`](crate::EssaResult).
    ///
    /// The [`crate::kvs_try_get`] and [`crate::kvs_get`] functions provide
    /// safe, higher-level abstractions for this function.
    pub fn essa_get_lattice_data(
        key_ptr: *const u8,
        key_len: usize,
        value_ptr: *mut u8,
        value_capacity: usize,
        value_len_ptr: *mut usize,
    ) -> i32;
}
// R related functionality
#[link(wasm_import_module = "host")]
extern "C" {
    /// Run a remote R function.
    pub fn essa_run_r(
        function_name_ptr: *const u8,
        function_name_len: usize,
        args_ptr: *const u8,
        args_len: usize,
        result_handle: *mut usize,
    ) -> i32;
}
