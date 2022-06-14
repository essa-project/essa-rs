//! The essa-rs API enables WebAssembly functions to perform external calls
//! and provides access to a key-value store.

#![warn(missing_docs)]

use std::{thread, time::Duration};

use crate::c_api::{
    essa_get_args, essa_get_lattice_data, essa_get_lattice_len, essa_put_lattice, essa_set_result,
};
use anna_api::{ClientKey, LatticeValue};
use c_api::{essa_call, essa_get_result, essa_get_result_len};

/// Re-export the dependencies on serde and bincode to allow downstream
/// crates to use the exact same version.
///
/// These reexports are required for the generated code of the `essa_wrap`
/// macro.
pub use {bincode, serde};
pub use {essa_macros::essa_wrap, remote_result::RemoteFunctionResult};

pub mod c_api;
#[doc(hidden)]
pub mod remote_result;

/// Requests the function arguments from the runtime as a serialized byte
/// array.
///
/// The runtime cannot write the arguments into the memory of the WASM
/// module directly since this requires a prior memory allocation. For this
/// reason, the runtime only passes the number of needed bytes as WASM
/// argument when invoking a function. Through this function, the actual
/// arguments can be requested from the runtime.
///
/// This function is an abstraction over [`c_api::essa_get_args`].
pub fn get_args_raw(args_raw_len: usize) -> Result<Vec<u8>, EssaResult> {
    let mut args = vec![0; args_raw_len];
    let result = unsafe { essa_get_args(args.as_mut_ptr(), args.len()) };
    match EssaResult::try_from(result) {
        Ok(EssaResult::Ok) => Ok(args),
        Ok(other) => Err(other),
        Err(unknown) => panic!("unknown essaResult variant `{}`", unknown),
    }
}

/// Sets the given byte array as the function's result.
///
/// We cannot use normal WASM return values for this since only primitive
/// types are supported as return values. For this reason, functions
/// should serialize their return value and then call `set_result`
/// to register the return value with the runtime. Only a single return
/// value must be set for each function.
///
/// This function is an abstraction over [`c_api::essa_set_result`].
pub fn set_result(result_serialized: &[u8]) -> Result<(), EssaResult> {
    let result = unsafe { essa_set_result(result_serialized.as_ptr(), result_serialized.len()) };
    match EssaResult::try_from(result) {
        Ok(EssaResult::Ok) => Ok(()),
        Ok(other) => Err(other),
        Err(unknown) => panic!("unknown essaResult variant `{}`", unknown),
    }
}

/// Invokes the specified function on a different node.
///
/// The `args` argument specifies a byte array that should be passed to
/// the external function as arguments. This is typically a serialized
/// struct.
///
/// This function is an abstraction over [`c_api::essa_call`].
pub fn call_function(function_name: &str, args: &[u8]) -> Result<ResultHandle, EssaResult> {
    let mut result_handle = 0;
    let result = unsafe {
        essa_call(
            function_name.as_ptr(),
            function_name.len(),
            args.as_ptr(),
            args.len(),
            &mut result_handle,
        )
    };

    match result {
        i if i == EssaResult::Ok as i32 => Ok(ResultHandle(result_handle)),
        other => return Err(other.try_into().unwrap()),
    }
}

/// Handle to retrieve an asynchronous result of a remote function call.
///
/// To wait on the associated result, use [`wait`](Self::wait).
pub struct ResultHandle(usize);

impl ResultHandle {
    /// Tries to read the lattice value stored for given key from the key-value
    /// store.
    pub fn wait(self) -> Result<Vec<u8>, EssaResult> {
        let mut value_len = 0;
        let result = unsafe { essa_get_result_len(self.0, &mut value_len) };
        let len = match EssaResult::try_from(result) {
            Ok(EssaResult::Ok) => value_len,
            Ok(other) => return Err(other),
            Err(unknown) => panic!("unknown EssaResult variant `{}`", unknown),
        };

        let mut value = vec![0u8; len];
        let result =
            unsafe { essa_get_result(self.0, value.as_mut_ptr(), value.len(), &mut value_len) };
        match result {
            i if i == EssaResult::Ok as i32 => Ok(value),
            other => return Err(other.try_into().unwrap()),
        }
    }
}

/// Tries to read the lattice value stored for given key from the key-value
/// store.
pub fn kvs_try_get(key: &ClientKey) -> Result<Option<LatticeValue>, EssaResult> {
    let mut value_len = 0;
    let result = unsafe { essa_get_lattice_len(key.as_ptr(), key.len(), &mut value_len) };
    let len = match EssaResult::try_from(result) {
        Ok(EssaResult::Ok) => value_len,
        Ok(EssaResult::NotFound) => return Ok(None),
        Ok(other) => return Err(other),
        Err(unknown) => panic!("unknown essaResult variant `{}`", unknown),
    };

    let mut value_serialized = vec![0u8; len];
    let result = unsafe {
        essa_get_lattice_data(
            key.as_ptr(),
            key.len(),
            value_serialized.as_mut_ptr(),
            value_serialized.len(),
            &mut value_len,
        )
    };
    let value_serialized = match result {
        i if i == EssaResult::Ok as i32 => value_serialized,
        other => return Err(other.try_into().unwrap()),
    };

    bincode::deserialize(&value_serialized)
        .map(Some)
        .map_err(|_| EssaResult::InvalidResult)
}

/// Read the lattice value stored for given key from the key-value store.
///
/// Blocks until the requested value exists.
pub fn kvs_get(key: &ClientKey) -> Result<LatticeValue, EssaResult> {
    loop {
        match kvs_try_get(key) {
            // TODO: do a proper wait instead of busy-looping
            Ok(None) | Err(EssaResult::NotFound) => {
                // wait a bit, then retry
                thread::sleep(Duration::from_millis(100));
            }
            Ok(Some(value)) => break Ok(value),
            Err(err) => break Err(err),
        }
    }
}

/// Stores the given byte array under the given key in the key-value-store.
///
/// This function is a safe abstraction over the [`c_api::essa_put_lattice`] function.
pub fn kvs_put(key: &ClientKey, value: &LatticeValue) -> Result<(), EssaResult> {
    let serialized = bincode::serialize(&value).map_err(|_| EssaResult::UnknownError)?;

    let result = unsafe {
        essa_put_lattice(
            key.as_ptr(),
            key.len(),
            serialized.as_ptr(),
            serialized.len(),
        )
    };
    match EssaResult::try_from(result) {
        Ok(EssaResult::Ok) => Ok(()),
        Ok(other) => return Err(other),
        Err(unknown) => panic!("unknown essaResult variant `{}`", unknown),
    }
}

/// Errors that can occur when interacting with the essa runtime.
#[derive(Debug, num_enum::IntoPrimitive, num_enum::TryFromPrimitive)]
#[repr(i32)]
#[non_exhaustive]
pub enum EssaResult {
    /// Finished successfully, equivalent to `Ok(())`.
    Ok = 0,
    /// Unspecified error.
    UnknownError = -1,
    /// The requested external function is not known to the runtime.
    NoSuchFunction = -2,
    /// A supplied memory buffer was not large enough.
    BufferTooSmall = -3,
    /// The requested value was not found, e.g. in the KVS.
    NotFound = -4,
    /// An invoked external function has an invalid signature.
    InvalidFunctionSignature = -5,
    /// A WASM function returned without a prior call to [`set_result`].
    NoResult = -6,
    /// Failed to deserialize the result of a called WASM function.
    InvalidResult = -8,
}

impl std::fmt::Display for EssaResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Ok => "finished successfully",
            Self::UnknownError => "unspecified error",
            Self::NoSuchFunction => "the requested external function is not known to the runtime",
            Self::BufferTooSmall => "a supplied memory buffer was not large enough",
            Self::NotFound => "the requested value was not found, e.g. in the KVS",
            Self::InvalidFunctionSignature => {
                "an invoked external function has an invalid signature"
            }
            Self::NoResult => "a WASM function returned without a prior call to `set_result`",
            Self::InvalidResult => "failed to deserialize the result of a called WASM function",
        };
        f.write_str(s)
    }
}

impl std::error::Error for EssaResult {}
