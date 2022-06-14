use crate::{EssaResult, ResultHandle};
use std::marker::PhantomData;

/// An external function result that might not be available yet.
///
/// Used by the `essa-macros` crate.
#[must_use]
pub struct RemoteFunctionResult<T> {
    result_handle: ResultHandle,
    data: PhantomData<T>,
}

impl<T> RemoteFunctionResult<T>
where
    T: for<'a> serde::Deserialize<'a>,
{
    /// Creates a new `RemoteFunctionResult` from the given handle.
    ///
    /// The caller must make sure that the handle is of type `T`, otherwise
    /// a deserialization error might occur on [`get`][Self::get].
    pub fn new(handle: ResultHandle) -> Self {
        Self {
            result_handle: handle,
            data: PhantomData,
        }
    }

    /// Waits until the result becomes available.
    pub fn get(self) -> Result<T, EssaResult> {
        let value = self.result_handle.wait()?;
        let deserialized = bincode::deserialize(&value).map_err(|_| EssaResult::InvalidResult)?;
        Ok(deserialized)
    }
}
