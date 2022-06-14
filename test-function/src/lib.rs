//! Contains some examples for essa-rs functions that can be called remotely.
//!
//! The `essa_wrap` attribute generates a wrapper function with the supplied
//! name to call the attributed function remotely. All functions can still be
//! called locally too, using their normal name.

use essa_api::essa_wrap;

/// Converts the given string to uppercase characters.
///
/// The `essa_wrap` attribute creates a wrapper function called
/// [`to_uppercase_extern`] that runs `to_uppercase` on a remote essa-rs node.
/// The wrapper function returns a [`EssaFuture`](anna-api::EssaFuture), which
/// can be used to retrieve the result asynchronously.
#[essa_wrap(name = "to_uppercase_extern")]
pub fn to_uppercase(val: String) -> String {
    val.to_ascii_uppercase()
}

/// Adds the string `"fooo"` to the given value.
#[essa_wrap(name = "append_foo")]
pub fn foo(val: String) -> String {
    val + " fooo"
}

/// Repeats the given string `count` times.
///
/// Example for a function with multiple arguments.
#[essa_wrap(name = "repeat_string_extern")]
pub fn repeat_string(string: String, count: usize) -> String {
    let mut s = String::new();
    for _ in 0..count {
        s += &string;
    }
    s
}

/// Stress-tests essa-rs and anna-rs through recursive calls and multiple KVS
/// writes.
///
/// The functions splits the given range into two halves and invokes itself
/// recursively for both halves. The recursive calls are done as remote
/// essa-rs calls. As soon as the range size becomes 1, the function writes
/// the set `[x]` to the KVS under the given `key`, where `x` is the single
/// number in that range.
///
/// Set lattices are merged by anna-rs when there are multiple writes. Thus,
/// the set value stored at the specified `key` should contain the full range
/// when this function returns.
#[essa_wrap(name = "concurrent_kvs_test_extern")]
pub fn concurrent_kvs_test(
    key: anna_api::ClientKey,
    range_start: usize,
    range_end: usize,
) -> Result<(), String> {
    let range_size = range_end - range_start;
    if range_size == 1 {
        // write the set `[range_start]` to the KVS under `key`
        //
        // Note that `SetLattice`s are not overwritten on multiple writes to
        // the same key, but merged instead.
        essa_api::kvs_put(
            &key,
            &anna_api::lattice::SetLattice::new([range_start.to_string().into()].into()).into(),
        )
        .unwrap();
    } else {
        // split the range into two halves and do a recursive call on both
        // halves as a remote essa-rs call
        let mid = range_start + range_size / 2;
        let result_1 =
            concurrent_kvs_test_extern(key.clone(), range_start, mid).map_err(|e| e.to_string())?;
        let result_2 =
            concurrent_kvs_test_extern(key.clone(), mid, range_end).map_err(|e| e.to_string())?;

        // wait until the recursive calls are done
        result_1.get().map_err(|e| format!("{:?}", e))??;
        result_2.get().map_err(|e| format!("{:?}", e))??;
    }

    Ok(())
}
