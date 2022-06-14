//! Provides the `essa_wrap` macro.
//!
//! This crate should not be used directly. Instead, the reexport at
//! `essa_api::essa_wrap` should be used.

#![warn(missing_docs)]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use std::fmt::Display;
use syn::{Ident, ItemFn, MetaNameValue};

extern crate proc_macro;

/// Makes the annotated function callable as an essa-rs function.
///
/// ## Example
///
/// Add the `essa_wrap` attribute to an arbitrary function to integrate it with essa-rs:
///
/// ```
/// # use essa_macros::essa_wrap;
///
/// #[essa_wrap(name = "to_uppercase_extern")]
/// pub fn to_uppercase(val: String) -> String {
///     val.to_ascii_uppercase()
/// }
/// ```
///
/// This creates a wrapper function named `to_uppercase_extern` that invokes
/// the `to_uppercase` function on a different essa-rs node. The generated
/// `to_uppercase_extern` function has the following signature:
///
/// ```ignore
/// fn to_uppercase_extern(String) -> Result<essa_api::RemoteFunctionResult<String>>;
/// ```
#[proc_macro_attribute]
pub fn essa_wrap(attr: TokenStream, item: TokenStream) -> TokenStream {
    // convert from `TokenStream` to `TokenStream2`, which is used by the
    // `syn` crate
    let attr = TokenStream2::from(attr);
    let item = TokenStream2::from(item);
    // generate the additional wrapper functions
    let generated = essa_wrap_impl(attr, &item).unwrap_or_else(|err| err.to_compile_error());
    // output the original item again, plus the generated functions
    let tokens = quote! {
        #item
        #generated
    };
    // convert the type back from `TokenStream2` to `TokenStream`
    tokens.into()
}

/// Generates the wrapper functions for the annotated function.
fn essa_wrap_impl(attr: TokenStream2, item: &TokenStream2) -> syn::Result<TokenStream2> {
    // parse the arguments given to the `essa_wrap` macro
    let attr_parsed: MetaNameValue = syn::parse2(attr.clone())
        .map_err(|e| syn::Error::new(e.span(), "expected `name = \"...\"` argument"))?;
    // we require a `name` argument
    if attr_parsed
        .path
        .get_ident()
        .map(|i| i.to_string() != "name")
        .unwrap_or(true)
    {
        return Err(err(attr_parsed.path, "expected argument `name`"));
    }

    // parse the annotated item as a function declaration
    let function: ItemFn = syn::parse2(item.clone()).map_err(|_| {
        err(
            attr,
            "the #[essa_wrap] attribute is only supported on functions",
        )
    })?;
    let function_name = function.sig.ident;

    // generate a new identifier for the wrapper function from the `name`
    // argument
    let remote_call_function_name: Ident = {
        let s = match &attr_parsed.lit {
            syn::Lit::Str(s) => s.value(),
            _ => return Err(err(attr_parsed.lit, "expected string")),
        };
        syn::parse_str(&s).map_err(|e| syn::Error::new(attr_parsed.lit.span(), e))?
    };
    // generate the API documentation for the new wrapper function
    let remote_call_function_name_doc = format!(
        "Calls the [`{}`] function asynchronously on a remote essa-rs node.",
        function_name
    );

    // collect all arguments that the annotated function takes
    let mut args = Vec::new();
    let mut args_call = Vec::new();
    for arg in function.sig.inputs {
        let pattern = match arg {
            syn::FnArg::Receiver(r) => {
                return Err(err(r, "argument not supported by #[essa_wrap] attribute"))
            }
            syn::FnArg::Typed(a) => a,
        };
        let ident = match *pattern.pat {
            syn::Pat::Ident(i) => i,
            other => {
                return Err(err(
                    other,
                    "pattern matching in arguments is not supported by #[essa_wrap] macro",
                ))
            }
        };
        let ty = *pattern.ty;
        args.push(quote!(#ident: #ty));
        args_call.push(quote!(#ident));
    }

    // generate a struct to hold all the function arguments
    let arg_struct_name = Ident::new(
        &format!("__essaArgs__{}", function_name),
        function_name.span(),
    );
    let arg_struct = quote! {
        #[allow(non_camel_case_types)]
        #[derive(essa_api::serde::Serialize, essa_api::serde::Deserialize)]
        #[serde(crate = "essa_api::serde")]
        struct #arg_struct_name {
            #(#args),*
        }
    };

    // generate a C-compatible wrapper for the function that uses the
    // `essa_api` functions for getting the arguments and setting the return
    // value
    let wrapper_function_name_str = format!("__essa_wrapper__{}", function_name);
    let wrapper_function_name = Ident::new(&wrapper_function_name_str, function_name.span());
    let wrapper_function = quote! {
        #[doc(hidden)]
        #[no_mangle]
        pub extern "C" fn #wrapper_function_name(args_raw_len: usize) {
            // get the serialized arguments from the essa-rs runtime
            let args_raw = essa_api::get_args_raw(args_raw_len).unwrap();

            // try to deserialize the arguments
            let args: #arg_struct_name = essa_api::bincode::deserialize(&args_raw)
                .expect("failed to deserialize args");

            // invoke the function annotated with the `essa_wrap` macro
            let result_val = #function_name( #(args.#args_call),* );

            // serialize the result and register it with the essa-rs runtime
            let result_serialized = essa_api::bincode::serialize(&result_val)
                .expect("failed to serialize result");
            essa_api::set_result(&result_serialized).unwrap();
        }
    };

    // generate a wrapper to call the annotated function on a remote node
    let func_ret = match function.sig.output {
        syn::ReturnType::Default => quote!(()),
        syn::ReturnType::Type(_, ty) => quote! { #ty },
    };
    let remote_call_wrapper = quote! {
        #[doc = #remote_call_function_name_doc]
        pub fn #remote_call_function_name(#(#args),*)
            -> Result<essa_api::RemoteFunctionResult<#func_ret>, essa_api::EssaResult>
        {
            // initialize the argument struct and serialize it
            let args = #arg_struct_name { #(#args_call),* };
            let args_serialized = essa_api::bincode::serialize(&args)
                .map_err(|_| essa_api::EssaResult::UnknownError)?;
            // call the function on a remote node
            let result_handle =
                essa_api::call_function(#wrapper_function_name_str, &args_serialized)?;
            Ok(essa_api::RemoteFunctionResult::new(result_handle))
        }
    };

    Ok(quote! {
        #arg_struct
        #wrapper_function
        #remote_call_wrapper
    })
}

/// Generate a new `syn::Error` spanned to the given tokens.
fn err(tokens: impl quote::ToTokens, message: impl Display) -> syn::Error {
    syn::Error::new_spanned(tokens, message)
}
