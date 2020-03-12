#![recursion_limit = "256"]

use proc_macro::TokenStream;

mod error;
mod task;

#[proc_macro_attribute]
pub fn task(args: TokenStream, input: TokenStream) -> TokenStream {
    task::impl_macro(args, input)
}
