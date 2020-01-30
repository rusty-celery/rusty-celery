// Adapted from https://github.com/kureuil/batch-rs/blob/master/batch-codegen/src/job.rs.

use proc_macro2::{Span, TokenStream};
use quote::{quote, ToTokens};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Comma;
use syn::visit_mut::VisitMut;
use syn::{parse, FnArg, Token};

use crate::error::Error;

#[derive(Clone)]
struct TaskAttrs {
    attrs: Vec<TaskAttr>,
}

#[derive(Clone)]
enum TaskAttr {
    Name(syn::LitStr),
    Wrapper(syn::Ident),
    Timeout(syn::LitInt),
    MaxRetries(syn::LitInt),
    MinRetryDelay(syn::LitInt),
    MaxRetryDelay(syn::LitInt),
}

#[derive(Clone)]
struct Task {
    errors: Vec<Error>,
    visibility: syn::Visibility,
    name: String,
    wrapper: Option<syn::Ident>,
    timeout: Option<syn::LitInt>,
    max_retries: Option<syn::LitInt>,
    min_retry_delay: Option<syn::LitInt>,
    max_retry_delay: Option<syn::LitInt>,
    original_args: Vec<syn::FnArg>,
    inputs: Option<Punctuated<FnArg, Comma>>,
    inner_block: Option<syn::Block>,
    ret: Option<syn::Type>,
}

impl TaskAttrs {
    fn name(&self) -> Option<String> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::Name(s) => Some(s.value()),
                _ => None,
            })
            .next()
    }

    fn wrapper(&self) -> Option<syn::Ident> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::Wrapper(i) => Some(i.clone()),
                _ => None,
            })
            .next()
    }

    fn timeout(&self) -> Option<syn::LitInt> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::Timeout(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }

    fn max_retries(&self) -> Option<syn::LitInt> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::MaxRetries(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }

    fn min_retry_delay(&self) -> Option<syn::LitInt> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::MinRetryDelay(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }

    fn max_retry_delay(&self) -> Option<syn::LitInt> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::MaxRetryDelay(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }
}

impl parse::Parse for TaskAttrs {
    fn parse(input: parse::ParseStream) -> parse::Result<Self> {
        let attrs: Punctuated<_, Token![,]> = input.parse_terminated(TaskAttr::parse)?;
        Ok(TaskAttrs {
            attrs: attrs.into_iter().collect(),
        })
    }
}

mod kw {
    syn::custom_keyword!(name);
    syn::custom_keyword!(wrapper);
    syn::custom_keyword!(timeout);
    syn::custom_keyword!(max_retries);
    syn::custom_keyword!(min_retry_delay);
    syn::custom_keyword!(max_retry_delay);
}

impl parse::Parse for TaskAttr {
    fn parse(input: parse::ParseStream) -> parse::Result<Self> {
        let lookahead = input.lookahead1();
        if lookahead.peek(kw::name) {
            input.parse::<kw::name>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::Name(input.parse()?))
        } else if lookahead.peek(kw::wrapper) {
            input.parse::<kw::wrapper>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::Wrapper(input.parse()?))
        } else if lookahead.peek(kw::timeout) {
            input.parse::<kw::timeout>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::Timeout(input.parse()?))
        } else if lookahead.peek(kw::max_retries) {
            input.parse::<kw::max_retries>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::MaxRetries(input.parse()?))
        } else if lookahead.peek(kw::min_retry_delay) {
            input.parse::<kw::min_retry_delay>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::MinRetryDelay(input.parse()?))
        } else if lookahead.peek(kw::max_retry_delay) {
            input.parse::<kw::max_retry_delay>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::MaxRetryDelay(input.parse()?))
        } else {
            Err(lookahead.error())
        }
    }
}

impl Task {
    fn new(attrs: TaskAttrs) -> Result<Self, Error> {
        let errors = Vec::new();
        let visibility = syn::Visibility::Inherited;
        let name = match attrs.name() {
            Some(name) => name,
            None => String::from(""),
        };
        let wrapper = attrs.wrapper();
        let timeout = attrs.timeout();
        let max_retries = attrs.max_retries();
        let min_retry_delay = attrs.min_retry_delay();
        let max_retry_delay = attrs.max_retry_delay();
        let original_args = Vec::new();
        let inputs = None;
        let inner_block = None;
        let ret = None;
        Ok(Task {
            errors,
            visibility,
            name,
            wrapper,
            timeout,
            max_retries,
            min_retry_delay,
            max_retry_delay,
            original_args,
            inputs,
            inner_block,
            ret,
        })
    }
}

impl VisitMut for Task {
    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        const ERR_ABI: &str = "functions with non-Rust ABI are not supported";
        let ident = node.ident.clone();

        self.visibility = node.vis.clone();
        if let Some(ref mut it) = node.abi {
            self.errors.push(Error::spanned(ERR_ABI, it.span()));
        };
        if self.name.is_empty() {
            self.name = ident.to_string()
        }
        if self.wrapper.is_none() {
            self.wrapper = Some(ident);
        }
        self.visit_fn_decl_mut(&mut *node.decl);
        self.inner_block = Some((*node.block).clone());
    }

    fn visit_fn_decl_mut(&mut self, node: &mut syn::FnDecl) {
        const ERR_GENERICS: &str = "functions with generic arguments are not supported";
        const ERR_VARIADIC: &str = "functions with variadic arguments are not supported";

        if !node.generics.params.is_empty() {
            self.errors
                .push(Error::spanned(ERR_GENERICS, node.generics.span()));
        }
        self.original_args = node.inputs.clone().into_iter().collect();
        self.inputs = Some(node.inputs.clone());
        if let Some(ref mut it) = node.variadic {
            self.errors.push(Error::spanned(ERR_VARIADIC, it.span()));
        }
        if let syn::ReturnType::Type(_arr, ref ty) = node.output {
            self.ret = Some((**ty).clone());
        }
    }
}

fn args2fields<'a>(args: impl IntoIterator<Item = &'a syn::FnArg>) -> TokenStream {
    args.into_iter()
        .fold(TokenStream::new(), |acc, arg| match arg {
            syn::FnArg::Captured(cap) => {
                let ident = match cap.pat {
                    syn::Pat::Ident(ref pat) => &pat.ident,
                    _ => return acc,
                };
                let ty = &cap.ty;
                quote! {
                    #acc
                    #ident: #ty,
                }
            }
            _ => acc,
        })
}

impl ToTokens for Task {
    fn to_tokens(&self, dst: &mut TokenStream) {
        let krate = quote!(::celery);
        let export = quote!(#krate::export);
        let vis = &self.visibility;
        let wrapper = self.wrapper.as_ref().unwrap();
        let timeout = self.timeout.as_ref().map(|r| {
            quote! {
                fn timeout(&self) -> Option<u32> {
                    Some(#r)
                }
            }
        });
        let max_retries = self.max_retries.as_ref().map(|r| {
            quote! {
                fn max_retries(&self) -> Option<u32> {
                    Some(#r)
                }
            }
        });
        let min_retry_delay = self.min_retry_delay.as_ref().map(|r| {
            quote! {
                fn min_retry_delay(&self) -> Option<u32> {
                    Some(#r)
                }
            }
        });
        let max_retry_delay = self.max_retry_delay.as_ref().map(|r| {
            quote! {
                fn max_retry_delay(&self) -> Option<u32> {
                    Some(#r)
                }
            }
        });
        let task_name = &self.name;
        let arg_names = self
            .original_args
            .iter()
            .fold(TokenStream::new(), |acc, arg| match arg {
                syn::FnArg::Captured(cap) => match cap.pat {
                    syn::Pat::Ident(ref pat) => {
                        let name = &pat.ident.to_string();
                        quote! {
                            #acc
                            #name,
                        }
                    }
                    _ => acc,
                },
                _ => acc,
            });
        let serialized_fields = args2fields(&self.original_args);
        let deserialized_bindings =
            self.original_args
                .iter()
                .fold(TokenStream::new(), |acc, arg| match arg {
                    syn::FnArg::Captured(cap) => match cap.pat {
                        syn::Pat::Ident(ref pat) => {
                            let ident = &pat.ident;
                            quote! {
                                #acc
                                let #ident = self.#ident;
                            }
                        }
                        _ => acc,
                    },
                    _ => acc,
                });
        let inner_block = {
            let block = &self.inner_block;
            quote!(#block)
        };

        let ret_ty = self
            .ret
            .as_ref()
            .map(|ty| quote!(#ty))
            .unwrap_or_else(|| quote!(()));

        let dummy_const = syn::Ident::new(
            &format!("__IMPL_BATCH_JOB_FOR_{}", wrapper.to_string()),
            Span::call_site(),
        );

        let original_args = self.inputs.clone();
        let wrapper_fields =
            self.original_args
                .iter()
                .fold(TokenStream::new(), |acc, arg| match arg {
                    syn::FnArg::Captured(cap) => match cap.pat {
                        syn::Pat::Ident(ref pat) => {
                            let ident = &pat.ident;
                            quote! {
                                #acc
                                #ident,
                            }
                        }
                        _ => acc,
                    },
                    _ => acc,
                });
        let wrapper_struct = quote! {
            #[allow(non_camel_case_types)]
            #[derive(#export::Deserialize, #export::Serialize)]
            #vis struct #wrapper {
                #serialized_fields
            }

            impl #wrapper {
                #vis fn s(#original_args) -> Self {
                    #wrapper {
                        #wrapper_fields
                    }
                }
            }
        };

        let output = quote! {
            #wrapper_struct

            const #dummy_const: () = {
                use #export::async_trait;

                #[async_trait]
                impl #krate::Task for #wrapper {
                    const NAME: &'static str = #task_name;
                    const ARGS: &'static [&'static str] = &[#arg_names];

                    type Returns = #ret_ty;

                    async fn run(mut self) -> Result<Self::Returns, #krate::Error> {
                        #deserialized_bindings
                        Ok(#inner_block)
                    }

                    #timeout

                    #max_retries

                    #min_retry_delay

                    #max_retry_delay
                }
            };
        };
        dst.extend(output);
    }
}

pub(crate) fn impl_macro(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let attrs = syn::parse_macro_input!(args as TaskAttrs);
    let mut item = syn::parse_macro_input!(input as syn::ItemFn);
    let mut task = match Task::new(attrs) {
        Ok(task) => task,
        Err(e) => return quote!(#e).into(),
    };
    task.visit_item_fn_mut(&mut item);
    if !task.errors.is_empty() {
        task.errors
            .iter()
            .fold(TokenStream::new(), |mut acc, err| {
                err.to_tokens(&mut acc);
                acc
            })
            .into()
    } else {
        let output = quote! {
            #task
        };
        output.into()
    }
}
