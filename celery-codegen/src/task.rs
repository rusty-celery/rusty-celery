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
    ParamsType(syn::Ident),
    TimeLimit(syn::LitInt),
    HardTimeLimit(syn::LitInt),
    MaxRetries(syn::LitInt),
    MinRetryDelay(syn::LitInt),
    MaxRetryDelay(syn::LitInt),
    ContentType(syn::Ident),
    RetryForUnexpected(syn::LitBool),
    AcksLate(syn::LitBool),
    Bind(syn::LitBool),
    OnFailure(syn::Ident),
    OnSuccess(syn::Ident),
}

#[derive(Clone)]
struct Task {
    errors: Vec<Error>,
    visibility: syn::Visibility,
    name: Option<String>,
    wrapper: Option<syn::Ident>,
    params_type: Option<syn::Ident>,
    time_limit: Option<syn::LitInt>,
    hard_time_limit: Option<syn::LitInt>,
    max_retries: Option<syn::LitInt>,
    min_retry_delay: Option<syn::LitInt>,
    max_retry_delay: Option<syn::LitInt>,
    retry_for_unexpected: Option<syn::LitBool>,
    acks_late: Option<syn::LitBool>,
    content_type: Option<syn::Ident>,
    original_args: Vec<syn::FnArg>,
    inputs: Option<Punctuated<FnArg, Comma>>,
    inner_block: Option<syn::Block>,
    return_type: Option<syn::Type>,
    is_async: bool,
    bind: bool,
    on_failure: Option<syn::Ident>,
    on_success: Option<syn::Ident>,
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

    fn params_type(&self) -> Option<syn::Ident> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::ParamsType(i) => Some(i.clone()),
                _ => None,
            })
            .next()
    }

    fn time_limit(&self) -> Option<syn::LitInt> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::TimeLimit(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }

    fn hard_time_limit(&self) -> Option<syn::LitInt> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::HardTimeLimit(r) => Some(r.clone()),
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

    fn retry_for_unexpected(&self) -> Option<syn::LitBool> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::RetryForUnexpected(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }

    fn acks_late(&self) -> Option<syn::LitBool> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::AcksLate(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }

    fn content_type(&self) -> Option<syn::Ident> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::ContentType(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }

    fn bind(&self) -> Option<syn::LitBool> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::Bind(r) => Some(r.clone()),
                _ => None,
            })
            .next()
    }

    fn on_failure(&self) -> Option<syn::Ident> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::OnFailure(i) => Some(i.clone()),
                _ => None,
            })
            .next()
    }

    fn on_success(&self) -> Option<syn::Ident> {
        self.attrs
            .iter()
            .filter_map(|a| match a {
                TaskAttr::OnSuccess(i) => Some(i.clone()),
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
    syn::custom_keyword!(params_type);
    syn::custom_keyword!(time_limit);
    syn::custom_keyword!(hard_time_limit);
    syn::custom_keyword!(max_retries);
    syn::custom_keyword!(min_retry_delay);
    syn::custom_keyword!(max_retry_delay);
    syn::custom_keyword!(retry_for_unexpected);
    syn::custom_keyword!(acks_late);
    syn::custom_keyword!(content_type);
    syn::custom_keyword!(bind);
    syn::custom_keyword!(on_failure);
    syn::custom_keyword!(on_success);
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
        } else if lookahead.peek(kw::params_type) {
            input.parse::<kw::params_type>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::ParamsType(input.parse()?))
        } else if lookahead.peek(kw::time_limit) {
            input.parse::<kw::time_limit>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::TimeLimit(input.parse()?))
        } else if lookahead.peek(kw::hard_time_limit) {
            input.parse::<kw::hard_time_limit>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::HardTimeLimit(input.parse()?))
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
        } else if lookahead.peek(kw::retry_for_unexpected) {
            input.parse::<kw::retry_for_unexpected>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::RetryForUnexpected(input.parse()?))
        } else if lookahead.peek(kw::acks_late) {
            input.parse::<kw::acks_late>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::AcksLate(input.parse()?))
        } else if lookahead.peek(kw::content_type) {
            input.parse::<kw::content_type>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::ContentType(input.parse()?))
        } else if lookahead.peek(kw::bind) {
            input.parse::<kw::bind>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::Bind(input.parse()?))
        } else if lookahead.peek(kw::on_failure) {
            input.parse::<kw::on_failure>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::OnFailure(input.parse()?))
        } else if lookahead.peek(kw::on_success) {
            input.parse::<kw::on_success>()?;
            input.parse::<Token![=]>()?;
            Ok(TaskAttr::OnSuccess(input.parse()?))
        } else {
            Err(lookahead.error())
        }
    }
}

impl Task {
    fn new(attrs: TaskAttrs) -> Self {
        Task {
            errors: Vec::new(),
            visibility: syn::Visibility::Inherited,
            name: attrs.name(),
            wrapper: attrs.wrapper(),
            params_type: attrs.params_type(),
            time_limit: attrs.time_limit(),
            hard_time_limit: attrs.hard_time_limit(),
            max_retries: attrs.max_retries(),
            min_retry_delay: attrs.min_retry_delay(),
            max_retry_delay: attrs.max_retry_delay(),
            retry_for_unexpected: attrs.retry_for_unexpected(),
            acks_late: attrs.acks_late(),
            content_type: attrs.content_type(),
            original_args: Vec::new(),
            inputs: None,
            inner_block: None,
            return_type: None,
            is_async: false,
            bind: attrs
                .bind()
                .map(|lit_bool| lit_bool.value)
                .unwrap_or_default(),
            on_failure: attrs.on_failure(),
            on_success: attrs.on_success(),
        }
    }
}

impl VisitMut for Task {
    fn visit_item_fn_mut(&mut self, node: &mut syn::ItemFn) {
        const ERR_GENERICS: &str = "functions with generic arguments are not supported";
        const ERR_VARIADIC: &str = "functions with variadic arguments are not supported";
        const ERR_MISSING_SELF: &str = "bound task should have &self as an argument";
        const ERR_ABI: &str = "functions with non-Rust ABI are not supported";

        if let Some(ref mut it) = node.sig.abi {
            self.errors.push(Error::spanned(ERR_ABI, it.span()));
        };

        if !node.sig.generics.params.is_empty() {
            self.errors
                .push(Error::spanned(ERR_GENERICS, node.sig.generics.span()));
        }

        if let Some(ref mut it) = node.sig.variadic {
            self.errors.push(Error::spanned(ERR_VARIADIC, it.span()));
        }

        let ident = node.sig.ident.clone();

        self.visibility = node.vis.clone();
        self.inner_block = Some((*node.block).clone());
        self.is_async = node.sig.asyncness.is_some();
        self.inputs = Some(node.sig.inputs.clone());

        if self.wrapper.is_none() {
            self.wrapper = Some(ident.clone());
        }

        if self.name.is_none() {
            self.name = Some(ident.to_string())
        }

        if self.params_type.is_none() {
            self.params_type = Some(syn::Ident::new(
                &format!("{ident}Params")[..],
                Span::call_site(),
            ));
        }

        self.original_args = node.sig.inputs.clone().into_iter().collect();
        if self.bind && self.original_args.is_empty() {
            self.errors
                .push(Error::spanned(ERR_MISSING_SELF, node.sig.inputs.span()));
        }

        if let syn::ReturnType::Type(_arr, ref ty) = node.sig.output {
            self.return_type = Some((**ty).clone());
        }
    }
}

fn args_to_fields<'a>(
    args: impl IntoIterator<Item = &'a syn::FnArg>,
    skip_first: bool,
) -> TokenStream {
    args.into_iter()
        .skip(usize::from(skip_first))
        .fold(TokenStream::new(), |acc, arg| match arg {
            syn::FnArg::Typed(cap) => {
                let ident = match *cap.pat {
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

fn args_to_arg_names<'a>(
    args: impl IntoIterator<Item = &'a syn::FnArg>,
    skip_first: bool,
) -> TokenStream {
    args.into_iter()
        .skip(usize::from(skip_first))
        .fold(TokenStream::new(), |acc, arg| match arg {
            syn::FnArg::Typed(cap) => match *cap.pat {
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
        })
}

fn args_to_bindings<'a>(args: impl IntoIterator<Item = &'a syn::FnArg>, bind: bool) -> TokenStream {
    args.into_iter()
        .enumerate()
        .fold(TokenStream::new(), |acc, (i, arg)| match arg {
            syn::FnArg::Typed(cap) => match *cap.pat {
                syn::Pat::Ident(ref pat) => {
                    let ident = &pat.ident;
                    if bind && i == 0 {
                        quote! {
                            let #ident = self;
                        }
                    } else {
                        quote! {
                            #acc
                            let #ident = params.#ident;
                        }
                    }
                }
                _ => acc,
            },
            _ => acc,
        })
}

fn args_to_calling_args<'a>(
    args: impl IntoIterator<Item = &'a syn::FnArg>,
    skip_first: bool,
) -> TokenStream {
    args.into_iter()
        .skip(usize::from(skip_first))
        .fold(TokenStream::new(), |acc, arg| match arg {
            syn::FnArg::Typed(cap) => match *cap.pat {
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
        })
}

fn args_to_typed_inputs<'a>(
    args: impl IntoIterator<Item = &'a syn::FnArg>,
    skip_first: bool,
) -> TokenStream {
    args.into_iter()
        .skip(usize::from(skip_first))
        .fold(TokenStream::new(), |acc, arg| match arg {
            syn::FnArg::Typed(cap) => {
                let ident = match *cap.pat {
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
        let params_type = self.params_type.as_ref().unwrap();
        let time_limit = self
            .time_limit
            .as_ref()
            .map(|r| quote! { Some(#r) })
            .unwrap_or_else(|| quote! { None });
        let hard_time_limit = self
            .hard_time_limit
            .as_ref()
            .map(|r| quote! { Some(#r) })
            .unwrap_or_else(|| quote! { None });
        let max_retries = self
            .max_retries
            .as_ref()
            .map(|r| quote! { Some(#r) })
            .unwrap_or_else(|| quote! { None });
        let min_retry_delay = self
            .min_retry_delay
            .as_ref()
            .map(|r| quote! { Some(#r) })
            .unwrap_or_else(|| quote! { None });
        let max_retry_delay = self
            .max_retry_delay
            .as_ref()
            .map(|r| quote! { Some(#r) })
            .unwrap_or_else(|| quote! { None });
        let retry_for_unexpected = self
            .retry_for_unexpected
            .as_ref()
            .map(|r| quote! { Some(#r) })
            .unwrap_or_else(|| quote! { None });
        let acks_late = self
            .acks_late
            .as_ref()
            .map(|r| quote! { Some(#r) })
            .unwrap_or_else(|| quote! { None });
        let content_type = self
            .content_type
            .as_ref()
            .map(|r| quote! { Some(#r) })
            .unwrap_or_else(|| quote! { Some(#krate::protocol::MessageContentType::Json) });
        let task_name = self.name.as_ref().unwrap();
        let arg_names = args_to_arg_names(&self.original_args, self.bind);
        let serialized_fields = args_to_fields(&self.original_args, self.bind);
        let deserialized_bindings = args_to_bindings(&self.original_args, self.bind);
        let inner_block = {
            let block = &self.inner_block;
            quote!(#block)
        };
        let return_type = self
            .return_type
            .as_ref()
            .map(|ty| quote!(#ty))
            .unwrap_or_else(|| quote!(#krate::task::TaskResult<()>));
        let typed_inputs = args_to_typed_inputs(&self.original_args, self.bind);
        let typed_run_inputs = args_to_typed_inputs(&self.original_args, false);
        let params_args = args_to_calling_args(&self.original_args, self.bind);
        let calling_args = args_to_calling_args(&self.original_args, false);

        let wrapper_struct = quote! {
            #[allow(non_camel_case_types)]
            #[derive(Clone)]
            #vis struct #wrapper {
                request: #krate::task::Request<Self>,
                options: #krate::task::TaskOptions,
            }

            impl #wrapper {
                #vis fn new(#typed_inputs) -> #krate::task::Signature<Self> {
                    #krate::task::Signature::<Self>::new(
                        #params_type {
                            #params_args
                        }
                    )
                }
            }
        };

        let run_implementation = if self.return_type.is_none() {
            if self.is_async {
                quote! {
                    impl #wrapper {
                        #[allow(clippy::unnecessary_wraps)]
                        async fn _run(#typed_run_inputs) -> #return_type {
                            Ok(#inner_block)
                        }
                    }
                }
            } else {
                quote! {
                    impl #wrapper {
                        #[allow(clippy::unnecessary_wraps)]
                        fn _run(#typed_run_inputs) -> #return_type {
                            Ok(#inner_block)
                        }
                    }
                }
            }
        } else if self.is_async {
            quote! {
                impl #wrapper {
                    #[allow(clippy::unnecessary_wraps)]
                    async fn _run(#typed_run_inputs) -> #return_type {
                        #inner_block
                    }
                }
            }
        } else {
            quote! {
                impl #wrapper {
                    #[allow(clippy::unnecessary_wraps)]
                    fn _run(#typed_run_inputs) -> #return_type {
                        #inner_block
                    }
                }
            }
        };

        let call_run_implementation = if self.is_async {
            quote! {
                Ok(#wrapper::_run(#calling_args).await?)
            }
        } else {
            quote! {
                Ok(#wrapper::_run(#calling_args)?)
            }
        };

        let call_on_failure = match self.on_failure.as_ref() {
            Some(ident) => quote! {
                #ident(self, err).await
            },
            None => quote! {},
        };

        let call_on_success = match self.on_success.as_ref() {
            Some(ident) => quote! {
                #ident(self, returned).await
            },
            None => quote! {},
        };

        let dummy_const = syn::Ident::new(
            &format!("__IMPL_CELERY_TASK_FOR_{wrapper}"),
            Span::call_site(),
        );

        let output = quote! {
            #wrapper_struct

            #run_implementation

            #[allow(non_camel_case_types)]
            #[derive(Clone, #export::Deserialize, #export::Serialize)]
            #vis struct #params_type {
                #serialized_fields
            }

            const #dummy_const: () = {
                use #export::async_trait;

                #[async_trait]
                impl #krate::task::Task for #wrapper {
                    const NAME: &'static str = #task_name;
                    const ARGS: &'static [&'static str] = &[#arg_names];
                    const DEFAULTS: #krate::task::TaskOptions = #krate::task::TaskOptions {
                        time_limit: #time_limit,
                        hard_time_limit: #hard_time_limit,
                        max_retries: #max_retries,
                        min_retry_delay: #min_retry_delay,
                        max_retry_delay: #max_retry_delay,
                        retry_for_unexpected: #retry_for_unexpected,
                        acks_late: #acks_late,
                        content_type: #content_type,
                    };

                    type Params = #params_type;
                    type Returns = <#return_type as #krate::task::AsTaskResult>::Returns;

                    fn from_request(
                        request: #krate::task::Request<Self>,
                        options: #krate::task::TaskOptions,
                    ) -> Self {
                        Self { request, options }
                    }

                    fn request(&self) -> &#krate::task::Request<Self> {
                        &self.request
                    }

                    fn options(&self) -> &#krate::task::TaskOptions {
                        &self.options
                    }

                    #[allow(unused_variables)]
                    async fn run(&self, params: Self::Params) -> #return_type {
                        #deserialized_bindings
                        #call_run_implementation
                    }

                    #[allow(unused_variables)]
                    async fn on_failure(&self, err: &#krate::error::TaskError) {
                        #call_on_failure
                    }

                    #[allow(unused_variables)]
                    async fn on_success(&self, returned: &Self::Returns) {
                        #call_on_success
                    }
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
    let mut task = Task::new(attrs);
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
