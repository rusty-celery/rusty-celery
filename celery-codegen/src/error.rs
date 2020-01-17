// Adapted from https://github.com/kureuil/batch-rs/blob/master/batch-codegen/src/error.rs.

use proc_macro2::{Delimiter, Group, Ident, Literal, Punct, Spacing, Span, TokenStream, TokenTree};
use quote::ToTokens;

#[derive(Clone)]
pub(crate) struct Error {
    message: String,
    start: Span,
    end: Span,
}

impl Error {
    pub(crate) fn spanned(message: impl Into<String>, span: Span) -> Self {
        Error {
            message: message.into(),
            start: span,
            end: span,
        }
    }
}

impl ToTokens for Error {
    fn to_tokens(&self, dst: &mut TokenStream) {
        fn into_iter(token: impl Into<TokenTree>) -> impl Iterator<Item = TokenTree> {
            let tree = token.into();
            Some(tree).into_iter()
        }
        dst.extend(into_iter(Ident::new("compile_error", self.start)));
        dst.extend(into_iter(Punct::new('!', Spacing::Alone)));
        let mut message = TokenStream::new();
        message.extend(into_iter(Literal::string(&self.message)));
        let mut group = Group::new(Delimiter::Brace, message);
        group.set_span(self.end);
        dst.extend(into_iter(group));
    }
}
