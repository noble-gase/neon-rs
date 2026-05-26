pub mod model;

use syn::{
    Ident, Path, Token, parenthesized,
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
};

/// 解析 #[model (Target(...))] 或 #[model !(Target(...))]
struct PartialAttr {
    target: Ident,
    exclude: bool,
    fields: Vec<Ident>,
    derives: Vec<Path>,
}

impl Parse for PartialAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let target: Ident = input.parse()?;

        let exclude = if input.peek(Token![!]) {
            input.parse::<Token![!]>()?;
            true
        } else {
            false
        };

        // fields
        let content;
        parenthesized!(content in input);
        let list: Punctuated<Ident, Token![,]> = content.parse_terminated(Ident::parse, Token![,])?;
        let fields = list.into_iter().collect();

        // derives
        let mut derives = Vec::new();
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.peek(Ident) {
                let kw: Ident = input.parse()?;
                if kw == "derive" {
                    let derives_content;
                    parenthesized!(derives_content in input);
                    let list: Punctuated<Path, Token![,]> = derives_content.parse_terminated(Path::parse, Token![,])?;
                    derives = list.into_iter().collect();
                } else {
                    return Err(syn::Error::new_spanned(kw, "expected `derive(...)` after ','"));
                }
            }
        }

        Ok(Self {
            target,
            exclude,
            fields,
            derives,
        })
    }
}
