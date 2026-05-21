mod derives;

use proc_macro::TokenStream;

use crate::derives::model;

#[proc_macro_derive(Model, attributes(model))]
pub fn derive_sqlx_model(input: TokenStream) -> TokenStream {
    model::expand_sqlx_model(input.into()).into()
}
