//! 过程宏：为 sqlx 模型生成辅助 derive

mod derives;

use proc_macro::TokenStream;

use crate::derives::model;

/// 为 struct 生成 sqlx model
///
/// 配合 `#[model(...)]` 属性使用，详见 `derives::model`
#[proc_macro_derive(Model, attributes(model))]
pub fn derive_sqlx_model(input: TokenStream) -> TokenStream {
    model::expand_sqlx_model(input.into()).into()
}
