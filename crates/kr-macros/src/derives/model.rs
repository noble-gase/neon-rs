use proc_macro2::TokenStream;
use quote::quote;
use syn::DeriveInput;

use crate::derives::PartialAttr;

pub fn expand_sqlx_model(input: TokenStream) -> TokenStream {
    let input: DeriveInput = match syn::parse2(input) {
        Ok(v) => v,
        Err(e) => return e.to_compile_error(),
    };
    let fields = match &input.data {
        syn::Data::Struct(s) => &s.fields,
        _ => {
            return syn::Error::new_spanned(&input.ident, "Model can only be derived for structs").to_compile_error();
        }
    };

    // 解析所有 #[model(...)]
    let mut generated: Vec<TokenStream> = Vec::new();
    for attr in &input.attrs {
        if attr.path().is_ident("model") {
            match attr.parse_args::<PartialAttr>() {
                Ok(p) => {
                    let target_ident = &p.target;

                    // 根据 include/exclude 模式筛选字段
                    let keep_fields: Vec<_> = fields
                        .iter()
                        .filter(|f| {
                            let ident = f.ident.as_ref().unwrap();
                            if p.exclude {
                                !p.fields.iter().any(|ex| ex == ident)
                            } else {
                                p.fields.iter().any(|ex| ex == ident)
                            }
                        })
                        .collect();

                    // 生成字段定义（保留属性）
                    let gen_fields = keep_fields.iter().map(|f| {
                        let ident = f.ident.as_ref().unwrap();
                        let ty = &f.ty;
                        let attrs = &f.attrs;
                        quote! {
                            #(#attrs)*
                            pub #ident: #ty
                        }
                    });

                    // 合并 derives: 默认(sqlx::FromRow) + 用户自定义
                    let mut derives = Vec::new();
                    derives.push(syn::parse_quote!(sqlx::FromRow));
                    for d in p.derives {
                        derives.push(d);
                    }
                    let derive_attr = quote! {
                        #[derive(#(#derives),*)]
                    };

                    generated.push(quote! {
                        #derive_attr
                        pub struct #target_ident {
                            #(#gen_fields,)*
                        }
                    });
                }
                Err(e) => return e.to_compile_error(),
            }
        }
    }
    quote! { #(#generated)* }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn expand(input: proc_macro2::TokenStream) -> String {
        expand_sqlx_model(input).to_string()
    }

    #[test]
    fn include_mode_keeps_only_listed_fields() {
        let out = expand(quote! {
            #[model(UserDto(id, name))]
            pub struct User {
                pub id: i64,
                pub name: String,
                pub age: i32,
                pub email: String,
            }
        });

        assert!(out.contains("pub struct UserDto"));
        assert!(out.contains("pub id : i64"));
        assert!(out.contains("pub name : String"));
        assert!(!out.contains("pub age"));
        assert!(!out.contains("pub email"));
    }

    #[test]
    fn exclude_mode_skips_listed_fields() {
        let out = expand(quote! {
            #[model(PublicUser !(password))]
            pub struct User {
                pub id: i64,
                pub name: String,
                pub password: String,
            }
        });

        assert!(out.contains("pub struct PublicUser"));
        assert!(out.contains("pub id : i64"));
        assert!(out.contains("pub name : String"));
        assert!(!out.contains("password"));
    }

    #[test]
    fn default_derive_includes_sqlx_fromrow() {
        let out = expand(quote! {
            #[model(Foo(id))]
            pub struct Demo {
                pub id: i64,
            }
        });

        assert!(out.contains("sqlx :: FromRow"));
    }

    #[test]
    fn additional_derives_are_appended() {
        let out = expand(quote! {
            #[model(Foo(id), derive(Debug, Clone))]
            pub struct Demo {
                pub id: i64,
            }
        });

        assert!(out.contains("derive (sqlx :: FromRow , Debug , Clone)"));
    }

    #[test]
    fn multiple_model_attrs_emit_multiple_structs() {
        let out = expand(quote! {
            #[model(Foo(id))]
            #[model(Bar(name))]
            pub struct Demo {
                pub id: i64,
                pub name: String,
            }
        });

        assert!(out.contains("pub struct Foo"));
        assert!(out.contains("pub struct Bar"));
    }

    #[test]
    fn no_model_attrs_produces_empty_output() {
        let out = expand(quote! {
            pub struct Demo {
                pub id: i64,
            }
        });

        assert!(out.trim().is_empty());
    }

    #[test]
    fn enum_input_produces_compile_error() {
        let out = expand(quote! {
            #[model(Foo(id))]
            pub enum Demo {
                A,
                B,
            }
        });

        assert!(out.contains("compile_error"));
        assert!(out.contains("can only be derived for structs"));
    }

    #[test]
    fn invalid_keyword_after_comma_emits_error() {
        let out = expand(quote! {
            #[model(Foo(id), notderive(Debug))]
            pub struct Demo {
                pub id: i64,
            }
        });

        assert!(out.contains("compile_error"));
        assert!(out.contains("expected `derive(...)`"));
    }

    #[test]
    fn field_attributes_are_preserved() {
        let out = expand(quote! {
            #[model(Foo(id))]
            pub struct Demo {
                #[sqlx(rename = "user_id")]
                pub id: i64,
            }
        });

        assert!(out.contains("# [sqlx (rename = \"user_id\")]"));
    }

    #[test]
    fn exclude_mode_with_all_fields_excluded_yields_empty_struct() {
        let out = expand(quote! {
            #[model(Empty !(id, name))]
            pub struct Demo {
                pub id: i64,
                pub name: String,
            }
        });

        assert!(out.contains("pub struct Empty"));
        assert!(!out.contains("pub id"));
        assert!(!out.contains("pub name"));
    }

    #[test]
    fn unparseable_input_emits_compile_error() {
        let out = expand(quote! {
            not a valid derive input ;
        });
        assert!(out.contains("compile_error"));
    }
}
