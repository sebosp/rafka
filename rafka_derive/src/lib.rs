/// Kafka Derives
///
/// For now only uses `path`, in the future should impl decode/encode.
extern crate proc_macro;

use proc_macro::{Group, TokenStream, TokenTree};
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// A ZNode handle gets the `path` from a ZNode
fn impl_znode_data_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl ZNodeHandle for #name {
            fn path(&self) -> &str {
                &self.path
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(ZNodeHandle)]
pub fn znode_data_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_znode_data_macro(&ast)
}

/// A ZNode handle gets the `path` from a structure that contains a ZNode
/// Must be a tuple with one item. This should change in the future.
fn impl_subznode_data_macro(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl ZNodeHandle for #name {
            fn path(&self) -> &str {
                &self.0.path()
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(SubZNodeHandle)]
pub fn subznode_data_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_subznode_data_macro(&ast)
}

#[proc_macro_derive(ConfigDef, attributes(config_def))]
pub fn config_def_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    eprintln!("{:#?}", ast);
    let name = &ast.ident;
    let fields = if let syn::Data::Struct(syn::DataStruct {
        fields: syn::Fields::Named(syn::FieldsNamed { ref named, .. }),
        ..
    }) = ast.data
    {
        named
    } else {
        unimplemented!();
    };
    let prop_names = fields.iter().map(|ref field_ref| {
        let mut key_name = String::from("");
        let mut default_value = String::from("");
        let mut is_config_def_segment = false;
        for attr in &field_ref.attrs {
            for segment in attr.path.segments.clone() {
                if segment.ident.to_string() == "config_def" {
                    is_config_def_segment = true;
                }
            }
            if is_config_def_segment {
                for token in attr.tokens.clone() {
                    if let proc_macro2::TokenTree::Group(group) = token {
                        let mut current_id = String::from("");
                        // We only support attr=value right now.
                        let mut is_known_operation = false;
                        for section in group.stream() {
                            match section {
                                proc_macro2::TokenTree::Ident(id) => {
                                    // Start of operation for a token?
                                    is_known_operation = false;
                                    current_id = id.to_string();
                                },
                                proc_macro2::TokenTree::Punct(punct) => {
                                    is_known_operation = punct.as_char() == '=';
                                },
                                proc_macro2::TokenTree::Literal(lit) => {
                                    if is_known_operation {
                                        let lit_value = lit.to_string();
                                        eprintln!("{}='{}'", current_id, lit_value);
                                        match current_id.as_ref() {
                                            "key" => key_name = lit_value,
                                            "default" => default_value = lit_value,
                                            _ => {},
                                        }
                                    }
                                },
                                _ => {},
                            }
                        }
                    }
                }
            }
        }

        let prop_name = syn::Ident::new(
            &format!("{}_PROP", field_ref.ident.clone().unwrap().to_string().to_uppercase()),
            name.span(),
        );
        quote! {
            pub const #prop_name: &str = #key_name;
        }
    });
    let expanded = quote! {
            #(#prop_names;)*
            impl #name {
                pub fn init(&self) {
                    println!("Hello");
                }
            }

    /*        impl Default for #name {
                fn default() -> #name {
                    #name {
                        #(#defaults,)*
                    }
                }
            }*/
        };
    eprintln!("{}", expanded);
    expanded.into()
}
