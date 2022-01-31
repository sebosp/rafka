/// Kafka Derives
///
/// For now only uses `path`, in the future should impl decode/encode.
extern crate proc_macro;

use proc_macro::TokenStream;
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

#[proc_macro_derive(ConfigDef)]
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
    let expanded = quote! {
        impl #name {
            pub fn properties_builder() -> #name {
            }
        }
    };
    expanded.into()
}

#[proc_macro_attribute]
pub fn config_def_attribute_derive(input: TokenStream, other: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    eprintln!("input1: {:#?}", ast);
    let ast = parse_macro_input!(other as DeriveInput);
    eprintln!("other: {:#?}", ast);
    let expanded = quote! {};
    expanded.into()
}
