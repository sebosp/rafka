/// Kafka Derives
///
/// For now only uses `path`, in the future should impl decode/encode.
extern crate proc_macro;

use proc_macro::TokenStream;
// use proc_macro2::Span;
use quote::quote;

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
// TODO: For  later, we could implement something like:
// #[derive(Properties)]
// pub struct segment {
//  #[Property("max.segment.size")]
//  #[PropertyType("storage-size")]
//  #[PropertyDoc("The max segment size, can be given in .mb, .kb suffix units", must be greater
//  than 16 bytes)]
//  #[PropertyValidator("> 16")]
//  #[PropertyDefault("100000")]
//  pub size: u64
// }
// struct Attrs<'a> {
// pub property: Option<Display<'a>>,
// pub doc: Option<Display<'a>>,
// pub importance: Option<Display<'a>>,
// pub default: Option<Display<'a>>,
// pub data_type: Option<Display<'a>>,
// }
//
// #[derive(Clone)]
// struct Display<'a> {
// pub original: &'a Attribute,
// pub fmt: LitStr,
// pub args: TokenStream,
// pub has_bonus_display: bool,
// }
//
// #[derive(Copy, Clone)]
// struct Transparent<'a> {
// pub original: &'a Attribute,
// pub span: Span,
// }
//
// fn get_attrs(input: &[Attribute]) -> Result<Attrs> {
// let mut attrs =
// Attrs { property: None, doc: None, importance: None, default: None, data_type: None };
//
// for attr in input {
// if attr.path.is_ident("error") {
// parse_error_attribute(&mut attrs, attr)?;
// } else if attr.path.is_ident("source") {
// }
// }
// Ok(attrs)
// }
//
// struct Struct<'a> {
// pub original: &'a DeriveInput,
// pub attrs: Attrs<'a>,
// pub ident: Ident,
// pub generics: &'a Generics,
// pub fields: Vec<Field<'a>>,
// }
//
// fn property_from_syn(node: &DeriveInput, data: &DataStruct) -> Result<Struct> {
// let mut attrs = attr_get(&node.attrs)?;
// let span = attrs.span().unwrap_or_else(Span::call_site);
// let fields = Field::multiple_from_syn(&data.fields, span)?;
// if let Some(display) = &mut attrs.display {
// display.expand_shorthand(&fields);
// }
// Ok(Struct {
// original: node,
// attrs,
// ident: node.ident.clone(),
// generics: &node.generics,
// fields,
// })
// }
//
// fn derive_property(node: &DeriveInput) -> Result<TokenStream> {
// let input = match &node.data {
// Data::Struct(data) => property_from_syn(node, data),
// _ => return Err(Error::new_spanned(node, "Only Struct are supported")),
// };
// input.validate()?;
// Ok(match input {
// Input::Struct(input) => impl_struct(input),
// Input::Enum(input) => impl_enum(input),
// })
// }
//
// #[proc_macro_derive(KafkaProperty, attributes(property))]
// pub fn derive_kafka_property(input: TokenStream) -> TokenStream {
// let input = parse_macro_input!(input as DeriveInput);
// derive_property(&input).unwrap_or_else(|err| err.to_compile_error()).into()
// }
