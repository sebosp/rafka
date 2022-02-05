/// Kafka Derives
///
/// For now only uses `path`, in the future should impl decode/encode.
extern crate proc_macro;

use heck::ToUpperCamelCase;
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

struct ConfigDefFieldAttrs {
    default_value: Option<proc_macro2::TokenStream>,
    config_key: Option<proc_macro2::TokenStream>,
}

/// `remove_enclosing_double_quotes` when a field attribute is a string it's enclosed in ""
/// And when read from the attributes it looks like "\"<value>\"", this removes one layer of ""
fn remove_enclosing_double_quotes(input: &str) -> String {
    let (_double_quote_start, partial_string_value) = input.split_at(1);
    let (string_value, _double_quote_end) =
        partial_string_value.split_at(partial_string_value.len() - 1);
    string_value.to_string()
}

/// `attr_parser` Checks the fields' ConfigDef setup. A ConfigDef internal type must impl FromStr
/// as required by the Trait Bounds.
/// NOTE: We may support ConfigDef<OneMoreGeneric<internal_type>> (Two layers) but currently this
/// process only iterates through the first layer. Maybe some recursion can do this in the future
/// but not needed right now...
fn attr_parser(field_ref: &syn::Field, name: &proc_macro2::Ident) -> ConfigDefFieldAttrs {
    let mut res = ConfigDefFieldAttrs { default_value: None, config_key: None };
    let mut config_key: Option<String> = None;
    let mut is_config_def_segment = false;
    let mut default_value: Option<String> = None;
    let mut internal_field_ty = String::from("");
    let field_ty = &field_ref.ty;
    if let syn::Type::Path(type_path) = field_ty {
        if type_path.path.segments.first().unwrap().ident.to_string() != "ConfigDef" {
            panic!(
                "Field {} must be enclosed by a ConfigDef as in 'pub some_field: ConfigDef<T>'",
                field_ref.ident.clone().unwrap().to_string()
            );
        }
        if let syn::PathArguments::AngleBracketed(ref bracketed) =
            type_path.path.segments.first().unwrap().arguments
        {
            eprintln!("bracketed: {:?}", bracketed);
            if bracketed.args.len() != 1 {
                panic!(
                    "Field {} only one argument expected as in 'pub some_field: ConfigDef<T>'",
                    field_ref.ident.clone().unwrap().to_string()
                );
            }
            if let syn::GenericArgument::Type(internal_ty) = &bracketed.args[0] {
                if let syn::Type::Path(type_path) = internal_ty {
                    internal_field_ty = type_path.path.segments.first().unwrap().ident.to_string();
                }
                if !type_path.path.segments.first().unwrap().arguments.is_empty() {
                    panic!(
                        "Field {} currently not supporting multiple levels inside ConfigDef<T>",
                        field_ref.ident.clone().unwrap().to_string()
                    );
                }
            } else {
                panic!(
                    "Field {} currently supporting only field structs, not unions/etc",
                    field_ref.ident.clone().unwrap().to_string()
                );
            }
        }
    } else {
        panic!("Expected a struct with field type to be like 'pub some_field: ConfigDef<T>'");
    }
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
                                    // match lit {
                                    // Integer(val) => default_value_maybe_integer = val,
                                    // };
                                    let lit_value = lit.to_string();
                                    eprintln!("{}='{}'", current_id, lit_value);
                                    match current_id.as_ref() {
                                        "key" => {
                                            if !lit_value.starts_with('"')
                                                || !lit_value.ends_with('"')
                                            {
                                                panic!(
                                                    "Field {} \"key\" attribute must be enclosed \
                                                     in double quotes.",
                                                    field_ref.ident.clone().unwrap().to_string()
                                                );
                                            } else {
                                                config_key = Some(remove_enclosing_double_quotes(
                                                    &lit_value,
                                                ));
                                            }
                                        },
                                        "default" => {
                                            if lit_value.starts_with('"')
                                                && lit_value.ends_with('"')
                                            {
                                                default_value = Some(
                                                    remove_enclosing_double_quotes(&lit_value),
                                                );
                                            } else {
                                                default_value = Some(lit_value);
                                            }
                                        },
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
    if let Some(config_key) = config_key {
        res.config_key = Some(quote! {
            pub const #prop_name: &str = #config_key
        });
    }
    if let Some(default_value) = default_value {
        let field_name = field_ref.ident.clone();
        res.default_value = Some(quote! {
            #field_name : ConfigDef::default()
                .with_key(#prop_name)
                .with_default(#internal_field_ty::from_str(#default_value).unwrap()),
        });
    }
    res
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
    let mut prop_names = vec![];
    let mut defaults = vec![];
    let mut enum_keys = vec![];
    let mut enum_displays = vec![];
    let mut from_strs = vec![];
    for field in fields {
        let field_attrs = attr_parser(field, name);
        if let Some(config_key) = field_attrs.config_key {
            prop_names.push(config_key);
        }
        if let Some(default_value) = field_attrs.default_value {
            defaults.push(default_value);
        }
        let enum_name = syn::Ident::new(
            &format!("{}", field.ident.clone().unwrap().to_string().to_upper_camel_case()),
            name.span(),
        );
        enum_keys.push(quote! {
            #enum_name,
        });
        let prop_name = syn::Ident::new(
            &format!("{}_PROP", field.ident.clone().unwrap().to_string().to_uppercase()),
            name.span(),
        );
        enum_displays.push(quote! {
            Self::#enum_name => write!(f, "{}", #prop_name),
        });
        from_strs.push(quote! {
            #prop_name => Ok(Self::#enum_name),
        });
    }
    let enum_key_name = syn::Ident::new(&format!("{}Key", name), name.span());
    let expanded = quote! {
            #(#prop_names;)*
            impl #name {
                pub fn init(&self) {
                    println!("Hello");
                }
            }

            impl Default for #name {
                fn default() -> #name {
                    #name {
                        #(#defaults)*
                    }
                }
            }

            #[derive(std::fmt::Debug, enum_iterator::IntoEnumIterator)]
            pub enum #enum_key_name {
                #(#enum_keys)*
            }

            impl std::fmt::Display for #enum_key_name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match self {
                        #(#enum_displays)*
                    }
                }
            }

    impl core::str::FromStr for #enum_key_name {
        type Err = KafkaConfigError;

        fn from_str(input: &str) -> Result<Self, Self::Err> {
            match input {
                #(#from_strs)*
                _ => Err(KafkaConfigError::UnknownKey(input.to_string())),
            }
        }
    }

        };
    eprintln!("{}", expanded);
    expanded.into()
}
