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
    /// The Default::default() function body
    default_attr: Option<proc_macro2::TokenStream>,
    /// The default value for simple types
    default_value: Option<String>,
    /// A "key" to set values from HashMap, etc.
    key_const: Option<String>,
    /// A level of importance for a field
    importance_value: Option<String>,
    /// A reference to a &'static str containing the documentation for the field
    doc_const: Option<String>,
    /// A field may require a call to its validator fn validator_<field> -> Result<...>
    with_validator_fn: bool,
    /// A field may require a call to a default fn default_<field> -> Result<...>
    with_default_fn: bool,
    /// A field may require special code to handle its internal value, not just a build() on
    /// ConfigDef but maybe inspect other fields for context/deriving
    with_default_resolver: bool,
    /// The caller may decide not to use a default resolver. The caller must call the validator if
    /// any. This may happen when we read for example a ConfigDef<String> and build a
    /// Vec<Something> and the default builder cannot know the Vec<Something>
    with_default_builder: bool,
    /// A field's internal type, i.e. i64 in ConfigDef<i64>
    internal_field_ty: String,
}

impl Default for ConfigDefFieldAttrs {
    fn default() -> Self {
        Self {
            default_attr: None,
            default_value: None,
            with_default_fn: false,
            key_const: None,
            importance_value: None,
            doc_const: None,
            internal_field_ty: String::from("UNSET"),
            with_default_resolver: true,
            with_default_builder: true,
            with_validator_fn: false,
        }
    }
}

impl ConfigDefFieldAttrs {
    fn add_to_default_impl(&mut self, input: proc_macro2::TokenStream) {
        if let Some(ref mut existing_default) = self.default_attr {
            existing_default.extend(input);
        } else {
            self.default_attr = Some(input);
        }
    }

    fn set(&mut self, field_name: &str, current_attr: &str, lit_value: String) {
        // eprintln!(".set({} {}='{}')", field_name, current_attr, lit_value);
        match current_attr.as_ref() {
            "key" => {
                self.key_const = Some(lit_value);
            },
            "default" => {
                self.default_value = Some(remove_if_enclosing_double_quotes(lit_value));
            },
            "importance" => {
                self.importance_value = Some(remove_if_enclosing_double_quotes(lit_value));
            },
            "doc" => {
                self.doc_const = Some(remove_if_enclosing_double_quotes(lit_value));
            },
            unknown_attr @ _ => {
                panic!("set({}) Unknown attr: {}", field_name, unknown_attr);
            },
        };
    }
}

/// `remove_enclosing_double_quotes` when a field attribute is a string it's enclosed in "" And when
/// read from the attributes it looks like "\"<value>\"", this removes one layer of ""
fn remove_if_enclosing_double_quotes(input: String) -> String {
    if input.starts_with('"') && input.ends_with('"') {
        let (_double_quote_start, partial_string_value) = input.split_at(1);
        let (string_value, _double_quote_end) =
            partial_string_value.split_at(partial_string_value.len() - 1);
        string_value.to_string()
    } else {
        input
    }
}

/// `attr_parser` Checks the fields' ConfigDef setup. A ConfigDef internal type must impl FromStr
/// as required by the Trait Bounds.
/// NOTE: We may support ConfigDef<OneMoreGeneric<internal_type>> (Two layers) but currently this
/// process only iterates through the first layer. Maybe some recursion can do this in the future
/// but not needed right now...
fn attr_parser(field_ref: &syn::Field, name: &proc_macro2::Ident) -> ConfigDefFieldAttrs {
    let mut res = ConfigDefFieldAttrs::default();
    let mut is_config_def_segment = false;
    let mut internal_field_ty: Option<syn::Ident> = None;
    let field_ty = &field_ref.ty;
    let field_ref_name = field_ref.ident.clone().unwrap().to_string();
    if let syn::Type::Path(type_path) = field_ty {
        if type_path.path.segments.first().unwrap().ident.to_string() != "ConfigDef" {
            panic!(
                "Field {} must be enclosed by a ConfigDef as in 'pub some_field: ConfigDef<T>'",
                field_ref_name
            );
        }
        if let syn::PathArguments::AngleBracketed(ref bracketed) =
            type_path.path.segments.first().unwrap().arguments
        {
            // eprintln!("bracketed: {:?}", bracketed);
            if bracketed.args.len() != 1 {
                panic!(
                    "Field {} only one argument expected as in 'pub some_field: ConfigDef<T>'",
                    field_ref_name
                );
            }
            if let syn::GenericArgument::Type(syn::Type::Path(internal_ty)) = &bracketed.args[0] {
                internal_field_ty = Some(internal_ty.path.segments.first().unwrap().ident.clone());
                // eprintln!("first unwrap: {:?}", internal_ty.path.segments.first().unwrap());
                if !internal_ty.path.segments.first().unwrap().arguments.is_empty() {
                    panic!(
                        "Field {} currently not supporting multiple levels inside ConfigDef<T>",
                        field_ref_name
                    );
                }
            } else {
                panic!(
                    "Field {} currently supporting only field structs, not unions/etc",
                    field_ref_name
                );
            }
        }
    } else {
        panic!(
            "field {} expected a struct with field type to be like 'pub some_field: ConfigDef<T>'",
            field_ref_name
        );
    }
    let mut is_assign_operation = false;
    for attr in &field_ref.attrs {
        for segment in attr.path().segments.clone() {
            if segment.ident == "config_def" {
                is_config_def_segment = true;
            }
        }
        if !is_config_def_segment {
            break;
        }
        for token in attr.meta.require_list().unwrap().tokens.clone() {
            if let proc_macro2::TokenTree::Group(group) = token {
                let mut current_attr = String::from("");
                // The value may be a negative number
                let mut has_dash = false;
                for section in group.stream() {
                    match section {
                        proc_macro2::TokenTree::Ident(id) => {
                            if is_assign_operation {
                                res.set(&field_ref_name, current_attr.as_ref(), id.to_string());
                                has_dash = false;
                            } else {
                                // But if there was no assignment, the ident is a new attribute
                                // key
                                current_attr = id.to_string();
                                match current_attr.as_ref() {
                                    "key" | "default" | "default_fn" | "importance" | "doc" => {},
                                    "with_validator_fn" => res.with_validator_fn = true,
                                    "with_default_fn" => res.with_default_fn = true,
                                    "no_default_resolver" => res.with_default_resolver = false,
                                    "no_default_builder" => res.with_default_builder = false,
                                    unknown_attr => {
                                        panic!("section Unknown attr: {}", unknown_attr);
                                    },
                                };
                            }
                        },
                        proc_macro2::TokenTree::Punct(punct) => {
                            if punct.as_char() == '=' {
                                is_assign_operation = true;
                            } else if punct.as_char() == ',' {
                                is_assign_operation = false;
                                has_dash = false;
                            } else if punct.as_char() == '-' {
                                has_dash = true;
                            } else {
                                panic!(
                                    "Only supporting assignment as in: \"attr '=' value,\" but \
                                     found punct: '{}'",
                                    punct.as_char()
                                );
                            }
                        },
                        proc_macro2::TokenTree::Literal(lit) => {
                            let mut lit_value = lit.to_string();
                            if has_dash {
                                lit_value = format!("-{}", lit_value);
                            }
                            res.set(&field_ref_name, current_attr.as_ref(), lit_value);
                        },
                        _ => {},
                    }
                }
            }
        }
    }
    let internal_field_ty = internal_field_ty.unwrap();
    res.internal_field_ty = internal_field_ty.to_string();
    if let Some(ref key_const) = res.key_const {
        let key_ident = syn::Ident::new(&key_const, name.span());
        let field_name = field_ref.ident.clone();
        res.default_attr = Some(quote! {
            #field_name : ConfigDef::default()
                .with_key(#key_ident)
        });
    } else {
        panic!("field {} Missing key=\"some.path\" as field attribute.", field_ref_name);
    }
    if let Some(ref doc_const) = res.doc_const {
        let doc_ident = syn::Ident::new(&doc_const, name.span());
        res.add_to_default_impl(quote! {
                .with_doc(#doc_ident)
        });
    }
    if let Some(ref default_value) = res.default_value {
        let default_value = default_value.clone();
        res.add_to_default_impl(quote! {
            .with_default(#internal_field_ty::from_str(#default_value).unwrap())
        });
        if res.with_default_fn {
            panic!("field {} Cannot use both default and default_fn attrs.", field_ref_name);
        }
    } else if res.with_default_fn {
        let default_fn_name = syn::Ident::new(&format!("default_{}", field_ref_name), name.span());
        res.add_to_default_impl(quote! {
            .with_default(#name::#default_fn_name())
        });
    }
    if let Some(ref importance_attr) = res.importance_value {
        let importance_ident = syn::Ident::new(&format!("{}", importance_attr), name.span());
        res.add_to_default_impl(quote! {
            .with_importance(ConfigDefImportance::#importance_ident)
        });
    }
    // Add the final "," separator in the field initialization
    res.add_to_default_impl(quote! {,});
    res
}

#[proc_macro_derive(ConfigDef, attributes(config_def))]
pub fn config_def_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    // eprintln!("{:#?}", ast);
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
    let enum_key_name = syn::Ident::new(&format!("{}Key", name), name.span());
    // The fields built will have defaults for builders such as
    // ConfigDef::default().with_key(SOMETHING_PROP).with_doc(SOMETHING_DOC)
    let mut defaults = vec![];
    // For each field, we create an enum variant, log_dir becomes LogDir
    let mut enum_keys = vec![];
    // The enum impls Display which turn the LogDir => SOMETHING_PROP
    let mut enum_displays = vec![];
    // The enum impls FromStr turning SOMETHING_PROP => LogDir
    let mut from_strs = vec![];
    // When a FromStr matches the Enum, it calls the log_dir.try_set_parsed_value(x,y)
    let mut try_set_parsed_entries = vec![];
    // A field log_dir may have a validator function, i.e. more than X, etc.
    let mut validators = vec![];
    // A field may also need a specialized resolver that turns the type into another type and may
    // use other fields for computations, i.e. turn different HOURS/MINUTES/SECONDS into properties
    // into a unified MILLIS, when a custom resolver is wanted, the field contains an attribute
    // no_default_resolver, when such attribute does not exist, this method is implemented and
    // calls the internal ConfigDef::build(). When the attribute does exist, such method is not
    // created.
    let mut resolvers = vec![];
    // A reference to all the resolvers so that the compiler generates friendly messages on missing
    // methods.
    let mut resolver_refs = vec![];
    // A builder_<field> for each field that calls both the vallidator (if any) and the resolver be
    // it custom or default.
    let mut builders = vec![];
    let mut all_keys = vec![];
    for field in fields {
        let field_name = field.ident.clone().unwrap();
        let field_attrs = attr_parser(field, name);
        let field_key = field_attrs.key_const.unwrap();
        if all_keys.contains(&field_key) {
            panic!("field {} key {} is repeated", field_name, field_key);
        }
        all_keys.push(remove_if_enclosing_double_quotes(field_key.clone()));
        if let Some(default_attr) = field_attrs.default_attr {
            defaults.push(default_attr);
        }
        let enum_name = syn::Ident::new(
            &format!("{}", field_name.to_string().to_upper_camel_case()),
            name.span(),
        );
        enum_keys.push(quote! {
            #enum_name,
        });
        let prop_name = syn::Ident::new(&field_key, name.span());
        enum_displays.push(quote! {
            Self::#enum_name => write!(f, "{}", #prop_name),
        });
        from_strs.push(quote! {
            #prop_name => Ok(Self::#enum_name),
        });
        try_set_parsed_entries.push(quote! {
            #enum_key_name::#enum_name => self.#field_name.try_set_parsed_value(property_value)?,
        });
        let validator_fn_name =
            syn::Ident::new(&format!("validate_{}", field_name.to_string()), name.span());
        let mut validator_ref_for_builder = vec![];
        if field_attrs.with_validator_fn {
            validators.push(quote! {
                self.#validator_fn_name()?;
            });
            validator_ref_for_builder.push(quote! {
                self.#validator_fn_name()?;
            })
        }
        // If there is a custom resolver, the derived struct must impl resolve_<field>.
        // If there is none, a default resolver is created that simply calls field.build().
        // Such resolver will be invoked by the builder_<field>, which calls the validator and
        // subsequently the default/custom field resolver, this so that we do not rely on the
        // user of the derive trait to remember to call the validator.
        // builder -> resolver & validator. The caller may opt-out of the use of the builder with
        // no_default_builder, which means the caller must call its validate_<field>() if any.
        let internal_field_ty = syn::Ident::new(&field_attrs.internal_field_ty, name.span());
        let builder_fn_name =
            syn::Ident::new(&format!("build_{}", field_name.to_string()), name.span());
        let resolver_fn_name =
            syn::Ident::new(&format!("resolve_{}", field_name.to_string()), name.span());
        if field_attrs.with_default_resolver {
            resolvers.push(quote! {
                pub fn #resolver_fn_name(&mut self) -> Result<#internal_field_ty, KafkaConfigError> {
                    self.#field_name.build()
                }
            });
        }
        if field_attrs.with_default_builder {
            builders.push(quote! {
                pub fn #builder_fn_name(&mut self) -> Result<#internal_field_ty, KafkaConfigError> {
                    #(#validator_ref_for_builder)*
                    self.#resolver_fn_name()
                }
            });
        }
        resolver_refs.push(quote! {
            let _ = self.#resolver_fn_name();
        });
    }
    let all_keys: Vec<syn::Ident> =
        all_keys.iter().map(|x| syn::Ident::new(x, name.span())).collect();
    let expanded = quote! {
        impl #name {
            pub fn validate_values(&self) -> Result<(), KafkaConfigError> {
                #(#validators)*
                Ok(())
            }
            #(#resolvers)*
            #(#builders)*

            #[allow(dead_code)]
            pub fn resolver_references(&mut self) {
                #(#resolver_refs)*
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
        impl TrySetProperty for #name {
            fn config_names() -> Vec<&'static str> {
                vec![
                    #(#all_keys,)*
                ]
            }
            fn try_set_property(
                &mut self,
                property_name: &str,
                property_value: &str,
            ) -> Result<(), KafkaConfigError> {
                let kafka_config_key = #enum_key_name::from_str(property_name)?;
                match kafka_config_key {
                    #(#try_set_parsed_entries)*
                    _ => return Err(KafkaConfigError::UnknownKey(property_name.to_string())),
                };
                Ok(())
            }
        }
    };
    // eprintln!("{}", expanded);
    expanded.into()
}
