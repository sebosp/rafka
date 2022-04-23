# Rafka Derive

## ConfigDef

### What
The Kafka code contains a myriad of properties/attributes, these are read from zookeeper/raft, from config-files, from API requests, from CLI.
They interact with each other sometimes and may need to be checked with each other for consistency. For example when a field needs to be computed
from  different time units (i.e. log.roll.ms vs log.roll.hours, which one takes precedence, they may need to be above 0, etc)

They are overloaded to provide a wide variaty of functions to act on a specific type such as:
- Data Types
- Field documentation
- Importance of field (for sorting of HTML-generated documentation)
- Validators
- Defaults
- Config Key names
- etc.
- Provide hashmap-like functions to assign one value from a config-file, etc.

Example:
```scala
ConfigDef()
    .define(LogSegmentBytesProp, INT, Defaults.LogSegmentBytes, atLeast(LegacyRecord.RECORD_OVERHEAD_V0), HIGH, LogSegmentBytesDoc)
```

### Rust ConfigDef derive

#### What

Provides similar functionality to above by using field attributes, an example from the above:

```rust
#[derive(Debug, ConfigDef)]
pub struct DefaultLogConfigProperties {
    #[config_def(
        key = LOG_SEGMENT_BYTES_PROP,
        importance = High,
        doc = LOG_SEGMENT_BYTES_DOC,
        with_default_fn,
        with_validator_fn,
    )]
    pub log_segment_bytes: ConfigDef<usize>,
}
```

#### How

A ConfigDef<T> encloses a T that may be read from a config-file/zookeeper/rust/param
T must impl FromStr to be able to cope with this

The following methods are implemented for it:

##### Default values for fields

A simple `default = true` or `default = ""` may be provided,
such value would be then provided as `T::from("true")` in the first example.

When it is more complex than a simple token the attribute `with_default_fn`
derives a call to a method `default_<field_name> -> T` which provides such default T
For example:
```rust
impl DefaultLogConfigProperties {
    fn default_log_segment_bytes() -> usize {
        1 * 1024 * 1024 * 1024
    }
}
```

TODO: In the future, this could be a `pub const` or an enum may have the defaults provided to make them easier to manage

#### impl Default for
An enum typed `<struct_name>Key` is created with a variant for each field.
Later on this will be used to provide per variant Validator/Default/etc.

Currently the field attributes are used to expand a Default such as:
```rust
impl Default for DefaultLogConfigProperties {
    fn default() -> DefaultLogConfigProperties {
        DefaultLogConfigProperties { 
            log_segment_bytes: ConfigDef::default()
                .with_key(LOG_SEGMENT_BYTES_PROP)
                .with_doc(LOG_SEGMENT_BYTES_DOC)
                .with_default(DefaultLogConfigProperties::default_log_segment_bytes())
                .with_importance(ConfigDefImportance::High)
        }
    }
}
```

##### Validators

A validator is a function that checks the current value of a given property.
The validator should not check consistency with other fields, this should be done later on `resolve` methods.
This is because fields need to be derived individually before they are checked against each other.

The attribute `with_validator_fn` derives a call to a method `validate_<field_name> -> Result<(), KafkaConfigError>`
For example:
```rust
impl DefaultLogConfigProperties {
    fn validate_log_segment_bytes(&self) -> Result<(), KafkaConfigError> {
        self.log_segment_bytes.validate_at_least(legacy_record::RECORD_OVERHEAD_V0)
    }
}
```

Validators are referenced by `build_<field_name>() -> Result<T, KafkaConfigError>` methods.
One may opt-out of such builder by using the attribute `no_default_builder` attribute.
This is needed when the `ConfigDef<T>` must be translated to another type `U`, not yet supported by the deriver.

##### Resolvers

A `resolve_<field_name>` validates a field's value in context with other fields it needs to be consistent with and returns a "final" value.
For example, the keys `"log.dir"` and `"log.dirs"` provide ways to affect the same final attribute.
One of them has precedence over the other, and they must in turn return a potentially different type once such precedence is resolved.

```rust
#[derive(Debug, ConfigDef)]
pub struct DefaultLogConfigProperties {
    #[config_def(
        key = LOG_DIR_PROP,
        importance = High,
        doc = LOG_DIR_DOC,
        default = "/tmp/kafka-logs",
    )]
    // Singular log.dir
    log_dir: ConfigDef<String>,

    // Multiple comma separated log.dirs, may include spaces after the comma (will be trimmed)
    #[config_def(
        key = LOG_DIRS_PROP,
        importance = High,
        doc = LOG_DIRS_DOC,
        no_default_resolver, // The default resolve_<field_name> returns T, but we are returning a Vec<> so we need to opt-out of the default resolver
        no_default_builder,  // As above, the we need to return another type.
    )]
    log_dirs: ConfigDef<String>,
}

impl DefaultLogConfigProperties {
    fn resolve_log_dirs(&mut self) -> Result<Vec<String>, KafkaConfigError> {
        if let Some(log_dirs) = &self.log_dirs.get_value() {
        // compute plural log_dirs for multiple comma-separated directories
        } else if let Some(log_dir) = &self.log_dir.get_value() {
        // use the single-directory property key
        }else{
        // missing key, in case this field didn't have a default value
        }
    }
}
```

##### Builders

A `build_<filed_name>` is provided by default to call a field's validator and its resolver if any.
You may opt-out of the default builder by using the field attribute `no_default_builder` and this is required when the
`ConfigDef<T>` returns another type `U`

### Patterns

#### DOS

Use the derive'd Default first.
This gives a concise, built, coherent and valid `<MyType>Properties` object.
Then call `try_set_property` for any field you want to provide/overwrite.
Afterwards, `<MyType>Properties.build()` will return `<MyType>` validating all the fields and their consistency with each other.

When opting out of the default builder, do not  forget to call the `validate_<fieldname>` for such case.

#### DONTS
