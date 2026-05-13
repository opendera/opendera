use progenitor::{GenerationSettings, InterfaceStyle};
use std::{env, fs, path::Path};

fn type_replacement() -> Vec<(&'static str, &'static str)> {
    vec![
        ("PipelineConfig", "opendera_types::config::PipelineConfig"),
        ("StorageConfig", "opendera_types::config::StorageConfig"),
        ("FtModel", "opendera_types::config::FtModel"),
        (
            "StorageCacheConfig",
            "opendera_types::config::StorageCacheConfig",
        ),
        (
            "StorageBackendConfig",
            "opendera_types::config::StorageBackendConfig",
        ),
        (
            "StorageCompression",
            "opendera_types::config::StorageCompression",
        ),
        ("RuntimeConfig", "opendera_types::config::RuntimeConfig"),
        (
            "InputEndpointConfig",
            "opendera_types::config::InputEndpointConfig",
        ),
        ("ConnectorConfig", "opendera_types::config::ConnectorConfig"),
        (
            "OutputBufferConfig",
            "opendera_types::config::OutputBufferConfig",
        ),
        (
            "OutputEndpointConfig",
            "opendera_types::config::OutputEndpointConfig",
        ),
        ("TransportConfig", "opendera_types::config::TransportConfig"),
        ("FormatConfig", "opendera_types::config::FormatConfig"),
        ("ResourceConfig", "opendera_types::config::ResourceConfig"),
        (
            "FileInputConfig",
            "opendera_types::transport::file::FileInputConfig",
        ),
        (
            "FileOutputConfig",
            "opendera_types::transport::file::FileOutputConfig",
        ),
        (
            "UrlInputConfig",
            "opendera_types::transport::url::UrlInputConfig",
        ),
        (
            "KafkaHeader",
            "opendera_types::transport::kafka::KafkaHeader",
        ),
        (
            "KafkaHeaderValue",
            "opendera_types::transport::kafka::KafkaHeaderValue",
        ),
        (
            "KafkaLogLevel",
            "opendera_types::transport::kafka::KafkaLogLevel",
        ),
        (
            "KafkaInputConfig",
            "opendera_types::transport::kafka::KafkaInputConfig",
        ),
        (
            "KafkaOutputConfig",
            "opendera_types::transport::kafka::KafkaOutputConfig",
        ),
        (
            "KafkaInputFtConfig",
            "opendera_types::transport::kafka::KafkaInputFtConfig",
        ),
        (
            "KafkaOutputFtConfig",
            "opendera_types::transport::kafka::KafkaOutputFtConfig",
        ),
        (
            "ConsumeStrategy",
            "opendera_types::transport::s3::ConsumeStrategy",
        ),
        (
            "ReadStrategy",
            "opendera_types::transport::s3::ReadStrategy",
        ),
        (
            "AwsCredentials",
            "opendera_types::transport::s3::AwsCredentials",
        ),
        (
            "S3InputConfig",
            "opendera_types::transport::s3::S3InputConfig",
        ),
        (
            "DatagenStrategy",
            "opendera_types::transport::datagen::DatagenStrategy",
        ),
        (
            "RngFieldSettings",
            "opendera_types::transport::datagen::RngFieldSettings",
        ),
        (
            "GenerationPlan",
            "opendera_types::transport::datagen::GenerationPlan",
        ),
        (
            "DatagenInputConfig",
            "opendera_types::transport::datagen::DatagenInputConfig",
        ),
        (
            "NexmarkInputConfig",
            "opendera_types::transport::nexmark::NexmarkInputConfig",
        ),
        (
            "NexmarkTable",
            "opendera_types::transport::nexmark::NexmarkTable",
        ),
        (
            "NexmarkInputOptions",
            "opendera_types::transport::nexmark::NexmarkInputOptions",
        ),
        (
            "DeltaTableIngestMode",
            "opendera_types::transport::delta_table::DeltaTableIngestMode",
        ),
        (
            "DeltaTableWriteMode",
            "opendera_types::transport::delta_table::DeltaTableWriteMode",
        ),
        (
            "DeltaTableReaderConfig",
            "opendera_types::transport::delta_table::DeltaTableReaderConfig",
        ),
        (
            "DeltaTableWriterConfig",
            "opendera_types::transport::delta_table::DeltaTableWriterConfig",
        ),
        ("Chunk", "opendera_types::transport::http::Chunk"),
        (
            "JsonUpdateFormat",
            "opendera_types::format::json::JsonUpdateFormat",
        ),
        (
            "ProgramSchema",
            "opendera_types::program_schema::ProgramSchema",
        ),
        ("Relation", "opendera_types::program_schema::Relation"),
        ("SqlType", "opendera_types::program_schema::SqlType"),
        ("Field", "opendera_types::program_schema::Field"),
        ("ColumnType", "opendera_types::program_schema::ColumnType"),
        (
            "IntervalUnit",
            "opendera_types::program_schema::IntervalUnit",
        ),
        (
            "SourcePosition",
            "opendera_types::program_schema::SourcePosition",
        ),
        (
            "PropertyValue",
            "opendera_types::program_schema::PropertyValue",
        ),
        ("ErrorResponse", "opendera_types::error::ErrorResponse"),
        (
            "OutputBufferConfig",
            "opendera_types::config::OutputBufferConfig",
        ),
        (
            "OutputEndpointConfig",
            "opendera_types::config::OutputEndpointConfig",
        ),
        ("FtConfig", "opendera_types::config::FtConfig"),
        (
            "CheckpointResponse",
            "opendera_types::checkpoint::CheckpointResponse",
        ),
        (
            "CheckpointStatus",
            "opendera_types::checkpoint::CheckpointStatus",
        ),
        (
            "CheckpointStatusFailure",
            "opendera_types::checkpoint::CheckpointFailure",
        ),
        (
            "ConsumerConfig",
            "opendera_types::transport::nats::ConsumerConfig",
        ),
        (
            "ConnectOptions",
            "opendera_types::transport::nats::ConnectOptions",
        ),
        (
            "ReplayPolicy",
            "opendera_types::transport::nats::ReplayPolicy",
        ),
        (
            "DeliverPolicy",
            "opendera_types::transport::nats::DeliverPolicy",
        ),
        (
            "Credentials",
            "opendera_types::transport::nats::Credentials",
        ),
        (
            "UserAndPassword",
            "opendera_types::transport::nats::UserAndPassword",
        ),
        ("Auth", "opendera_types::transport::nats::Auth"),
        (
            "NatsInputConfig",
            "opendera_types::transport::nats::NatsInputConfig",
        ),
    ]
}

fn main() {
    let openapi = include_bytes!("openapi.json");
    println!("cargo:rerun-if-changed=../../openapi.json");
    let spec = serde_json::from_reader(&openapi[..]).unwrap();
    let mut settings = GenerationSettings::new();
    settings.with_interface(InterfaceStyle::Builder);
    for (from, to) in type_replacement() {
        let impls = vec![];
        settings.with_replacement(from, to, impls.into_iter());
    }

    let mut generator = progenitor::Generator::new(&settings);

    let tokens = generator.generate_tokens(&spec).unwrap();
    let ast = syn::parse2(tokens).unwrap();
    let mut content = prettyplease::unparse(&ast);
    for verb in ["get", "post", "put", "patch", "delete"] {
        let pattern = format!(".{verb}(url)");
        let replacement = format!(".{verb}(url)\n                .with_sentry_tracing()");
        content = content.replace(&pattern, &replacement);
    }
    content = content.replace(
        "pub mod builder {",
        "pub mod builder {\n    use opendera_observability::ReqwestTracingExt;",
    );
    let content = content.replace(
        "impl Client",
        "#[rustversion::attr(since(1.89), allow(mismatched_lifetime_syntaxes))]\nimpl Client",
    );

    let mut out_file = Path::new(&env::var("OUT_DIR").unwrap()).to_path_buf();
    out_file.push("codegen.rs");

    fs::write(out_file, content).unwrap();
}
