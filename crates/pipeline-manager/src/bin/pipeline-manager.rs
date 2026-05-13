/// A binary that brings up all three of the api-server, compiler and local
/// runner services.
use clap::{Args, Command, FromArgMatches};

use colored::Colorize;
use opendera_observability as observability;
use pipeline_manager::api::main::ApiDoc;
use pipeline_manager::cluster_monitor::{cluster_monitor, LocalResourcesPoller};
use pipeline_manager::compiler::main::{compiler_main, compiler_precompile};
#[cfg(feature = "postgresql_embedded")]
use pipeline_manager::config::PgEmbedConfig;
use pipeline_manager::config::{
    ApiServerConfig, CommonConfig, CompilerConfig, DatabaseConfig, FlyRunnerConfig,
    LocalRunnerConfig, RunnerKind, RunnerSelectionConfig, ServiceMode,
};
use pipeline_manager::db::storage_postgres::StoragePostgres;
use pipeline_manager::events_cleaner::events_cleaner;
use pipeline_manager::runner::fly_runner::FlyRunner;
use pipeline_manager::runner::local_runner::LocalRunner;
use pipeline_manager::runner::main::runner_main;
use pipeline_manager::usage_collector::usage_collector;
use pipeline_manager::{ensure_default_crypto_provider, init_fd_limit, platform_enable_unstable};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;
use utoipa::OpenApi;

fn main() -> anyhow::Result<()> {
    ensure_default_crypto_provider();
    init_fd_limit();
    let _guard = observability::init("https://18aa37ae23e7130b57b91aaad432bc18@o4510219052253184.ingest.us.sentry.io/4510298809827328", "pipeline-manager", env!("CARGO_PKG_VERSION"));
    pipeline_manager::logging::init_service_logging(
        "[manager]".cyan(),
        opendera_observability::json_logging::ServiceName::Manager,
    );
    if let Some(provider) = rustls::crypto::CryptoProvider::get_default() {
        observability::fips::log_rustls_provider_fips_status(
            "startup default provider",
            provider.fips(),
        );
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let cli = Command::new("Pipeline manager CLI");
            let cli = CommonConfig::augment_args(cli);
            #[cfg(feature = "postgresql_embedded")]
            let cli = PgEmbedConfig::augment_args(cli);
            let cli = DatabaseConfig::augment_args(cli);
            let cli = ApiServerConfig::augment_args(cli);
            let cli = CompilerConfig::augment_args(cli);
            let cli = RunnerSelectionConfig::augment_args(cli);
            let cli = LocalRunnerConfig::augment_args(cli);
            let cli = FlyRunnerConfig::augment_args(cli);
            let matches = cli.get_matches();
            let common_config = CommonConfig::from_arg_matches(&matches)
                .map_err(|err| err.exit())
                .unwrap();
            if let Some(features) = &common_config.unstable_features {
                platform_enable_unstable(features);
            }
            #[cfg(feature = "postgresql_embedded")]
            let pg_embed_config = PgEmbedConfig::from_arg_matches(&matches)
                .map_err(|err| err.exit())
                .unwrap();
            let api_config = ApiServerConfig::from_arg_matches(&matches)
                .map_err(|err| err.exit())
                .unwrap();
            if api_config.dump_openapi {
                let openapi_json = ApiDoc::openapi().to_pretty_json()?;
                tokio::fs::write("openapi.json", openapi_json.as_bytes()).await?;
                return Ok(());
            }
            let compiler_config = CompilerConfig::from_arg_matches(&matches)
                .map_err(|err| err.exit())
                .unwrap();
            let runner_selection = RunnerSelectionConfig::from_arg_matches(&matches)
                .map_err(|err| err.exit())
                .unwrap();
            let local_runner_config = LocalRunnerConfig::from_arg_matches(&matches)
                .map_err(|err| err.exit())
                .unwrap();
            let fly_runner_config = FlyRunnerConfig::from_arg_matches(&matches)
                .map_err(|err| err.exit())
                .unwrap();

            let common_config = common_config.canonicalize()?;
            #[cfg(feature = "postgresql_embedded")]
            let pg_embed_config = pg_embed_config.canonicalize()?;
            // `api_config` currently does not have any paths
            let compiler_config = compiler_config.canonicalize()?;
            let local_runner_config = local_runner_config.canonicalize()?;
            // Only validate the Fly config if the operator actually
            // selected the Fly executor; otherwise leave it untouched
            // so Local-mode deployments don't need to set any Fly env
            // vars.
            let fly_runner_config = match runner_selection.runner_kind {
                RunnerKind::Local => fly_runner_config,
                RunnerKind::Fly => fly_runner_config.canonicalize()?,
            };

            if compiler_config.precompile {
                compiler_precompile(common_config, compiler_config).await?;
                info!("Pre-compilation finished");
                return Ok(());
            }
            let database_config = DatabaseConfig::from_arg_matches(&matches)
                .map_err(|err| err.exit())
                .unwrap();
            let db: StoragePostgres = StoragePostgres::connect(
                &database_config,
                #[cfg(feature = "postgresql_embedded")]
                pg_embed_config.clone(),
            )
            .await
            .expect("Could not open connection to database");

            // Run migrations before starting any service
            db.run_migrations().await?;
            let db = Arc::new(Mutex::new(db));

            info!("Starting in service mode: {:?}", common_config.service_mode);

            // Branch on service mode. Three shapes:
            //   Full     — everything in one process (default).
            //   Manager  — API + runner + monitors; no compiler.
            //   Compiler — compiler HTTP server only; foreground.
            match common_config.service_mode {
                ServiceMode::Compiler => {
                    // Compile-only worker: blocks on compiler_main. Skips
                    // API, runner, monitors, usage collector.
                    let worker_id = 0;
                    let total_workers = 1;
                    compiler_main(
                        common_config,
                        compiler_config,
                        db,
                        worker_id,
                        total_workers,
                    )
                    .await
                    .expect("Compiler server main failed");
                    Ok(())
                }
                ServiceMode::Full | ServiceMode::Manager => {
                    if common_config.service_mode == ServiceMode::Full {
                        // Spawn the in-process compiler. In Manager mode this
                        // is delegated to a separate `Compiler`-mode process
                        // reachable at `--compiler-host`.
                        let db_clone = db.clone();
                        let common_config_clone = common_config.clone();
                        let compiler_config_clone = compiler_config.clone();
                        let worker_id = 0;
                        let total_workers = 1;
                        let _compiler = tokio::spawn(async move {
                            compiler_main(
                                common_config_clone,
                                compiler_config_clone,
                                db_clone,
                                worker_id,
                                total_workers,
                            )
                            .await
                            .expect("Compiler server main failed");
                        });
                    }

                    // Spawn the runner selected at startup. Only one executor
                    // runs in a given manager process; the others are inert.
                    let db_clone = db.clone();
                    let common_config_clone = common_config.clone();
                    let _runner = match runner_selection.runner_kind {
                        RunnerKind::Local => {
                            info!("Starting pipeline runner: local");
                            tokio::spawn(async move {
                                runner_main::<LocalRunner>(
                                    db_clone,
                                    common_config_clone,
                                    local_runner_config.clone(),
                                )
                                .await;
                            })
                        }
                        RunnerKind::Fly => {
                            info!(
                                "Starting pipeline runner: fly (org={}, region={})",
                                fly_runner_config.org_slug, fly_runner_config.region
                            );
                            tokio::spawn(async move {
                                runner_main::<FlyRunner>(
                                    db_clone,
                                    common_config_clone,
                                    fly_runner_config.clone(),
                                )
                                .await;
                            })
                        }
                    };

                    // Cluster monitor
                    let common_config_clone = common_config.clone();
                    let db_clone = db.clone();
                    tokio::spawn(async move {
                        cluster_monitor(db_clone, common_config_clone, LocalResourcesPoller {})
                            .await;
                    });

                    // Events cleaner
                    let db_clone = db.clone();
                    let common_config_clone = common_config.clone();
                    tokio::spawn(async move {
                        events_cleaner(db_clone, common_config_clone).await;
                    });

                    // Usage collector: closes one usage bucket per minute
                    // per running pipeline; rows are read by the cloud-side
                    // Stripe metering daemon through `/internal/v0/usage`.
                    let db_clone = db.clone();
                    let common_config_clone = common_config.clone();
                    tokio::spawn(async move {
                        usage_collector(db_clone, common_config_clone).await;
                    });

                    // The api-server blocks forever
                    pipeline_manager::api::main::run(db, common_config, api_config)
                        .await
                        .expect("API server main failed");
                    Ok(())
                }
            }
        })
}
