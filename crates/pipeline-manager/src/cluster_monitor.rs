use crate::config::CommonConfig;
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::monitor::{MonitorStatus, NewClusterMonitorEvent};
use crate::error::source_error;
use crate::unstable_features;
use async_trait::async_trait;
use opendera_observability::ReqwestTracingExt;
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::{error, info};
use uuid::Uuid;

/// Interval at which the monitor occurs to check the current status.
const MONITOR_INTERVAL: Duration = Duration::from_secs(10);

/// Every N intervals (which are spaced by `MONITOR_INTERVAL`) if the overall status is healthy,
/// does it insert a new row into the database.
const MONITOR_STORE_EVENT_NUM_INTERVALS: u64 = 60;

// The maximum retention duration and number of events.
// Suppose we want to use at most 200 MiB in the database for storing these,
// then we can allow events up to 200 MiB / 1000 ~= 205 KiB.
pub const MONITOR_RETENTION_HOURS: u16 = 72; // 72 hours / 10 minutes = 432 events
pub const MONITOR_RETENTION_NUM: u16 = 1000;

/// The self-provided information by each service is capped to prevent the database row becoming
/// too large. Unicode characters are at most 4 bytes. As such, this should be at most 32 KiB.
/// We have 6 information entries, as such the row should not exceed the 205 KiB maximum we set
/// above.
const INFO_MAXIMUM_NUM_CHARS: usize = 8192;

/// Default HTTP request timeout to use
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Message when the resources information is not available.
const RESOURCES_INFO_NOT_AVAILABLE: &str =
    "Resources information not available in Community edition.";

/// Message when the resources information gathering is not enabled.
const RESOURCES_INFO_NOT_ENABLED: &str = "Resources information is not enabled. \
    Cluster monitoring resources is currently an unstable feature. It can be enabled by \
    setting the control plane environment variable FELDERA_UNSTABLE_FEATURES and adding to it \
    `cluster_monitor_resources` as one of the comma-separated entries.";

/// Target to poll resources of.
pub enum PollResourcesTarget {
    Api,
    Compiler,
    Runner,
}

#[async_trait]
pub trait ResourcesPoller {
    async fn poll_resources(&mut self, target: PollResourcesTarget) -> (bool, String);
}

/// Indefinitely monitor the local cluster by polling the endpoints.
pub async fn cluster_monitor<P: ResourcesPoller>(
    db: Arc<Mutex<StoragePostgres>>,
    common_config: CommonConfig,
    mut resources_poller: P,
) {
    // Cluster monitoring should be enabled if this non-returning function is called
    info!("Cluster monitor is starting");

    // Determine URLs of the services for their self-reporting
    let protocol = if common_config.enable_https {
        "https"
    } else {
        "http"
    };
    let api_url = format!(
        "{protocol}://{}:{}/healthz",
        common_config.api_host, common_config.api_port
    );
    let compiler_url = format!(
        "{protocol}://{}:{}/healthz",
        common_config.compiler_host, common_config.compiler_port
    );
    let runner_url = format!(
        "{protocol}://{}:{}/healthz",
        common_config.runner_host, common_config.runner_port
    );

    // Indefinitely loop checking status
    let client = common_config.reqwest_client().await;
    let mut iterations_without_insert = 0;
    let mut backoff_threshold: u64 = 1;
    let mut first = true;
    loop {
        // Retrieve the latest event
        let latest_event = db
            .lock()
            .await
            .get_latest_cluster_monitor_event_extended()
            .await;
        let latest_event = match latest_event {
            Ok(latest_event) => Some(latest_event),
            Err(e) => {
                if matches!(e, DBError::NoClusterMonitorEventsAvailable) {
                    None
                } else {
                    error!("Cluster monitor cannot perform monitoring because it is unable to retrieve the latest event due to: {e}");
                    tokio::time::sleep(MONITOR_INTERVAL).await;
                    continue;
                }
            }
        };

        // Perform polling for self-reported info. When a service is
        // declared autostop=true (e.g. running on Fly Machines with
        // auto_stop_machines), connection-refused / timeout means the
        // service is intentionally scaled to zero and will wake on the
        // next real request — we report it as Healthy (operational,
        // idle) so the user-facing health page stays green.
        let (api_self_ok, api_self_info) =
            poll_service_health_endpoint("api", &api_url, &client, common_config.api_autostop)
                .await;
        let (compiler_self_ok, compiler_self_info) = poll_service_health_endpoint(
            "compiler",
            &compiler_url,
            &client,
            common_config.compiler_autostop,
        )
        .await;
        let (runner_self_ok, runner_self_info) = poll_service_health_endpoint(
            "runner",
            &runner_url,
            &client,
            common_config.runner_autostop,
        )
        .await;
        let api_self_info = truncate_info(api_self_info);
        let compiler_self_info = truncate_info(compiler_self_info);
        let runner_self_info = truncate_info(runner_self_info);

        // Perform polling of the resources backing the services
        let (
            api_resources_ok,
            compiler_resources_ok,
            runner_resources_ok,
            api_resources_info,
            compiler_resources_info,
            runner_resources_info,
        ) = if unstable_features().is_some_and(|activated_unstable_features| {
            activated_unstable_features.contains("cluster_monitor_resources")
        }) {
            let (api_resources_ok, api_resources_info) = resources_poller
                .poll_resources(PollResourcesTarget::Api)
                .await;
            let (compiler_resources_ok, compiler_resources_info) = resources_poller
                .poll_resources(PollResourcesTarget::Compiler)
                .await;
            let (runner_resources_ok, runner_resources_info) = resources_poller
                .poll_resources(PollResourcesTarget::Runner)
                .await;
            (
                api_resources_ok,
                compiler_resources_ok,
                runner_resources_ok,
                truncate_info(api_resources_info),
                truncate_info(compiler_resources_info),
                truncate_info(runner_resources_info),
            )
        } else {
            (
                true,
                true,
                true,
                RESOURCES_INFO_NOT_ENABLED.to_string(),
                RESOURCES_INFO_NOT_ENABLED.to_string(),
                RESOURCES_INFO_NOT_ENABLED.to_string(),
            )
        };

        // Whether to insert the event into the database
        let insert_into_database = match &latest_event {
            Some(latest_event) => {
                let latest_healthy = latest_event.api_status == MonitorStatus::Healthy
                    && latest_event.compiler_status == MonitorStatus::Healthy
                    && latest_event.runner_status == MonitorStatus::Healthy;

                let new_healthy = api_self_ok
                    && api_resources_ok
                    && compiler_self_ok
                    && compiler_resources_ok
                    && runner_self_ok
                    && runner_resources_ok;
                if first {
                    first = false;
                    true
                } else if latest_healthy && new_healthy {
                    // Remains healthy, as such just a regular update
                    backoff_threshold = 1;
                    iterations_without_insert >= MONITOR_STORE_EVENT_NUM_INTERVALS - 1
                } else if !latest_healthy && !new_healthy {
                    // Remains unhealthy, in which case we will update with exponential backoff
                    if iterations_without_insert >= backoff_threshold - 1 {
                        backoff_threshold =
                            std::cmp::min(backoff_threshold * 2, MONITOR_STORE_EVENT_NUM_INTERVALS);
                        true
                    } else {
                        false
                    }
                } else {
                    // Changes are always inserted
                    backoff_threshold = 1;
                    true
                }
            }
            None => true,
        };

        // Only insert into the database if required
        if insert_into_database {
            iterations_without_insert = 0;

            // Insert new event
            let stored = if let Err(e) = db
                .lock()
                .await
                .new_cluster_monitor_event(
                    Uuid::now_v7(),
                    NewClusterMonitorEvent {
                        api_status: poll_success_to_status(
                            latest_event.as_ref().map(|v| v.api_status),
                            api_self_ok && api_resources_ok,
                        ),
                        api_self_info,
                        api_resources_info,
                        compiler_status: poll_success_to_status(
                            latest_event.as_ref().map(|v| v.compiler_status),
                            compiler_self_ok && compiler_resources_ok,
                        ),
                        compiler_self_info,
                        compiler_resources_info,
                        runner_status: poll_success_to_status(
                            latest_event.as_ref().map(|v| v.runner_status),
                            runner_self_ok && runner_resources_ok,
                        ),
                        runner_self_info,
                        runner_resources_info,
                    },
                )
                .await
            {
                error!("Cluster monitor is unable to store event due to: {e}");
                false
            } else {
                true
            };

            // Clean up events that no longer need to be retained
            if stored {
                if let Err(e) = db
                    .lock()
                    .await
                    .delete_cluster_monitor_events_beyond_retention(
                        MONITOR_RETENTION_HOURS,
                        MONITOR_RETENTION_NUM,
                    )
                    .await
                {
                    error!("Cluster monitor is unable to clean up based on retention due to: {e}");
                }
            }
        } else {
            iterations_without_insert += 1;
        }

        // Sleep till next monitor attempt
        tokio::time::sleep(MONITOR_INTERVAL).await;
    }
}

/// Truncate the information message to a maximum number of Unicode characters.
fn truncate_info(mut info: String) -> String {
    if info.chars().count() > INFO_MAXIMUM_NUM_CHARS {
        info = info.chars().take(INFO_MAXIMUM_NUM_CHARS).collect();
        info.push_str(" (truncated due to exceeding maximum number of characters)");
    }
    info
}

/// Polls the service health endpoint, which is used as the service reporting its own status
/// information.
///
/// When `autostop` is true the probe is skipped entirely and the
/// service is reported as Healthy without making an HTTP request.
/// Background: on platforms like Fly.io, dialling an autostopped
/// service still wakes it through the proxy's auto-start path —
/// remapping the probe's failure interpretation to "Healthy" is not
/// enough, because by the time the probe response arrives the
/// underlying machine is already running, and the next probe a few
/// seconds later finds it warm. With `MONITOR_INTERVAL = 10s` and
/// any sane Fly idle timeout, the machine never gets back to sleep.
/// Treating autostop=true as "we don't probe — wake-on-demand is the
/// contract" preserves the user-visible status (the service is
/// either running and serving, or asleep and instantly wakable) and
/// stops the cluster monitor from accidentally driving the bill up.
///
/// When `autostop` is false the probe behaves as before. Non-2xx
/// HTTP responses and generic errors still report Unhealthy.
async fn poll_service_health_endpoint(
    service_name: &str,
    url: &str,
    client: &reqwest::Client,
    autostop: bool,
) -> (bool, String) {
    if autostop {
        return (
            true,
            format!(
                "Operational: {service_name} at {url} is configured for autostop on idle; \
                 health probes are skipped so the proxy's wake-on-connect path doesn't \
                 keep the service warm. Status reverts to live probing as soon as a \
                 real request lands."
            ),
        );
    }
    match client
        .get(url)
        .timeout(DEFAULT_REQUEST_TIMEOUT)
        .with_sentry_tracing()
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => (
            true,
            format!("Healthy: The {service_name} service responded successfully to the last health check."),
        ),
        Ok(resp) => {
            let status = resp.status();
            let message = resp.json::<serde_json::Value>().await.map_or_else(
                |_| format!(
                    "Unhealthy: {service_name} at {url} responded with HTTP {status} and an invalid JSON body. \
                     Please check the {service_name} logs for error details."
                ),
                |v| format!(
                    "Unhealthy: {service_name} at {url} responded with HTTP {status} and body: {v}. \
                     Please check the {service_name} logs for more information.",
                ),
            );
            (false, message)
        }
        Err(e) if e.is_connect() => (
            false,
            format!(
                "Unreachable: Unable to connect to the {service_name} at {url}. This likely means the service \
                 is not running, has crashed, or is not listening on the expected port. Underlying connection error: {}. \
                 Please ensure that the {service_name} is running and check its logs for details.",
                source_error(&e)
            ),
        ),
        Err(e) if e.is_timeout() => (
            false,
            format!(
                "Timeout: The health check request to {service_name} at {url} did not respond within {} seconds. \
                 This usually means the service is running, but it is overloaded, unresponsive, or stuck processing. \
                 Please check the {service_name} logs for any errors or performance issues. Timeout error: {}.",
                DEFAULT_REQUEST_TIMEOUT.as_secs(),
                source_error(&e)
            ),
        ),
        Err(e) => (
            false,
            format!(
                "Error: An unexpected error occurred while checking the health of {service_name} at {url}: {e}, \
                 source: {}. Please check the {service_name} logs for more information.",
                source_error(&e)
            ),

        ),
    }
}

#[cfg(test)]
mod poll_service_health_endpoint_tests {
    use super::poll_service_health_endpoint;
    use reqwest::Client;

    /// With `autostop = true` the probe must not make any network call.
    /// We give it a URL that would be expensive (a non-routable test
    /// address that would otherwise take the full `DEFAULT_REQUEST_TIMEOUT`
    /// to fail) and assert the call returns immediately with Healthy.
    /// This is the regression test for the bug where the engine's 10s
    /// probe interval was keeping a Fly-autostopped machine warm via
    /// the proxy's wake-on-connect path.
    #[tokio::test]
    async fn autostop_true_skips_dial_and_reports_healthy() {
        let client = Client::new();
        // RFC 5737 TEST-NET-1; routes nowhere. If the function tried
        // to actually probe this URL, the call would block until the
        // request timeout — well over 10ms.
        let url = "http://192.0.2.1:9999/healthz";
        let start = std::time::Instant::now();
        let (ok, info) =
            poll_service_health_endpoint("compiler", url, &client, /* autostop */ true).await;
        let elapsed = start.elapsed();

        assert!(ok, "autostop=true must report Healthy");
        assert!(
            info.starts_with("Operational:"),
            "autostop=true should produce an Operational status, got: {info}"
        );
        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "probe must return without dialling (took {elapsed:?})"
        );
    }
}

/// Combines the poll outcome with the previous status to return the new monitor status.
/// If the monitor status was previously `InitialUnhealthy`, it only transitions from that
/// upon a successful poll.
fn poll_success_to_status(previous_status: Option<MonitorStatus>, success: bool) -> MonitorStatus {
    if let Some(previous_status) = previous_status {
        if previous_status == MonitorStatus::InitialUnhealthy && !success {
            MonitorStatus::InitialUnhealthy
        } else if success {
            MonitorStatus::Healthy
        } else {
            MonitorStatus::Unhealthy
        }
    } else if success {
        MonitorStatus::Healthy
    } else {
        MonitorStatus::InitialUnhealthy
    }
}

/// Poller for local resources.
pub struct LocalResourcesPoller {}

#[async_trait]
impl ResourcesPoller for LocalResourcesPoller {
    /// The local resources cannot be polled, as such it returns a default message indicating as such.
    async fn poll_resources(&mut self, _target: PollResourcesTarget) -> (bool, String) {
        (true, RESOURCES_INFO_NOT_AVAILABLE.to_string())
    }
}
