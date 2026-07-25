use std::{error::Error, time::Duration};

use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RemoteSource {
    RegistryDirect { host: String },
    RegistryP2p,
    OssObject,
}

impl RemoteSource {
    fn as_label(&self) -> &'static str {
        match self {
            Self::RegistryDirect { .. } => "registry_direct",
            Self::RegistryP2p => "registry_p2p",
            Self::OssObject => "oss_object",
        }
    }

    /// Origin registry host, only meaningful for direct registry reads. P2P
    /// reads are served via the local facade and OSS reads target an object
    /// store, so both report `"none"`.
    fn registry_label(&self) -> &str {
        match self {
            Self::RegistryDirect { host } => host,
            Self::RegistryP2p | Self::OssObject => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteReadOperation {
    ReadRange,
    ReadRangeInto,
}

impl RemoteReadOperation {
    fn as_label(self) -> &'static str {
        match self {
            Self::ReadRange => "read_range",
            Self::ReadRangeInto => "read_range_into",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteReadStatus {
    Ok,
    RateLimited,
    Timeout,
    Error,
}

impl RemoteReadStatus {
    fn from_result<T>(result: &Result<T>) -> Self {
        match result {
            Ok(_) => Self::Ok,
            Err(err) => err
                .chain()
                .find_map(classify_remote_read_error)
                .unwrap_or(Self::Error),
        }
    }

    fn from_http_status(status: u16) -> Self {
        match status {
            429 => Self::RateLimited,
            408 | 504 => Self::Timeout,
            _ => Self::Error,
        }
    }

    fn as_label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::RateLimited => "rate_limited",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }
}

pub(crate) fn registry_source(accelerate_address: &str, url: &str) -> RemoteSource {
    if accelerate_address.trim().is_empty() {
        RemoteSource::RegistryDirect {
            host: normalize_host(url),
        }
    } else {
        RemoteSource::RegistryP2p
    }
}

/// Extract a low-cardinality host/domain label from a URL. Strips any scheme,
/// path, query, fragment and userinfo, keeping `host[:port]`. Returns
/// `"unknown"` when nothing usable remains.
fn normalize_host(raw: &str) -> String {
    let without_scheme = raw.split_once("://").map(|(_, rest)| rest).unwrap_or(raw);
    let authority = without_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(without_scheme);
    let host = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority)
        .trim();
    if host.is_empty() {
        "unknown".to_string()
    } else {
        host.to_string()
    }
}

pub(crate) fn record_remote_read<T>(
    source: &RemoteSource,
    operation: RemoteReadOperation,
    result: &Result<T>,
    bytes: impl FnOnce(&T) -> u64,
    elapsed: Duration,
) {
    let status = RemoteReadStatus::from_result(result);
    ::metrics::histogram!(
        "agentenv_overlaybd_remote_read_duration_seconds",
        "source" => source.as_label(),
        "registry" => source.registry_label().to_string(),
        "operation" => operation.as_label(),
        "status" => status.as_label(),
    )
    .record(elapsed.as_secs_f64());
    if let Ok(value) = result {
        ::metrics::counter!(
            "agentenv_overlaybd_remote_read_bytes_total",
            "source" => source.as_label(),
            "registry" => source.registry_label().to_string(),
            "operation" => operation.as_label(),
        )
        .increment(bytes(value));
    }
}

pub(crate) fn record_remote_metadata<T>(
    source: &RemoteSource,
    result: &Result<T>,
    elapsed: Duration,
) {
    let status = RemoteReadStatus::from_result(result);
    ::metrics::histogram!(
        "agentenv_overlaybd_remote_metadata_duration_seconds",
        "source" => source.as_label(),
        "registry" => source.registry_label().to_string(),
        "status" => status.as_label(),
    )
    .record(elapsed.as_secs_f64());
}

fn classify_remote_read_error(cause: &(dyn Error + 'static)) -> Option<RemoteReadStatus> {
    if cause.is::<tokio::time::error::Elapsed>() {
        return Some(RemoteReadStatus::Timeout);
    }

    classify_remote_read_error_from_dependencies(cause)
}

#[cfg(feature = "full")]
fn classify_remote_read_error_from_dependencies(
    cause: &(dyn Error + 'static),
) -> Option<RemoteReadStatus> {
    if let Some(reqwest_error) = cause.downcast_ref::<reqwest::Error>() {
        if reqwest_error.is_timeout() {
            return Some(RemoteReadStatus::Timeout);
        }
        return reqwest_error
            .status()
            .map(|status| RemoteReadStatus::from_http_status(status.as_u16()));
    }

    if let Some(opendal_error) = cause.downcast_ref::<object_store_operator::OpenDalError>() {
        return classify_opendal_error(opendal_error);
    }

    if let Some(object_store_error) =
        cause.downcast_ref::<object_store_operator::ObjectStoreOperatorError>()
    {
        match object_store_error {
            object_store_operator::ObjectStoreOperatorError::OpenDal(opendal_error) => {
                return classify_opendal_error(opendal_error);
            }
            object_store_operator::ObjectStoreOperatorError::CredentialRefresh(_)
            | object_store_operator::ObjectStoreOperatorError::OperatorBuild(_) => {}
        }
    }

    None
}

#[cfg(feature = "full")]
fn classify_opendal_error(error: &object_store_operator::OpenDalError) -> Option<RemoteReadStatus> {
    match error.kind() {
        object_store_operator::OpenDalErrorKind::RateLimited => Some(RemoteReadStatus::RateLimited),
        _ => None,
    }
}

#[cfg(not(feature = "full"))]
fn classify_remote_read_error_from_dependencies(
    _cause: &(dyn Error + 'static),
) -> Option<RemoteReadStatus> {
    None
}

#[cfg(test)]
mod tests {
    use super::{normalize_host, registry_source, RemoteReadStatus, RemoteSource};

    #[test]
    fn remote_read_status_labels_http_statuses() {
        assert_eq!(
            RemoteReadStatus::from_http_status(429).as_label(),
            "rate_limited"
        );
        assert_eq!(
            RemoteReadStatus::from_http_status(408).as_label(),
            "timeout"
        );
        assert_eq!(RemoteReadStatus::from_http_status(500).as_label(), "error");
    }

    #[test]
    fn normalize_host_extracts_authority_from_url() {
        assert_eq!(
            normalize_host("https://registry-1.docker.io/v2/library/ubuntu/blobs/sha256:abc"),
            "registry-1.docker.io"
        );
        assert_eq!(
            normalize_host("http://localhost:5000/v2/foo"),
            "localhost:5000"
        );
        assert_eq!(
            normalize_host("oss-cn-beijing.aliyuncs.com"),
            "oss-cn-beijing.aliyuncs.com"
        );
        assert_eq!(
            normalize_host("https://user:pass@reg.example.com/path"),
            "reg.example.com"
        );
        assert_eq!(normalize_host("   "), "unknown");
        assert_eq!(normalize_host(""), "unknown");
    }

    #[test]
    fn registry_source_selects_variant_and_carries_host() {
        let direct = registry_source("", "https://reg.example.com/v2/blob");
        assert_eq!(direct.as_label(), "registry_direct");
        assert_eq!(direct.registry_label(), "reg.example.com");

        // P2P and OSS reads do not carry a meaningful registry host.
        let p2p = registry_source("http://127.0.0.1:9000", "https://reg.example.com/v2/blob");
        assert_eq!(p2p.as_label(), "registry_p2p");
        assert_eq!(p2p.registry_label(), "none");

        let oss = RemoteSource::OssObject;
        assert_eq!(oss.as_label(), "oss_object");
        assert_eq!(oss.registry_label(), "none");
    }
}
