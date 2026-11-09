use anyhow::{anyhow, Result};
use std::sync::Arc;
use arrow::{
    array::{Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use serde::{Deserialize, Serialize};

// Define a struct that matches the Akamai log structure
// Field names are the DataStream 2 JSON keys VERBATIM. Do not add
// serde(rename_all): camelCase cannot produce embedded acronyms, so it
// silently dropped UA, cliIP, edgeIP and edgeIPBinding for four months.
// The struct field name, the JSON key and the Parquet column are one name.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct Datastream2Log {
    version: Option<String>,
    isMidgress: Option<String>,
    streamId: Option<String>,
    cp: Option<String>,
    reqId: Option<String>,
    reqTimeSec: Option<String>,
    edgeAttempts: Option<String>,
    bytes: Option<String>,
    cliIP: Option<String>,
    statusCode: Option<String>,
    proto: Option<String>,
    reqHost: Option<String>,
    reqMethod: Option<String>,
    reqPath: Option<String>,
    reqPort: Option<String>,
    rspContentLen: Option<String>,
    rspContentType: Option<String>,
    UA: Option<String>,
    tlsOverheadTimeMSec: Option<String>,
    tlsVersion: Option<String>,
    objSize: Option<String>,
    uncompressedSize: Option<String>,
    overheadBytes: Option<String>,
    totalBytes: Option<String>,
    queryStr: Option<String>,
    fileSizeBucket: Option<String>,
    brotliStatus: Option<String>,
    originContentLen: Option<String>,
    earlyHints: Option<String>,
    edgeIPBinding: Option<String>,
    accLang: Option<String>,
    cookie: Option<String>,
    range: Option<String>,
    referer: Option<String>,
    xForwardedFor: Option<String>,
    maxAgeSec: Option<String>,
    reqEndTimeMSec: Option<String>,
    errorCode: Option<String>,
    turnAroundTimeMSec: Option<String>,
    transferTimeMSec: Option<String>,
    dnsLookupTimeMSec: Option<String>,
    lastByte: Option<String>,
    asn: Option<String>,
    timeToFirstByte: Option<String>,
    startupError: Option<String>,
    downloadTime: Option<String>,
    throughput: Option<String>,
    prefetchMidgressHits: Option<String>,
    cacheStatus: Option<String>,
    cacheable: Option<String>,
    breadcrumbs: Option<String>,
    edgeIP: Option<String>,
    country: Option<String>,
    state: Option<String>,
    city: Option<String>,
    serverCountry: Option<String>,
    billingRegion: Option<String>,
    ewUsageInfo: Option<String>,
    ewExecutionInfo: Option<String>,
    contentProtectionInfo: Option<String>,
    customField: Option<String>,
    securityRules: Option<String>,
    // Appended 19 Oct 2026 (change #16). DirecTV has both enabled and we were
    // discarding them: arlid had no column, cmcd carries player telemetry
    // (buffer length, measured throughput, bitrate). Appended rather than
    // inserted because their Athena table matches our schema positionally.
    arlid: Option<String>,
    cmcd: Option<String>,
}

// Function to create the Parquet schema
pub fn create_schema() -> SchemaRef {
    let schema = Schema::new(vec![
        Field::new("version", DataType::Int64, true),
        Field::new("isMidgress", DataType::Utf8, true),
        Field::new("streamId", DataType::Utf8, true),
        Field::new("cp", DataType::Utf8, true),
        Field::new("reqId", DataType::Utf8, true),
        Field::new("reqTimeSec", DataType::Utf8, true),
        Field::new("edgeAttempts", DataType::Utf8, true),
        Field::new("bytes", DataType::Utf8, true),
        Field::new("cliIP", DataType::Utf8, true),
        Field::new("statusCode", DataType::Utf8, true),
        Field::new("proto", DataType::Utf8, true),
        Field::new("reqHost", DataType::Utf8, true),
        Field::new("reqMethod", DataType::Utf8, true),
        Field::new("reqPath", DataType::Utf8, true),
        Field::new("reqPort", DataType::Utf8, true),
        Field::new("rspContentLen", DataType::Utf8, true),
        Field::new("rspContentType", DataType::Utf8, true),
        Field::new("UA", DataType::Utf8, true),
        Field::new("tlsOverheadTimeMSec", DataType::Utf8, true),
        Field::new("tlsVersion", DataType::Utf8, true),
        Field::new("objSize", DataType::Utf8, true),
        Field::new("uncompressedSize", DataType::Utf8, true),
        Field::new("overheadBytes", DataType::Utf8, true),
        Field::new("totalBytes", DataType::Utf8, true),
        Field::new("queryStr", DataType::Utf8, true),
        Field::new("fileSizeBucket", DataType::Utf8, true),
        Field::new("brotliStatus", DataType::Utf8, true),
        Field::new("originContentLen", DataType::Utf8, true),
        Field::new("earlyHints", DataType::Utf8, true),
        Field::new("edgeIPBinding", DataType::Utf8, true),
        Field::new("accLang", DataType::Utf8, true),
        Field::new("cookie", DataType::Utf8, true),
        Field::new("range", DataType::Utf8, true),
        Field::new("referer", DataType::Utf8, true),
        Field::new("xForwardedFor", DataType::Utf8, true),
        Field::new("maxAgeSec", DataType::Utf8, true),
        Field::new("reqEndTimeMSec", DataType::Utf8, true),
        Field::new("errorCode", DataType::Utf8, true),
        Field::new("turnAroundTimeMSec", DataType::Utf8, true),
        Field::new("transferTimeMSec", DataType::Utf8, true),
        Field::new("dnsLookupTimeMSec", DataType::Utf8, true),
        Field::new("lastByte", DataType::Utf8, true),
        Field::new("asn", DataType::Utf8, true),
        Field::new("timeToFirstByte", DataType::Utf8, true),
        Field::new("startupError", DataType::Utf8, true),
        Field::new("downloadTime", DataType::Utf8, true),
        Field::new("throughput", DataType::Utf8, true),
        Field::new("prefetchMidgressHits", DataType::Utf8, true),
        Field::new("cacheStatus", DataType::Utf8, true),
        Field::new("cacheable", DataType::Utf8, true),
        Field::new("breadcrumbs", DataType::Utf8, true),
        Field::new("edgeIP", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("city", DataType::Utf8, true),
        Field::new("serverCountry", DataType::Utf8, true),
        Field::new("billingRegion", DataType::Utf8, true),
        Field::new("ewUsageInfo", DataType::Utf8, true),
        Field::new("ewExecutionInfo", DataType::Utf8, true),
        Field::new("contentProtectionInfo", DataType::Utf8, true),
        Field::new("customField", DataType::Utf8, true),
        Field::new("securityRules", DataType::Utf8, true),
        // Appended - see the note on Datastream2Log.
        Field::new("arlid", DataType::Utf8, true),
        Field::new("cmcd", DataType::Utf8, true),
    ]);
    Arc::new(schema)
}

// Function to convert a batch of logs into a single columnar RecordBatch
pub fn logs_to_record_batch(logs: &[Datastream2Log], schema: SchemaRef) -> Result<RecordBatch> {
    // Create arrays for each field, with one value per log
    let version = Int64Array::from(
        logs.iter()
            .map(|log| log.version.as_ref().and_then(|v| v.parse::<i64>().ok()))
            .collect::<Vec<_>>(),
    );
    let is_midgress =
        StringArray::from(logs.iter().map(|log| log.isMidgress.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let stream_id =
        StringArray::from(logs.iter().map(|log| log.streamId.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let cp = StringArray::from(logs.iter().map(|log| log.cp.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_id =
        StringArray::from(logs.iter().map(|log| log.reqId.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_time_sec =
        StringArray::from(logs.iter().map(|log| log.reqTimeSec.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let edge_attempts = StringArray::from(
        logs.iter().map(|log| log.edgeAttempts.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let bytes =
        StringArray::from(logs.iter().map(|log| log.bytes.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let cli_ip =
        StringArray::from(logs.iter().map(|log| log.cliIP.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let status_code = StringArray::from(
        logs.iter().map(|log| log.statusCode.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let proto =
        StringArray::from(logs.iter().map(|log| log.proto.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_host =
        StringArray::from(logs.iter().map(|log| log.reqHost.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_method = StringArray::from(
        logs.iter().map(|log| log.reqMethod.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let req_path =
        StringArray::from(logs.iter().map(|log| log.reqPath.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let req_port =
        StringArray::from(logs.iter().map(|log| log.reqPort.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let rsp_content_len = StringArray::from(
        logs.iter().map(|log| log.rspContentLen.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let rsp_content_type = StringArray::from(
        logs.iter().map(|log| log.rspContentType.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let ua = StringArray::from(logs.iter().map(|log| log.UA.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let tls_overhead_time_m_sec = StringArray::from(
        logs.iter()
            .map(|log| log.tlsOverheadTimeMSec.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let tls_version = StringArray::from(
        logs.iter().map(|log| log.tlsVersion.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let obj_size =
        StringArray::from(logs.iter().map(|log| log.objSize.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let uncompressed_size = StringArray::from(
        logs.iter().map(|log| log.uncompressedSize.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let overhead_bytes = StringArray::from(
        logs.iter().map(|log| log.overheadBytes.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let total_bytes = StringArray::from(
        logs.iter().map(|log| log.totalBytes.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let query_str = StringArray::from(
        logs.iter().map(|log| log.queryStr.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let file_size_bucket = StringArray::from(
        logs.iter().map(|log| log.fileSizeBucket.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let brotli_status = StringArray::from(
        logs.iter().map(|log| log.brotliStatus.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let origin_content_len = StringArray::from(
        logs.iter().map(|log| log.originContentLen.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let early_hints = StringArray::from(
        logs.iter().map(|log| log.earlyHints.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let edge_ip_binding = StringArray::from(
        logs.iter().map(|log| log.edgeIPBinding.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let acc_lang = StringArray::from(
        logs.iter().map(|log| log.accLang.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let cookie =
        StringArray::from(logs.iter().map(|log| log.cookie.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let range =
        StringArray::from(logs.iter().map(|log| log.range.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let referer =
        StringArray::from(logs.iter().map(|log| log.referer.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let x_forwarded_for = StringArray::from(
        logs.iter().map(|log| log.xForwardedFor.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let max_age_sec = StringArray::from(
        logs.iter().map(|log| log.maxAgeSec.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let req_end_time_m_sec = StringArray::from(
        logs.iter().map(|log| log.reqEndTimeMSec.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let error_code = StringArray::from(
        logs.iter().map(|log| log.errorCode.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let turn_around_time_m_sec = StringArray::from(
        logs.iter()
            .map(|log| log.turnAroundTimeMSec.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let transfer_time_m_sec = StringArray::from(
        logs.iter().map(|log| log.transferTimeMSec.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let dns_lookup_time_m_sec = StringArray::from(
        logs.iter()
            .map(|log| log.dnsLookupTimeMSec.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let last_byte = StringArray::from(
        logs.iter().map(|log| log.lastByte.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let asn = StringArray::from(logs.iter().map(|log| log.asn.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let time_to_first_byte = StringArray::from(
        logs.iter().map(|log| log.timeToFirstByte.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let startup_error = StringArray::from(
        logs.iter().map(|log| log.startupError.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let download_time = StringArray::from(
        logs.iter().map(|log| log.downloadTime.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let throughput = StringArray::from(
        logs.iter().map(|log| log.throughput.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let prefetch_midgress_hits = StringArray::from(
        logs.iter()
            .map(|log| log.prefetchMidgressHits.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let cache_status = StringArray::from(
        logs.iter().map(|log| log.cacheStatus.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let cacheable = StringArray::from(
        logs.iter().map(|log| log.cacheable.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let breadcrumbs = StringArray::from(
        logs.iter().map(|log| log.breadcrumbs.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let edge_ip =
        StringArray::from(logs.iter().map(|log| log.edgeIP.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let country =
        StringArray::from(logs.iter().map(|log| log.country.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let state =
        StringArray::from(logs.iter().map(|log| log.state.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let city =
        StringArray::from(logs.iter().map(|log| log.city.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let server_country = StringArray::from(
        logs.iter().map(|log| log.serverCountry.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let billing_region = StringArray::from(
        logs.iter().map(|log| log.billingRegion.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let ew_usage_info = StringArray::from(
        logs.iter().map(|log| log.ewUsageInfo.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let ew_execution_info = StringArray::from(
        logs.iter().map(|log| log.ewExecutionInfo.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let content_protection = StringArray::from(
        logs.iter()
            .map(|log| log.contentProtectionInfo.as_deref().unwrap_or(""))
            .collect::<Vec<_>>(),
    );
    let custom_field = StringArray::from(
        logs.iter().map(|log| log.customField.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let security_rules = StringArray::from(
        logs.iter().map(|log| log.securityRules.as_deref().unwrap_or("")).collect::<Vec<_>>(),
    );
    let arlid =
        StringArray::from(logs.iter().map(|log| log.arlid.as_deref().unwrap_or("")).collect::<Vec<_>>());
    let cmcd =
        StringArray::from(logs.iter().map(|log| log.cmcd.as_deref().unwrap_or("")).collect::<Vec<_>>());

    // Create the record batch
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(version),
            Arc::new(is_midgress),
            Arc::new(stream_id),
            Arc::new(cp),
            Arc::new(req_id),
            Arc::new(req_time_sec),
            Arc::new(edge_attempts),
            Arc::new(bytes),
            Arc::new(cli_ip),
            Arc::new(status_code),
            Arc::new(proto),
            Arc::new(req_host),
            Arc::new(req_method),
            Arc::new(req_path),
            Arc::new(req_port),
            Arc::new(rsp_content_len),
            Arc::new(rsp_content_type),
            Arc::new(ua),
            Arc::new(tls_overhead_time_m_sec),
            Arc::new(tls_version),
            Arc::new(obj_size),
            Arc::new(uncompressed_size),
            Arc::new(overhead_bytes),
            Arc::new(total_bytes),
            Arc::new(query_str),
            Arc::new(file_size_bucket),
            Arc::new(brotli_status),
            Arc::new(origin_content_len),
            Arc::new(early_hints),
            Arc::new(edge_ip_binding),
            Arc::new(acc_lang),
            Arc::new(cookie),
            Arc::new(range),
            Arc::new(referer),
            Arc::new(x_forwarded_for),
            Arc::new(max_age_sec),
            Arc::new(req_end_time_m_sec),
            Arc::new(error_code),
            Arc::new(turn_around_time_m_sec),
            Arc::new(transfer_time_m_sec),
            Arc::new(dns_lookup_time_m_sec),
            Arc::new(last_byte),
            Arc::new(asn),
            Arc::new(time_to_first_byte),
            Arc::new(startup_error),
            Arc::new(download_time),
            Arc::new(throughput),
            Arc::new(prefetch_midgress_hits),
            Arc::new(cache_status),
            Arc::new(cacheable),
            Arc::new(breadcrumbs),
            Arc::new(edge_ip),
            Arc::new(country),
            Arc::new(state),
            Arc::new(city),
            Arc::new(server_country),
            Arc::new(billing_region),
            Arc::new(ew_usage_info),
            Arc::new(ew_execution_info),
            Arc::new(content_protection),
            Arc::new(custom_field),
            Arc::new(security_rules),
            Arc::new(arlid),
            Arc::new(cmcd),
        ],
    )
    .map_err(|e| anyhow!("Failed to create record batch: {}", e))
}

/// Every DataStream 2 key we capture. Used to spot field-set drift: a key
/// that is not here is being discarded.
const KNOWN_FIELDS: [&str; 64] = [
    "version", "isMidgress", "streamId", "cp",
    "reqId", "reqTimeSec", "edgeAttempts", "bytes",
    "cliIP", "statusCode", "proto", "reqHost",
    "reqMethod", "reqPath", "reqPort", "rspContentLen",
    "rspContentType", "UA", "tlsOverheadTimeMSec", "tlsVersion",
    "objSize", "uncompressedSize", "overheadBytes", "totalBytes",
    "queryStr", "fileSizeBucket", "brotliStatus", "originContentLen",
    "earlyHints", "edgeIPBinding", "accLang", "cookie",
    "range", "referer", "xForwardedFor", "maxAgeSec",
    "reqEndTimeMSec", "errorCode", "turnAroundTimeMSec", "transferTimeMSec",
    "dnsLookupTimeMSec", "lastByte", "asn", "timeToFirstByte",
    "startupError", "downloadTime", "throughput", "prefetchMidgressHits",
    "cacheStatus", "cacheable", "breadcrumbs", "edgeIP",
    "country", "state", "city", "serverCountry",
    "billingRegion", "ewUsageInfo", "ewExecutionInfo", "contentProtectionInfo",
    "customField", "securityRules", "arlid", "cmcd",
];

pub fn extract_logs(body_bytes: &[u8]) -> Result<Vec<Datastream2Log>> {
    // The request body from DataStream2 is a new-line-separated list of JSON
    // objects of type Datastream2Log.
    let body = std::str::from_utf8(body_bytes)
        .map_err(|e| anyhow!("Failed to parse request body as UTF-8: {}", e))?;

    let json_objects = extract_json_objects(body);
    let total = json_objects.len();
    let mut logs = Vec::with_capacity(total);
    let mut dropped = 0usize;
    let mut first_error: Option<String> = None;

    for json_str in &json_objects {
        match serde_json::from_str::<Datastream2Log>(json_str) {
            Ok(log) => logs.push(log),
            Err(e) => {
                dropped += 1;
                if first_error.is_none() {
                    first_error = Some(e.to_string());
                }
            }
        }
    }

    // Records that fail to bind are skipped, so a partial batch would otherwise
    // upload with no signal anywhere. Warn once per request with a count rather
    // than once per record, which would flood the log at 100% sampling.
    // Field-set drift. DataStream omits fields that have no value, so a *missing*
    // key is normal and must not warn. An *unrecognised* key means Akamai added or
    // renamed something and we are silently discarding it — which is exactly how
    // UA, cliIP and edgeIP went unnoticed for four months. Every record in a batch
    // comes from one stream and carries the same field set, so checking the first
    // costs one extra parse per request rather than one per record.
    if let Some(first) = json_objects.first() {
        if let Ok(map) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(first) {
            let unknown: Vec<&str> = map
                .keys()
                .map(|k| k.as_str())
                .filter(|k| !KNOWN_FIELDS.iter().any(|f| f == k))
                .collect();
            if !unknown.is_empty() {
                tracing::warn!(
                    "unrecognised DataStream fields are being discarded: {:?}; \
                     add them to Datastream2Log and create_schema",
                    unknown
                );
            }
        }
    }

    if dropped > 0 {
        tracing::warn!(
            "dropped {} of {} records that did not parse as Datastream2Log; first error: {}",
            dropped,
            total,
            first_error.as_deref().unwrap_or("unknown")
        );
    }

    Ok(logs)
}

// Extract all JSON objects from a string.
// This is a pretty naive implementation that tries to handle weird cases of DataStream2 sending
// incomplete JSON objects.
fn extract_json_objects(s: &str) -> Vec<String> {
    let mut objects = Vec::new();
    let mut start_index = None;
    let mut depth = 0;
    let mut in_string = false;
    let mut escape_next = false;

    for (i, c) in s.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }

        match c {
            '\\' if in_string => escape_next = true,
            '"' if !escape_next => in_string = !in_string,
            '{' if !in_string => {
                depth += 1;
                if depth == 1 {
                    start_index = Some(i);
                }
            }
            '}' if !in_string => {
                depth -= 1;
                if depth == 0 && start_index.is_some() {
                    let start = start_index.unwrap();
                    objects.push(s[start..=i].to_string());
                    start_index = None;
                }
            }
            _ => {}
        }
    }

    objects
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn every_field_populated() -> Datastream2Log {
        Datastream2Log {
            version: Some("x".into()),
            isMidgress: Some("x".into()),
            streamId: Some("x".into()),
            cp: Some("x".into()),
            reqId: Some("x".into()),
            reqTimeSec: Some("x".into()),
            edgeAttempts: Some("x".into()),
            bytes: Some("x".into()),
            cliIP: Some("x".into()),
            statusCode: Some("x".into()),
            proto: Some("x".into()),
            reqHost: Some("x".into()),
            reqMethod: Some("x".into()),
            reqPath: Some("x".into()),
            reqPort: Some("x".into()),
            rspContentLen: Some("x".into()),
            rspContentType: Some("x".into()),
            UA: Some("x".into()),
            tlsOverheadTimeMSec: Some("x".into()),
            tlsVersion: Some("x".into()),
            objSize: Some("x".into()),
            uncompressedSize: Some("x".into()),
            overheadBytes: Some("x".into()),
            totalBytes: Some("x".into()),
            queryStr: Some("x".into()),
            fileSizeBucket: Some("x".into()),
            brotliStatus: Some("x".into()),
            originContentLen: Some("x".into()),
            earlyHints: Some("x".into()),
            edgeIPBinding: Some("x".into()),
            accLang: Some("x".into()),
            cookie: Some("x".into()),
            range: Some("x".into()),
            referer: Some("x".into()),
            xForwardedFor: Some("x".into()),
            maxAgeSec: Some("x".into()),
            reqEndTimeMSec: Some("x".into()),
            errorCode: Some("x".into()),
            turnAroundTimeMSec: Some("x".into()),
            transferTimeMSec: Some("x".into()),
            dnsLookupTimeMSec: Some("x".into()),
            lastByte: Some("x".into()),
            asn: Some("x".into()),
            timeToFirstByte: Some("x".into()),
            startupError: Some("x".into()),
            downloadTime: Some("x".into()),
            throughput: Some("x".into()),
            prefetchMidgressHits: Some("x".into()),
            cacheStatus: Some("x".into()),
            cacheable: Some("x".into()),
            breadcrumbs: Some("x".into()),
            edgeIP: Some("x".into()),
            country: Some("x".into()),
            state: Some("x".into()),
            city: Some("x".into()),
            serverCountry: Some("x".into()),
            billingRegion: Some("x".into()),
            ewUsageInfo: Some("x".into()),
            ewExecutionInfo: Some("x".into()),
            contentProtectionInfo: Some("x".into()),
            customField: Some("x".into()),
            securityRules: Some("x".into()),
            arlid: Some("x".into()),
            cmcd: Some("x".into()),
        }
    }

    /// The bug this prevents: the Parquet schema said "UA" while serde asked for
    /// "ua", so the column existed and was always empty. Serialising the struct
    /// yields exactly the keys serde will accept on the way in, so comparing them
    /// to the schema catches any future drift at build time.
    #[test]
    fn serde_names_match_the_parquet_schema() {
        let value = serde_json::to_value(every_field_populated()).unwrap();
        let serde_names: BTreeSet<String> =
            value.as_object().unwrap().keys().cloned().collect();
        let schema_names: BTreeSet<String> = create_schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(serde_names, schema_names, "serde keys drifted from the Parquet schema");
    }

    #[test]
    fn known_fields_matches_the_schema() {
        let known: BTreeSet<String> = KNOWN_FIELDS.iter().map(|s| s.to_string()).collect();
        let schema_ref = create_schema();
        let schema: BTreeSet<String> =
            schema_ref.fields().iter().map(|f| f.name().clone()).collect();
        assert_eq!(known.len(), KNOWN_FIELDS.len(), "duplicate in KNOWN_FIELDS");
        assert_eq!(known, schema, "KNOWN_FIELDS drifted from the schema");
    }

    /// Guards array ORDER in logs_to_record_batch. Every column is Utf8, so two
    /// swapped Arc::new entries would build a valid batch with values in the
    /// wrong columns and nothing would complain. Checks a value at the start,
    /// middle and both appended columns at the end.
    #[test]
    fn record_batch_columns_are_in_schema_order() {
        let line = r#"{"version":"1","cp":"CPCODE","UA":"THE-UA","cliIP":"1.2.3.4","edgeIP":"5.6.7.8","securityRules":"SEC","arlid":"ARL","cmcd":"CMCD","city":"HERNDON"}"#;
        let log: Datastream2Log = serde_json::from_str(line).unwrap();
        let schema = create_schema();
        let batch = logs_to_record_batch(&[log], schema.clone()).unwrap();
        let col = |name: &str| -> String {
            let i = schema.index_of(name).unwrap();
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(col("cp"), "CPCODE");
        assert_eq!(col("UA"), "THE-UA");
        assert_eq!(col("cliIP"), "1.2.3.4");
        assert_eq!(col("edgeIP"), "5.6.7.8");
        assert_eq!(col("city"), "HERNDON");
        assert_eq!(col("securityRules"), "SEC");
        assert_eq!(col("arlid"), "ARL");
        assert_eq!(col("cmcd"), "CMCD");
        assert_eq!(batch.num_columns(), 64);
    }

    /// Akamai's own sample for DirecTV's field selection. Every selected field
    /// must survive the round trip.
    #[test]
    fn akamai_sample_fields_all_bind() {
        let line = r#"{"version":"1","cliIP":"128.147.28.68","UA":"Mozilla%2F5.0","edgeIP":"23.50.51.173","arlid":"602093","cmcd":"//1.0@V/bl=21300","cp":"123456","statusCode":"206"}"#;
        let log: Datastream2Log = serde_json::from_str(line).unwrap();
        assert_eq!(log.cliIP.as_deref(), Some("128.147.28.68"));
        assert_eq!(log.UA.as_deref(), Some("Mozilla%2F5.0"));
        assert_eq!(log.edgeIP.as_deref(), Some("23.50.51.173"));
        assert_eq!(log.arlid.as_deref(), Some("602093"));
        assert_eq!(log.cmcd.as_deref(), Some("//1.0@V/bl=21300"));
    }
}
