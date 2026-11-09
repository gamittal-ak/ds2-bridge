use aws_smithy_async::{
    rt::sleep::{AsyncSleep, Sleep},
    time::TimeSource,
};
use aws_smithy_runtime_api::{
    client::{
        http::{
            HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings,
            SharedHttpConnector,
        },
        orchestrator::HttpRequest,
        result::ConnectorError,
        runtime_components::RuntimeComponents,
    },
    http::Response as SdkResponse,
    shared::IntoShared,
};
use aws_smithy_types::body::SdkBody;
use spin_sdk::http::{self, body::IncomingBodyExt, FullBody, Response};
use std::time::{Duration, Instant, SystemTime};

/// A WASI-compatible `TimeSource` for AWS SDK.
#[derive(Debug)]
pub struct WasiNow;

impl TimeSource for WasiNow {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// A WASI-compatible async sleep for AWS SDK (no native async timers).
#[derive(Debug, Clone)]
pub struct WasiSleep;

impl AsyncSleep for WasiSleep {
    fn sleep(&self, duration: Duration) -> Sleep {
        Sleep::new(async move {
            let start = Instant::now();
            while Instant::now().duration_since(start) < duration {
                async { () }.await;
            }
        })
    }
}

/// A WASI-compatible HTTP adapter that implements AWS SDK's `HttpClient`.
#[derive(Debug, Clone)]
pub struct WasiHttpClient;

impl HttpConnector for WasiHttpClient {
    fn call(&self, req: HttpRequest) -> HttpConnectorFuture {
        let uri = req.uri().to_string();
        let method = req.method().to_string();
        let headers = req.headers().clone();
        let body_bytes = req.body().bytes().unwrap_or_default().to_vec();
        let body_len = body_bytes.len();

        tracing::debug!(
            "AWS HTTP request: {} {} (body size: {} bytes)",
            method,
            uri,
            body_len
        );

        // Log request headers for debugging auth issues
        for (name, value) in headers.iter() {
            // Don't log sensitive auth headers in full, just confirm they exist
            let name_str = name.to_string();
            if name_str.to_lowercase().contains("authorization") {
                tracing::debug!("Request header: {} = [REDACTED {} chars]", name_str, value.len());
            } else {
                tracing::debug!("Request header: {} = {:?}", name_str, value);
            }
        }

        HttpConnectorFuture::new(async move {
            // Build HTTP request with all headers from AWS SDK
            let mut request_builder = match method.as_str() {
                "GET" => ::http::Request::get(&uri),
                "POST" => ::http::Request::post(&uri),
                "PUT" => ::http::Request::put(&uri),
                "PATCH" => ::http::Request::patch(&uri),
                "DELETE" => ::http::Request::delete(&uri),
                "HEAD" => ::http::Request::head(&uri),
                other => {
                    tracing::error!("Unsupported HTTP method: {}", other);
                    return Err(ConnectorError::other(
                        format!("Unsupported HTTP method: {}", other).into(),
                        None,
                    ));
                }
            };

            // Add all headers from the AWS SDK request
            for (name, value) in headers.iter() {
                request_builder = request_builder.header(name, value);
            }

            // Use FullBody for binary data (Parquet files are binary).
            // Move body_bytes directly into the request instead of cloning it -
            // for large Parquet uploads a clone here would double peak memory usage.
            tracing::debug!("Building HTTP request with {} bytes of body data", body_len);

            let body_data: bytes::Bytes = body_bytes.into();
            let http_request = request_builder
                .body(FullBody::new(body_data))
                .map_err(|e| {
                    tracing::error!("Failed to build HTTP request: {}", e);
                    ConnectorError::other(format!("Failed to build request: {}", e).into(), None)
                })?;

            tracing::debug!("Sending {} request to {} with {} headers and {} bytes body",
                method, uri, headers.len(), body_len);

            let response: Response = http::send(http_request)
                .await
                .map_err(|e| {
                    tracing::error!("Failed to send HTTP request to {}: {:?}", uri, e);
                    tracing::error!("Request details - Method: {}, Headers count: {}, Body size: {} bytes",
                        method, headers.len(), body_len);
                    ConnectorError::other(format!("Failed to send request: {:?}", e).into(), None)
                })?;

            tracing::debug!("AWS HTTP response: status={}", response.status());

            let status = aws_smithy_runtime_api::http::StatusCode::try_from(response.status())
                .map_err(|e| {
                    tracing::error!("Invalid status code: {}", e);
                    ConnectorError::other(format!("Invalid status code: {}", e).into(), None)
                })?;

            let headers: Vec<(String, String)> = response
                .headers()
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
                .collect();

            // Log response headers
            tracing::debug!("Response headers:");
            for (k, v) in &headers {
                tracing::debug!("  {} = {}", k, v);
            }

            // Read the response body
            tracing::debug!("Reading response body...");
            let body = response.into_body();
            let body_bytes = body.bytes().await
                .map_err(|e| {
                    tracing::error!("Failed to read response body: {:?}", e);
                    ConnectorError::other(format!("Failed to read response body: {:?}", e).into(), None)
                })?;

            tracing::debug!("Response body size: {} bytes", body_bytes.len());

            // Log body content for non-success status codes
            if status.as_u16() >= 400 {
                let body_preview = String::from_utf8_lossy(&body_bytes);
                tracing::error!(
                    "S3 error response (status {}): {}",
                    status.as_u16(),
                    body_preview
                );
            } else {
                tracing::debug!("S3 request successful (status {})", status.as_u16());
            }

            let sdk_body = SdkBody::from(body_bytes.to_vec());
            let mut sdk_response = SdkResponse::new(status, sdk_body);
            for (k, v) in headers {
                sdk_response.headers_mut().insert(k, v);
            }

            Ok::<_, ConnectorError>(sdk_response)
        })
    }
}

impl HttpClient for WasiHttpClient {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.clone().into_shared()
    }
}
