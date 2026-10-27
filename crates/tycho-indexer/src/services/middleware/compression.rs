use actix_web::{
    body::{BoxBody, MessageBody},
    http::header::{self, AcceptEncoding, Encoding, HeaderValue},
    middleware::Compress,
    HttpMessage, HttpRequest, HttpResponse,
};
use tracing::error;

use crate::services::rpc::RpcError;

/// zstd level of [`compress_state_response`]. On state responses, level 1 takes about half the CPU
/// time of level 3, the level the middleware uses, and its output is no larger: in hex-heavy JSON
/// the 5-byte matches level 3 also takes cost more bits than the bytes they replace.
const STATE_ZSTD_LEVEL: i32 = 1;

/// Creates the compression middleware for RPC responses.
///
/// Enables zstd compression while being backwards compatible with clients that do not support it.
/// The middleware automatically handles Accept-Encoding negotiation:
/// - Clients that send "Accept-Encoding: zstd" will receive compressed responses
/// - Clients that don't specify Accept-Encoding will receive uncompressed responses
/// - Clients that explicitly send "Accept-Encoding: identity" will receive uncompressed responses
///
/// It compresses at zstd level 3 and leaves responses that already carry a `Content-Encoding`
/// unchanged, such as those of [`compress_state_response`].
pub(in crate::services) fn compression_middleware() -> Compress {
    Compress::default()
}

/// Compresses `response` with zstd at [`STATE_ZSTD_LEVEL`] on the blocking thread pool if the
/// client accepts zstd and the body is complete, not a stream. The response then carries
/// `Content-Encoding: zstd`, so [`compression_middleware`] sends it as it is. Any other response
/// is returned unchanged, for the middleware to handle.
///
/// # Errors
///
/// Returns [`RpcError::Unknown`] if compression fails or panics.
pub(in crate::services) async fn compress_state_response(
    req: &HttpRequest,
    response: HttpResponse,
) -> Result<HttpResponse, RpcError> {
    if !accepts_zstd(req) {
        return Ok(response);
    }
    let (response, body) = response.into_parts();
    let body = match body.try_into_bytes() {
        Ok(bytes) => bytes,
        Err(body) => return Ok(response.set_body(body)),
    };
    let compressed = tokio::task::spawn_blocking(move || {
        let mut compressed = zstd::bulk::compress(&body, STATE_ZSTD_LEVEL)?;
        // `bulk::compress` reserves about the uncompressed size, and the response keeps the whole
        // allocation until the client reads the last byte.
        compressed.shrink_to_fit();
        Ok::<_, std::io::Error>(compressed)
    })
    .await
    .map_err(|err| RpcError::Unknown(format!("Response compression panicked: {err}")))
    .and_then(|compressed| {
        compressed.map_err(|err| RpcError::Unknown(format!("Failed to compress response: {err}")))
    })
    .inspect_err(|err| error!(error = %err, "Error while compressing a state response."))?;
    let mut response = response.set_body(BoxBody::new(compressed));
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("zstd"));
    headers.insert(header::VARY, HeaderValue::from_static("accept-encoding"));
    Ok(response)
}

/// Whether the client accepts zstd, decided as [`compression_middleware`] decides: a request
/// without `Accept-Encoding` gets identity.
fn accepts_zstd(req: &HttpRequest) -> bool {
    req.get_header::<AcceptEncoding>()
        .and_then(|accept| accept.negotiate([Encoding::identity(), Encoding::zstd()].iter())) ==
        Some(Encoding::zstd())
}

#[cfg(test)]
mod tests {
    use actix_web::{test, web, App};

    use super::*;
    use crate::services::rpc::health as health_endpoint;

    #[actix_web::test]
    async fn test_without_accept_encoding_header() {
        let app = test::init_service(
            App::new()
                .wrap(compression_middleware())
                .service(web::resource("/health").route(web::get().to(health_endpoint))),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/health")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        assert!(resp
            .headers()
            .get("content-encoding")
            .is_none());

        let body = test::read_body(resp).await;
        assert!(!body.is_empty());
    }

    #[actix_web::test]
    async fn test_with_zstd_encoding() {
        let app = test::init_service(
            App::new()
                .wrap(compression_middleware())
                .service(web::resource("/health").route(web::get().to(health_endpoint))),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/health")
            .insert_header(("accept-encoding", "zstd"))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());

        let content_encoding = resp
            .headers()
            .get("content-encoding")
            .and_then(|h| h.to_str().ok());
        assert_eq!(content_encoding, Some("zstd"));

        let body = test::read_body(resp).await;
        assert!(!body.is_empty());
    }

    #[actix_web::test]
    async fn test_with_explicit_identity() {
        let app = test::init_service(
            App::new()
                .wrap(compression_middleware())
                .service(web::resource("/health").route(web::get().to(health_endpoint))),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/health")
            .insert_header(("accept-encoding", "identity"))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        assert!(resp
            .headers()
            .get("content-encoding")
            .is_none());

        let body = test::read_body(resp).await;
        assert!(!body.is_empty());
    }
}
