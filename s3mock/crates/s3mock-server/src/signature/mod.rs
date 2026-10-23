// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! SigV4 header authentication with disk-backed verification before store dispatch.

mod payload;

use std::collections::BTreeMap;
use std::io;

use futures_util::{StreamExt, TryStreamExt};
use hmac::{Hmac, Mac};
use http::{Request, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use percent_encoding::percent_decode_str;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;

use crate::config::{ServerConfig, SignatureVerification};
use crate::s3_error::S3Error;
use crate::server::RequestBody;

pub(crate) struct SignatureContext {
    key: Vec<u8>,
    timestamp: String,
    scope: String,
    signature: String,
    payload_hash: String,
    trailer_names: Vec<String>,
}

pub(crate) async fn verify_request(
    request: Request<RequestBody>,
    config: &ServerConfig,
) -> Result<Request<RequestBody>, S3Error> {
    if config.signature_verification == SignatureVerification::Off {
        return Ok(request);
    }
    let context = verify_headers(&request, config)?;
    let (parts, mut body) = request.into_parts();
    let file = tokio::task::spawn_blocking(tempfile::tempfile)
        .await
        .map_err(|error| S3Error::internal(format!("create signature spool task: {error}")))?
        .map_err(|error| S3Error::internal(format!("create signature spool file: {error}")))?;
    let mut file = tokio::fs::File::from_std(file);
    let mut trailers = None;
    while let Some(frame) = body.frame().await {
        let frame = frame
            .map_err(|error| S3Error::incomplete_body(format!("read signed body: {error}")))?;
        match frame.into_data() {
            Ok(data) => file
                .write_all(&data)
                .await
                .map_err(|error| S3Error::internal(format!("spool signed body: {error}")))?,
            Err(frame) => {
                if let Ok(headers) = frame.into_trailers() {
                    trailers = Some(headers);
                }
            }
        }
    }
    file.rewind()
        .await
        .map_err(|error| S3Error::internal(format!("rewind signed body: {error}")))?;
    let file = file.into_std().await;
    let file = tokio::task::spawn_blocking(move || payload::verify(file, context))
        .await
        .map_err(|error| S3Error::internal(format!("verify signed body task: {error}")))??;
    let stream = ReaderStream::new(tokio::fs::File::from_std(file)).map_ok(Frame::data);
    let trailer_stream = futures_util::stream::iter(
        trailers
            .into_iter()
            .map(|headers| Ok::<_, io::Error>(Frame::trailers(headers))),
    );
    Ok(Request::from_parts(
        parts,
        BodyExt::boxed(StreamBody::new(stream.chain(trailer_stream))),
    ))
}

fn verify_headers(
    request: &Request<RequestBody>,
    config: &ServerConfig,
) -> Result<SignatureContext, S3Error> {
    if request
        .uri()
        .query()
        .is_some_and(|query| query.split('&').any(|pair| pair.starts_with("X-Amz-")))
    {
        return Err(S3Error::not_implemented(
            "presigned URLs in strict signature mode",
        ));
    }
    let header = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(access_denied)
    };
    let authorization = header("authorization")?
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or_else(access_denied)?;
    let mut fields = BTreeMap::new();
    for field in authorization.split(',') {
        let (name, value) = field.trim().split_once('=').ok_or_else(access_denied)?;
        if fields.insert(name, value).is_some() {
            return Err(access_denied());
        }
    }
    if fields.len() != 3 {
        return Err(access_denied());
    }
    let credential = fields.get("Credential").ok_or_else(access_denied)?;
    let components: Vec<_> = credential.split('/').collect();
    if components.len() != 5
        || components[0] != config.access_key_id
        || components[3] != "s3"
        || components[4] != "aws4_request"
        || components[2].is_empty()
    {
        return Err(access_denied());
    }
    let timestamp = header("x-amz-date")?;
    if timestamp.len() != 16
        || timestamp.get(..8) != Some(components[1])
        || timestamp.as_bytes()[8] != b'T'
        || !timestamp.ends_with('Z')
    {
        return Err(access_denied());
    }
    let signed_headers = *fields.get("SignedHeaders").ok_or_else(access_denied)?;
    let names: Vec<_> = signed_headers.split(';').collect();
    if !names.contains(&"host")
        || !names.contains(&"x-amz-date")
        || names.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(access_denied());
    }
    let mut canonical_headers = String::new();
    for name in names {
        if name.is_empty() || name != name.to_ascii_lowercase() {
            return Err(access_denied());
        }
        let values: Vec<_> = request
            .headers()
            .get_all(name)
            .iter()
            .map(|value| {
                value
                    .to_str()
                    .map(|value| value.split_whitespace().collect::<Vec<_>>().join(" "))
                    .map_err(|_| access_denied())
            })
            .collect::<Result<_, _>>()?;
        if values.is_empty() {
            return Err(access_denied());
        }
        canonical_headers.push_str(&format!("{name}:{}\n", values.join(",")));
    }
    let payload_hash = header("x-amz-content-sha256")?;
    let mut query = Vec::new();
    for pair in request
        .uri()
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|pair| !pair.is_empty())
    {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        query.push((
            encode(&percent_decode_str(name).collect::<Vec<_>>(), false),
            encode(&percent_decode_str(value).collect::<Vec<_>>(), false),
        ));
    }
    query.sort();
    let canonical_query = query
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let path = request.uri().path();
    let canonical = format!(
        "{}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        request.method()
    );
    let scope = components[1..].join("/");
    let key = hmac(
        format!("AWS4{}", config.secret_access_key).as_bytes(),
        components[1].as_bytes(),
    );
    let key = hmac(&key, components[2].as_bytes());
    let key = hmac(&key, b"s3");
    let key = hmac(&key, b"aws4_request");
    let signature = fields
        .get("Signature")
        .ok_or_else(access_denied)?
        .to_string();
    verify_hmac(
        &key,
        &format!(
            "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
            hash(canonical.as_bytes())
        ),
        &signature,
    )?;
    let trailer_names = request
        .headers()
        .get("x-amz-trailer")
        .map(|value| value.to_str().map_err(|_| access_denied()))
        .transpose()?
        .unwrap_or_default()
        .split(',')
        .filter(|name| !name.is_empty())
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    Ok(SignatureContext {
        key,
        timestamp: timestamp.into(),
        scope,
        signature,
        payload_hash: payload_hash.into(),
        trailer_names,
    })
}

fn encode(bytes: &[u8], preserve_slash: bool) -> String {
    let mut encoded = String::new();
    for byte in bytes {
        if byte.is_ascii_alphanumeric()
            || b"-._~".contains(byte)
            || (preserve_slash && *byte == b'/')
        {
            encoded.push(char::from(*byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn hmac(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}

fn verify_hmac(key: &[u8], message: &str, expected: &str) -> Result<(), S3Error> {
    let signature = hex::decode(expected).map_err(|_| mismatch())?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts keys of any length");
    mac.update(message.as_bytes());
    mac.verify_slice(&signature).map_err(|_| mismatch())
}

fn mismatch() -> S3Error {
    S3Error::new(
        StatusCode::FORBIDDEN,
        "SignatureDoesNotMatch",
        "the request signature does not match",
    )
}

fn access_denied() -> S3Error {
    S3Error::new(
        StatusCode::FORBIDDEN,
        "AccessDenied",
        "valid SigV4 header credentials are required",
    )
}
