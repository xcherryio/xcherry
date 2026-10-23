// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Maps a path-style request to an S3 operation; unsupported subresources fail with NotImplemented.

use http::{Method, StatusCode, Uri};
use percent_encoding::percent_decode_str;

use crate::s3_error::S3Error;

/// Query parameters that only route or tune a request and never select another operation.
const PASSIVE_PARAMETERS: [&str; 1] = ["x-id"];
const LIST_OBJECTS_PARAMETERS: [&str; 7] = [
    "list-type",
    "prefix",
    "delimiter",
    "max-keys",
    "continuation-token",
    "start-after",
    "encoding-type",
];
const LIST_VERSIONS_PARAMETERS: [&str; 7] = [
    "versions",
    "prefix",
    "delimiter",
    "max-keys",
    "key-marker",
    "version-id-marker",
    "encoding-type",
];
const OBJECT_READ_PARAMETERS: [&str; 7] = [
    "versionId",
    "response-content-type",
    "response-content-disposition",
    "response-content-encoding",
    "response-content-language",
    "response-cache-control",
    "response-expires",
];

const LIST_UPLOADS_PARAMETERS: [&str; 7] = [
    "uploads",
    "prefix",
    "delimiter",
    "max-uploads",
    "key-marker",
    "upload-id-marker",
    "encoding-type",
];
const LIST_PARTS_PARAMETERS: [&str; 3] = ["uploadId", "max-parts", "part-number-marker"];

/// An S3 operation with its path parameters.
#[derive(Debug)]
pub(crate) enum S3Operation {
    ListBuckets,
    CreateBucket {
        bucket: String,
    },
    HeadBucket {
        bucket: String,
    },
    DeleteBucket {
        bucket: String,
    },
    GetBucketLocation {
        bucket: String,
    },
    GetBucketVersioning {
        bucket: String,
    },
    PutBucketVersioning {
        bucket: String,
    },
    GetObjectLockConfiguration {
        bucket: String,
    },
    PutObjectLockConfiguration {
        bucket: String,
    },
    ListMultipartUploads {
        bucket: String,
        query: QueryParameters,
    },
    DeleteObjects {
        bucket: String,
    },
    CreateMultipartUpload {
        bucket: String,
        key: String,
    },
    UploadPart {
        bucket: String,
        key: String,
        query: QueryParameters,
    },
    CompleteMultipartUpload {
        bucket: String,
        key: String,
        query: QueryParameters,
    },
    AbortMultipartUpload {
        bucket: String,
        key: String,
        query: QueryParameters,
    },
    ListParts {
        bucket: String,
        key: String,
        query: QueryParameters,
    },
    ListObjectsV2 {
        bucket: String,
        query: QueryParameters,
    },
    ListObjectVersions {
        bucket: String,
        query: QueryParameters,
    },
    PutObject {
        bucket: String,
        key: String,
    },
    GetObject {
        bucket: String,
        key: String,
        query: QueryParameters,
    },
    HeadObject {
        bucket: String,
        key: String,
        query: QueryParameters,
    },
    DeleteObject {
        bucket: String,
        key: String,
        query: QueryParameters,
    },
}

/// Decoded query parameters in request order.
#[derive(Debug, Default)]
pub(crate) struct QueryParameters {
    parameters: Vec<(String, String)>,
}

impl QueryParameters {
    fn parse(query: Option<&str>) -> Result<QueryParameters, S3Error> {
        let mut parameters = Vec::new();
        for pair in query
            .unwrap_or_default()
            .split('&')
            .filter(|pair| !pair.is_empty())
        {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            parameters.push((decode_component(name)?, decode_component(value)?));
        }
        Ok(QueryParameters { parameters })
    }

    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.parameters
            .iter()
            .find(|(parameter, _)| parameter == name)
            .map(|(_, value)| value.as_str())
    }

    fn contains(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Fails with NotImplemented when a parameter outside `allowed` selects an unsupported subresource.
    fn require_only(&self, allowed: &[&str], operation: &str) -> Result<(), S3Error> {
        match self.parameters.iter().find(|(name, _)| {
            !allowed.contains(&name.as_str()) && !PASSIVE_PARAMETERS.contains(&name.as_str())
        }) {
            Some((name, _)) => Err(S3Error::not_implemented(&format!(
                "{operation} with the {name:?} parameter"
            ))),
            None => Ok(()),
        }
    }
}

/// Resolves the operation from the method, path-style URI, and query.
pub(crate) fn route(method: &Method, uri: &Uri) -> Result<S3Operation, S3Error> {
    let query = QueryParameters::parse(uri.query())?;
    let path = uri.path().strip_prefix('/').unwrap_or(uri.path());
    let (raw_bucket, raw_key) = path.split_once('/').unwrap_or((path, ""));
    let bucket = decode_component(raw_bucket)?;
    let key = decode_component(raw_key)?;
    if bucket.is_empty() {
        return match *method {
            Method::GET => {
                query.require_only(&[], "ListBuckets")?;
                Ok(S3Operation::ListBuckets)
            }
            _ => Err(method_not_allowed()),
        };
    }
    if key.is_empty() {
        route_bucket(method, bucket, query)
    } else {
        route_object(method, bucket, key, query)
    }
}

fn route_bucket(
    method: &Method,
    bucket: String,
    query: QueryParameters,
) -> Result<S3Operation, S3Error> {
    match *method {
        Method::PUT if query.contains("object-lock") => {
            query.require_only(&["object-lock"], "PutObjectLockConfiguration")?;
            Ok(S3Operation::PutObjectLockConfiguration { bucket })
        }
        Method::GET if query.contains("object-lock") => {
            query.require_only(&["object-lock"], "GetObjectLockConfiguration")?;
            Ok(S3Operation::GetObjectLockConfiguration { bucket })
        }
        Method::GET if query.contains("uploads") => {
            query.require_only(&LIST_UPLOADS_PARAMETERS, "ListMultipartUploads")?;
            Ok(S3Operation::ListMultipartUploads { bucket, query })
        }
        Method::PUT if query.contains("versioning") => {
            query.require_only(&["versioning"], "PutBucketVersioning")?;
            Ok(S3Operation::PutBucketVersioning { bucket })
        }
        Method::PUT => {
            query.require_only(&[], "this bucket PUT")?;
            Ok(S3Operation::CreateBucket { bucket })
        }
        Method::HEAD => Ok(S3Operation::HeadBucket { bucket }),
        Method::DELETE => {
            query.require_only(&[], "this bucket DELETE")?;
            Ok(S3Operation::DeleteBucket { bucket })
        }
        Method::GET if query.contains("location") => Ok(S3Operation::GetBucketLocation { bucket }),
        Method::GET if query.contains("versioning") => {
            Ok(S3Operation::GetBucketVersioning { bucket })
        }
        Method::GET if query.contains("versions") => {
            query.require_only(&LIST_VERSIONS_PARAMETERS, "ListObjectVersions")?;
            Ok(S3Operation::ListObjectVersions { bucket, query })
        }
        Method::GET if query.get("list-type") == Some("2") => {
            query.require_only(&LIST_OBJECTS_PARAMETERS, "ListObjectsV2")?;
            Ok(S3Operation::ListObjectsV2 { bucket, query })
        }
        Method::GET => Err(S3Error::not_implemented("ListObjects (version 1)")),
        Method::POST if query.contains("delete") => {
            query.require_only(&["delete"], "DeleteObjects")?;
            Ok(S3Operation::DeleteObjects { bucket })
        }
        _ => Err(method_not_allowed()),
    }
}

fn route_object(
    method: &Method,
    bucket: String,
    key: String,
    query: QueryParameters,
) -> Result<S3Operation, S3Error> {
    match *method {
        Method::PUT if query.contains("uploadId") => {
            query.require_only(&["uploadId", "partNumber"], "UploadPart")?;
            Ok(S3Operation::UploadPart { bucket, key, query })
        }
        Method::PUT => {
            query.require_only(&[], "this object PUT")?;
            Ok(S3Operation::PutObject { bucket, key })
        }
        Method::POST if query.contains("uploads") => {
            query.require_only(&["uploads"], "CreateMultipartUpload")?;
            Ok(S3Operation::CreateMultipartUpload { bucket, key })
        }
        Method::POST if query.contains("uploadId") => {
            query.require_only(&["uploadId"], "CompleteMultipartUpload")?;
            Ok(S3Operation::CompleteMultipartUpload { bucket, key, query })
        }
        Method::DELETE if query.contains("uploadId") => {
            query.require_only(&["uploadId"], "AbortMultipartUpload")?;
            Ok(S3Operation::AbortMultipartUpload { bucket, key, query })
        }
        Method::GET if query.contains("uploadId") => {
            query.require_only(&LIST_PARTS_PARAMETERS, "ListParts")?;
            Ok(S3Operation::ListParts { bucket, key, query })
        }
        Method::GET => {
            query.require_only(&OBJECT_READ_PARAMETERS, "GetObject")?;
            Ok(S3Operation::GetObject { bucket, key, query })
        }
        Method::HEAD => {
            query.require_only(&OBJECT_READ_PARAMETERS, "HeadObject")?;
            Ok(S3Operation::HeadObject { bucket, key, query })
        }
        Method::DELETE => {
            query.require_only(&["versionId"], "DeleteObject")?;
            Ok(S3Operation::DeleteObject { bucket, key, query })
        }
        Method::POST => Err(S3Error::not_implemented("this object POST")),
        _ => Err(method_not_allowed()),
    }
}

fn decode_component(raw: &str) -> Result<String, S3Error> {
    percent_decode_str(raw)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| {
            S3Error::new(
                StatusCode::BAD_REQUEST,
                "InvalidURI",
                "the URI is not valid UTF-8",
            )
        })
}

fn method_not_allowed() -> S3Error {
    S3Error::new(
        StatusCode::METHOD_NOT_ALLOWED,
        "MethodNotAllowed",
        "the specified method is not allowed against this resource",
    )
}
