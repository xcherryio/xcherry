// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Constant-memory payload and chained chunk signature verification on the spool file.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek};

use sha2::{Digest, Sha256};

use super::{SignatureContext, hash, mismatch, verify_hmac};
use crate::s3_error::S3Error;

pub(super) fn verify(mut file: File, mut context: SignatureContext) -> Result<File, S3Error> {
    match context.payload_hash.as_str() {
        "UNSIGNED-PAYLOAD" | "STREAMING-UNSIGNED-PAYLOAD-TRAILER" => {}
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" | "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" => {
            verify_chunks(&mut BufReader::new(&mut file), &mut context)?;
        }
        expected => {
            let actual = digest_reader(&mut file)?;
            if expected != actual {
                return Err(mismatch());
            }
        }
    }
    file.rewind()
        .map_err(|error| S3Error::internal(format!("rewind verified payload: {error}")))?;
    Ok(file)
}

fn verify_chunks(reader: &mut impl BufRead, context: &mut SignatureContext) -> Result<(), S3Error> {
    loop {
        let line = read_line(reader)?;
        let (size, signature) = line.split_once(";chunk-signature=").ok_or_else(mismatch)?;
        let size = u64::from_str_radix(size, 16).map_err(|_| mismatch())?;
        let mut chunk = reader.take(size);
        let digest = digest_reader(&mut chunk)?;
        if chunk.limit() != 0 {
            return Err(mismatch());
        }
        let message = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{digest}",
            context.timestamp,
            context.scope,
            context.signature,
            hash(b"")
        );
        verify_hmac(&context.key, &message, signature)?;
        context.signature = signature.into();
        if size == 0 {
            break;
        }
        if !read_line(reader)?.is_empty() {
            return Err(mismatch());
        }
    }
    if context.payload_hash.ends_with("-TRAILER") {
        verify_trailers(reader, context)?;
    } else if !read_line(reader)?.is_empty() {
        return Err(mismatch());
    }
    if !reader.fill_buf().map_err(|_| mismatch())?.is_empty() {
        return Err(mismatch());
    }
    Ok(())
}

fn verify_trailers(reader: &mut impl BufRead, context: &SignatureContext) -> Result<(), S3Error> {
    let mut trailers = BTreeMap::new();
    loop {
        let line = read_line(reader)?;
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').ok_or_else(mismatch)?;
        if trailers.len() >= 8
            || trailers
                .insert(name.to_ascii_lowercase(), value.trim().to_owned())
                .is_some()
        {
            return Err(mismatch());
        }
    }
    let signature = trailers
        .remove("x-amz-trailer-signature")
        .ok_or_else(mismatch)?;
    let mut declared = context.trailer_names.clone();
    declared.sort();
    if declared != trailers.keys().cloned().collect::<Vec<_>>() {
        return Err(mismatch());
    }
    let canonical = trailers
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect::<String>();
    let message = format!(
        "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
        context.timestamp,
        context.scope,
        context.signature,
        hash(canonical.as_bytes())
    );
    verify_hmac(&context.key, &message, &signature)
}

fn read_line(reader: &mut impl BufRead) -> Result<String, S3Error> {
    let mut line = Vec::new();
    reader
        .take(4097)
        .read_until(b'\n', &mut line)
        .map_err(|_| mismatch())?;
    if line.len() > 4096 || !line.ends_with(b"\r\n") {
        return Err(mismatch());
    }
    line.truncate(line.len() - 2);
    String::from_utf8(line).map_err(|_| mismatch())
}

fn digest_reader(reader: &mut impl Read) -> Result<String, S3Error> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 65536];
    loop {
        let length = reader
            .read(&mut buffer)
            .map_err(|error| S3Error::internal(format!("read signature spool: {error}")))?;
        if length == 0 {
            break;
        }
        digest.update(&buffer[..length]);
    }
    Ok(hex::encode(digest.finalize()))
}
