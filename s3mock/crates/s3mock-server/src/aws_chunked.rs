// Copyright (c) 2026 Super Durable, Inc.
//
// Licensed under the Sustainable Use License 1.0.
// You may not use this file except in compliance with the License.
// See the LICENSE file in the repository root.
//
// SPDX-License-Identifier: LicenseRef-Sustainable-Use-1.0

//! Incremental decoder for `Content-Encoding: aws-chunked` request bodies.
//!
//! Each chunk is `<hex size>[;chunk-signature=<sig>]\r\n<data>\r\n`. The zero-size chunk is
//! followed by optional `name:value\r\n` trailers and a blank line. Signatures are not verified.

use bytes::{Bytes, BytesMut};

use crate::s3_error::S3Error;

/// Longest chunk header or trailer line accepted, which bounds buffering of malformed input.
const MAX_LINE_LENGTH: usize = 4096;
/// Clients send a checksum and a signature trailer; more indicates abuse.
const MAX_TRAILER_COUNT: usize = 8;

enum DecoderState {
    ChunkHeader,
    ChunkData { remaining: u64 },
    ChunkDataEnd,
    Trailers,
    Done,
}

/// Decodes an aws-chunked body fed in arbitrary slices.
pub(crate) struct AwsChunkedDecoder {
    state: DecoderState,
    buffer: BytesMut,
    trailers: Vec<(String, String)>,
}

impl AwsChunkedDecoder {
    pub(crate) fn new() -> AwsChunkedDecoder {
        AwsChunkedDecoder {
            state: DecoderState::ChunkHeader,
            buffer: BytesMut::new(),
            trailers: Vec::new(),
        }
    }

    /// Consumes encoded bytes and appends the decoded object bytes to `decoded`.
    pub(crate) fn push(&mut self, input: &[u8], decoded: &mut Vec<Bytes>) -> Result<(), S3Error> {
        self.buffer.extend_from_slice(input);
        loop {
            match self.state {
                DecoderState::ChunkHeader => {
                    let Some(line) = self.take_line()? else {
                        return Ok(());
                    };
                    let size_text = line.split(';').next().unwrap_or_default().trim();
                    let size = u64::from_str_radix(size_text, 16).map_err(|_| {
                        S3Error::incomplete_body(format!("invalid aws-chunked size {size_text:?}"))
                    })?;
                    self.state = if size == 0 {
                        DecoderState::Trailers
                    } else {
                        DecoderState::ChunkData { remaining: size }
                    };
                }
                DecoderState::ChunkData { remaining } => {
                    if self.buffer.is_empty() {
                        return Ok(());
                    }
                    let available =
                        u64::try_from(self.buffer.len()).expect("buffer lengths fit in u64");
                    let taken = remaining.min(available);
                    let taken_length =
                        usize::try_from(taken).expect("taken is at most the buffer length");
                    decoded.push(self.buffer.split_to(taken_length).freeze());
                    self.state = if taken == remaining {
                        DecoderState::ChunkDataEnd
                    } else {
                        DecoderState::ChunkData {
                            remaining: remaining - taken,
                        }
                    };
                }
                DecoderState::ChunkDataEnd => {
                    if self.buffer.len() < 2 {
                        return Ok(());
                    }
                    if &self.buffer[..2] != b"\r\n" {
                        return Err(S3Error::incomplete_body(
                            "aws-chunked data is not followed by CRLF",
                        ));
                    }
                    let _crlf = self.buffer.split_to(2);
                    self.state = DecoderState::ChunkHeader;
                }
                DecoderState::Trailers => {
                    let Some(line) = self.take_line()? else {
                        return Ok(());
                    };
                    if line.is_empty() {
                        self.state = DecoderState::Done;
                        continue;
                    }
                    let (name, value) = line.split_once(':').ok_or_else(|| {
                        S3Error::incomplete_body(format!("invalid aws-chunked trailer {line:?}"))
                    })?;
                    if self.trailers.len() == MAX_TRAILER_COUNT {
                        return Err(S3Error::incomplete_body(
                            "the aws-chunked body has too many trailers",
                        ));
                    }
                    self.trailers
                        .push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
                }
                DecoderState::Done => {
                    if !self.buffer.is_empty() {
                        return Err(S3Error::incomplete_body(
                            "unexpected bytes after the final aws-chunked chunk",
                        ));
                    }
                    return Ok(());
                }
            }
        }
    }

    /// Returns the trailers once the body ended exactly at the end of the encoding.
    pub(crate) fn finish(self) -> Result<Vec<(String, String)>, S3Error> {
        match self.state {
            DecoderState::Done => Ok(self.trailers),
            // Some clients omit the blank line after the final chunk when they send no trailers.
            DecoderState::Trailers if self.buffer.is_empty() => Ok(self.trailers),
            DecoderState::ChunkHeader
            | DecoderState::ChunkData { .. }
            | DecoderState::ChunkDataEnd
            | DecoderState::Trailers => {
                Err(S3Error::incomplete_body("the aws-chunked body ended early"))
            }
        }
    }

    fn take_line(&mut self) -> Result<Option<String>, S3Error> {
        let Some(end) = self.buffer.windows(2).position(|window| window == b"\r\n") else {
            if self.buffer.len() > MAX_LINE_LENGTH {
                return Err(S3Error::incomplete_body("aws-chunked line is too long"));
            }
            return Ok(None);
        };
        if end > MAX_LINE_LENGTH {
            return Err(S3Error::incomplete_body("aws-chunked line is too long"));
        }
        let line = self.buffer.split_to(end + 2);
        String::from_utf8(line[..end].to_vec())
            .map(Some)
            .map_err(|_| S3Error::incomplete_body("aws-chunked line is not UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIGNED_BODY: &[u8] = b"5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n0;chunk-signature=ghi\r\nx-amz-checksum-crc32:DUoRhQ==\r\nx-amz-trailer-signature:jkl\r\n\r\n";

    fn decode_in_slices(body: &[u8], slice_length: usize) -> (Vec<u8>, Vec<(String, String)>) {
        let mut decoder = AwsChunkedDecoder::new();
        let mut decoded = Vec::new();
        for slice in body.chunks(slice_length) {
            decoder.push(slice, &mut decoded).unwrap();
        }
        let trailers = decoder.finish().unwrap();
        (decoded.concat(), trailers)
    }

    #[test]
    fn decodes_regardless_of_slice_boundaries() {
        for slice_length in [1, 2, 3, 7, SIGNED_BODY.len()] {
            let (data, trailers) = decode_in_slices(SIGNED_BODY, slice_length);
            assert_eq!(data, b"hello world");
            assert_eq!(
                trailers[0],
                ("x-amz-checksum-crc32".to_owned(), "DUoRhQ==".to_owned())
            );
        }
    }

    #[test]
    fn rejects_truncated_bodies() {
        let mut decoder = AwsChunkedDecoder::new();
        let mut decoded = Vec::new();
        decoder.push(&SIGNED_BODY[..20], &mut decoded).unwrap();
        assert!(decoder.finish().is_err());
    }
}
