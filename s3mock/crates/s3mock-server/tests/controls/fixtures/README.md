The AWS streaming fixture uses the published seed and three chunk signatures from
https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html.
The payload consists of 65536 and 1024 ASCII `a` bytes followed by the zero chunk.
The test never computes a signature with the server implementation or its Rust test signer.

The minio trailer fixture was signed with the independent review’s Python HMAC
signer, which reproduced the AWS known answers. Its LF followed by CRLF trailer
framing and CRC32C value reproduce the captured minio-go v7.0.90 upload described
in the review. Host is fixed to `s3.amazonaws.com` so the bytes stay constant when
the loopback listener port changes. All credentials are AWS’s public example credentials.
