//! Shared helpers used by every service module: AWS config loader, S3 URI
//! parser, output writers.

use std::io::{self, BufWriter, Write};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use aws_config::{BehaviorVersion, Region};

/// Build a shared AWS config from the parent CLI options. The resulting
/// `SdkConfig` is then passed to each service client's `::new(&cfg)`.
pub async fn load_aws_config(
    region: Option<&str>,
    profile: Option<&str>,
    endpoint: Option<&str>,
) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(r) = region {
        loader = loader.region(Region::new(r.to_string()));
    }
    if let Some(p) = profile {
        loader = loader.profile_name(p);
    }
    if let Some(ep) = endpoint {
        loader = loader.endpoint_url(ep);
    }
    loader = loader.timeout_config(
        aws_config::timeout::TimeoutConfig::builder()
            .operation_timeout(Duration::from_secs(60))
            .build(),
    );
    loader.load().await
}

/// Write a single JSON object to stdout, followed by a newline.
pub fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

/// Write a single NDJSON line to stdout. Use in `for` loops to stream rows.
pub fn emit_ndjson_line<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

/// `s3://bucket/key/path/with/slashes` → `("bucket", "key/path/with/slashes")`.
/// Returns `("bucket", "")` when only the bucket is given.
pub fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow!("expected `s3://bucket/key…`, got `{uri}`"))?;
    if let Some((bucket, key)) = rest.split_once('/') {
        Ok((bucket.to_string(), key.to_string()))
    } else {
        Ok((rest.to_string(), String::new()))
    }
}

/// Read `--input PATH` into a Vec. `-` means stdin.
pub async fn read_input_bytes(path: &str) -> Result<Vec<u8>> {
    if path == "-" {
        let mut buf = Vec::new();
        use tokio::io::AsyncReadExt;
        let mut stdin = tokio::io::stdin();
        stdin
            .read_to_end(&mut buf)
            .await
            .context("reading stdin")?;
        Ok(buf)
    } else {
        tokio::fs::read(path)
            .await
            .with_context(|| format!("reading {path}"))
    }
}

/// Write bytes to `--output PATH`. `-` means stdout.
pub async fn write_output_bytes(path: &str, bytes: &[u8]) -> Result<()> {
    if path == "-" {
        use tokio::io::AsyncWriteExt;
        let mut stdout = tokio::io::stdout();
        stdout.write_all(bytes).await?;
        stdout.flush().await?;
    } else {
        tokio::fs::write(path, bytes)
            .await
            .with_context(|| format!("writing {path}"))?;
    }
    Ok(())
}
