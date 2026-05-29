//! S3 commands.

use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use clap::Subcommand;
use serde_json::json;

use crate::common::{
    emit_json, emit_ndjson_line, parse_s3_uri, read_input_bytes, write_output_bytes,
};

#[derive(Subcommand, Debug)]
pub enum S3Cmd {
    /// List objects under `s3://bucket/prefix`. Streams NDJSON.
    Ls {
        uri: String,
        /// Maximum keys per page (AWS caps at 1000).
        #[arg(long, default_value_t = 1000)]
        page_size: i32,
        /// Stop after this many keys total (across pages).
        #[arg(long)]
        limit: Option<usize>,
        /// List only the immediate "directory" — group by `/` delimiter.
        #[arg(long)]
        delimiter: Option<String>,
    },
    /// Fetch an object's body. `-` for stdout.
    Get {
        uri: String,
        #[arg(long, default_value = "-")]
        output: String,
    },
    /// Upload bytes to `s3://bucket/key`. `-` for stdin.
    Put {
        uri: String,
        #[arg(long, default_value = "-")]
        input: String,
        #[arg(long)]
        content_type: Option<String>,
        #[arg(long)]
        cache_control: Option<String>,
        /// Read input fully into memory before uploading (default; safer
        /// for small/medium files). Off: stream via ByteStream from path.
        #[arg(long, default_value_t = true)]
        buffered: bool,
    },
    /// HEAD an object → metadata.
    Head { uri: String },
    /// Delete an object.
    Rm { uri: String },
    /// Generate a presigned URL.
    Presign {
        uri: String,
        /// `GET` or `PUT`. Default `GET`.
        #[arg(long, default_value = "GET")]
        method: String,
        /// Seconds until the URL expires.
        #[arg(long, default_value_t = 3600)]
        expires: u64,
    },
    /// List buckets.
    Buckets,
}

pub async fn dispatch(cfg: &aws_config::SdkConfig, cmd: S3Cmd) -> Result<()> {
    let client = Client::new(cfg);
    match cmd {
        S3Cmd::Ls {
            uri,
            page_size,
            limit,
            delimiter,
        } => ls(&client, &uri, page_size, limit, delimiter.as_deref()).await,
        S3Cmd::Get { uri, output } => get(&client, &uri, &output).await,
        S3Cmd::Put {
            uri,
            input,
            content_type,
            cache_control,
            buffered,
        } => {
            put(
                &client,
                &uri,
                &input,
                content_type.as_deref(),
                cache_control.as_deref(),
                buffered,
            )
            .await
        }
        S3Cmd::Head { uri } => head(&client, &uri).await,
        S3Cmd::Rm { uri } => rm(&client, &uri).await,
        S3Cmd::Presign {
            uri,
            method,
            expires,
        } => presign(&client, &uri, &method, expires).await,
        S3Cmd::Buckets => buckets(&client).await,
    }
}

async fn ls(
    client: &Client,
    uri: &str,
    page_size: i32,
    limit: Option<usize>,
    delimiter: Option<&str>,
) -> Result<()> {
    let (bucket, prefix) = parse_s3_uri(uri)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let mut continuation: Option<String> = None;
    let mut emitted: usize = 0;

    loop {
        let mut req = client.list_objects_v2().bucket(&bucket).max_keys(page_size);
        if !prefix.is_empty() {
            req = req.prefix(&prefix);
        }
        if let Some(d) = delimiter {
            req = req.delimiter(d);
        }
        if let Some(ct) = &continuation {
            req = req.continuation_token(ct);
        }
        let resp = req.send().await.context("list_objects_v2")?;

        // Common prefixes (when --delimiter is set) come back as "directories".
        for cp in resp.common_prefixes() {
            if let Some(p) = cp.prefix() {
                emit_ndjson_line(&mut out, &json!({ "type": "prefix", "key": p }))?;
                emitted += 1;
                if limit.is_some_and(|l| emitted >= l) {
                    return Ok(());
                }
            }
        }

        for obj in resp.contents() {
            let key = obj.key().unwrap_or("");
            let size = obj.size().unwrap_or(0);
            let etag = obj.e_tag().map(|s| s.trim_matches('"').to_string());
            let last_modified = obj.last_modified().map(|t| t.to_string());
            let storage_class = obj.storage_class().map(|s| s.as_str().to_string());

            emit_ndjson_line(
                &mut out,
                &json!({
                    "type": "object",
                    "key": key,
                    "size": size,
                    "etag": etag,
                    "last_modified": last_modified,
                    "storage_class": storage_class,
                }),
            )?;
            emitted += 1;
            if limit.is_some_and(|l| emitted >= l) {
                return Ok(());
            }
        }

        if resp.is_truncated().unwrap_or(false) {
            continuation = resp.next_continuation_token().map(|s| s.to_string());
            if continuation.is_none() {
                break;
            }
        } else {
            break;
        }
    }
    out.flush()?;
    Ok(())
}

async fn get(client: &Client, uri: &str, output: &str) -> Result<()> {
    let (bucket, key) = parse_s3_uri(uri)?;
    if key.is_empty() {
        anyhow::bail!("get needs a full object URI (s3://bucket/key)");
    }
    let resp = client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .context("get_object")?;
    let data = resp
        .body
        .collect()
        .await
        .context("collecting body")?
        .into_bytes();
    write_output_bytes(output, &data).await?;
    Ok(())
}

async fn put(
    client: &Client,
    uri: &str,
    input: &str,
    content_type: Option<&str>,
    cache_control: Option<&str>,
    buffered: bool,
) -> Result<()> {
    let (bucket, key) = parse_s3_uri(uri)?;
    if key.is_empty() {
        anyhow::bail!("put needs a full object URI (s3://bucket/key)");
    }
    let body = if buffered || input == "-" {
        let bytes = read_input_bytes(input).await?;
        ByteStream::from(bytes)
    } else {
        ByteStream::from_path(PathBuf::from(input))
            .await
            .with_context(|| format!("opening {input}"))?
    };

    let mut req = client.put_object().bucket(&bucket).key(&key).body(body);
    if let Some(ct) = content_type {
        req = req.content_type(ct);
    }
    if let Some(cc) = cache_control {
        req = req.cache_control(cc);
    }
    let resp = req.send().await.context("put_object")?;
    emit_json(&json!({
        "bucket": bucket,
        "key": key,
        "etag": resp.e_tag().map(|s| s.trim_matches('"').to_string()),
        "version_id": resp.version_id(),
    }))
}

async fn head(client: &Client, uri: &str) -> Result<()> {
    let (bucket, key) = parse_s3_uri(uri)?;
    let resp = client
        .head_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .context("head_object")?;
    emit_json(&json!({
        "bucket": bucket,
        "key": key,
        "size": resp.content_length(),
        "etag": resp.e_tag().map(|s| s.trim_matches('"').to_string()),
        "content_type": resp.content_type(),
        "cache_control": resp.cache_control(),
        "last_modified": resp.last_modified().map(|t| t.to_string()),
        "storage_class": resp.storage_class().map(|s| s.as_str().to_string()),
        "version_id": resp.version_id(),
    }))
}

async fn rm(client: &Client, uri: &str) -> Result<()> {
    let (bucket, key) = parse_s3_uri(uri)?;
    if key.is_empty() {
        anyhow::bail!("rm needs a full object URI (s3://bucket/key)");
    }
    client
        .delete_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .context("delete_object")?;
    emit_json(&json!({ "bucket": bucket, "key": key, "deleted": true }))
}

async fn presign(client: &Client, uri: &str, method: &str, expires: u64) -> Result<()> {
    let (bucket, key) = parse_s3_uri(uri)?;
    let cfg =
        PresigningConfig::expires_in(Duration::from_secs(expires)).context("invalid expires")?;
    let url = match method.to_ascii_uppercase().as_str() {
        "GET" => client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .presigned(cfg)
            .await
            .context("presigning GET")?
            .uri()
            .to_string(),
        "PUT" => client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .presigned(cfg)
            .await
            .context("presigning PUT")?
            .uri()
            .to_string(),
        other => anyhow::bail!("presign method must be GET or PUT (got {other})"),
    };
    emit_json(&json!({
        "method": method.to_ascii_uppercase(),
        "bucket": bucket,
        "key": key,
        "expires_in": expires,
        "url": url,
    }))
}

async fn buckets(client: &Client) -> Result<()> {
    let resp = client.list_buckets().send().await.context("list_buckets")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for b in resp.buckets() {
        emit_ndjson_line(
            &mut out,
            &json!({
                "name": b.name(),
                "creation_date": b.creation_date().map(|t| t.to_string()),
            }),
        )?;
    }
    out.flush()?;
    Ok(())
}
