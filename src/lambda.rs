//! Lambda commands.

use std::io::{self, BufWriter};

use anyhow::{Context, Result};
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::InvocationType;
use aws_sdk_lambda::Client;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::Subcommand;
use serde_json::{json, Value};

use crate::common::{emit_json, emit_ndjson_line};

#[derive(Subcommand, Debug)]
pub enum LambdaCmd {
    /// Invoke a function. Payload defaults to `{}`.
    Invoke {
        name: String,
        /// JSON payload. `-` reads from stdin.
        #[arg(long, default_value = "{}")]
        payload: String,
        /// `request-response` (default, sync) or `event` (async).
        #[arg(long, default_value = "request-response")]
        invocation_type: String,
        /// Function version / alias qualifier.
        #[arg(long)]
        qualifier: Option<String>,
        /// Include execution log tail (sync only).
        #[arg(long)]
        log_tail: bool,
    },
    /// List functions.
    List,
}

pub async fn dispatch(cfg: &aws_config::SdkConfig, cmd: LambdaCmd) -> Result<()> {
    let client = Client::new(cfg);
    match cmd {
        LambdaCmd::Invoke {
            name,
            payload,
            invocation_type,
            qualifier,
            log_tail,
        } => {
            invoke(
                &client,
                &name,
                &payload,
                &invocation_type,
                qualifier.as_deref(),
                log_tail,
            )
            .await
        }
        LambdaCmd::List => list(&client).await,
    }
}

async fn invoke(
    client: &Client,
    name: &str,
    payload: &str,
    inv_type: &str,
    qualifier: Option<&str>,
    log_tail: bool,
) -> Result<()> {
    let raw = if payload == "-" {
        use tokio::io::AsyncReadExt;
        let mut buf = String::new();
        tokio::io::stdin().read_to_string(&mut buf).await?;
        buf
    } else {
        payload.to_string()
    };
    // Validate parses as JSON for clearer errors than the lambda runtime.
    let _v: Value = serde_json::from_str(&raw).context("payload must be JSON")?;

    let it = match inv_type.to_ascii_lowercase().as_str() {
        "request-response" | "sync" => InvocationType::RequestResponse,
        "event" | "async" => InvocationType::Event,
        "dry-run" | "dryrun" => InvocationType::DryRun,
        other => {
            anyhow::bail!("invocation_type: {other} (expected request-response|event|dry-run)")
        }
    };
    let log_type = if log_tail {
        aws_sdk_lambda::types::LogType::Tail
    } else {
        aws_sdk_lambda::types::LogType::None
    };

    let mut req = client
        .invoke()
        .function_name(name)
        .invocation_type(it)
        .log_type(log_type)
        .payload(Blob::new(raw.into_bytes()));
    if let Some(q) = qualifier {
        req = req.qualifier(q);
    }
    let resp = req.send().await.context("invoke")?;

    let body: Value = resp
        .payload
        .as_ref()
        .map(|b| {
            serde_json::from_slice(b.as_ref())
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(b.as_ref()).to_string()))
        })
        .unwrap_or(Value::Null);

    let log_decoded = resp.log_result().map(|s| {
        B64.decode(s)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_else(|| s.to_string())
    });

    emit_json(&json!({
        "status_code": resp.status_code(),
        "function_error": resp.function_error(),
        "executed_version": resp.executed_version(),
        "log_tail": log_decoded,
        "payload": body,
    }))
}

async fn list(client: &Client) -> Result<()> {
    let mut marker: Option<String> = None;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    loop {
        let mut req = client.list_functions();
        if let Some(m) = &marker {
            req = req.marker(m);
        }
        let resp = req.send().await.context("list_functions")?;
        for f in resp.functions() {
            emit_ndjson_line(
                &mut out,
                &json!({
                    "name": f.function_name(),
                    "runtime": f.runtime().map(|r| r.as_str().to_string()),
                    "arn": f.function_arn(),
                    "memory_mb": f.memory_size(),
                    "timeout_s": f.timeout(),
                    "last_modified": f.last_modified(),
                    "handler": f.handler(),
                }),
            )?;
        }
        match resp.next_marker {
            Some(m) => marker = Some(m),
            None => break,
        }
    }
    Ok(())
}
