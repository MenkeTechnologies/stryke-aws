//! SQS commands.

use std::io::{self, BufWriter};

use anyhow::{Context, Result};
use aws_sdk_sqs::Client;
use clap::Subcommand;
use serde_json::json;

use crate::common::{emit_json, emit_ndjson_line};

#[derive(Subcommand, Debug)]
pub enum SqsCmd {
    /// Send one message.
    Send {
        queue_url: String,
        #[arg(long)]
        body: String,
        /// Delay (FIFO and standard queues both accept this).
        #[arg(long)]
        delay_seconds: Option<i32>,
        /// FIFO: deduplication ID.
        #[arg(long)]
        dedup_id: Option<String>,
        /// FIFO: message group.
        #[arg(long)]
        group_id: Option<String>,
    },
    /// Long-poll receive. Emits NDJSON messages.
    Receive {
        queue_url: String,
        #[arg(long, default_value_t = 10)]
        max: i32,
        /// Long-poll seconds (0–20).
        #[arg(long, default_value_t = 20)]
        wait: i32,
        /// VisibilityTimeout override (seconds).
        #[arg(long)]
        visibility: Option<i32>,
    },
    /// Delete one message by receipt handle.
    Delete {
        queue_url: String,
        #[arg(long)]
        receipt: String,
    },
    /// Purge the queue. Use sparingly.
    Purge { queue_url: String },
    /// `GetQueueAttributes` → JSON.
    Attrs { queue_url: String },
    /// `ListQueues` with optional prefix.
    List {
        #[arg(long)]
        prefix: Option<String>,
    },
}

pub async fn dispatch(cfg: &aws_config::SdkConfig, cmd: SqsCmd) -> Result<()> {
    let client = Client::new(cfg);
    match cmd {
        SqsCmd::Send { queue_url, body, delay_seconds, dedup_id, group_id } => {
            send(&client, &queue_url, &body, delay_seconds, dedup_id.as_deref(), group_id.as_deref()).await
        }
        SqsCmd::Receive { queue_url, max, wait, visibility } => {
            receive(&client, &queue_url, max, wait, visibility).await
        }
        SqsCmd::Delete { queue_url, receipt } => delete(&client, &queue_url, &receipt).await,
        SqsCmd::Purge { queue_url } => purge(&client, &queue_url).await,
        SqsCmd::Attrs { queue_url } => attrs(&client, &queue_url).await,
        SqsCmd::List { prefix } => list(&client, prefix.as_deref()).await,
    }
}

async fn send(
    client: &Client,
    queue: &str,
    body: &str,
    delay: Option<i32>,
    dedup_id: Option<&str>,
    group_id: Option<&str>,
) -> Result<()> {
    let mut req = client.send_message().queue_url(queue).message_body(body);
    if let Some(d) = delay {
        req = req.delay_seconds(d);
    }
    if let Some(d) = dedup_id {
        req = req.message_deduplication_id(d);
    }
    if let Some(g) = group_id {
        req = req.message_group_id(g);
    }
    let resp = req.send().await.context("send_message")?;
    emit_json(&json!({
        "message_id": resp.message_id(),
        "md5_of_body": resp.md5_of_message_body(),
        "sequence_number": resp.sequence_number(),
    }))
}

async fn receive(
    client: &Client,
    queue: &str,
    max: i32,
    wait: i32,
    visibility: Option<i32>,
) -> Result<()> {
    let mut req = client
        .receive_message()
        .queue_url(queue)
        .max_number_of_messages(max.min(10))
        .wait_time_seconds(wait.clamp(0, 20));
    if let Some(v) = visibility {
        req = req.visibility_timeout(v);
    }
    let resp = req.send().await.context("receive_message")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for m in resp.messages() {
        emit_ndjson_line(
            &mut out,
            &json!({
                "message_id": m.message_id(),
                "receipt_handle": m.receipt_handle(),
                "md5": m.md5_of_body(),
                "body": m.body(),
            }),
        )?;
    }
    Ok(())
}

async fn delete(client: &Client, queue: &str, receipt: &str) -> Result<()> {
    client
        .delete_message()
        .queue_url(queue)
        .receipt_handle(receipt)
        .send()
        .await
        .context("delete_message")?;
    emit_json(&json!({ "ok": true }))
}

async fn purge(client: &Client, queue: &str) -> Result<()> {
    client
        .purge_queue()
        .queue_url(queue)
        .send()
        .await
        .context("purge_queue")?;
    emit_json(&json!({ "ok": true }))
}

async fn attrs(client: &Client, queue: &str) -> Result<()> {
    let resp = client
        .get_queue_attributes()
        .queue_url(queue)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::All)
        .send()
        .await
        .context("get_queue_attributes")?;
    let mut out = serde_json::Map::new();
    if let Some(a) = resp.attributes() {
        for (k, v) in a {
            out.insert(k.as_str().to_string(), json!(v));
        }
    }
    emit_json(&serde_json::Value::Object(out))
}

async fn list(client: &Client, prefix: Option<&str>) -> Result<()> {
    let mut next: Option<String> = None;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    loop {
        let mut req = client.list_queues();
        if let Some(p) = prefix {
            req = req.queue_name_prefix(p);
        }
        if let Some(t) = &next {
            req = req.next_token(t);
        }
        let resp = req.send().await.context("list_queues")?;
        for url in resp.queue_urls() {
            emit_ndjson_line(&mut out, &json!({ "url": url }))?;
        }
        match resp.next_token {
            Some(t) => next = Some(t),
            None => break,
        }
    }
    Ok(())
}
