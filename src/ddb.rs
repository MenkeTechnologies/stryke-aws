//! DynamoDB commands. Plain-JSON `{"id": 42, "name": "x"}` round-trips to
//! and from the wire-level AttributeValue map automatically — stryke users
//! never see `{"S": "..."}` envelopes.

use std::collections::HashMap;
use std::io::{self, BufRead, BufReader, BufWriter, Write};

use anyhow::{anyhow, bail, Context, Result};
use aws_sdk_dynamodb::types::{AttributeValue, WriteRequest, DeleteRequest, PutRequest};
use aws_sdk_dynamodb::Client;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::Subcommand;
use serde_json::{json, Map as JMap, Value};

use crate::common::{emit_json, emit_ndjson_line};

#[derive(Subcommand, Debug)]
pub enum DdbCmd {
    /// Fetch a single item by key. `--key='{"id":42}'`
    Get {
        table: String,
        #[arg(long)]
        key: String,
        /// Force consistent read.
        #[arg(long)]
        consistent: bool,
    },
    /// Insert / overwrite one item. `--item='{"id":42,"name":"x"}'`
    Put {
        table: String,
        #[arg(long)]
        item: String,
    },
    /// Delete one item by key.
    Delete {
        table: String,
        #[arg(long)]
        key: String,
    },
    /// Run a Query. `--expr='id = :id' --vals='{":id":42}'`
    Query {
        table: String,
        #[arg(long)]
        expr: String,
        #[arg(long)]
        vals: Option<String>,
        #[arg(long)]
        names: Option<String>,
        #[arg(long)]
        index: Option<String>,
        #[arg(long)]
        filter: Option<String>,
        #[arg(long)]
        limit: Option<i32>,
        #[arg(long)]
        consistent: bool,
    },
    /// Scan a table (full read). Streams NDJSON items.
    Scan {
        table: String,
        #[arg(long)]
        filter: Option<String>,
        #[arg(long)]
        vals: Option<String>,
        #[arg(long)]
        names: Option<String>,
        #[arg(long)]
        index: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        page_size: Option<i32>,
    },
    /// Bulk PutItem / DeleteItem from NDJSON on stdin. Each line is either
    /// an item to put, or `{"_delete": {...key...}}` to delete.
    BatchWrite { table: String },
    /// List tables.
    Tables,
    /// Describe one table.
    Describe { table: String },
}

pub async fn dispatch(cfg: &aws_config::SdkConfig, cmd: DdbCmd) -> Result<()> {
    let client = Client::new(cfg);
    match cmd {
        DdbCmd::Get { table, key, consistent } => get(&client, &table, &key, consistent).await,
        DdbCmd::Put { table, item } => put(&client, &table, &item).await,
        DdbCmd::Delete { table, key } => delete(&client, &table, &key).await,
        DdbCmd::Query {
            table, expr, vals, names, index, filter, limit, consistent,
        } => query(&client, &table, &expr, vals.as_deref(), names.as_deref(),
                   index.as_deref(), filter.as_deref(), limit, consistent).await,
        DdbCmd::Scan {
            table, filter, vals, names, index, limit, page_size,
        } => scan(&client, &table, filter.as_deref(), vals.as_deref(),
                  names.as_deref(), index.as_deref(), limit, page_size).await,
        DdbCmd::BatchWrite { table } => batch_write(&client, &table).await,
        DdbCmd::Tables => tables(&client).await,
        DdbCmd::Describe { table } => describe(&client, &table).await,
    }
}

/* ------------------------------------------------------------------------- */
/* AttributeValue ↔ JSON                                                      */
/* ------------------------------------------------------------------------- */

pub fn json_to_av(v: &Value) -> AttributeValue {
    match v {
        Value::Null => AttributeValue::Null(true),
        Value::Bool(b) => AttributeValue::Bool(*b),
        Value::Number(n) => AttributeValue::N(n.to_string()),
        Value::String(s) => {
            // Preserve the binary sentinel from our row encoding.
            if let Some(rest) = s.strip_prefix("base64:") {
                if let Ok(bytes) = B64.decode(rest) {
                    return AttributeValue::B(aws_smithy_types::Blob::new(bytes));
                }
            }
            AttributeValue::S(s.clone())
        }
        Value::Array(arr) => {
            AttributeValue::L(arr.iter().map(json_to_av).collect())
        }
        Value::Object(obj) => {
            let m: HashMap<String, AttributeValue> = obj
                .iter()
                .map(|(k, vv)| (k.clone(), json_to_av(vv)))
                .collect();
            AttributeValue::M(m)
        }
    }
}

pub fn av_to_json(av: &AttributeValue) -> Value {
    match av {
        AttributeValue::S(s) => Value::String(s.clone()),
        AttributeValue::N(n) => {
            // DDB stores numbers as strings to preserve precision; parse
            // back to JSON number when it fits, otherwise leave as string.
            if let Ok(i) = n.parse::<i64>() {
                json!(i)
            } else if let Ok(f) = n.parse::<f64>() {
                json!(f)
            } else {
                Value::String(n.clone())
            }
        }
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::Null(_) => Value::Null,
        AttributeValue::B(b) => {
            let mut out = String::from("base64:");
            out.push_str(&B64.encode(b.as_ref()));
            Value::String(out)
        }
        AttributeValue::Ss(arr) => json!(arr),
        AttributeValue::Ns(arr) => Value::Array(
            arr.iter()
                .map(|n| {
                    if let Ok(i) = n.parse::<i64>() {
                        json!(i)
                    } else if let Ok(f) = n.parse::<f64>() {
                        json!(f)
                    } else {
                        Value::String(n.clone())
                    }
                })
                .collect(),
        ),
        AttributeValue::Bs(arr) => Value::Array(
            arr.iter()
                .map(|b| {
                    let mut s = String::from("base64:");
                    s.push_str(&B64.encode(b.as_ref()));
                    Value::String(s)
                })
                .collect(),
        ),
        AttributeValue::L(arr) => Value::Array(arr.iter().map(av_to_json).collect()),
        AttributeValue::M(m) => {
            let mut out = JMap::new();
            for (k, v) in m {
                out.insert(k.clone(), av_to_json(v));
            }
            Value::Object(out)
        }
        _ => Value::Null, // forward-compat for new AV variants
    }
}

pub fn json_obj_to_av_map(s: &str) -> Result<HashMap<String, AttributeValue>> {
    let v: Value = serde_json::from_str(s).context("parsing JSON object")?;
    match v {
        Value::Object(o) => Ok(o
            .into_iter()
            .map(|(k, vv)| (k, json_to_av(&vv)))
            .collect()),
        _ => bail!("expected a JSON object"),
    }
}

pub fn av_map_to_json(m: &HashMap<String, AttributeValue>) -> Value {
    let mut out = JMap::new();
    for (k, v) in m {
        out.insert(k.clone(), av_to_json(v));
    }
    Value::Object(out)
}

/* ------------------------------------------------------------------------- */
/* commands                                                                  */
/* ------------------------------------------------------------------------- */

async fn get(client: &Client, table: &str, key_json: &str, consistent: bool) -> Result<()> {
    let key = json_obj_to_av_map(key_json)?;
    let mut req = client.get_item().table_name(table).set_key(Some(key));
    if consistent {
        req = req.consistent_read(true);
    }
    let resp = req.send().await.context("get_item")?;
    match resp.item {
        Some(m) => emit_json(&av_map_to_json(&m)),
        None => emit_json(&Value::Null),
    }
}

async fn put(client: &Client, table: &str, item_json: &str) -> Result<()> {
    let item = if item_json == "-" {
        let mut buf = String::new();
        std::io::stdin().read_line(&mut buf)?;
        json_obj_to_av_map(buf.trim())?
    } else {
        json_obj_to_av_map(item_json)?
    };
    client
        .put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .context("put_item")?;
    emit_json(&json!({ "ok": true }))
}

async fn delete(client: &Client, table: &str, key_json: &str) -> Result<()> {
    let key = json_obj_to_av_map(key_json)?;
    client
        .delete_item()
        .table_name(table)
        .set_key(Some(key))
        .send()
        .await
        .context("delete_item")?;
    emit_json(&json!({ "ok": true }))
}

fn parse_vals(s: Option<&str>) -> Result<Option<HashMap<String, AttributeValue>>> {
    let Some(raw) = s else {
        return Ok(None);
    };
    Ok(Some(json_obj_to_av_map(raw)?))
}

fn parse_names(s: Option<&str>) -> Result<Option<HashMap<String, String>>> {
    let Some(raw) = s else {
        return Ok(None);
    };
    let v: Value = serde_json::from_str(raw).context("parsing --names JSON")?;
    let Value::Object(obj) = v else {
        bail!("--names must be a JSON object");
    };
    Ok(Some(
        obj.into_iter()
            .map(|(k, vv)| {
                let s = vv.as_str().map(String::from).unwrap_or_default();
                (k, s)
            })
            .collect(),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn query(
    client: &Client,
    table: &str,
    expr: &str,
    vals: Option<&str>,
    names: Option<&str>,
    index: Option<&str>,
    filter: Option<&str>,
    limit: Option<i32>,
    consistent: bool,
) -> Result<()> {
    let vals = parse_vals(vals)?;
    let names = parse_names(names)?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let mut last_key: Option<HashMap<String, AttributeValue>> = None;
    let mut emitted: usize = 0;

    loop {
        let mut req = client
            .query()
            .table_name(table)
            .key_condition_expression(expr);
        if let Some(v) = &vals {
            req = req.set_expression_attribute_values(Some(v.clone()));
        }
        if let Some(n) = &names {
            req = req.set_expression_attribute_names(Some(n.clone()));
        }
        if let Some(i) = index {
            req = req.index_name(i);
        }
        if let Some(f) = filter {
            req = req.filter_expression(f);
        }
        if let Some(l) = limit {
            req = req.limit(l);
        }
        if consistent {
            req = req.consistent_read(true);
        }
        if let Some(lk) = &last_key {
            req = req.set_exclusive_start_key(Some(lk.clone()));
        }
        let resp = req.send().await.context("query")?;

        for item in resp.items() {
            emit_ndjson_line(&mut out, &av_map_to_json(item))?;
            emitted += 1;
            if let Some(l) = limit {
                if emitted as i32 >= l {
                    return Ok(());
                }
            }
        }

        match resp.last_evaluated_key {
            Some(k) if !k.is_empty() => {
                last_key = Some(k);
            }
            _ => break,
        }
    }
    out.flush()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn scan(
    client: &Client,
    table: &str,
    filter: Option<&str>,
    vals: Option<&str>,
    names: Option<&str>,
    index: Option<&str>,
    limit: Option<usize>,
    page_size: Option<i32>,
) -> Result<()> {
    let vals = parse_vals(vals)?;
    let names = parse_names(names)?;

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let mut last_key: Option<HashMap<String, AttributeValue>> = None;
    let mut emitted: usize = 0;

    loop {
        let mut req = client.scan().table_name(table);
        if let Some(v) = &vals {
            req = req.set_expression_attribute_values(Some(v.clone()));
        }
        if let Some(n) = &names {
            req = req.set_expression_attribute_names(Some(n.clone()));
        }
        if let Some(i) = index {
            req = req.index_name(i);
        }
        if let Some(f) = filter {
            req = req.filter_expression(f);
        }
        if let Some(ps) = page_size {
            req = req.limit(ps);
        }
        if let Some(lk) = &last_key {
            req = req.set_exclusive_start_key(Some(lk.clone()));
        }
        let resp = req.send().await.context("scan")?;

        for item in resp.items() {
            emit_ndjson_line(&mut out, &av_map_to_json(item))?;
            emitted += 1;
            if let Some(l) = limit {
                if emitted >= l {
                    return Ok(());
                }
            }
        }

        match resp.last_evaluated_key {
            Some(k) if !k.is_empty() => {
                last_key = Some(k);
            }
            _ => break,
        }
    }
    out.flush()?;
    Ok(())
}

async fn batch_write(client: &Client, table: &str) -> Result<()> {
    let stdin = io::stdin();
    let reader = BufReader::new(stdin.lock());
    let mut batch: Vec<WriteRequest> = Vec::with_capacity(25);
    let mut total: u64 = 0;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(&line).context("parsing NDJSON line")?;

        let req = match &v {
            Value::Object(obj) if obj.contains_key("_delete") => {
                let key_val = obj.get("_delete").ok_or_else(|| anyhow!("_delete missing"))?;
                let key_obj = key_val
                    .as_object()
                    .ok_or_else(|| anyhow!("_delete must be an object"))?;
                let key: HashMap<String, AttributeValue> = key_obj
                    .iter()
                    .map(|(k, vv)| (k.clone(), json_to_av(vv)))
                    .collect();
                WriteRequest::builder()
                    .delete_request(DeleteRequest::builder().set_key(Some(key)).build()?)
                    .build()
            }
            Value::Object(_) => {
                let item: HashMap<String, AttributeValue> = match v {
                    Value::Object(o) => o.into_iter().map(|(k, vv)| (k, json_to_av(&vv))).collect(),
                    _ => unreachable!(),
                };
                WriteRequest::builder()
                    .put_request(PutRequest::builder().set_item(Some(item)).build()?)
                    .build()
            }
            _ => bail!("each NDJSON line must be a JSON object (item or {{_delete: key}})"),
        };

        batch.push(req);
        if batch.len() == 25 {
            flush_batch(client, table, &mut batch).await?;
            total += 25;
        }
    }
    if !batch.is_empty() {
        let n = batch.len() as u64;
        flush_batch(client, table, &mut batch).await?;
        total += n;
    }
    emit_json(&json!({ "written": total }))
}

async fn flush_batch(
    client: &Client,
    table: &str,
    batch: &mut Vec<WriteRequest>,
) -> Result<()> {
    let mut req_items = HashMap::new();
    req_items.insert(table.to_string(), std::mem::take(batch));
    let resp = client
        .batch_write_item()
        .set_request_items(Some(req_items))
        .send()
        .await
        .context("batch_write_item")?;
    // Retry unprocessed items inline (simple linear back-off).
    if let Some(unproc) = resp.unprocessed_items {
        if let Some(remaining) = unproc.get(table) {
            if !remaining.is_empty() {
                *batch = remaining.clone();
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                if !batch.is_empty() {
                    // Avoid infinite recursion in pathological cases.
                    let again = std::mem::take(batch);
                    let mut retry = HashMap::new();
                    retry.insert(table.to_string(), again);
                    client
                        .batch_write_item()
                        .set_request_items(Some(retry))
                        .send()
                        .await
                        .context("batch_write_item retry")?;
                }
            }
        }
    }
    Ok(())
}

async fn tables(client: &Client) -> Result<()> {
    let mut start: Option<String> = None;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    loop {
        let mut req = client.list_tables();
        if let Some(s) = &start {
            req = req.exclusive_start_table_name(s);
        }
        let resp = req.send().await.context("list_tables")?;
        for name in resp.table_names() {
            emit_ndjson_line(&mut out, &json!({ "name": name }))?;
        }
        match resp.last_evaluated_table_name {
            Some(n) => start = Some(n),
            None => break,
        }
    }
    out.flush()?;
    Ok(())
}

async fn describe(client: &Client, table: &str) -> Result<()> {
    let resp = client
        .describe_table()
        .table_name(table)
        .send()
        .await
        .context("describe_table")?;
    let t = resp.table.ok_or_else(|| anyhow!("no table in response"))?;
    let key_schema: Vec<Value> = t
        .key_schema()
        .iter()
        .map(|k| {
            json!({
                "attribute": k.attribute_name(),
                "type": k.key_type().as_str(),
            })
        })
        .collect();
    let attrs: Vec<Value> = t
        .attribute_definitions()
        .iter()
        .map(|a| {
            json!({
                "name": a.attribute_name(),
                "type": a.attribute_type().as_str(),
            })
        })
        .collect();
    emit_json(&json!({
        "name": t.table_name(),
        "status": t.table_status().map(|s| s.as_str().to_string()),
        "item_count": t.item_count(),
        "size_bytes": t.table_size_bytes(),
        "key_schema": key_schema,
        "attributes": attrs,
        "arn": t.table_arn(),
    }))
}
