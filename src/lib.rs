//! stryke-aws — AWS cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn aws__*` is a JSON-string-in /
//! JSON-string-out wrapper around the official `aws-sdk-rust` crates.
//! stryke's FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these
//! symbols at first `use AWS`.
//!
//! Persistent state:
//!   * `RUNTIME` — one shared `tokio` runtime drives every async call.
//!   * `CONFIGS` — `aws_config::SdkConfig` cache per region. The v1
//!     helper rebuilt the SDK config (creds chain + region resolve) per
//!     fork — paying full IMDS/SSO/env lookup each call.
//!
//! v0.2.0 surface: focused subset across S3, DynamoDB, SQS, Lambda,
//! STS. The v1 helper's broader op set can be added incrementally;
//! every export here is one entry point per (service, operation).

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

use anyhow::{anyhow, Result};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};

// ── runtime + config cache ──────────────────────────────────────────────────

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

static CONFIGS: OnceCell<Mutex<HashMap<String, SdkConfig>>> = OnceCell::new();

fn configs() -> &'static Mutex<HashMap<String, SdkConfig>> {
    CONFIGS.get_or_init(|| Mutex::new(HashMap::new()))
}

async fn get_config(opts: &Value) -> SdkConfig {
    let region = opts
        .get("region")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| std::env::var("AWS_REGION").ok())
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());
    {
        let map = configs().lock();
        if let Some(c) = map.get(&region) {
            return c.clone();
        }
    }
    let cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(region.clone()))
        .load()
        .await;
    configs().lock().insert(region, cfg.clone());
    cfg
}

// ── shared helpers ───────────────────────────────────────────────────────────

/// JSON scalar → DynamoDB AttributeValue. Mirrors the inline conversion in
/// `op_ddb_put_item`: strings → S, numbers → N, bools → BOOL, null → NULL,
/// anything else stringified to S.
fn json_to_av(v: &Value) -> aws_sdk_dynamodb::types::AttributeValue {
    use aws_sdk_dynamodb::types::AttributeValue;
    match v {
        Value::String(s) => AttributeValue::S(s.clone()),
        Value::Number(n) => AttributeValue::N(n.to_string()),
        Value::Bool(b) => AttributeValue::Bool(*b),
        Value::Null => AttributeValue::Null(true),
        other => AttributeValue::S(other.to_string()),
    }
}

/// Accept a JSON array of strings or a single string; return Vec<String>.
fn string_vec(v: &Value) -> Result<Vec<String>> {
    match v {
        Value::Array(a) => a
            .iter()
            .map(|x| {
                x.as_str()
                    .map(String::from)
                    .ok_or_else(|| anyhow!("non-string in array"))
            })
            .collect(),
        Value::String(s) => Ok(vec![s.clone()]),
        Value::Null => Ok(Vec::new()),
        _ => Err(anyhow!("expected string or array of strings")),
    }
}

// ── S3 ──────────────────────────────────────────────────────────────────────

async fn op_s3_list_buckets(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let r = client.list_buckets().send().await?;
    let names: Vec<String> = r
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(String::from))
        .collect();
    Ok(json!({"buckets": names}))
}

async fn op_s3_list_objects(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?
        .to_string();
    let prefix = opts["prefix"].as_str().unwrap_or("");
    let r = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(prefix)
        .send()
        .await?;
    let keys: Vec<Value> = r
        .contents()
        .iter()
        .map(|o| {
            json!({
                "key": o.key().unwrap_or(""),
                "size": o.size(),
            })
        })
        .collect();
    Ok(json!({"bucket": bucket, "objects": keys}))
}

async fn op_s3_get_object(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let key = opts["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
    let r = client.get_object().bucket(bucket).key(key).send().await?;
    let bytes = r.body.collect().await?.into_bytes();
    let body = match std::str::from_utf8(&bytes) {
        Ok(s) => Value::String(s.to_string()),
        Err(_) => {
            use base64::Engine as _;
            Value::String(format!(
                "base64:{}",
                base64::engine::general_purpose::STANDARD.encode(&bytes)
            ))
        }
    };
    Ok(json!({"bucket": bucket, "key": key, "body": body}))
}

async fn op_s3_put_object(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?
        .to_string();
    let key = opts["key"]
        .as_str()
        .ok_or_else(|| anyhow!("missing key"))?
        .to_string();
    let body = opts["body"].as_str().unwrap_or("").as_bytes().to_vec();
    let r = client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .body(body.into())
        .send()
        .await?;
    Ok(json!({
        "bucket": bucket,
        "key": key,
        "etag": r.e_tag().unwrap_or(""),
    }))
}

async fn op_s3_delete_object(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?
        .to_string();
    let key = opts["key"]
        .as_str()
        .ok_or_else(|| anyhow!("missing key"))?
        .to_string();
    client
        .delete_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await?;
    Ok(json!({"bucket": bucket, "key": key, "deleted": true}))
}

// ── STS ─────────────────────────────────────────────────────────────────────

async fn op_sts_caller_identity(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sts::Client::new(&cfg);
    let r = client.get_caller_identity().send().await?;
    Ok(json!({
        "account": r.account().unwrap_or(""),
        "arn": r.arn().unwrap_or(""),
        "user_id": r.user_id().unwrap_or(""),
    }))
}

// ── DynamoDB ────────────────────────────────────────────────────────────────

async fn op_ddb_list_tables(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let r = client.list_tables().send().await?;
    Ok(json!({"tables": r.table_names()}))
}

async fn op_ddb_put_item(opts: Value) -> Result<Value> {
    use aws_sdk_dynamodb::types::AttributeValue;
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    let item = opts["item"]
        .as_object()
        .ok_or_else(|| anyhow!("missing item (object)"))?
        .clone();
    let mut req = client.put_item().table_name(&table);
    for (k, v) in item {
        let av = match v {
            Value::String(s) => AttributeValue::S(s),
            Value::Number(n) => AttributeValue::N(n.to_string()),
            Value::Bool(b) => AttributeValue::Bool(b),
            Value::Null => AttributeValue::Null(true),
            other => AttributeValue::S(other.to_string()),
        };
        req = req.item(k, av);
    }
    req.send().await?;
    Ok(json!({"table": table, "put": true}))
}

async fn op_ddb_get_item(opts: Value) -> Result<Value> {
    use aws_sdk_dynamodb::types::AttributeValue;
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    let key = opts["key"]
        .as_object()
        .ok_or_else(|| anyhow!("missing key (object)"))?
        .clone();
    let mut req = client.get_item().table_name(&table);
    for (k, v) in key {
        let av = match v {
            Value::String(s) => AttributeValue::S(s),
            Value::Number(n) => AttributeValue::N(n.to_string()),
            other => AttributeValue::S(other.to_string()),
        };
        req = req.key(k, av);
    }
    let r = req.send().await?;
    let item = r.item().map(|m| {
        let obj: serde_json::Map<String, Value> = m
            .iter()
            .map(|(k, v)| (k.clone(), attribute_value_to_json(v)))
            .collect();
        Value::Object(obj)
    });
    Ok(json!({"table": table, "item": item}))
}

fn attribute_value_to_json(v: &aws_sdk_dynamodb::types::AttributeValue) -> Value {
    use aws_sdk_dynamodb::types::AttributeValue;
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD;
    match v {
        AttributeValue::S(s) => Value::String(s.clone()),
        AttributeValue::N(n) => n
            .parse::<i64>()
            .map(|n| json!(n))
            .or_else(|_| n.parse::<f64>().map(|f| json!(f)))
            .unwrap_or_else(|_| Value::String(n.clone())),
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::Null(_) => Value::Null,
        AttributeValue::Ss(arr) => json!(arr),
        AttributeValue::Ns(arr) => json!(arr),
        // Binary: render as base64-encoded string. Pre-fix B/Bs fell through
        // the `_ => format!("{:?}", v)` arm and emitted Rust Debug strings
        // like "B(Blob { inner: [104, 101, 108, 108, 111] })" — unusable as
        // a round-trip-ready AttributeValue representation. base64 is the
        // standard cross-language encoding for DynamoDB binary attributes.
        AttributeValue::B(blob) => Value::String(b64.encode(blob.as_ref())),
        AttributeValue::Bs(arr) => Value::Array(
            arr.iter()
                .map(|b| Value::String(b64.encode(b.as_ref())))
                .collect(),
        ),
        AttributeValue::L(arr) => Value::Array(arr.iter().map(attribute_value_to_json).collect()),
        AttributeValue::M(m) => {
            let obj: serde_json::Map<String, Value> = m
                .iter()
                .map(|(k, v)| (k.clone(), attribute_value_to_json(v)))
                .collect();
            Value::Object(obj)
        }
        _ => Value::String(format!("{:?}", v)),
    }
}

/// JSON value → DynamoDB `AttributeValue`. Mirrors the put_item mapping and
/// recurses for arrays (L) and objects (M).
fn json_to_attribute_value(v: &Value) -> aws_sdk_dynamodb::types::AttributeValue {
    use aws_sdk_dynamodb::types::AttributeValue;
    match v {
        Value::String(s) => AttributeValue::S(s.clone()),
        Value::Number(n) => AttributeValue::N(n.to_string()),
        Value::Bool(b) => AttributeValue::Bool(*b),
        Value::Null => AttributeValue::Null(true),
        Value::Array(a) => AttributeValue::L(a.iter().map(json_to_attribute_value).collect()),
        Value::Object(m) => AttributeValue::M(
            m.iter()
                .map(|(k, v)| (k.clone(), json_to_attribute_value(v)))
                .collect(),
        ),
    }
}

/// Build a DynamoDB attribute map from a JSON object.
fn json_obj_to_av_map(
    obj: &serde_json::Map<String, Value>,
) -> std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue> {
    obj.iter()
        .map(|(k, v)| (k.clone(), json_to_attribute_value(v)))
        .collect()
}

/// Render a DynamoDB item map to a JSON object.
fn ddb_item_to_json(
    m: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Value {
    Value::Object(
        m.iter()
            .map(|(k, v)| (k.clone(), attribute_value_to_json(v)))
            .collect(),
    )
}

async fn op_ddb_delete_item(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    let key = opts["key"]
        .as_object()
        .ok_or_else(|| anyhow!("missing key (object)"))?;
    let mut req = client.delete_item().table_name(&table);
    for (k, v) in key {
        req = req.key(k, json_to_attribute_value(v));
    }
    req.send().await?;
    Ok(json!({"table": table, "deleted": true}))
}

async fn op_ddb_query(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    let key_cond = opts["key_condition"]
        .as_str()
        .ok_or_else(|| anyhow!("missing key_condition (a KeyConditionExpression)"))?;
    let mut req = client
        .query()
        .table_name(&table)
        .key_condition_expression(key_cond);
    if let Some(values) = opts["values"].as_object() {
        req = req.set_expression_attribute_values(Some(json_obj_to_av_map(values)));
    }
    if let Some(names) = opts["names"].as_object() {
        let map = names
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();
        req = req.set_expression_attribute_names(Some(map));
    }
    if let Some(f) = opts["filter"].as_str() {
        req = req.filter_expression(f);
    }
    if let Some(i) = opts["index"].as_str() {
        req = req.index_name(i);
    }
    if let Some(n) = opts["limit"].as_i64() {
        req = req.limit(n as i32);
    }
    let r = req.send().await?;
    let items: Vec<Value> = r.items().iter().map(ddb_item_to_json).collect();
    Ok(json!({"table": table, "items": items, "count": r.count()}))
}

async fn op_ddb_scan(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    let mut req = client.scan().table_name(&table);
    if let Some(f) = opts["filter"].as_str() {
        req = req.filter_expression(f);
    }
    if let Some(values) = opts["values"].as_object() {
        req = req.set_expression_attribute_values(Some(json_obj_to_av_map(values)));
    }
    if let Some(n) = opts["limit"].as_i64() {
        req = req.limit(n as i32);
    }
    let r = req.send().await?;
    let items: Vec<Value> = r.items().iter().map(ddb_item_to_json).collect();
    Ok(json!({"table": table, "items": items, "count": r.count(), "scanned": r.scanned_count()}))
}

async fn op_ddb_describe_table(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?
        .to_string();
    let r = client.describe_table().table_name(&table).send().await?;
    let t = r.table();
    Ok(json!({
        "table": table,
        "status": t.and_then(|t| t.table_status()).map(|s| s.as_str()),
        "item_count": t.and_then(|t| t.item_count()),
        "size_bytes": t.and_then(|t| t.table_size_bytes()),
        "arn": t.and_then(|t| t.table_arn()),
        "key_schema": t.map(|t| {
            t.key_schema()
                .iter()
                .map(|k| json!({"name": k.attribute_name(), "type": k.key_type().as_str()}))
                .collect::<Vec<_>>()
        }),
    }))
}

// ── S3 head ──────────────────────────────────────────────────────────────────

async fn op_s3_head_object(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?
        .to_string();
    let key = opts["key"]
        .as_str()
        .ok_or_else(|| anyhow!("missing key"))?
        .to_string();
    let r = client
        .head_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await?;
    Ok(json!({
        "bucket": bucket,
        "key": key,
        "content_length": r.content_length(),
        "content_type": r.content_type(),
        "etag": r.e_tag(),
        "last_modified": r.last_modified().map(|t| t.to_string()),
    }))
}

// ── STS assume role ──────────────────────────────────────────────────────────

async fn op_sts_assume_role(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sts::Client::new(&cfg);
    let role_arn = opts["role_arn"]
        .as_str()
        .ok_or_else(|| anyhow!("missing role_arn"))?;
    let session = opts["session_name"].as_str().unwrap_or("stryke-aws");
    let r = client
        .assume_role()
        .role_arn(role_arn)
        .role_session_name(session)
        .send()
        .await?;
    let creds = r.credentials();
    Ok(json!({
        "assumed_role": r.assumed_role_user().map(|u| u.arn().to_string()),
        "access_key_id": creds.map(|c| c.access_key_id().to_string()),
        "secret_access_key": creds.map(|c| c.secret_access_key().to_string()),
        "session_token": creds.map(|c| c.session_token().to_string()),
        "expiration": creds.map(|c| c.expiration().to_string()),
    }))
}

// ── SQS ─────────────────────────────────────────────────────────────────────

async fn op_sqs_purge_queue(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sqs::Client::new(&cfg);
    let queue_url = opts["queue_url"]
        .as_str()
        .ok_or_else(|| anyhow!("missing queue_url"))?
        .to_string();
    client.purge_queue().queue_url(&queue_url).send().await?;
    Ok(json!({"queue_url": queue_url, "purged": true}))
}

async fn op_sqs_get_queue_attributes(opts: Value) -> Result<Value> {
    use aws_sdk_sqs::types::QueueAttributeName;
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sqs::Client::new(&cfg);
    let queue_url = opts["queue_url"]
        .as_str()
        .ok_or_else(|| anyhow!("missing queue_url"))?
        .to_string();
    let r = client
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::All)
        .send()
        .await?;
    let attrs: serde_json::Map<String, Value> = r
        .attributes()
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.as_str().to_string(), Value::String(v.clone())))
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"queue_url": queue_url, "attributes": attrs}))
}

async fn op_sqs_list_queues(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sqs::Client::new(&cfg);
    let r = client.list_queues().send().await?;
    Ok(json!({"queues": r.queue_urls()}))
}

async fn op_sqs_send_message(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sqs::Client::new(&cfg);
    let queue_url = opts["queue_url"]
        .as_str()
        .ok_or_else(|| anyhow!("missing queue_url"))?
        .to_string();
    let body = opts["body"]
        .as_str()
        .ok_or_else(|| anyhow!("missing body"))?
        .to_string();
    let r = client
        .send_message()
        .queue_url(&queue_url)
        .message_body(&body)
        .send()
        .await?;
    Ok(json!({
        "queue_url": queue_url,
        "message_id": r.message_id().unwrap_or(""),
    }))
}

async fn op_sqs_receive_message(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sqs::Client::new(&cfg);
    let queue_url = opts["queue_url"]
        .as_str()
        .ok_or_else(|| anyhow!("missing queue_url"))?
        .to_string();
    let max = opts["max"].as_i64().unwrap_or(1) as i32;
    let r = client
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(max)
        .send()
        .await?;
    let msgs: Vec<Value> = r
        .messages()
        .iter()
        .map(|m| {
            json!({
                "message_id": m.message_id().unwrap_or(""),
                "receipt_handle": m.receipt_handle().unwrap_or(""),
                "body": m.body().unwrap_or(""),
            })
        })
        .collect();
    Ok(json!({"queue_url": queue_url, "messages": msgs}))
}

async fn op_sqs_delete_message(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sqs::Client::new(&cfg);
    let queue_url = opts["queue_url"]
        .as_str()
        .ok_or_else(|| anyhow!("missing queue_url"))?;
    let receipt_handle = opts["receipt_handle"]
        .as_str()
        .ok_or_else(|| anyhow!("missing receipt_handle"))?;
    client
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(receipt_handle)
        .send()
        .await?;
    Ok(json!({"deleted": true}))
}

// ── Lambda ──────────────────────────────────────────────────────────────────

async fn op_lambda_invoke(opts: Value) -> Result<Value> {
    use aws_sdk_lambda::primitives::Blob;
    let cfg = get_config(&opts).await;
    let client = aws_sdk_lambda::Client::new(&cfg);
    let function = opts["function"]
        .as_str()
        .ok_or_else(|| anyhow!("missing function"))?
        .to_string();
    let payload = opts["payload"].to_string();
    let r = client
        .invoke()
        .function_name(&function)
        .payload(Blob::new(payload.into_bytes()))
        .send()
        .await?;
    let body = r
        .payload()
        .and_then(|b| {
            let bytes = b.as_ref();
            std::str::from_utf8(bytes).ok().map(|s| s.to_string())
        })
        .unwrap_or_default();
    let result: Value = serde_json::from_str(&body).unwrap_or(Value::String(body));
    Ok(json!({
        "function": function,
        "status_code": r.status_code(),
        "result": result,
    }))
}

async fn op_lambda_list_functions(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_lambda::Client::new(&cfg);
    let r = client.list_functions().send().await?;
    let names: Vec<String> = r
        .functions()
        .iter()
        .filter_map(|f| f.function_name().map(String::from))
        .collect();
    Ok(json!({"functions": names}))
}

// ── S3 copy + batch delete ───────────────────────────────────────────────────

async fn op_s3_copy_object(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let src_bucket = opts["source_bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing source_bucket"))?;
    let src_key = opts["source_key"]
        .as_str()
        .ok_or_else(|| anyhow!("missing source_key"))?;
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let key = opts["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
    let r = client
        .copy_object()
        .copy_source(format!("{}/{}", src_bucket, src_key))
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    Ok(json!({
        "bucket": bucket,
        "key": key,
        "etag": r.copy_object_result().and_then(|c| c.e_tag()).unwrap_or(""),
    }))
}

async fn op_s3_delete_objects(opts: Value) -> Result<Value> {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    let cfg = get_config(&opts).await;
    let client = aws_sdk_s3::Client::new(&cfg);
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let keys = string_vec(&opts["keys"])?;
    if keys.is_empty() {
        return Err(anyhow!("keys must be a non-empty array"));
    }
    let mut ids = Vec::with_capacity(keys.len());
    for k in &keys {
        ids.push(ObjectIdentifier::builder().key(k).build()?);
    }
    let delete = Delete::builder().set_objects(Some(ids)).build()?;
    let r = client
        .delete_objects()
        .bucket(bucket)
        .delete(delete)
        .send()
        .await?;
    let deleted: Vec<String> = r
        .deleted()
        .iter()
        .filter_map(|d| d.key().map(String::from))
        .collect();
    Ok(json!({"bucket": bucket, "deleted": deleted}))
}

// ── DynamoDB update ───────────────────────────────────────────────────────────

async fn op_ddb_update_item(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?;
    let key = opts["key"]
        .as_object()
        .ok_or_else(|| anyhow!("missing key (object)"))?;
    let updates = opts["updates"]
        .as_object()
        .ok_or_else(|| anyhow!("missing updates (object of attr => value)"))?;
    if updates.is_empty() {
        return Err(anyhow!("updates must be non-empty"));
    }
    // Build `SET #a0 = :v0, #a1 = :v1, ...` with name/value placeholders so
    // reserved words and arbitrary attribute names are always legal.
    let mut set_parts = Vec::new();
    let mut req = client.update_item().table_name(table);
    for (k, v) in key {
        req = req.key(k, json_to_av(v));
    }
    for (i, (attr, val)) in updates.iter().enumerate() {
        let nph = format!("#a{}", i);
        let vph = format!(":v{}", i);
        set_parts.push(format!("{} = {}", nph, vph));
        req = req
            .expression_attribute_names(nph, attr)
            .expression_attribute_values(vph, json_to_av(val));
    }
    req = req.update_expression(format!("SET {}", set_parts.join(", ")));
    req.send().await?;
    Ok(json!({"table": table, "updated": true}))
}

async fn op_ddb_batch_get_item(opts: Value) -> Result<Value> {
    use aws_sdk_dynamodb::types::KeysAndAttributes;
    use std::collections::HashMap;
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?;
    let keys = opts["keys"]
        .as_array()
        .ok_or_else(|| anyhow!("missing keys (array of key objects)"))?;
    if keys.is_empty() {
        return Err(anyhow!("keys must be non-empty"));
    }
    let mut kaa = KeysAndAttributes::builder();
    for key in keys {
        let obj = key
            .as_object()
            .ok_or_else(|| anyhow!("each key must be an object"))?;
        let m: HashMap<String, _> = obj
            .iter()
            .map(|(k, v)| (k.clone(), json_to_av(v)))
            .collect();
        kaa = kaa.keys(m);
    }
    let r = client
        .batch_get_item()
        .request_items(table, kaa.build()?)
        .send()
        .await?;
    let items: Vec<Value> = r
        .responses()
        .and_then(|m| m.get(table))
        .map(|rows| {
            rows.iter()
                .map(|item| {
                    let obj: serde_json::Map<String, Value> = item
                        .iter()
                        .map(|(k, av)| (k.clone(), attribute_value_to_json(av)))
                        .collect();
                    Value::Object(obj)
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({ "table": table, "items": items }))
}

async fn op_ddb_batch_write_item(opts: Value) -> Result<Value> {
    use aws_sdk_dynamodb::types::{DeleteRequest, PutRequest, WriteRequest};
    let cfg = get_config(&opts).await;
    let client = aws_sdk_dynamodb::Client::new(&cfg);
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?;
    let mut reqs: Vec<WriteRequest> = Vec::new();
    if let Some(puts) = opts["puts"].as_array() {
        for item in puts {
            let obj = item
                .as_object()
                .ok_or_else(|| anyhow!("each put must be an item object"))?;
            let mut pr = PutRequest::builder();
            for (k, v) in obj {
                pr = pr.item(k, json_to_av(v));
            }
            reqs.push(WriteRequest::builder().put_request(pr.build()?).build());
        }
    }
    if let Some(dels) = opts["deletes"].as_array() {
        for key in dels {
            let obj = key
                .as_object()
                .ok_or_else(|| anyhow!("each delete must be a key object"))?;
            let mut dr = DeleteRequest::builder();
            for (k, v) in obj {
                dr = dr.key(k, json_to_av(v));
            }
            reqs.push(WriteRequest::builder().delete_request(dr.build()?).build());
        }
    }
    if reqs.is_empty() {
        return Err(anyhow!("provide puts and/or deletes"));
    }
    let n = reqs.len();
    client
        .batch_write_item()
        .request_items(table, reqs)
        .send()
        .await?;
    Ok(json!({ "table": table, "written": n }))
}

// ── SNS ───────────────────────────────────────────────────────────────────────

async fn op_sns_list_topics(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sns::Client::new(&cfg);
    let r = client.list_topics().send().await?;
    let topics: Vec<String> = r
        .topics()
        .iter()
        .filter_map(|t| t.topic_arn().map(String::from))
        .collect();
    Ok(json!({"topics": topics}))
}

async fn op_sns_publish(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sns::Client::new(&cfg);
    let message = opts["message"]
        .as_str()
        .ok_or_else(|| anyhow!("missing message"))?;
    let mut req = client.publish().message(message);
    if let Some(t) = opts["topic_arn"].as_str() {
        req = req.topic_arn(t);
    } else if let Some(p) = opts["phone_number"].as_str() {
        req = req.phone_number(p);
    } else if let Some(t) = opts["target_arn"].as_str() {
        req = req.target_arn(t);
    } else {
        return Err(anyhow!("need topic_arn, target_arn, or phone_number"));
    }
    if let Some(s) = opts["subject"].as_str() {
        req = req.subject(s);
    }
    let r = req.send().await?;
    Ok(json!({"message_id": r.message_id().unwrap_or("")}))
}

async fn op_sns_create_topic(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sns::Client::new(&cfg);
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let r = client.create_topic().name(name).send().await?;
    Ok(json!({"topic_arn": r.topic_arn().unwrap_or("")}))
}

async fn op_sns_subscribe(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sns::Client::new(&cfg);
    let topic_arn = opts["topic_arn"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic_arn"))?;
    let protocol = opts["protocol"]
        .as_str()
        .ok_or_else(|| anyhow!("missing protocol"))?;
    let endpoint = opts["endpoint"]
        .as_str()
        .ok_or_else(|| anyhow!("missing endpoint"))?;
    let r = client
        .subscribe()
        .topic_arn(topic_arn)
        .protocol(protocol)
        .endpoint(endpoint)
        .return_subscription_arn(true)
        .send()
        .await?;
    Ok(json!({"subscription_arn": r.subscription_arn().unwrap_or("")}))
}

// ── SSM Parameter Store ────────────────────────────────────────────────────────

async fn op_ssm_get_parameter(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_ssm::Client::new(&cfg);
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let decrypt = opts["with_decryption"].as_bool().unwrap_or(false);
    let r = client
        .get_parameter()
        .name(name)
        .with_decryption(decrypt)
        .send()
        .await?;
    let p = r.parameter();
    Ok(json!({
        "name": p.and_then(|p| p.name()).unwrap_or(""),
        "value": p.and_then(|p| p.value()),
        "version": p.map(|p| p.version()).unwrap_or(0),
    }))
}

async fn op_ssm_put_parameter(opts: Value) -> Result<Value> {
    use aws_sdk_ssm::types::ParameterType;
    let cfg = get_config(&opts).await;
    let client = aws_sdk_ssm::Client::new(&cfg);
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let value = opts["value"]
        .as_str()
        .ok_or_else(|| anyhow!("missing value"))?;
    let ptype = opts["type"].as_str().unwrap_or("String");
    let overwrite = opts["overwrite"].as_bool().unwrap_or(false);
    let r = client
        .put_parameter()
        .name(name)
        .value(value)
        .r#type(ParameterType::from(ptype))
        .overwrite(overwrite)
        .send()
        .await?;
    Ok(json!({"name": name, "version": r.version()}))
}

async fn op_ssm_get_parameters_by_path(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_ssm::Client::new(&cfg);
    let path = opts["path"]
        .as_str()
        .ok_or_else(|| anyhow!("missing path"))?;
    let recursive = opts["recursive"].as_bool().unwrap_or(false);
    let decrypt = opts["with_decryption"].as_bool().unwrap_or(false);
    let r = client
        .get_parameters_by_path()
        .path(path)
        .recursive(recursive)
        .with_decryption(decrypt)
        .send()
        .await?;
    let params: Vec<Value> = r
        .parameters()
        .iter()
        .map(|p| json!({"name": p.name().unwrap_or(""), "value": p.value()}))
        .collect();
    Ok(json!({"parameters": params}))
}

async fn op_ssm_delete_parameter(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_ssm::Client::new(&cfg);
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    client.delete_parameter().name(name).send().await?;
    Ok(json!({"name": name, "deleted": true}))
}

// ── Secrets Manager ────────────────────────────────────────────────────────────

async fn op_secrets_get_value(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_secretsmanager::Client::new(&cfg);
    let id = opts["secret_id"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret_id"))?;
    let r = client.get_secret_value().secret_id(id).send().await?;
    Ok(json!({
        "name": r.name().unwrap_or(""),
        "secret_string": r.secret_string(),
        "version_id": r.version_id().unwrap_or(""),
    }))
}

async fn op_secrets_create(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_secretsmanager::Client::new(&cfg);
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let secret = opts["secret_string"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret_string"))?;
    let r = client
        .create_secret()
        .name(name)
        .secret_string(secret)
        .send()
        .await?;
    Ok(json!({"arn": r.arn().unwrap_or(""), "name": r.name().unwrap_or("")}))
}

async fn op_secrets_put_value(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_secretsmanager::Client::new(&cfg);
    let id = opts["secret_id"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret_id"))?;
    let secret = opts["secret_string"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret_string"))?;
    let r = client
        .put_secret_value()
        .secret_id(id)
        .secret_string(secret)
        .send()
        .await?;
    Ok(json!({"version_id": r.version_id().unwrap_or("")}))
}

async fn op_secrets_list(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_secretsmanager::Client::new(&cfg);
    let r = client.list_secrets().send().await?;
    let secrets: Vec<Value> = r
        .secret_list()
        .iter()
        .map(|s| json!({"name": s.name().unwrap_or(""), "arn": s.arn().unwrap_or("")}))
        .collect();
    Ok(json!({"secrets": secrets}))
}

// ── SES (email) ───────────────────────────────────────────────────────────────

async fn op_ses_send_email(opts: Value) -> Result<Value> {
    use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
    let cfg = get_config(&opts).await;
    let client = aws_sdk_sesv2::Client::new(&cfg);
    let from = opts["from"]
        .as_str()
        .ok_or_else(|| anyhow!("missing from"))?;
    let to = string_vec(&opts["to"])?;
    if to.is_empty() {
        return Err(anyhow!("to must name at least one recipient"));
    }
    let subject = opts["subject"].as_str().unwrap_or("");
    let body_text = opts["body"].as_str().unwrap_or("");
    let html = opts["html"].as_bool().unwrap_or(false);
    let mut dest = Destination::builder();
    for addr in &to {
        dest = dest.to_addresses(addr);
    }
    let subject_content = Content::builder().data(subject).build()?;
    let body_content = Content::builder().data(body_text).build()?;
    let body = if html {
        Body::builder().html(body_content).build()
    } else {
        Body::builder().text(body_content).build()
    };
    let message = Message::builder()
        .subject(subject_content)
        .body(body)
        .build();
    let content = EmailContent::builder().simple(message).build();
    let r = client
        .send_email()
        .from_email_address(from)
        .destination(dest.build())
        .content(content)
        .send()
        .await?;
    Ok(json!({"message_id": r.message_id().unwrap_or("")}))
}

// ── CloudWatch ────────────────────────────────────────────────────────────────

async fn op_cloudwatch_put_metric(opts: Value) -> Result<Value> {
    use aws_sdk_cloudwatch::types::{MetricDatum, StandardUnit};
    let cfg = get_config(&opts).await;
    let client = aws_sdk_cloudwatch::Client::new(&cfg);
    let namespace = opts["namespace"]
        .as_str()
        .ok_or_else(|| anyhow!("missing namespace"))?;
    let name = opts["metric_name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing metric_name"))?;
    let value = opts["value"]
        .as_f64()
        .ok_or_else(|| anyhow!("missing value"))?;
    let mut datum = MetricDatum::builder().metric_name(name).value(value);
    if let Some(unit) = opts["unit"].as_str() {
        datum = datum.unit(StandardUnit::from(unit));
    }
    client
        .put_metric_data()
        .namespace(namespace)
        .metric_data(datum.build())
        .send()
        .await?;
    Ok(json!({"namespace": namespace, "metric_name": name, "value": value, "ok": true}))
}

async fn op_cloudwatch_list_metrics(opts: Value) -> Result<Value> {
    let cfg = get_config(&opts).await;
    let client = aws_sdk_cloudwatch::Client::new(&cfg);
    let mut req = client.list_metrics();
    if let Some(ns) = opts["namespace"].as_str() {
        req = req.namespace(ns);
    }
    if let Some(name) = opts["metric_name"].as_str() {
        req = req.metric_name(name);
    }
    let r = req.send().await?;
    let metrics: Vec<Value> = r
        .metrics()
        .iter()
        .map(|m| {
            json!({
                "namespace": m.namespace(),
                "metric_name": m.metric_name(),
                "dimensions": m.dimensions().iter()
                    .map(|d| json!({"name": d.name(), "value": d.value()}))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(json!({"metrics": metrics}))
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call_async<F, Fut>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let fut = handler(input);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt().block_on(fut)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-aws handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── pure helpers (no AWS) ────────────────────────────────────────────────────

/// Parse an ARN `arn:partition:service:region:account-id:resource` into its
/// fields. Everything after the 5th colon is the resource, further split into
/// `resource_type` + `resource_id` on the first `/` or `:`. Pure.
fn op_parse_arn(opts: Value) -> Result<Value> {
    let arn = opts
        .get("arn")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing arn"))?;
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() < 6 || parts[0] != "arn" {
        return Err(anyhow!(
            "not an ARN (want arn:partition:service:region:account:resource): {arn}"
        ));
    }
    let (partition, service, region, account, resource) =
        (parts[1], parts[2], parts[3], parts[4], parts[5]);
    let (resource_type, resource_id): (Option<&str>, &str) =
        if let Some((t, id)) = resource.split_once('/') {
            (Some(t), id)
        } else if let Some((t, id)) = resource.split_once(':') {
            (Some(t), id)
        } else {
            (None, resource)
        };
    let or_null = |s: &str| {
        if s.is_empty() {
            Value::Null
        } else {
            json!(s)
        }
    };
    Ok(json!({
        "partition": partition,
        "service": service,
        "region": or_null(region),
        "account_id": or_null(account),
        "resource": resource,
        "resource_type": resource_type,
        "resource_id": resource_id,
    }))
}

/// Build an ARN from parts. opts: partition (default `aws`), service, region,
/// account_id, resource (service + resource required). Inverse of `parse_arn`.
fn op_build_arn(opts: Value) -> Result<Value> {
    let partition = opts
        .get("partition")
        .and_then(Value::as_str)
        .unwrap_or("aws");
    let service = opts
        .get("service")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing service"))?;
    let region = opts.get("region").and_then(Value::as_str).unwrap_or("");
    let account = opts.get("account_id").and_then(Value::as_str).unwrap_or("");
    let resource = opts
        .get("resource")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing resource"))?;
    Ok(json!({"arn": format!("arn:{partition}:{service}:{region}:{account}:{resource}")}))
}

/// Parse an `s3://bucket/key` URI into `{bucket, key}` (key is null when the
/// URI is just a bucket). Pure.
fn op_parse_s3_uri(opts: Value) -> Result<Value> {
    let uri = opts
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing uri"))?;
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow!("not an s3:// URI: {uri}"))?;
    let (bucket, key) = match rest.split_once('/') {
        Some((b, k)) => (b, json!(k)),
        None => (rest, Value::Null),
    };
    if bucket.is_empty() {
        return Err(anyhow!("s3 URI missing bucket: {uri}"));
    }
    Ok(json!({"bucket": bucket, "key": key}))
}

/// Build an `s3://bucket/key` URI from parts. opts: bucket (required), key
/// (optional — omitted yields the bare `s3://bucket`). Leading slashes on the
/// key are trimmed so callers can pass either `key` or `/key`. Inverse of
/// `parse_s3_uri`. Pure.
fn op_build_s3_uri(opts: Value) -> Result<Value> {
    let bucket = opts
        .get("bucket")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing bucket"))?;
    if bucket.is_empty() {
        return Err(anyhow!("bucket must not be empty"));
    }
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .map(|k| k.trim_start_matches('/'))
        .filter(|k| !k.is_empty());
    let uri = match key {
        Some(k) => format!("s3://{bucket}/{k}"),
        None => format!("s3://{bucket}"),
    };
    Ok(json!({"uri": uri}))
}

/// Convert an `s3://bucket/key` URI to its S3 ARN `arn:partition:s3:::bucket[/key]`.
/// S3 bucket/object ARNs carry no region or account, so those segments stay
/// empty. opts: uri (required), partition (default `aws`; e.g. `aws-cn`,
/// `aws-us-gov`). Returns `{arn, bucket, key}`. Pure.
fn op_s3_uri_to_arn(opts: Value) -> Result<Value> {
    let uri = opts
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing uri"))?;
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow!("not an s3:// URI: {uri}"))?;
    let (bucket, key) = match rest.split_once('/') {
        Some((b, k)) if !k.is_empty() => (b, Some(k)),
        Some((b, _)) => (b, None),
        None => (rest, None),
    };
    if bucket.is_empty() {
        return Err(anyhow!("s3 URI missing bucket: {uri}"));
    }
    let partition = opts
        .get("partition")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("aws");
    let resource = match key {
        Some(k) => format!("{bucket}/{k}"),
        None => bucket.to_string(),
    };
    Ok(json!({
        "arn": format!("arn:{partition}:s3:::{resource}"),
        "bucket": bucket,
        "key": key.map(|k| json!(k)).unwrap_or(Value::Null),
    }))
}

/// Convert an S3 ARN `arn:partition:s3:::bucket[/key]` back to its
/// `s3://bucket[/key]` URI. S3 resource ARNs carry no region or account, so the
/// 5th/6th colon-segments are empty and the bucket/key live in the resource
/// tail. opts: arn (required). Returns `{uri, bucket, key}`. Inverse of
/// `s3_uri_to_arn`. Pure.
fn op_arn_to_s3_uri(opts: Value) -> Result<Value> {
    let arn = opts
        .get("arn")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing arn"))?;
    // arn:partition:s3:::resource — split into exactly 6 parts (resource may
    // itself contain '/', so cap the split count to keep the key intact).
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() != 6 || parts[0] != "arn" || parts[2] != "s3" {
        return Err(anyhow!("not an s3 ARN: {arn}"));
    }
    if !parts[3].is_empty() || !parts[4].is_empty() {
        return Err(anyhow!("s3 ARN must have empty region and account: {arn}"));
    }
    let resource = parts[5];
    let (bucket, key) = match resource.split_once('/') {
        Some((b, k)) if !k.is_empty() => (b, Some(k)),
        Some((b, _)) => (b, None),
        None => (resource, None),
    };
    if bucket.is_empty() {
        return Err(anyhow!("s3 ARN missing bucket: {arn}"));
    }
    let uri = match key {
        Some(k) => format!("s3://{bucket}/{k}"),
        None => format!("s3://{bucket}"),
    };
    Ok(json!({
        "uri": uri,
        "bucket": bucket,
        "key": key.map(|k| json!(k)).unwrap_or(Value::Null),
    }))
}

/// Validate an S3 bucket name against AWS's documented rules: 3–63 chars of
/// `[a-z0-9.-]`, start/end alphanumeric, no `..`, not an IPv4 literal, no
/// `xn--` prefix, no `-s3alias` suffix. Returns `{valid, reason}`. Pure.
fn op_valid_bucket_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let bytes = name.as_bytes();
    let reason: Option<&str> = if name.len() < 3 || name.len() > 63 {
        Some("must be 3-63 characters")
    } else if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
    {
        Some("only lowercase letters, numbers, dots, and hyphens")
    } else if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        Some("must start and end with a letter or number")
    } else if name.contains("..") {
        Some("must not contain two adjacent periods")
    } else if name.parse::<std::net::Ipv4Addr>().is_ok() {
        Some("must not be formatted as an IP address")
    } else if name.starts_with("xn--") {
        Some("must not start with `xn--`")
    } else if name.ends_with("-s3alias") {
        Some("must not end with `-s3alias`")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate an S3 object key — a non-empty sequence of Unicode characters whose
/// UTF-8 encoding is at most 1,024 bytes, per the S3 object-key reference. Any
/// character (including `/`) is allowed; only emptiness and the byte-length cap
/// are hard errors. The object-key counterpart of `valid_bucket_name`, mirroring
/// its `{valid, reason}` shape. opts: `key` (or `name`, required). Returns `{key,
/// valid, reason, bytes}` where `bytes` is the UTF-8 length. Pure.
fn op_valid_s3_key(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .or_else(|| opts.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let reason: Option<&str> = if key.is_empty() {
        Some("must not be empty")
    } else if key.len() > 1024 {
        Some("UTF-8 encoding must be at most 1024 bytes")
    } else {
        None
    };
    Ok(json!({"key": key, "valid": reason.is_none(), "reason": reason, "bytes": key.len()}))
}

/// Validate an Amazon SQS queue name per the CreateQueue reference: up to 80
/// characters of alphanumeric characters, hyphens (`-`) and underscores (`_`),
/// case-sensitive. A FIFO queue name additionally ends with the `.fifo` suffix
/// (which counts toward the 80-character limit); the base name before `.fifo`
/// must still match the standard charset, and a `.` is allowed only as part of
/// that suffix. opts: `name` (required). Returns `{name, valid, reason, fifo}`
/// where `fifo` is whether the name carries the `.fifo` suffix. Pure.
fn op_valid_sqs_queue_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let (base, is_fifo) = match name.strip_suffix(".fifo") {
        Some(b) => (b, true),
        None => (name, false),
    };
    let charset_ok = base
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    let reason: Option<&str> = if name.is_empty() {
        Some("must not be empty")
    } else if name.len() > 80 {
        Some("must be at most 80 characters (including any .fifo suffix)")
    } else if is_fifo && base.is_empty() {
        Some("FIFO queue name needs a base name before the .fifo suffix")
    } else if !charset_ok {
        if is_fifo {
            Some("only alphanumeric characters, hyphens, and underscores before the .fifo suffix")
        } else {
            Some("only alphanumeric characters, hyphens, and underscores (a `.` is allowed only in a .fifo suffix)")
        }
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason, "fifo": is_fifo}))
}

/// Validate an AWS account ID — exactly 12 decimal digits, leading zeros allowed
/// (e.g. `012345678901`), per the AWS account-identifier reference. `parse_arn`
/// surfaces the account field but never checks it; this is the standalone
/// predicate, mirroring `valid_bucket_name`'s `{valid, reason}` shape. opts:
/// `account_id` (or `id`/`value`, required). Returns `{account_id, valid,
/// reason}`. Pure.
fn op_valid_account_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("account_id")
        .or_else(|| opts.get("id"))
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing account_id"))?;
    let reason: Option<&str> = if id.len() != 12 {
        Some("must be exactly 12 digits")
    } else if !id.bytes().all(|b| b.is_ascii_digit()) {
        Some("must contain only digits")
    } else {
        None
    };
    Ok(json!({"account_id": id, "valid": reason.is_none(), "reason": reason}))
}

/// Validate the structure of an ARN as a non-throwing predicate — the
/// `{valid, reason}` companion of `parse_arn` (which throws and returns the
/// components). An ARN is `arn:partition:service:region:account:resource`: six
/// `:`-delimited fields (the resource may itself contain `:`), the literal `arn`
/// prefix, and a non-empty partition, service, and resource. Region and account
/// may be empty (as in `arn:aws:s3:::bucket` or an IAM ARN). opts: `arn`
/// (required). Returns `{arn, valid, reason}`. Pure.
fn op_valid_arn(opts: Value) -> Result<Value> {
    let arn = opts
        .get("arn")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing arn"))?;
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let reason: Option<&str> = if parts.len() < 6 {
        Some("must have 6 colon-delimited fields (arn:partition:service:region:account:resource)")
    } else if parts[0] != "arn" {
        Some("must begin with the literal `arn`")
    } else if parts[1].is_empty() {
        Some("partition (field 2) must not be empty")
    } else if parts[2].is_empty() {
        Some("service (field 3) must not be empty")
    } else if parts[5].is_empty() {
        Some("resource (field 6) must not be empty")
    } else {
        None
    };
    Ok(json!({"arn": arn, "valid": reason.is_none(), "reason": reason}))
}

/// IAM-style wildcard match of `text` against `pattern`: `*` matches any sequence
/// of characters (including none, and spanning `:`/`/` segment boundaries) and `?`
/// matches exactly one character. Anchored (the whole string must match). Classic
/// iterative glob with `*`-backtracking — byte-level, since ARNs are ASCII. Used
/// by `arn_matches`.
fn iam_glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star, mut star_t): (Option<usize>, usize) = (None, 0);
    while t < text.len() {
        if p < pattern.len() && (pattern[p] == b'?' || pattern[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            star_t = t;
            p += 1;
        } else if let Some(sp) = star {
            // Backtrack: let the last `*` consume one more character.
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

/// Test whether an `arn` matches an IAM policy ARN `pattern` with `*`/`?`
/// wildcards — the resource-matching IAM does when evaluating a policy. `*`
/// matches any run of characters (it spans `:` and `/`, so
/// `arn:aws:s3:::bucket/*` matches every object key) and `?` matches one
/// character; literals match exactly and case-sensitively. The whole ARN must
/// match (anchored). opts: `pattern` (required), `arn` (or `value`, required).
/// Returns `{pattern, arn, matches}`. Pure.
fn op_arn_matches(opts: Value) -> Result<Value> {
    let pattern = opts
        .get("pattern")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing pattern"))?;
    let arn = opts
        .get("arn")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing arn"))?;
    let matches = iam_glob_match(pattern.as_bytes(), arn.as_bytes());
    Ok(json!({"pattern": pattern, "arn": arn, "matches": matches}))
}

/// Resolve the ARN partition for an AWS region. AWS groups regions into five
/// partitions: `aws-cn` (`cn-*`), `aws-us-gov` (`us-gov-*`), `aws-iso-b`
/// (`us-isob-*`, Top Secret), `aws-iso` (`us-iso-*`, Secret), and `aws` for
/// everything else. The `us-isob-` prefix is tested before `us-iso-` so the Top
/// Secret regions don't fall into the Secret partition. ARN building needs the
/// right partition. opts: `region` (required). Returns `{region, partition}`.
/// Pure.
/// The AWS partition a region belongs to, by region-name prefix. Single source
/// of truth for `op_partition_for_region` and `op_service_endpoint`.
fn partition_of(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else {
        "aws"
    }
}

/// The DNS suffix for a partition, from botocore's `partitions.json`. Single
/// source of truth for `op_dns_suffix_for_partition` and `op_service_endpoint`.
fn dns_suffix_of(partition: &str) -> Option<&'static str> {
    match partition {
        "aws" => Some("amazonaws.com"),
        "aws-cn" => Some("amazonaws.com.cn"),
        "aws-us-gov" => Some("amazonaws.com"),
        "aws-iso" => Some("c2s.ic.gov"),
        "aws-iso-b" => Some("sc2s.sgov.gov"),
        _ => None,
    }
}

fn op_partition_for_region(opts: Value) -> Result<Value> {
    let region = opts
        .get("region")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing region"))?;
    Ok(json!({"region": region, "partition": partition_of(region)}))
}

/// Validate an AWS region name against botocore `partitions.json` `regionRegex`
/// — the partition is resolved from the region prefix (so it works across
/// aws/aws-cn/aws-us-gov/aws-iso/aws-iso-b) and the name is checked against that
/// partition's documented shape: standard `(us|eu|ap|sa|ca|me|af|il|mx)-<area>-<n>`,
/// `cn-<area>-<n>`, `us-gov-<area>-<n>`, `us-iso-<area>-<n>`, `us-isob-<area>-<n>`
/// (`<area>` is `\w+`, `<n>` is `\d+`). Unlike `partition_for_region`, which
/// classifies any string, this rejects malformed names — the region-name member
/// of the `valid_*` family, mirroring its `{valid, reason}` shape. opts: `region`
/// (or `name`, required). Returns `{region, valid, partition, reason}`. Pure.
fn op_valid_region(opts: Value) -> Result<Value> {
    let region = opts
        .get("region")
        .or_else(|| opts.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing region"))?;
    let partition = partition_of(region);
    let is_area =
        |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    let is_num = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    let parts: Vec<&str> = region.split('-').collect();
    let reason: Option<String> = match partition {
        "aws" => {
            const GEOS: [&str; 9] = ["us", "eu", "ap", "sa", "ca", "me", "af", "il", "mx"];
            if parts.len() != 3 || !is_area(parts[1]) || !is_num(parts[2]) {
                Some("must be <geo>-<area>-<number> (e.g. us-east-1)".into())
            } else if !GEOS.contains(&parts[0]) {
                Some(format!(
                    "unknown geography `{}` (us|eu|ap|sa|ca|me|af|il|mx)",
                    parts[0]
                ))
            } else {
                None
            }
        }
        "aws-cn" => (!(parts.len() == 3 && is_area(parts[1]) && is_num(parts[2])))
            .then(|| "China region must be cn-<area>-<number>".into()),
        "aws-us-gov" => (!(parts.len() == 4 && is_area(parts[2]) && is_num(parts[3])))
            .then(|| "GovCloud region must be us-gov-<area>-<number>".into()),
        "aws-iso" => (!(parts.len() == 4 && is_area(parts[2]) && is_num(parts[3])))
            .then(|| "ISO region must be us-iso-<area>-<number>".into()),
        "aws-iso-b" => (!(parts.len() == 4 && is_area(parts[2]) && is_num(parts[3])))
            .then(|| "ISO-B region must be us-isob-<area>-<number>".into()),
        _ => Some("unknown partition".into()),
    };
    Ok(
        json!({"region": region, "valid": reason.is_none(), "partition": partition, "reason": reason}),
    )
}

/// Build a regional service endpoint hostname `<service>.<region>.<dns_suffix>`
/// — the canonical AWS endpoint form (e.g. `s3.us-east-1.amazonaws.com`,
/// `dynamodb.cn-north-1.amazonaws.com.cn`). Resolves the region's partition and
/// its DNS suffix internally, so it works across aws/aws-cn/aws-us-gov/iso.
/// opts: `service`, `region`. Returns `{service, region, partition, dns_suffix,
/// endpoint, url}`. Pure.
fn op_service_endpoint(opts: Value) -> Result<Value> {
    let service = opts
        .get("service")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing service"))?;
    let region = opts
        .get("region")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing region"))?;
    let partition = partition_of(region);
    let dns_suffix = dns_suffix_of(partition)
        .ok_or_else(|| anyhow!("no DNS suffix for partition `{partition}`"))?;
    let endpoint = format!("{service}.{region}.{dns_suffix}");
    Ok(json!({
        "service": service,
        "region": region,
        "partition": partition,
        "dns_suffix": dns_suffix,
        "endpoint": endpoint,
        "url": format!("https://{endpoint}"),
    }))
}

/// Parse a regional service endpoint back into its parts — the inverse of
/// `service_endpoint`. Accepts a bare host (`s3.us-east-1.amazonaws.com`) or a
/// full URL (scheme and any path stripped). The host splits into
/// `<service>.<region>.<dns_suffix>`; the suffix is matched against the known
/// partition suffixes and the region's partition resolved from it. opts:
/// `endpoint` (or `url`). Returns `{endpoint, service, region, partition,
/// dns_suffix, url}`. Pure.
fn op_parse_service_endpoint(opts: Value) -> Result<Value> {
    let raw = opts
        .get("endpoint")
        .or_else(|| opts.get("url"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing endpoint"))?;
    let host = raw
        .strip_prefix("https://")
        .or_else(|| raw.strip_prefix("http://"))
        .unwrap_or(raw);
    let host = host.split(['/', '?']).next().unwrap_or(host);
    let (service, rest) = host
        .split_once('.')
        .ok_or_else(|| anyhow!("not a service endpoint host `{host}`"))?;
    if service.is_empty() {
        return Err(anyhow!("service endpoint has no service: `{host}`"));
    }
    let (region, suffix) = split_s3_endpoint(rest)
        .map_err(|_| anyhow!("unrecognized service endpoint host `{host}`"))?;
    if region.is_empty() {
        return Err(anyhow!("service endpoint has no region: `{host}`"));
    }
    let partition = partition_of(&region);
    let endpoint = format!("{service}.{region}.{suffix}");
    Ok(json!({
        "endpoint": endpoint,
        "service": service,
        "region": region,
        "partition": partition,
        "dns_suffix": suffix,
        "url": format!("https://{endpoint}"),
    }))
}

/// The DNS suffix for an AWS partition — the domain its service endpoints live
/// under, from botocore's `partitions.json`: `aws` → `amazonaws.com`, `aws-cn` →
/// `amazonaws.com.cn`, `aws-us-gov` → `amazonaws.com`, `aws-iso` → `c2s.ic.gov`,
/// `aws-iso-b` → `sc2s.sgov.gov`. Pairs with `partition_for_region` to build
/// endpoint hostnames. opts: `partition` (required). Returns
/// `{partition, dns_suffix}`; errors on an unknown partition. Pure.
fn op_dns_suffix_for_partition(opts: Value) -> Result<Value> {
    let partition = opts
        .get("partition")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing partition"))?;
    let suffix =
        dns_suffix_of(partition).ok_or_else(|| anyhow!("unknown partition `{partition}`"))?;
    Ok(json!({"partition": partition, "dns_suffix": suffix}))
}

/// Percent-encode an S3 object key for a URL path per RFC 3986: unreserved
/// characters (`A-Za-z0-9-._~`) and the path separator `/` pass through; every
/// other byte becomes `%XX` (uppercase hex).
fn encode_s3_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for &b in key.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Build the virtual-hosted–style HTTPS URL for an S3 object, per the AWS S3
/// user guide: `https://<bucket>.s3.<region>.<dns_suffix>/<key>`. The region's
/// partition and DNS suffix are resolved the same way as `service_endpoint`, so
/// it works across aws/aws-cn/aws-us-gov/iso (`amazonaws.com.cn` etc.). The key
/// is percent-encoded (unreserved chars and `/` pass through) and any leading
/// slash trimmed; omit `key` for the bucket root. opts: `bucket`, `region`,
/// optional `key`. Returns `{url, bucket, region, partition, host}`. Pure.
fn op_s3_object_url(opts: Value) -> Result<Value> {
    let bucket = opts
        .get("bucket")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let region = opts
        .get("region")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing region"))?;
    let partition = partition_of(region);
    let dns_suffix = dns_suffix_of(partition)
        .ok_or_else(|| anyhow!("no DNS suffix for partition `{partition}`"))?;
    let host = format!("{bucket}.s3.{region}.{dns_suffix}");
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .map(|k| k.trim_start_matches('/'))
        .filter(|k| !k.is_empty());
    let url = match key {
        Some(k) => format!("https://{host}/{}", encode_s3_key(k)),
        None => format!("https://{host}"),
    };
    Ok(json!({
        "url": url,
        "bucket": bucket,
        "region": region,
        "partition": partition,
        "host": host,
    }))
}

/// DNS suffixes botocore knows, longest first so the longest match wins
/// (`amazonaws.com.cn` before `amazonaws.com`). Used by `parse_s3_url` to peel
/// the suffix off an S3 endpoint host.
const KNOWN_DNS_SUFFIXES: &[&str] = &[
    "amazonaws.com.cn",
    "sc2s.sgov.gov",
    "c2s.ic.gov",
    "amazonaws.com",
];

/// Percent-decode a URL path (inverse of `encode_s3_key`): `%XX` becomes its
/// byte; a malformed escape is left literal. The decoded bytes are interpreted
/// as UTF-8 (lossily, like the rest of the crate).
fn decode_s3_key(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Split an S3 endpoint host's `<region>.<dns_suffix>` portion into the region
/// (possibly empty for a legacy global endpoint) and the matched suffix.
fn split_s3_endpoint(ep: &str) -> Result<(String, &'static str)> {
    for suf in KNOWN_DNS_SUFFIXES {
        if let Some(head) = ep.strip_suffix(suf) {
            return Ok((head.trim_end_matches('.').to_string(), *suf));
        }
    }
    Err(anyhow!("unrecognized S3 endpoint host `{ep}`"))
}

/// Parse an S3 HTTPS URL back into its parts — the inverse of `s3_object_url`.
/// Handles virtual-hosted style (`https://<bucket>.s3.<region>.<suffix>/<key>`,
/// what `s3_object_url` emits) and path style
/// (`https://s3.<region>.<suffix>/<bucket>/<key>`); the region may be absent for
/// a legacy global endpoint. The key is percent-decoded back to its raw form and
/// the partition is resolved from the region. opts: `url`. Returns `{url, bucket,
/// key, region, partition, style, host}`; `key` is null for a bucket-root URL.
/// A bucket literally named `s3` in virtual-hosted form is read as path style.
/// Pure.
fn op_parse_s3_url(opts: Value) -> Result<Value> {
    let url = opts
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing url"))?;
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| anyhow!("S3 URL must start with http(s)://: `{url}`"))?;
    let (host, path) = match rest.split_once('/') {
        Some((h, p)) => (h, p),
        None => (rest, ""),
    };
    let (style, bucket, region, suffix, key): (&str, String, String, &'static str, Option<String>) =
        if let Some(after) = host.strip_prefix("s3.") {
            // Path style: host is the bare S3 endpoint, bucket is the 1st segment.
            let (region, suffix) = split_s3_endpoint(after)?;
            let (b, k) = match path.split_once('/') {
                Some((b, k)) => (b, k),
                None => (path, ""),
            };
            if b.is_empty() {
                return Err(anyhow!("path-style S3 URL has no bucket: `{url}`"));
            }
            let key = (!k.is_empty()).then(|| decode_s3_key(k));
            ("path", b.to_string(), region, suffix, key)
        } else {
            // Virtual-hosted: bucket is everything before the `.s3.` marker.
            let (b, after) = host
                .split_once(".s3.")
                .ok_or_else(|| anyhow!("not an S3 URL host `{host}`"))?;
            if b.is_empty() {
                return Err(anyhow!("virtual-hosted S3 URL has no bucket: `{url}`"));
            }
            let (region, suffix) = split_s3_endpoint(after)?;
            let key = (!path.is_empty()).then(|| decode_s3_key(path));
            ("virtual-hosted", b.to_string(), region, suffix, key)
        };
    let partition = partition_of(&region);
    Ok(json!({
        "url": url,
        "bucket": bucket,
        "key": key,
        "region": region,
        "partition": partition,
        "style": style,
        "host": host,
        "dns_suffix": suffix,
    }))
}

/// Derive the AWS region from an availability-zone name. A standard AZ is the
/// region followed by a single zone letter (`us-east-1a` → `us-east-1`,
/// `eu-west-2c` → `eu-west-2`), so the region is the AZ with that trailing
/// letter removed; the preceding character must be the region's trailing digit.
/// opts: `az` (or `zone`). Returns `{az, region, zone_letter}`; errors if the
/// suffix isn't a region-digit followed by one letter. Pure.
fn op_region_for_az(opts: Value) -> Result<Value> {
    let az = opts
        .get("az")
        .or_else(|| opts.get("zone"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing az"))?;
    let bytes = az.as_bytes();
    if az.len() < 2
        || !bytes[az.len() - 1].is_ascii_lowercase()
        || !bytes[az.len() - 2].is_ascii_digit()
    {
        return Err(anyhow!(
            "not a standard AZ (want <region><letter>, e.g. us-east-1a): {az}"
        ));
    }
    let region = &az[..az.len() - 1];
    let zone_letter = &az[az.len() - 1..];
    Ok(json!({"az": az, "region": region, "zone_letter": zone_letter}))
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn aws__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |_| async {
        Ok(json!({"version": env!("CARGO_PKG_VERSION")}))
    })
}

#[no_mangle]
pub extern "C" fn aws__sts_caller_identity(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sts_caller_identity)
}

#[no_mangle]
pub extern "C" fn aws__s3_list_buckets(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_s3_list_buckets)
}

#[no_mangle]
pub extern "C" fn aws__s3_list_objects(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_s3_list_objects)
}

#[no_mangle]
pub extern "C" fn aws__s3_get_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_s3_get_object)
}

#[no_mangle]
pub extern "C" fn aws__s3_put_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_s3_put_object)
}

#[no_mangle]
pub extern "C" fn aws__s3_delete_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_s3_delete_object)
}

#[no_mangle]
pub extern "C" fn aws__ddb_list_tables(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_list_tables)
}

#[no_mangle]
pub extern "C" fn aws__ddb_put_item(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_put_item)
}

#[no_mangle]
pub extern "C" fn aws__ddb_get_item(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_get_item)
}

#[no_mangle]
pub extern "C" fn aws__sqs_list_queues(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sqs_list_queues)
}

#[no_mangle]
pub extern "C" fn aws__sqs_send_message(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sqs_send_message)
}

#[no_mangle]
pub extern "C" fn aws__sqs_receive_message(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sqs_receive_message)
}

#[no_mangle]
pub extern "C" fn aws__sqs_delete_message(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sqs_delete_message)
}

#[no_mangle]
pub extern "C" fn aws__lambda_invoke(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_lambda_invoke)
}

#[no_mangle]
pub extern "C" fn aws__lambda_list_functions(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_lambda_list_functions)
}

#[no_mangle]
pub extern "C" fn aws__s3_head_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_s3_head_object)
}

#[no_mangle]
pub extern "C" fn aws__ddb_delete_item(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_delete_item)
}

#[no_mangle]
pub extern "C" fn aws__ddb_query(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_query)
}

#[no_mangle]
pub extern "C" fn aws__ddb_scan(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_scan)
}

#[no_mangle]
pub extern "C" fn aws__ddb_describe_table(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_describe_table)
}

#[no_mangle]
pub extern "C" fn aws__sqs_purge_queue(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sqs_purge_queue)
}

#[no_mangle]
pub extern "C" fn aws__sqs_get_queue_attributes(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sqs_get_queue_attributes)
}

#[no_mangle]
pub extern "C" fn aws__sts_assume_role(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sts_assume_role)
}

#[no_mangle]
pub extern "C" fn aws__s3_copy_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_s3_copy_object)
}

#[no_mangle]
pub extern "C" fn aws__s3_delete_objects(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_s3_delete_objects)
}

#[no_mangle]
pub extern "C" fn aws__ddb_update_item(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_update_item)
}

#[no_mangle]
pub extern "C" fn aws__ddb_batch_get_item(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_batch_get_item)
}

#[no_mangle]
pub extern "C" fn aws__ddb_batch_write_item(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ddb_batch_write_item)
}

#[no_mangle]
pub extern "C" fn aws__sns_list_topics(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sns_list_topics)
}

#[no_mangle]
pub extern "C" fn aws__sns_publish(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sns_publish)
}

#[no_mangle]
pub extern "C" fn aws__sns_create_topic(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sns_create_topic)
}

#[no_mangle]
pub extern "C" fn aws__sns_subscribe(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_sns_subscribe)
}

#[no_mangle]
pub extern "C" fn aws__ssm_get_parameter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ssm_get_parameter)
}

#[no_mangle]
pub extern "C" fn aws__ssm_put_parameter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ssm_put_parameter)
}

#[no_mangle]
pub extern "C" fn aws__ssm_get_parameters_by_path(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ssm_get_parameters_by_path)
}

#[no_mangle]
pub extern "C" fn aws__ssm_delete_parameter(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ssm_delete_parameter)
}

#[no_mangle]
pub extern "C" fn aws__secrets_get_value(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secrets_get_value)
}

#[no_mangle]
pub extern "C" fn aws__secrets_create(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secrets_create)
}

#[no_mangle]
pub extern "C" fn aws__secrets_put_value(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secrets_put_value)
}

#[no_mangle]
pub extern "C" fn aws__secrets_list(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secrets_list)
}

#[no_mangle]
pub extern "C" fn aws__ses_send_email(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ses_send_email)
}

#[no_mangle]
pub extern "C" fn aws__cloudwatch_put_metric(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_cloudwatch_put_metric)
}

#[no_mangle]
pub extern "C" fn aws__cloudwatch_list_metrics(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_cloudwatch_list_metrics)
}

#[no_mangle]
pub extern "C" fn aws__parse_arn(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_arn(opts) })
}

#[no_mangle]
pub extern "C" fn aws__build_arn(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_arn(opts) })
}

#[no_mangle]
pub extern "C" fn aws__parse_s3_uri(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_s3_uri(opts) })
}

#[no_mangle]
pub extern "C" fn aws__build_s3_uri(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_s3_uri(opts) })
}

#[no_mangle]
pub extern "C" fn aws__s3_uri_to_arn(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_s3_uri_to_arn(opts) })
}

#[no_mangle]
pub extern "C" fn aws__arn_to_s3_uri(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_arn_to_s3_uri(opts) })
}

#[no_mangle]
pub extern "C" fn aws__valid_bucket_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_bucket_name(opts) })
}

#[no_mangle]
pub extern "C" fn aws__valid_s3_key(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_s3_key(opts) })
}

#[no_mangle]
pub extern "C" fn aws__valid_sqs_queue_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_sqs_queue_name(opts) })
}

#[no_mangle]
pub extern "C" fn aws__valid_account_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_account_id(opts) })
}

#[no_mangle]
pub extern "C" fn aws__valid_arn(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_arn(opts) })
}

#[no_mangle]
pub extern "C" fn aws__arn_matches(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_arn_matches(opts) })
}

#[no_mangle]
pub extern "C" fn aws__partition_for_region(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_partition_for_region(opts) })
}

#[no_mangle]
pub extern "C" fn aws__valid_region(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_region(opts) })
}

#[no_mangle]
pub extern "C" fn aws__dns_suffix_for_partition(args: *const c_char) -> *const c_char {
    ffi_call_async(
        args,
        |opts| async move { op_dns_suffix_for_partition(opts) },
    )
}

#[no_mangle]
pub extern "C" fn aws__service_endpoint(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_service_endpoint(opts) })
}

#[no_mangle]
pub extern "C" fn aws__parse_service_endpoint(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_service_endpoint(opts) })
}

#[no_mangle]
pub extern "C" fn aws__s3_object_url(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_s3_object_url(opts) })
}

#[no_mangle]
pub extern "C" fn aws__parse_s3_url(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_s3_url(opts) })
}

#[no_mangle]
pub extern "C" fn aws__region_for_az(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_region_for_az(opts) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::collections::HashMap;

    /// `json_to_av` (used by `op_ddb_update_item` and the new write paths) is
    /// the inverse of `attribute_value_to_json`. Pin each scalar mapping:
    /// string→S, integer/float→N (verbatim, no reformatting), bool→BOOL,
    /// null→NULL. A regression that, e.g., maps numbers to S would silently
    /// store a DynamoDB string where a number was intended (breaking range
    /// queries on that attribute).
    #[test]
    fn json_to_av_maps_scalars_to_matching_attribute_types() {
        assert_eq!(json_to_av(&json!("x")), AttributeValue::S("x".into()));
        assert_eq!(json_to_av(&json!(42)), AttributeValue::N("42".into()));
        assert_eq!(json_to_av(&json!(1.25)), AttributeValue::N("1.25".into()));
        assert_eq!(json_to_av(&json!(true)), AttributeValue::Bool(true));
        assert_eq!(json_to_av(&Value::Null), AttributeValue::Null(true));
    }

    /// A large integer must not be reformatted via float (which would lose
    /// precision past 2^53). `serde_json::Number::to_string` preserves the
    /// exact digits — pin it so a refactor through `as_f64().to_string()`
    /// is caught.
    #[test]
    fn json_to_av_large_integer_preserves_exact_digits() {
        let big = json!(9_007_199_254_740_993_i64); // 2^53 + 1
        assert_eq!(
            json_to_av(&big),
            AttributeValue::N("9007199254740993".into())
        );
    }

    #[test]
    fn string_vec_accepts_array_single_and_null() {
        assert_eq!(string_vec(&json!(["a", "b"])).unwrap(), vec!["a", "b"]);
        assert_eq!(string_vec(&json!("solo")).unwrap(), vec!["solo"]);
        assert!(string_vec(&Value::Null).unwrap().is_empty());
        assert!(
            string_vec(&json!(["a", 1])).is_err(),
            "non-string element must error"
        );
    }

    #[test]
    fn av_string_roundtrips_as_json_string() {
        let av = AttributeValue::S("hello".into());
        assert_eq!(attribute_value_to_json(&av), json!("hello"));
    }

    #[test]
    fn av_number_int_decodes_to_i64() {
        let av = AttributeValue::N("42".into());
        assert_eq!(attribute_value_to_json(&av), json!(42_i64));
    }

    #[test]
    fn av_number_float_decodes_to_f64() {
        let av = AttributeValue::N("1.25".into());
        // f64 NaN-safe equality via serde Value.
        // Avoid 3.14 to dodge clippy::approx_constant (≈ PI).
        assert_eq!(attribute_value_to_json(&av), json!(1.25_f64));
    }

    #[test]
    fn av_number_non_numeric_falls_back_to_string() {
        // DynamoDB N type accepts numeric strings; if a caller wrote a
        // non-numeric there, surface it as a string rather than panicking.
        let av = AttributeValue::N("not-a-number".into());
        assert_eq!(attribute_value_to_json(&av), json!("not-a-number"));
    }

    #[test]
    fn av_bool_and_null() {
        assert_eq!(
            attribute_value_to_json(&AttributeValue::Bool(true)),
            json!(true)
        );
        assert_eq!(
            attribute_value_to_json(&AttributeValue::Bool(false)),
            json!(false)
        );
        assert_eq!(
            attribute_value_to_json(&AttributeValue::Null(true)),
            Value::Null
        );
    }

    #[test]
    fn av_string_set_renders_as_json_array() {
        let av = AttributeValue::Ss(vec!["a".into(), "b".into()]);
        assert_eq!(attribute_value_to_json(&av), json!(["a", "b"]));
    }

    /// Number-set type — parallel to string-set above. NS ships as
    /// number-STRINGS in the DDB API; we preserve that shape rather than
    /// parsing through to numeric JSON because some apps round-trip values
    /// like "01" / scientific notation through DDB and care about exact
    /// serialization. Pin so a refactor that "helpfully" parses NS
    /// entries to f64 gets caught.
    #[test]
    fn av_number_set_renders_as_json_array_of_strings() {
        let av = AttributeValue::Ns(vec!["1".into(), "2.5".into(), "3e10".into()]);
        assert_eq!(attribute_value_to_json(&av), json!(["1", "2.5", "3e10"]));
    }

    #[test]
    fn av_list_recurses_per_element() {
        let av = AttributeValue::L(vec![
            AttributeValue::S("x".into()),
            AttributeValue::N("7".into()),
            AttributeValue::Bool(true),
        ]);
        assert_eq!(attribute_value_to_json(&av), json!(["x", 7, true]));
    }

    #[test]
    fn av_map_preserves_keys_and_recurses_values() {
        let mut m = HashMap::new();
        m.insert("name".to_string(), AttributeValue::S("ada".into()));
        m.insert("age".to_string(), AttributeValue::N("36".into()));
        let av = AttributeValue::M(m);
        let v = attribute_value_to_json(&av);
        assert_eq!(v["name"], json!("ada"));
        assert_eq!(v["age"], json!(36_i64));
    }

    #[test]
    fn av_nested_map_in_list_round_trips() {
        let mut inner = HashMap::new();
        inner.insert("k".to_string(), AttributeValue::S("v".into()));
        let av = AttributeValue::L(vec![AttributeValue::M(inner)]);
        let v = attribute_value_to_json(&av);
        assert_eq!(v[0]["k"], json!("v"));
    }

    // Regression catcher: the N→i64→f64→string fallback chain MUST try
    // i64 before f64. If a future refactor swaps the order, every integer
    // boundary (incl. i64::MAX / i64::MIN, which a DynamoDB sort key or
    // counter can hit exactly) silently degrades through f64 and loses
    // the last 3 bits of precision (f64 mantissa is 53 bits, i64 is 64).
    // Hard-coded literals here are the boundary; not the value-under-test.
    #[test]
    fn av_number_i64_boundary_round_trips_exactly() {
        let max = AttributeValue::N(i64::MAX.to_string());
        let got_max = attribute_value_to_json(&max);
        assert_eq!(got_max, json!(i64::MAX));
        // serde_json::Value::as_i64() must succeed — proves it's still in
        // the i64 lane, not the f64 lane (where it would lose precision).
        assert_eq!(got_max.as_i64(), Some(i64::MAX));

        let min = AttributeValue::N(i64::MIN.to_string());
        let got_min = attribute_value_to_json(&min);
        assert_eq!(got_min, json!(i64::MIN));
        assert_eq!(got_min.as_i64(), Some(i64::MIN));
    }

    // Sign-handling regression catcher: the existing tests only cover
    // positive integers. A future swap to `parse::<u64>()` (a common
    // "optimization" for non-negative IDs) would silently reject all
    // negative N values and divert them through the f64 lane, which
    // accepts negatives but loses precision near i64::MIN. Pin the
    // negative-int → i64 path explicitly.
    #[test]
    fn av_number_negative_integer_decodes_to_i64() {
        let av = AttributeValue::N("-12345".into());
        let v = attribute_value_to_json(&av);
        assert_eq!(v, json!(-12345_i64));
        assert_eq!(v.as_i64(), Some(-12345));
    }

    // Empty-collection bug-class catcher: a naive refactor that indexes
    // arr[0] / m.iter().next().unwrap() on the recursive branch would
    // panic here. The .iter().map().collect() pattern must tolerate
    // zero-length input silently. Also pins the JSON shape: empty L
    // renders [] (not null), empty M renders {} (not null) — DynamoDB
    // round-trip consumers depend on this discrimination.
    #[test]
    fn av_empty_list_and_map_render_as_empty_collections() {
        let empty_l = AttributeValue::L(vec![]);
        assert_eq!(attribute_value_to_json(&empty_l), json!([]));
        assert!(attribute_value_to_json(&empty_l).is_array());

        let empty_m = AttributeValue::M(HashMap::new());
        assert_eq!(attribute_value_to_json(&empty_m), json!({}));
        assert!(attribute_value_to_json(&empty_m).is_object());
    }

    // Scientific-notation regression catcher. The i64 lane uses
    // `str::parse::<i64>`, which rejects "1e10" (digit-only required).
    // The f64 lane accepts it and produces 10000000000.0 — a value that
    // FITS in i64 exactly but is materialized as a float. A future
    // refactor that "helpfully" detects integer-shaped floats and
    // promotes them to i64 would change this output. That change is a
    // behavioral break for any caller relying on JSON number `.is_f64()`
    // to mean "the source string had a decimal point or exponent". Pin
    // the current lane discrimination explicitly.
    #[test]
    fn av_number_scientific_notation_integer_stays_in_f64_lane() {
        let av = AttributeValue::N("1e10".into());
        let v = attribute_value_to_json(&av);
        assert_eq!(v, json!(10_000_000_000.0_f64));
        // The strong invariant: it must NOT be reported as i64, even
        // though the value fits exactly. This is what distinguishes the
        // current "string-shape" routing from a hypothetical
        // "value-shape" routing.
        assert!(v.is_f64(), "1e10 must stay in f64 lane, got {v:?}");
        assert_eq!(v.as_i64(), None, "1e10 must not be reachable via as_i64");
    }

    // Number-overflow regression catcher. Values above i64::MAX (or
    // below i64::MIN) cannot use the i64 lane and silently degrade to
    // f64 with precision loss. Pin that the degradation is to f64 (NOT
    // to the String fallback) so a future refactor that adds a u64 lane
    // — or that diverts overflow to the String catch-all — is caught.
    // The specific value chosen (i64::MAX + 1 = 2^63) is the smallest
    // u64 that exceeds i64::MAX; it is exactly representable in f64
    // (well within the 53-bit mantissa with all-zero low bits), so
    // f64 → as_f64 must equal 9.223372036854776e18. The +1 vs i64::MAX
    // is invisible in f64 — that is the precision loss being pinned.
    #[test]
    fn av_number_above_i64_max_degrades_to_f64_not_string() {
        // i64::MAX = 9223372036854775807; +1 overflows.
        let av = AttributeValue::N("9223372036854775808".into());
        let v = attribute_value_to_json(&av);
        // Must be a JSON number (not a String fallback).
        assert!(
            v.is_number(),
            "i64-overflow value must remain numeric, got {v:?}"
        );
        // Specifically in the f64 lane (not i64).
        assert!(v.is_f64(), "i64-overflow value must be f64, got {v:?}");
        // The exact f64 value — 2^63 = 9.223372036854776e18.
        assert_eq!(v.as_f64(), Some(9_223_372_036_854_775_808_f64));
    }

    // Empty-string N regression catcher. `AttributeValue::N("")`
    // should not reach AWS (the DDB API rejects empty number strings),
    // but a buggy caller could synthesize one. Both parse::<i64> and
    // parse::<f64> reject empty input, so the chain must fall through
    // to the Value::String fallback. Pin this — a refactor that
    // panics on empty input here (e.g., switching to `unwrap`) would
    // be UB across the FFI when the panic escapes catch_unwind on a
    // synchronous code path.
    #[test]
    fn av_number_empty_string_falls_back_to_empty_json_string() {
        let av = AttributeValue::N(String::new());
        let v = attribute_value_to_json(&av);
        assert_eq!(v, json!(""));
        assert!(v.is_string());
        // Importantly NOT a number (not 0, not null).
        assert!(!v.is_number());
        assert!(!v.is_null());
    }
}

#[cfg(test)]
mod ffi_tests {
    //! FFI safety pins. The cdylib is dlopened in-process by stryke;
    //! a panic here crashes the host shell, a UB read here corrupts
    //! the host's address space. These tests defend the C-ABI contract
    //! against regressions that would only surface at runtime on a
    //! caller's machine.
    use super::*;
    use std::ffi::CStr;
    use std::ptr;

    // Helper: drain an export's *const c_char into an owned String AND
    // free the underlying CString through the public free symbol, so
    // we exercise the full alloc/free contract every test.
    unsafe fn drain(p: *const c_char) -> String {
        assert!(!p.is_null(), "export returned null pointer");
        let s = CStr::from_ptr(p).to_string_lossy().into_owned();
        stryke_free_cstring(p as *mut c_char);
        s
    }

    // Catches: regression where someone removes the `args.is_null()`
    // branch in `ffi_call_async` and unconditionally dereferences via
    // `CStr::from_ptr(args)`. A null deref here is instant UB — the
    // host shell would segfault on `use AWS; AWS::pkg_version()` if
    // stryke ever passes a null args pointer (which it does for
    // zero-arg ops). The branch is on line 415; pin its behavior.
    #[test]
    fn ffi_null_args_returns_well_formed_json_not_segfault() {
        let raw = aws__pkg_version(ptr::null());
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("output must be valid JSON");
        // pkg_version specifically must surface a version string, not
        // an error — proves the null-args path reached the handler.
        assert_eq!(
            v["version"],
            json!(env!("CARGO_PKG_VERSION")),
            "pkg_version should ignore args and return crate version, got {s}"
        );
        assert!(v.get("error").is_none(), "null args must not produce error");
    }

    // Catches: regression where invalid JSON in `args` either (a)
    // panics across the FFI boundary (instant UB in C callers) or (b)
    // is silently propagated as garbage. Current contract per line
    // 419 (`unwrap_or(Value::Null)`) is "silently substitute Null" —
    // handlers that don't read args must still succeed. If a future
    // refactor swaps to `.unwrap()` or `.expect(...)`, the panic
    // would be caught by `catch_unwind` and surface as an error JSON
    // instead of the version. This test pins the "tolerate garbage,
    // succeed when handler ignores it" contract.
    #[test]
    fn ffi_invalid_json_args_does_not_crash_or_propagate_to_handler() {
        // Embed an interior NUL-incompatible value via a CString with
        // raw bytes that parse as definitely-not-JSON.
        let garbage = std::ffi::CString::new("this is not json {[ ").unwrap();
        let raw = aws__pkg_version(garbage.as_ptr());
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("output must be valid JSON");
        // Handler ignores args, so output should be the version, not
        // an error from JSON parsing.
        assert_eq!(v["version"], json!(env!("CARGO_PKG_VERSION")));
        assert!(
            v.get("error").is_none(),
            "garbage args must be coerced to Null, not surfaced as error"
        );
    }

    // Catches: regression where `std::panic::catch_unwind` or the
    // `AssertUnwindSafe` wrapper is removed from `ffi_call_async`.
    // Without panic recovery, a panic in any handler unwinds across
    // the C ABI — undefined behavior, and in practice on Linux/macOS
    // it aborts the host process (the stryke shell). This test calls
    // the private `ffi_call_async` with a deliberately-panicking
    // handler and asserts the panic is caught and surfaced as the
    // contract's error JSON (`{"error":"stryke-aws handler panicked"}`).
    #[test]
    fn ffi_handler_panic_is_caught_and_returned_as_error_json() {
        let raw = ffi_call_async(ptr::null(), |_v| async {
            panic!("intentional test panic — must not cross FFI");
        });
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("output must be valid JSON");
        assert_eq!(
            v["error"],
            json!("stryke-aws handler panicked"),
            "panic must be caught and surfaced as the documented error \
             string, got {s}"
        );
    }

    // Catches: regression in `stryke_free_cstring`'s null guard at
    // line 444. Removing the `if p.is_null() { return; }` would make
    // `CString::from_raw(null)` an instant UB. Callers (stryke's FFI
    // bridge) free EVERY returned pointer including ones from failed
    // allocations, so null-tolerance is load-bearing. Also pins that
    // free is idempotent against null — never reaches the drop path.
    #[test]
    fn ffi_free_cstring_tolerates_null_without_ub() {
        unsafe {
            stryke_free_cstring(ptr::null_mut());
            // Calling twice would only matter if the first call had
            // side effects — proving no side effects by surviving
            // a second call.
            stryke_free_cstring(ptr::null_mut());
        }
    }

    // Catches: regression in the `Ok(Err(e))` arm of `ffi_call_async`
    // (line 438). Every real op handler returns `anyhow::Result` and the
    // common path is failure (missing `bucket`/`table`/`key`, network
    // error, AWS API error). The contract is that a handler `Err` is
    // mapped to `{"error": "<e.to_string()>"}` — verbatim, no wrapping,
    // no key rename. stryke's bridge inspects the `error` key to decide
    // success vs failure, so a regression that renamed the key, nested
    // the message, or dropped the original `e.to_string()` text would
    // silently break error reporting for every op. The success and
    // panic arms are already pinned; this pins the handler-error arm.
    #[test]
    fn ffi_handler_err_maps_to_error_key_with_verbatim_message() {
        let raw = ffi_call_async(ptr::null(), |_v| async { Err(anyhow!("missing bucket")) });
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("output must be valid JSON");
        assert_eq!(
            v["error"],
            json!("missing bucket"),
            "handler Err must surface as {{\"error\": <verbatim message>}}, got {s}"
        );
        // The error message must be the top-level value of `error`, not
        // nested under another object/array.
        assert!(
            v["error"].is_string(),
            "error must be a flat string, not a nested structure: {s}"
        );
    }

    // Catches: regression where `ffi_call_async` stops threading the
    // parsed args `Value` through to the handler — e.g. a refactor that
    // hardcodes `handler(Value::Null)` or parses into the wrong binding.
    // Every existing FFI test drives `aws__pkg_version`, which IGNORES
    // its args, so none of them would notice if args silently became
    // Null. This handler echoes back what it received, proving a
    // non-null args pointer is (a) parsed as JSON and (b) delivered
    // intact — including a nested structure and a unicode string, which
    // also pins that `CStr::to_bytes()` (NOT `to_bytes_with_nul()`) is
    // used so the trailing C NUL is stripped before serde parses. If
    // the terminating NUL leaked into the parse buffer, serde would
    // reject the doc and the handler would receive Null instead.
    #[test]
    fn ffi_args_are_parsed_and_threaded_to_handler_intact() {
        let doc = std::ffi::CString::new(r#"{"region":"eu-wést-1","nested":{"n":42}}"#).unwrap();
        let raw = ffi_call_async(doc.as_ptr(), |v| async move {
            // Echo the received value straight back so the test can
            // assert what the handler actually saw.
            Ok(v)
        });
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("output must be valid JSON");
        assert_eq!(
            v["region"],
            json!("eu-wést-1"),
            "unicode arg value must reach the handler byte-for-byte, got {s}"
        );
        assert_eq!(
            v["nested"]["n"],
            json!(42),
            "nested arg structure must reach the handler intact, got {s}"
        );
        assert!(
            v.get("error").is_none(),
            "valid JSON args must parse cleanly, not degrade to error/Null: {s}"
        );
    }

    // Catches: regression where the args-parse failure path (line 432,
    // `unwrap_or(Value::Null)`) is changed so that MALFORMED JSON in a
    // NON-NULL pointer reaches the handler as garbage rather than Null.
    // Distinct from the existing `ffi_invalid_json_args_*` test, which
    // drives the args-ignoring `pkg_version`: this one uses a handler
    // that branches on whether it received Null, proving the malformed
    // bytes were coerced to `Value::Null` specifically (not, say, an
    // empty object or a panic). The contract "unparseable args ⇒ Null
    // handed to handler" is what lets every op treat `opts["x"]` lookups
    // as a clean miss rather than a parse crash.
    #[test]
    fn ffi_malformed_nonnull_args_reach_handler_as_null() {
        let garbage = std::ffi::CString::new("{not valid json").unwrap();
        let raw = ffi_call_async(garbage.as_ptr(), |v| async move {
            // Report back exactly which Value the plumbing produced.
            Ok(json!({ "was_null": v.is_null(), "received": v }))
        });
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("output must be valid JSON");
        assert_eq!(
            v["was_null"],
            json!(true),
            "malformed non-null args must be coerced to Value::Null, got {s}"
        );
        assert_eq!(
            v["received"],
            Value::Null,
            "handler must receive JSON null, not a partial/garbage value: {s}"
        );
    }

    /// `json_to_attribute_value` ↔ `attribute_value_to_json` must round-trip
    /// the scalar + nested-collection shapes query/scan/delete depend on. A
    /// regression that drops the L/M recursion would silently flatten nested
    /// items to Debug strings on the way back.
    #[test]
    fn attribute_value_round_trips_nested() {
        let input = json!({
            "id": "abc",
            "n": 42,
            "ok": true,
            "tags": ["x", "y"],
            "meta": { "k": "v", "nested": [1, 2] }
        });
        let obj = input.as_object().unwrap();
        let av_map = json_obj_to_av_map(obj);
        let back = ddb_item_to_json(&av_map);
        // Numbers come back as JSON numbers, strings as strings, nested intact.
        assert_eq!(back["id"], "abc");
        assert_eq!(back["n"], 42);
        assert_eq!(back["ok"], true);
        assert_eq!(back["tags"], json!(["x", "y"]));
        assert_eq!(back["meta"]["k"], "v");
        assert_eq!(back["meta"]["nested"], json!([1, 2]));
    }

    // ── pure helpers (no AWS) ────────────────────────────────────────────────

    #[test]
    fn parse_arn_slash_resource_form() {
        let v = op_parse_arn(json!({
            "arn": "arn:aws:iam::123456789012:role/Admin"
        }))
        .unwrap();
        assert_eq!(v["partition"], json!("aws"));
        assert_eq!(v["service"], json!("iam"));
        assert_eq!(v["region"], Value::Null, "IAM is global → empty region");
        assert_eq!(v["account_id"], json!("123456789012"));
        assert_eq!(v["resource_type"], json!("role"));
        assert_eq!(v["resource_id"], json!("Admin"));
    }

    #[test]
    fn parse_arn_colon_and_bare_resource_forms() {
        // colon-separated resource (e.g. Lambda, SNS).
        let lambda = op_parse_arn(json!({
            "arn": "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
        }))
        .unwrap();
        assert_eq!(lambda["region"], json!("us-east-1"));
        assert_eq!(lambda["resource_type"], json!("function"));
        assert_eq!(lambda["resource_id"], json!("my-fn"));
        // bare resource (e.g. S3 bucket): no type.
        let s3 = op_parse_arn(json!({"arn": "arn:aws:s3:::my-bucket"})).unwrap();
        assert_eq!(s3["resource_type"], Value::Null);
        assert_eq!(s3["resource_id"], json!("my-bucket"));
        assert_eq!(s3["account_id"], Value::Null, "S3 ARNs carry no account");
    }

    #[test]
    fn parse_arn_rejects_non_arn() {
        assert!(op_parse_arn(json!({"arn": "not:an:arn"})).is_err());
        assert!(op_parse_arn(json!({})).is_err());
    }

    #[test]
    fn build_arn_round_trips_through_parse() {
        let built = op_build_arn(json!({
            "service": "iam", "account_id": "123456789012", "resource": "role/Admin"
        }))
        .unwrap();
        let arn = built["arn"].as_str().unwrap();
        // partition defaults to aws, region empty → adjacent `::`.
        assert_eq!(arn, "arn:aws:iam::123456789012:role/Admin");
        let parsed = op_parse_arn(json!({"arn": arn})).unwrap();
        assert_eq!(parsed["service"], json!("iam"));
        assert_eq!(parsed["resource_id"], json!("Admin"));
    }

    #[test]
    fn parse_s3_uri_splits_bucket_and_key() {
        let v = op_parse_s3_uri(json!({"uri": "s3://my-bucket/path/to/object.txt"})).unwrap();
        assert_eq!(v["bucket"], json!("my-bucket"));
        assert_eq!(v["key"], json!("path/to/object.txt"));
        let bucket_only = op_parse_s3_uri(json!({"uri": "s3://my-bucket"})).unwrap();
        assert_eq!(bucket_only["key"], Value::Null);
        assert!(op_parse_s3_uri(json!({"uri": "http://x/y"})).is_err());
    }

    #[test]
    fn build_s3_uri_round_trips_through_parse() {
        let built = op_build_s3_uri(json!({"bucket": "my-bucket", "key": "path/to/object.txt"}))
            .unwrap()["uri"]
            .clone();
        assert_eq!(built, json!("s3://my-bucket/path/to/object.txt"));
        let back = op_parse_s3_uri(json!({"uri": built})).unwrap();
        assert_eq!(back["bucket"], json!("my-bucket"));
        assert_eq!(back["key"], json!("path/to/object.txt"));
        // Bare bucket when key omitted or empty; leading slash on key trimmed.
        assert_eq!(
            op_build_s3_uri(json!({"bucket": "b"})).unwrap()["uri"],
            json!("s3://b")
        );
        assert_eq!(
            op_build_s3_uri(json!({"bucket": "b", "key": ""})).unwrap()["uri"],
            json!("s3://b")
        );
        assert_eq!(
            op_build_s3_uri(json!({"bucket": "b", "key": "/leading"})).unwrap()["uri"],
            json!("s3://b/leading")
        );
        assert!(op_build_s3_uri(json!({"bucket": ""})).is_err());
    }

    #[test]
    fn s3_uri_to_arn_maps_uri_to_object_and_bucket_arns() {
        // Object URI → object ARN (region/account empty for S3).
        let obj = op_s3_uri_to_arn(json!({"uri": "s3://my-bucket/path/to/object.txt"})).unwrap();
        assert_eq!(
            obj["arn"],
            json!("arn:aws:s3:::my-bucket/path/to/object.txt")
        );
        assert_eq!(obj["bucket"], json!("my-bucket"));
        assert_eq!(obj["key"], json!("path/to/object.txt"));
        // Bare bucket URI → bucket ARN, null key.
        let buck = op_s3_uri_to_arn(json!({"uri": "s3://my-bucket"})).unwrap();
        assert_eq!(buck["arn"], json!("arn:aws:s3:::my-bucket"));
        assert_eq!(buck["key"], Value::Null);
        // Trailing slash with no key is still a bucket ARN.
        assert_eq!(
            op_s3_uri_to_arn(json!({"uri": "s3://b/"})).unwrap()["arn"],
            json!("arn:aws:s3:::b")
        );
        // China partition.
        assert_eq!(
            op_s3_uri_to_arn(json!({"uri": "s3://b/k", "partition": "aws-cn"})).unwrap()["arn"],
            json!("arn:aws-cn:s3:::b/k")
        );
        assert!(op_s3_uri_to_arn(json!({"uri": "http://x/y"})).is_err());
    }

    #[test]
    fn arn_to_s3_uri_inverts_s3_uri_to_arn() {
        // Object ARN → object URI.
        let obj =
            op_arn_to_s3_uri(json!({"arn": "arn:aws:s3:::my-bucket/path/to/object.txt"})).unwrap();
        assert_eq!(obj["uri"], json!("s3://my-bucket/path/to/object.txt"));
        assert_eq!(obj["bucket"], json!("my-bucket"));
        assert_eq!(obj["key"], json!("path/to/object.txt"));
        // Bucket ARN → bare bucket URI, null key.
        let buck = op_arn_to_s3_uri(json!({"arn": "arn:aws:s3:::my-bucket"})).unwrap();
        assert_eq!(buck["uri"], json!("s3://my-bucket"));
        assert_eq!(buck["key"], Value::Null);
        // Round-trips with s3_uri_to_arn for any partition.
        for uri in ["s3://b/k", "s3://b", "s3://my-bucket/a/b/c.txt"] {
            let arn = op_s3_uri_to_arn(json!({"uri": uri, "partition": "aws-cn"})).unwrap()["arn"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(
                op_arn_to_s3_uri(json!({"arn": arn})).unwrap()["uri"],
                json!(uri)
            );
        }
        // Non-S3 service, IAM-style ARN with region/account, and junk all reject.
        assert!(
            op_arn_to_s3_uri(json!({"arn": "arn:aws:ec2:us-east-1:123:instance/i-1"})).is_err()
        );
        assert!(op_arn_to_s3_uri(json!({"arn": "arn:aws:s3:us-east-1:123:bucket/k"})).is_err());
        assert!(op_arn_to_s3_uri(json!({"arn": "s3://not-an-arn"})).is_err());
    }

    #[test]
    fn valid_bucket_name_enforces_s3_rules() {
        assert_eq!(
            op_valid_bucket_name(json!({"name": "my-logs.2025"})).unwrap()["valid"],
            json!(true)
        );
        for (name, want) in [
            ("ab", "3-63"),
            ("MyBucket", "lowercase"),
            ("-bad", "start and end"),
            ("a..b", "adjacent periods"),
            ("192.168.0.1", "IP address"),
        ] {
            let v = op_valid_bucket_name(json!({"name": name})).unwrap();
            assert_eq!(v["valid"], json!(false), "{name} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{name}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
    }

    #[test]
    fn valid_s3_key_enforces_byte_length_and_non_empty() {
        // Ordinary keys (with slashes and unicode) are valid.
        let v = op_valid_s3_key(json!({"key": "logs/2025/06/events.json"})).unwrap();
        assert_eq!(v["valid"], json!(true));
        assert_eq!(v["bytes"], json!(24));
        // A multibyte key counts UTF-8 bytes, not chars.
        let u = op_valid_s3_key(json!({"key": "café"})).unwrap();
        assert_eq!(u["bytes"], json!(5)); // é is 2 bytes
        assert_eq!(u["valid"], json!(true));
        // Empty is invalid.
        let e = op_valid_s3_key(json!({"key": ""})).unwrap();
        assert_eq!(e["valid"], json!(false));
        assert!(e["reason"].as_str().unwrap().contains("empty"));
        // Exactly 1024 bytes is valid; 1025 is not.
        let k1024 = "a".repeat(1024);
        assert_eq!(
            op_valid_s3_key(json!({"key": k1024})).unwrap()["valid"],
            json!(true)
        );
        let k1025 = "a".repeat(1025);
        let over = op_valid_s3_key(json!({"key": k1025})).unwrap();
        assert_eq!(over["valid"], json!(false));
        assert!(over["reason"].as_str().unwrap().contains("1024 bytes"));
        // `name` alias; missing arg errors.
        assert_eq!(
            op_valid_s3_key(json!({"name": "k"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_s3_key(json!({})).is_err());
    }

    #[test]
    fn valid_sqs_queue_name_enforces_create_queue_rules() {
        let chk = |n: &str| op_valid_sqs_queue_name(json!({ "name": n })).unwrap();
        // Standard queue names: alphanumeric, hyphen, underscore; case-sensitive.
        let ok = chk("my-Queue_1");
        assert_eq!(ok["valid"], json!(true));
        assert_eq!(ok["fifo"], json!(false));
        // A FIFO queue name ends with .fifo.
        let fifo = chk("orders.fifo");
        assert_eq!(fifo["valid"], json!(true));
        assert_eq!(fifo["fifo"], json!(true));
        // 80 chars is the limit (including .fifo).
        assert_eq!(chk(&"a".repeat(80))["valid"], json!(true));
        assert_eq!(chk(&"a".repeat(81))["valid"], json!(false));
        assert_eq!(
            chk(&("a".repeat(75) + ".fifo"))["valid"],
            json!(true),
            "75 + .fifo = 80, the limit"
        );
        assert_eq!(
            chk(&("a".repeat(76) + ".fifo"))["valid"],
            json!(false),
            "76 + .fifo = 81 > 80"
        );
        // A `.` is only allowed in the .fifo suffix.
        assert_eq!(chk("my.queue")["valid"], json!(false));
        // Other disallowed characters.
        for bad in ["", "has space", "slash/no", "dollar$"] {
            assert_eq!(chk(bad)["valid"], json!(false), "`{bad}` should be invalid");
        }
        // A bare `.fifo` has no base name.
        assert_eq!(chk(".fifo")["valid"], json!(false));
        assert!(op_valid_sqs_queue_name(json!({})).is_err());
    }

    #[test]
    fn valid_account_id_requires_exactly_twelve_digits() {
        // Canonical 12-digit IDs, including leading zeros.
        assert_eq!(
            op_valid_account_id(json!({"account_id": "123456789012"})).unwrap()["valid"],
            json!(true)
        );
        assert_eq!(
            op_valid_account_id(json!({"account_id": "012345678901"})).unwrap()["valid"],
            json!(true),
            "leading zeros are allowed"
        );
        // Wrong length and non-digit characters are rejected with a reason.
        for (id, want) in [
            ("12345678901", "12 digits"),   // 11 digits
            ("1234567890123", "12 digits"), // 13 digits
            ("12345678901a", "only digits"),
            ("", "12 digits"),
        ] {
            let v = op_valid_account_id(json!({ "account_id": id })).unwrap();
            assert_eq!(v["valid"], json!(false), "{id:?} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{id:?}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        // `id` / `value` aliases and the missing-arg error.
        assert_eq!(
            op_valid_account_id(json!({"id": "123456789012"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_account_id(json!({})).is_err());
    }

    #[test]
    fn valid_arn_checks_the_six_field_structure() {
        let ok = |arn: &str| {
            op_valid_arn(json!({ "arn": arn })).unwrap()["valid"]
                .as_bool()
                .unwrap()
        };
        // Fully-populated ARN, and the legitimately-empty region/account forms.
        assert!(ok("arn:aws:iam::123456789012:user/bob"));
        assert!(ok("arn:aws:s3:::my-bucket"));
        assert!(ok("arn:aws-cn:ec2:cn-north-1:123456789012:instance/i-0abc"));
        // The resource may itself contain colons (splitn keeps it whole).
        assert!(ok("arn:aws:lambda:us-east-1:123456789012:function:my-fn:1"));
        // Each structural violation is reported with a reason.
        for (arn, want) in [
            ("aws:iam::123:user/x", "6 colon-delimited fields"), // no `arn` and only 5 fields
            ("xyz:aws:s3:::b", "literal `arn`"),
            ("arn::s3:::b", "partition"),
            ("arn:aws::us-east-1:123:r", "service"),
            ("arn:aws:s3:::", "resource"),
        ] {
            let v = op_valid_arn(json!({ "arn": arn })).unwrap();
            assert_eq!(v["valid"], json!(false), "{arn:?} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{arn:?}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        // `value` alias + missing-arg error.
        assert_eq!(
            op_valid_arn(json!({"value": "arn:aws:s3:::b"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_arn(json!({})).is_err());
    }

    #[test]
    fn arn_matches_does_iam_wildcard_resource_matching() {
        let m = |pattern: &str, arn: &str| {
            op_arn_matches(json!({"pattern": pattern, "arn": arn})).unwrap()["matches"]
                .as_bool()
                .unwrap()
        };
        // An exact ARN matches itself.
        assert!(m("arn:aws:s3:::bucket", "arn:aws:s3:::bucket"));
        // `*` spans `/` — a bucket-wide pattern matches any object key.
        assert!(m("arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/a/b/c.txt"));
        // `*` is greedy across `:` segments too.
        assert!(m("arn:aws:s3:*", "arn:aws:s3:::bucket"));
        // The AWS-documented mid-pattern example: foo/*/bar spans extra segments.
        assert!(m(
            "arn:aws:s3:::my-bucket/foo/*/bar",
            "arn:aws:s3:::my-bucket/foo/1/2/bar"
        ));
        // `?` matches exactly one character.
        assert!(m(
            "arn:aws:ec2:us-east-?:*:instance/*",
            "arn:aws:ec2:us-east-1:123456789012:instance/i-0abc"
        ));
        assert!(!m("arn:aws:ec2:us-east-??:*", "arn:aws:ec2:us-east-1:1:r"));
        // A non-match: different bucket; matching is anchored and case-sensitive.
        assert!(!m("arn:aws:s3:::bucket/*", "arn:aws:s3:::other/key"));
        assert!(!m("arn:aws:s3:::Bucket", "arn:aws:s3:::bucket"));
        // A trailing literal after `*` must still be present.
        assert!(!m("arn:aws:s3:::bucket/*.png", "arn:aws:s3:::bucket/a.txt"));
        // `value` alias; missing args error.
        assert_eq!(
            op_arn_matches(json!({"pattern": "*", "value": "arn:aws:s3:::b"})).unwrap()["matches"],
            json!(true)
        );
        assert!(op_arn_matches(json!({"pattern": "*"})).is_err());
        assert!(op_arn_matches(json!({"arn": "x"})).is_err());
    }

    #[test]
    fn partition_for_region_resolves_all_five_partitions() {
        for (region, partition) in [
            ("us-east-1", "aws"),
            ("eu-west-3", "aws"),
            ("ap-southeast-2", "aws"),
            ("cn-north-1", "aws-cn"),
            ("cn-northwest-1", "aws-cn"),
            ("us-gov-west-1", "aws-us-gov"),
            ("us-gov-east-1", "aws-us-gov"),
            ("us-iso-east-1", "aws-iso"),
            ("us-isob-east-1", "aws-iso-b"),
        ] {
            assert_eq!(
                op_partition_for_region(json!({ "region": region })).unwrap()["partition"],
                json!(partition),
                "{region} → {partition}"
            );
        }
        // The us-isob- prefix must win over us-iso- (Top Secret, not Secret).
        assert_ne!(
            op_partition_for_region(json!({"region": "us-isob-east-1"})).unwrap()["partition"],
            json!("aws-iso")
        );
        assert!(op_partition_for_region(json!({})).is_err());
    }

    #[test]
    fn valid_region_enforces_botocore_region_regex() {
        let ok = |r: &str| op_valid_region(json!({ "region": r })).unwrap()["valid"] == json!(true);
        // Valid across every modeled partition.
        for r in [
            "us-east-1",
            "eu-west-3",
            "ap-southeast-2",
            "mx-central-1",
            "il-central-1",
            "cn-north-1",
            "us-gov-west-1",
            "us-iso-east-1",
            "us-isob-east-1",
        ] {
            assert!(ok(r), "{r} should be valid");
        }
        // The resolved partition is reported.
        let g = op_valid_region(json!({"region": "us-gov-east-1"})).unwrap();
        assert_eq!(g["partition"], json!("aws-us-gov"));
        assert_eq!(g["valid"], json!(true));
        // Invalid: unknown geography, wrong segment count, non-numeric trailer,
        // GovCloud/cn shape violations, empty.
        for bad in [
            "xx-east-1",   // unknown geo
            "us-east",     // missing number
            "us-east-one", // non-numeric
            "useast1",     // no separators
            "us-gov-1",    // gov needs an area segment
            "cn-1",        // cn needs an area segment
            "",
        ] {
            assert!(!ok(bad), "{bad} should be invalid");
        }
        // The reason is populated when invalid.
        let r = op_valid_region(json!({"region": "xx-east-1"})).unwrap();
        assert!(r["reason"].is_string(), "invalid region carries a reason");
        assert!(op_valid_region(json!({})).is_err());
    }

    #[test]
    fn dns_suffix_for_partition_matches_botocore() {
        for (partition, suffix) in [
            ("aws", "amazonaws.com"),
            ("aws-cn", "amazonaws.com.cn"),
            ("aws-us-gov", "amazonaws.com"),
            ("aws-iso", "c2s.ic.gov"),
            ("aws-iso-b", "sc2s.sgov.gov"),
        ] {
            assert_eq!(
                op_dns_suffix_for_partition(json!({ "partition": partition })).unwrap()
                    ["dns_suffix"],
                json!(suffix),
                "{partition} → {suffix}"
            );
        }
        // Round-trips with partition_for_region: region → partition → suffix.
        let part = op_partition_for_region(json!({"region": "cn-north-1"})).unwrap()["partition"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            op_dns_suffix_for_partition(json!({ "partition": part })).unwrap()["dns_suffix"],
            json!("amazonaws.com.cn")
        );
        // Unknown partition and missing input reject.
        assert!(op_dns_suffix_for_partition(json!({"partition": "aws-mars"})).is_err());
        assert!(op_dns_suffix_for_partition(json!({})).is_err());
    }

    #[test]
    fn service_endpoint_builds_regional_hostnames_across_partitions() {
        // Standard commercial partition.
        let v = op_service_endpoint(json!({"service": "s3", "region": "us-east-1"})).unwrap();
        assert_eq!(v["endpoint"], json!("s3.us-east-1.amazonaws.com"));
        assert_eq!(v["url"], json!("https://s3.us-east-1.amazonaws.com"));
        assert_eq!(v["partition"], json!("aws"));
        assert_eq!(v["dns_suffix"], json!("amazonaws.com"));
        // China partition uses amazonaws.com.cn.
        assert_eq!(
            op_service_endpoint(json!({"service": "dynamodb", "region": "cn-north-1"})).unwrap()
                ["endpoint"],
            json!("dynamodb.cn-north-1.amazonaws.com.cn")
        );
        // GovCloud + ISO partitions.
        assert_eq!(
            op_service_endpoint(json!({"service": "ec2", "region": "us-gov-west-1"})).unwrap()
                ["endpoint"],
            json!("ec2.us-gov-west-1.amazonaws.com")
        );
        assert_eq!(
            op_service_endpoint(json!({"service": "s3", "region": "us-iso-east-1"})).unwrap()
                ["endpoint"],
            json!("s3.us-iso-east-1.c2s.ic.gov")
        );
        // Missing service/region reject.
        assert!(op_service_endpoint(json!({"region": "us-east-1"})).is_err());
        assert!(op_service_endpoint(json!({"service": "s3"})).is_err());
    }

    #[test]
    fn parse_service_endpoint_inverts_service_endpoint() {
        // Bare host: every part recovered.
        let v =
            op_parse_service_endpoint(json!({"endpoint": "s3.us-east-1.amazonaws.com"})).unwrap();
        assert_eq!(v["service"], json!("s3"));
        assert_eq!(v["region"], json!("us-east-1"));
        assert_eq!(v["partition"], json!("aws"));
        assert_eq!(v["dns_suffix"], json!("amazonaws.com"));
        assert_eq!(v["url"], json!("https://s3.us-east-1.amazonaws.com"));
        // A full URL with a path resolves to the same host.
        assert_eq!(
            op_parse_service_endpoint(
                json!({"url": "https://dynamodb.cn-north-1.amazonaws.com.cn/"})
            )
            .unwrap()["partition"],
            json!("aws-cn")
        );
        // Round-trips service_endpoint across every partition.
        for (service, region) in [
            ("s3", "us-east-1"),
            ("dynamodb", "cn-north-1"),
            ("ec2", "us-gov-west-1"),
            ("s3", "us-iso-east-1"),
        ] {
            let built = op_service_endpoint(json!({"service": service, "region": region})).unwrap()
                ["endpoint"]
                .as_str()
                .unwrap()
                .to_string();
            let p = op_parse_service_endpoint(json!({ "endpoint": built })).unwrap();
            assert_eq!(
                p["service"],
                json!(service),
                "service round-trip {service}/{region}"
            );
            assert_eq!(
                p["region"],
                json!(region),
                "region round-trip {service}/{region}"
            );
        }
        // Errors: not a host, unrecognized suffix, missing.
        assert!(op_parse_service_endpoint(json!({"endpoint": "s3"})).is_err());
        assert!(
            op_parse_service_endpoint(json!({"endpoint": "s3.us-east-1.example.com"})).is_err()
        );
        assert!(op_parse_service_endpoint(json!({})).is_err());
    }

    #[test]
    fn region_for_az_strips_the_zone_letter() {
        let v = op_region_for_az(json!({"az": "us-east-1a"})).unwrap();
        assert_eq!(v["region"], json!("us-east-1"));
        assert_eq!(v["zone_letter"], json!("a"));
        assert_eq!(
            op_region_for_az(json!({"az": "eu-west-2c"})).unwrap()["region"],
            json!("eu-west-2")
        );
        assert_eq!(
            op_region_for_az(json!({"az": "ap-southeast-1b"})).unwrap()["region"],
            json!("ap-southeast-1")
        );
        // A bare region (ends in a digit) or a non-letter suffix errors.
        assert!(op_region_for_az(json!({"az": "us-east-1"})).is_err());
        assert!(op_region_for_az(json!({"az": "us-east-1A"})).is_err());
        assert!(op_region_for_az(json!({})).is_err());
    }

    #[test]
    fn s3_object_url_builds_virtual_hosted_style() {
        // The documented form: https://<bucket>.s3.<region>.<dns_suffix>/<key>.
        let v = op_s3_object_url(json!({
            "bucket": "amzn-s3-demo-bucket1", "region": "us-west-2", "key": "puppy.png",
        }))
        .unwrap();
        assert_eq!(
            v["url"],
            json!("https://amzn-s3-demo-bucket1.s3.us-west-2.amazonaws.com/puppy.png")
        );
        assert_eq!(
            v["host"],
            json!("amzn-s3-demo-bucket1.s3.us-west-2.amazonaws.com")
        );
        assert_eq!(v["partition"], json!("aws"));
        // No key → bucket root, no trailing slash.
        assert_eq!(
            op_s3_object_url(json!({"bucket": "b", "region": "eu-west-1"})).unwrap()["url"],
            json!("https://b.s3.eu-west-1.amazonaws.com")
        );
        // A leading slash on the key is trimmed.
        assert_eq!(
            op_s3_object_url(json!({"bucket": "b", "region": "us-east-1", "key": "/a/b.txt"}))
                .unwrap()["url"],
            json!("https://b.s3.us-east-1.amazonaws.com/a/b.txt")
        );
        // The key is percent-encoded; `/` is preserved as a path separator.
        assert_eq!(
            op_s3_object_url(
                json!({"bucket": "b", "region": "us-east-1", "key": "my dir/file (1).png"})
            )
            .unwrap()["url"],
            json!("https://b.s3.us-east-1.amazonaws.com/my%20dir/file%20%281%29.png")
        );
        // China partition's DNS suffix is honored.
        assert_eq!(
            op_s3_object_url(json!({"bucket": "b", "region": "cn-north-1", "key": "k"})).unwrap()
                ["url"],
            json!("https://b.s3.cn-north-1.amazonaws.com.cn/k")
        );
        // Missing bucket/region reject.
        assert!(op_s3_object_url(json!({"region": "us-east-1"})).is_err());
        assert!(op_s3_object_url(json!({"bucket": "b"})).is_err());
    }

    #[test]
    fn parse_s3_url_inverts_s3_object_url() {
        // Virtual-hosted (what s3_object_url emits): recover every part.
        let v = op_parse_s3_url(json!({
            "url": "https://amzn-s3-demo-bucket1.s3.us-west-2.amazonaws.com/puppy.png",
        }))
        .unwrap();
        assert_eq!(v["style"], json!("virtual-hosted"));
        assert_eq!(v["bucket"], json!("amzn-s3-demo-bucket1"));
        assert_eq!(v["key"], json!("puppy.png"));
        assert_eq!(v["region"], json!("us-west-2"));
        assert_eq!(v["partition"], json!("aws"));
        assert_eq!(v["dns_suffix"], json!("amazonaws.com"));
        // Round-trip: build then parse recovers bucket/region/key for every form.
        for (bucket, region, key) in [
            ("b", "us-east-1", Some("my dir/file (1).png")),
            ("amzn-s3-demo-bucket1", "us-west-2", Some("puppy.png")),
            ("b", "cn-north-1", Some("k")),
            ("b", "eu-west-1", None),
        ] {
            let mut build = json!({"bucket": bucket, "region": region});
            if let Some(k) = key {
                build["key"] = json!(k);
            }
            let url = op_s3_object_url(build).unwrap()["url"]
                .as_str()
                .unwrap()
                .to_string();
            let p = op_parse_s3_url(json!({ "url": url })).unwrap();
            assert_eq!(p["bucket"], json!(bucket), "bucket round-trip for {url}");
            assert_eq!(p["region"], json!(region), "region round-trip for {url}");
            match key {
                Some(k) => assert_eq!(p["key"], json!(k), "key round-trip for {url}"),
                None => assert_eq!(p["key"], Value::Null, "no key for {url}"),
            }
        }
        // Path style: bucket is the first path segment.
        let p = op_parse_s3_url(json!({
            "url": "https://s3.eu-west-1.amazonaws.com/my-bucket/a/b.txt",
        }))
        .unwrap();
        assert_eq!(p["style"], json!("path"));
        assert_eq!(p["bucket"], json!("my-bucket"));
        assert_eq!(p["key"], json!("a/b.txt"));
        assert_eq!(p["region"], json!("eu-west-1"));
        // China suffix resolves to its partition.
        let cn =
            op_parse_s3_url(json!({"url": "https://b.s3.cn-north-1.amazonaws.com.cn/k"})).unwrap();
        assert_eq!(cn["region"], json!("cn-north-1"));
        assert_eq!(cn["partition"], json!("aws-cn"));
        assert_eq!(cn["dns_suffix"], json!("amazonaws.com.cn"));
        // Region-less legacy global endpoint: empty region, aws partition.
        let g = op_parse_s3_url(json!({"url": "https://b.s3.amazonaws.com/k"})).unwrap();
        assert_eq!(g["region"], json!(""));
        assert_eq!(g["partition"], json!("aws"));
        // Bucket root → null key.
        assert_eq!(
            op_parse_s3_url(json!({"url": "https://b.s3.eu-west-1.amazonaws.com"})).unwrap()["key"],
            Value::Null
        );
        // Errors: no scheme, unrecognized endpoint, empty bucket.
        assert!(op_parse_s3_url(json!({"url": "b.s3.us-east-1.amazonaws.com/k"})).is_err());
        assert!(op_parse_s3_url(json!({"url": "https://b.s3.us-west-2.example.com/k"})).is_err());
        assert!(op_parse_s3_url(json!({"url": "https://s3.us-east-1.amazonaws.com/"})).is_err());
        assert!(op_parse_s3_url(json!({})).is_err());
    }
}
