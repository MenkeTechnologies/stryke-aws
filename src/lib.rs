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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::collections::HashMap;

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
}
