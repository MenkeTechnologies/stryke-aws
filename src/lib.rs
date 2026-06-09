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

// ── SQS ─────────────────────────────────────────────────────────────────────

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
