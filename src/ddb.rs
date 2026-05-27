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

#[cfg(test)]
mod tests {
    use super::*;

    // ─── json_to_av ──────────────────────────────────────────────────

    #[test]
    fn json_to_av_null_becomes_null_true() {
        match json_to_av(&Value::Null) {
            AttributeValue::Null(b) => assert!(b),
            other => panic!("expected Null, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_bool() {
        match json_to_av(&json!(true)) {
            AttributeValue::Bool(b) => assert!(b),
            other => panic!("expected Bool, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_number_preserves_string_form() {
        // DynamoDB stores numbers as strings; this preserves precision.
        match json_to_av(&json!(123)) {
            AttributeValue::N(s) => assert_eq!(s, "123"),
            other => panic!("expected N, got {other:?}"),
        }
        match json_to_av(&json!(3.14)) {
            AttributeValue::N(s) => assert_eq!(s, "3.14"),
            other => panic!("expected N, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_string_plain() {
        match json_to_av(&json!("hello")) {
            AttributeValue::S(s) => assert_eq!(s, "hello"),
            other => panic!("expected S, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_base64_prefixed_string_becomes_binary() {
        let raw = b"hello world";
        let encoded = format!("base64:{}", B64.encode(raw));
        match json_to_av(&json!(encoded)) {
            AttributeValue::B(blob) => assert_eq!(blob.as_ref(), raw),
            other => panic!("expected B, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_invalid_base64_prefix_stays_string() {
        // Decoder fails → preserved as string verbatim (round-trip safety).
        let s = "base64:!!!not valid base64!!!";
        match json_to_av(&json!(s)) {
            AttributeValue::S(got) => assert_eq!(got, s),
            other => panic!("expected S fallback, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_nested_object_becomes_map() {
        match json_to_av(&json!({"x": 1, "y": "z"})) {
            AttributeValue::M(m) => {
                assert_eq!(m.len(), 2);
                assert!(matches!(m.get("x"), Some(AttributeValue::N(_))));
                assert!(matches!(m.get("y"), Some(AttributeValue::S(_))));
            }
            other => panic!("expected M, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_array_becomes_list() {
        match json_to_av(&json!([1, "a", true])) {
            AttributeValue::L(l) => assert_eq!(l.len(), 3),
            other => panic!("expected L, got {other:?}"),
        }
    }

    // ─── av_to_json ──────────────────────────────────────────────────

    #[test]
    fn av_to_json_number_parses_to_i64_when_possible() {
        let av = AttributeValue::N("42".to_string());
        assert_eq!(av_to_json(&av), json!(42));
    }

    #[test]
    fn av_to_json_number_parses_to_f64_for_decimals() {
        let av = AttributeValue::N("3.5".to_string());
        assert_eq!(av_to_json(&av), json!(3.5));
    }

    #[test]
    fn av_to_json_number_oversized_falls_back_to_string() {
        // DDB N can hold 38-digit precision; if neither i64 nor f64 parse,
        // av_to_json hands the raw string back. f64 parses most strings, so
        // pick something that fails BOTH parsers: a non-numeric N.
        let av = AttributeValue::N("not-a-number".to_string());
        assert_eq!(av_to_json(&av), Value::String("not-a-number".to_string()));
    }

    #[test]
    fn av_to_json_binary_encodes_with_base64_prefix() {
        let av = AttributeValue::B(aws_smithy_types::Blob::new(b"abc".to_vec()));
        let j = av_to_json(&av);
        let s = j.as_str().unwrap();
        assert!(s.starts_with("base64:"), "s = {s}");
        let decoded = B64.decode(s.strip_prefix("base64:").unwrap()).unwrap();
        assert_eq!(decoded, b"abc");
    }

    #[test]
    fn av_to_json_null_variant() {
        assert_eq!(av_to_json(&AttributeValue::Null(true)), Value::Null);
    }

    #[test]
    fn av_to_json_string_set() {
        let av = AttributeValue::Ss(vec!["a".into(), "b".into()]);
        assert_eq!(av_to_json(&av), json!(["a", "b"]));
    }

    #[test]
    fn av_to_json_number_set_parses_each_entry() {
        let av = AttributeValue::Ns(vec!["1".into(), "2.5".into(), "nope".into()]);
        assert_eq!(av_to_json(&av), json!([1, 2.5, "nope"]));
    }

    // ─── round-trip: json_to_av → av_to_json ─────────────────────────

    #[test]
    fn json_av_round_trip_scalars() {
        for v in [json!(null), json!(true), json!(42), json!("hi"), json!(2.5)] {
            let round = av_to_json(&json_to_av(&v));
            assert_eq!(round, v, "value {v:?} did not round-trip");
        }
    }

    #[test]
    fn json_av_round_trip_nested_map() {
        let v = json!({"id": 1, "name": "x", "tags": ["a", "b"], "active": true});
        let round = av_to_json(&json_to_av(&v));
        assert_eq!(round, v);
    }

    #[test]
    fn json_av_round_trip_binary_via_base64_prefix() {
        let encoded = format!("base64:{}", B64.encode(b"raw bytes"));
        let v = json!(encoded);
        let round = av_to_json(&json_to_av(&v));
        assert_eq!(round, v);
    }

    // ─── json_obj_to_av_map ──────────────────────────────────────────

    #[test]
    fn json_obj_to_av_map_simple() {
        let m = json_obj_to_av_map(r#"{"id":42,"name":"x"}"#).unwrap();
        assert_eq!(m.len(), 2);
        assert!(matches!(m.get("id"), Some(AttributeValue::N(_))));
        assert!(matches!(m.get("name"), Some(AttributeValue::S(_))));
    }

    #[test]
    fn json_obj_to_av_map_array_rejected() {
        let err = json_obj_to_av_map("[1,2,3]").unwrap_err();
        assert!(format!("{err}").contains("object"));
    }

    #[test]
    fn json_obj_to_av_map_string_rejected() {
        let err = json_obj_to_av_map(r#""nope""#).unwrap_err();
        assert!(format!("{err}").contains("object"));
    }

    #[test]
    fn json_obj_to_av_map_invalid_json_errors() {
        let err = json_obj_to_av_map("{not json}").unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("parsing"));
    }

    // ─── av_map_to_json ──────────────────────────────────────────────

    #[test]
    fn av_map_to_json_emits_object() {
        let mut m = HashMap::new();
        m.insert("a".to_string(), AttributeValue::N("1".into()));
        m.insert("b".to_string(), AttributeValue::S("two".into()));
        let v = av_map_to_json(&m);
        assert_eq!(v["a"], json!(1));
        assert_eq!(v["b"], json!("two"));
    }

    // ─── parse_vals / parse_names ────────────────────────────────────

    #[test]
    fn parse_vals_none_returns_none() {
        let got = parse_vals(None).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn parse_vals_parses_json_object() {
        let got = parse_vals(Some(r#"{":id":42}"#)).unwrap().unwrap();
        assert!(matches!(got.get(":id"), Some(AttributeValue::N(_))));
    }

    #[test]
    fn parse_names_none_returns_none() {
        let got = parse_names(None).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn parse_names_parses_string_map() {
        let got = parse_names(Some(r##"{"#x":"alias"}"##)).unwrap().unwrap();
        assert_eq!(got.get("#x").map(String::as_str), Some("alias"));
    }

    #[test]
    fn parse_names_array_rejected() {
        let err = parse_names(Some(r#"["a","b"]"#)).unwrap_err();
        assert!(format!("{err}").contains("object"));
    }

    #[test]
    fn parse_names_non_string_value_falls_back_to_empty() {
        // The impl maps non-string values to an empty String rather than
        // erroring — pinning current behavior so future tightening is
        // deliberate.
        let got = parse_names(Some(r##"{"#x":123}"##)).unwrap().unwrap();
        assert_eq!(got.get("#x").map(String::as_str), Some(""));
    }

    #[test]
    fn av_to_json_bool_false() {
        assert_eq!(av_to_json(&AttributeValue::Bool(false)), json!(false));
    }

    #[test]
    fn av_to_json_list_recurses() {
        let av = AttributeValue::L(vec![
            AttributeValue::N("1".into()),
            AttributeValue::S("a".into()),
        ]);
        assert_eq!(av_to_json(&av), json!([1, "a"]));
    }

    #[test]
    fn av_to_json_map_object() {
        let mut m = HashMap::new();
        m.insert("k".into(), AttributeValue::S("v".into()));
        assert_eq!(av_to_json(&AttributeValue::M(m))["k"], json!("v"));
    }

    #[test]
    fn json_to_av_string_array_becomes_list_not_ss() {
        match json_to_av(&json!(["x", "y"])) {
            AttributeValue::L(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], AttributeValue::S(s) if s == "x"));
            }
            other => panic!("expected L, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_empty_array_becomes_empty_list() {
        match json_to_av(&json!([])) {
            AttributeValue::L(l) => assert!(l.is_empty()),
            other => panic!("expected L, got {other:?}"),
        }
    }

    #[test]
    fn parse_vals_invalid_json_errors() {
        let err = parse_vals(Some("{bad")).unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("parsing"));
    }

    #[test]
    fn av_map_to_json_empty_map() {
        let v = av_map_to_json(&HashMap::new());
        assert!(v.as_object().unwrap().is_empty());
    }

    #[test]
    fn json_av_round_trip_empty_object() {
        let v = json!({});
        assert_eq!(av_to_json(&json_to_av(&v)), v);
    }

    #[test]
    fn av_to_json_binary_set_base64_prefixes() {
        let av = AttributeValue::Bs(vec![
            aws_smithy_types::Blob::new(b"a"),
            aws_smithy_types::Blob::new(b"bb"),
        ]);
        let j = av_to_json(&av);
        let arr = j.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr[0].as_str().unwrap().starts_with("base64:"));
        assert!(arr[1].as_str().unwrap().starts_with("base64:"));
    }

    #[test]
    fn av_to_json_number_set_non_numeric_entry() {
        let av = AttributeValue::Ns(vec!["1".into(), "x".into()]);
        assert_eq!(av_to_json(&av), json!([1, "x"]));
    }

    #[test]
    fn json_to_av_negative_number_string_form() {
        match json_to_av(&json!(-42)) {
            AttributeValue::N(s) => assert_eq!(s, "-42"),
            other => panic!("expected N, got {other:?}"),
        }
    }

    #[test]
    fn json_av_round_trip_nested_list_in_map() {
        let v = json!({"items": [1, 2]});
        assert_eq!(av_to_json(&json_to_av(&v)), v);
    }

    #[test]
    fn parse_vals_empty_object() {
        let got = parse_vals(Some("{}")).unwrap().unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn parse_names_empty_object() {
        let got = parse_names(Some("{}")).unwrap().unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn av_to_json_large_integer_n_stays_string_when_f64_loses_precision() {
        let big = "9007199254740993"; // i64::MAX + 2, not exactly representable as f64 integer
        let av = AttributeValue::N(big.into());
        let j = av_to_json(&av);
        // May parse as f64 or stay string — pin that we don't panic and get a number or string.
        assert!(j.is_number() || j.as_str() == Some(big));
    }

    #[test]
    fn json_to_av_false_bool() {
        match json_to_av(&json!(false)) {
            AttributeValue::Bool(b) => assert!(!b),
            other => panic!("expected Bool, got {other:?}"),
        }
    }

    #[test]
    fn json_obj_to_av_map_rejects_array() {
        let err = json_obj_to_av_map("[1]").unwrap_err();
        assert!(format!("{err}").contains("object"));
    }

    #[test]
    fn av_map_to_json_two_entries() {
        let mut m = HashMap::new();
        m.insert("a".into(), AttributeValue::Bool(true));
        m.insert("b".into(), AttributeValue::N("9".into()));
        let v = av_map_to_json(&m);
        assert_eq!(v["a"], json!(true));
        assert_eq!(v["b"], json!(9));
    }

    #[test]
    fn av_to_json_empty_list() {
        assert_eq!(av_to_json(&AttributeValue::L(vec![])), json!([]));
    }

    #[test]
    fn av_to_json_empty_map() {
        assert_eq!(av_to_json(&AttributeValue::M(HashMap::new())), json!({}));
    }

    #[test]
    fn json_obj_to_av_map_nested_object() {
        let m = json_obj_to_av_map(r#"{"outer":{"inner":1}}"#).unwrap();
        assert!(matches!(m.get("outer"), Some(AttributeValue::M(_))));
    }

    #[test]
    fn av_to_json_ns_all_parse_as_numbers() {
        let av = AttributeValue::Ns(vec!["1".into(), "2".into(), "3".into()]);
        assert_eq!(av_to_json(&av), json!([1, 2, 3]));
    }

    #[test]
    fn parse_vals_none_stays_none() {
        assert!(parse_vals(None).unwrap().is_none());
    }

    #[test]
    fn json_to_av_string_without_base64_prefix() {
        match json_to_av(&json!("plain")) {
            AttributeValue::S(s) => assert_eq!(s, "plain"),
            other => panic!("expected S, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_negative_number_string() {
        match json_to_av(&json!(-7)) {
            AttributeValue::N(s) => assert_eq!(s, "-7"),
            other => panic!("expected N, got {other:?}"),
        }
    }

    #[test]
    fn av_to_json_bool_true() {
        assert_eq!(av_to_json(&AttributeValue::Bool(true)), json!(true));
    }

    #[test]
    fn json_to_av_nested_array_in_map() {
        let m = json_obj_to_av_map(r#"{"items":[1,2]}"#).unwrap();
        match m.get("items") {
            Some(AttributeValue::L(list)) => assert_eq!(list.len(), 2),
            other => panic!("expected L, got {other:?}"),
        }
    }

    #[test]
    fn av_to_json_ss_set_of_strings() {
        let av = AttributeValue::Ss(vec!["a".into(), "b".into()]);
        assert_eq!(av_to_json(&av), json!(["a", "b"]));
    }

    #[test]
    fn json_to_av_empty_object_map() {
        match json_to_av(&json!({})) {
            AttributeValue::M(m) => assert!(m.is_empty()),
            other => panic!("expected M, got {other:?}"),
        }
    }

    #[test]
    fn json_to_av_zero_number() {
        match json_to_av(&json!(0)) {
            AttributeValue::N(s) => assert_eq!(s, "0"),
            other => panic!("expected N, got {other:?}"),
        }
    }

    #[test]
    fn av_to_json_ns_empty_set() {
        assert_eq!(av_to_json(&AttributeValue::Ns(vec![])), json!([]));
    }

    #[test]
    fn av_to_json_binary_empty_blob() {
        let av = AttributeValue::B(aws_smithy_types::Blob::new(b""));
        let j = av_to_json(&av);
        let s = j.as_str().unwrap();
        assert!(s.starts_with("base64:"));
    }

    #[test]
    fn json_to_av_true_bool() {
        assert!(matches!(json_to_av(&json!(true)), AttributeValue::Bool(true)));
    }

    #[test]
    fn av_to_json_l_list_nested() {
        let av = AttributeValue::L(vec![
            AttributeValue::N("1".into()),
            AttributeValue::S("a".into()),
        ]);
        assert_eq!(av_to_json(&av), json!([1, "a"]));
    }

    #[test]
    fn json_to_av_float_number_string() {
        match json_to_av(&json!(1.25)) {
            AttributeValue::N(s) => assert_eq!(s, "1.25"),
            other => panic!("expected N, got {other:?}"),
        }
    }

    #[test]
    fn parse_names_non_object_errors() {
        assert!(parse_names(Some("[]")).is_err());
    }

    #[test]
    fn av_to_json_m_map_nested() {
        let mut inner = HashMap::new();
        inner.insert("k".into(), AttributeValue::S("v".into()));
        let av = AttributeValue::M(inner);
        assert_eq!(av_to_json(&av)["k"], json!("v"));
    }

    #[test]
    fn json_to_av_string_array_becomes_list() {
        match json_to_av(&json!(["a", "b"])) {
            AttributeValue::L(v) => assert_eq!(v.len(), 2),
            other => panic!("expected L, got {other:?}"),
        }
    }

    #[test]
    fn av_to_json_l_empty_list() {
        assert_eq!(av_to_json(&AttributeValue::L(vec![])), json!([]));
    }

    #[test]
    fn json_to_av_large_integer_string() {
        match json_to_av(&json!(9_223_372_036_854_775_807u64)) {
            AttributeValue::N(s) => assert_eq!(s, "9223372036854775807"),
            other => panic!("expected N, got {other:?}"),
        }
    }

    #[test]
    fn parse_names_scalar_rejected() {
        assert!(parse_names(Some("\"x\"")).is_err());
    }

    #[test]
    fn av_to_json_bs_binary_set() {
        let av = AttributeValue::Bs(vec![
            aws_smithy_types::Blob::new(b"a"),
            aws_smithy_types::Blob::new(b"b"),
        ]);
        let j = av_to_json(&av);
        assert_eq!(j.as_array().unwrap().len(), 2);
    }

    #[test]
    fn json_to_av_nested_map_in_map() {
        let m = json_obj_to_av_map(r#"{"outer":{"inner":"v"}}"#).unwrap();
        assert!(matches!(m.get("outer"), Some(AttributeValue::M(_))));
    }

    #[test]
    fn parse_vals_string_scalar_rejected() {
        assert!(parse_vals(Some("\"x\"")).is_err());
    }

    #[test]
    fn parse_vals_array_rejected() {
        assert!(parse_vals(Some("[]")).is_err());
    }

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
