```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ a w s ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-aws/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-aws/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[AWS CLIENT FOR STRYKE // S3 + DYNAMODB + SQS + LAMBDA + STS + SNS + SSM + SECRETS + SES + CLOUDWATCH]`

> *"The cloud, one stryke pipe away."*

AWS client for stryke — S3, DynamoDB, SQS, Lambda, STS, SNS, SSM
Parameter Store, and Secrets Manager. Opt-in package
tier, kept out of the stryke core binary so the daily-driver install stays
slim.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-gcp`](https://github.com/MenkeTechnologies/stryke-gcp) · [`stryke-docker`](https://github.com/MenkeTechnologies/stryke-docker) · [`stryke-k8s`](https://github.com/MenkeTechnologies/stryke-k8s) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] API reference](#0x03-api-reference)
- [\[0x04\] FFI layer](#0x04-ffi-layer)
- [\[0x05\] DynamoDB type encoding](#0x05-dynamodb-type-encoding)
- [\[0x06\] LocalStack / MinIO](#0x06-localstack-minio)
- [\[0x07\] Tests](#0x07-tests)
- [\[0x08\] Dev workflow](#0x08-dev-workflow)
- [\[0x09\] Layout](#0x09-layout)
- [\[0x0A\] Roadmap](#0x0a-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is a package, not a builtin

The official aws-sdk-rust crates pull in tokio, hyper, rustls, and a fat
chain of smithy / signing / endpoint-resolution support code. Five SDKs
combined are way too much to bake into stryke core. This package ships
them once, opt-in.

`stryke-aws` ships a thin stryke library plus a Rust cdylib
(`libstryke_aws.{dylib,so}`) that stryke's FFI bridge dlopens in-process
on first `use AWS` — no helper-binary fork per call (the v1 helper-binary
model was replaced in v0.2.0). Credentials and region come from the
standard AWS chain (env vars, `~/.aws/config|credentials`, IMDS) — same
as the `aws` CLI.

## [0x01] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-aws
```

From a local checkout:

```sh
cd ~/projects/stryke-aws
cargo build --release            # produces target/release/libstryke_aws.{dylib,so}
s pkg install -g .               # cdylib lands in ~/.stryke/store/aws@<version>/
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use AWS`. A shared tokio
runtime + `aws_config::SdkConfig` cache per region is held in `OnceCell`
— no fork-per-call, no full IMDS/SSO/env creds chain on each call. v0.2.1
covers S3, DynamoDB, SQS, Lambda, STS, SNS, SSM Parameter Store, and
Secrets Manager; further ops can be added incrementally.

## [0x02] Quick start

```stryke
use AWS::S3
use AWS::Dynamo
use AWS::SQS
use AWS::Lambda
use AWS::STS

# Identity check.
p to_json AWS::STS::caller_identity()

# S3 — list, get, put, head, rm.
my @keys = AWS::S3::ls "s3://my-bucket/prefix/", delimiter => "/"
for my $e (@keys) {
    p "$e->{type}: $e->{key}"
}

AWS::S3::put "s3://my-bucket/hello.txt", data => "hello from stryke"
p AWS::S3::get  "s3://my-bucket/hello.txt"
p to_json AWS::S3::head "s3://my-bucket/hello.txt"

# DynamoDB — items. Plain JSON in/out (no AttributeValue).
AWS::Dynamo::put "users", { id => "u-42", name => "alice", score => 1.5 }
p to_json AWS::Dynamo::get "users", { id => "u-42" }
p for AWS::Dynamo::tables()

# SQS — send / long-poll / pump.
AWS::SQS::send $queue_url, "payload"
my @msgs = AWS::SQS::receive $queue_url, max => 10, wait => 20

# pump = receive → callback → delete-on-success
AWS::SQS::pump $queue_url, iterations => 5, callback => sub ($m) {
    handle_message $m->{body}
}

# Lambda — invoke (sync or fire-and-forget).
my $reply = AWS::Lambda::call "my-fn", { hello => "world" }
p to_json $reply
```

Per-call connection overrides (named opts on every public fn):

```stryke
AWS::S3::ls "s3://...",
    region   => "us-west-2",
    profile  => "prod",
    endpoint => "http://localstack:4566"       # for LocalStack / MinIO
```

## [0x03] API reference

### `use AWS`

Plumbing only — `AWS::version()` (cdylib package version) and
`AWS::ping(%opts)` (STS-backed connectivity probe), plus the flat
`AWS::<service>_<op>` fns that the namespaced wrappers below delegate to.

### `use AWS::S3`

```stryke
AWS::S3::ls       $uri, %opts → @entries
AWS::S3::get      $uri, %opts → $body (or $path when output=>"PATH")
AWS::S3::put      $uri, %opts → \%resp        # data=>$bytes | input=>"PATH"
AWS::S3::head     $uri, %opts → \%resp
AWS::S3::rm       $uri, %opts → \%resp
AWS::S3::presign  $uri, %opts → dies          # deferred in the v0.2.x cdylib
AWS::S3::buckets  %opts        → @buckets
```

`ls` entries: `{type=>"object", key, size, etag, last_modified,
storage_class}` or `{type=>"prefix", key}` when `delimiter` is set.

### `use AWS::Dynamo`

```stryke
AWS::Dynamo::get          $table, $key, %opts → \%item | undef
AWS::Dynamo::put          $table, $item, %opts → { ok: 1 }
AWS::Dynamo::delete       $table, $key, %opts → 1 | 0
AWS::Dynamo::query        $table, %opts → @items   # opts: key_condition, values, names, filter, index, limit
AWS::Dynamo::scan         $table, %opts → @items   # opts: filter, values, limit
AWS::Dynamo::describe     $table, %opts → \%info   # status, item_count, key_schema, arn
AWS::Dynamo::tables       %opts → @names
```

`query`/`scan` take a DynamoDB expression plus a `values` hashref of
`:placeholder => value` bindings (and optional `names` for `#alias`). Nested
maps/lists round-trip recursively; binary attributes come back as
`"base64:…"`. `batch_write` and `scan_stream` remain deferred.

### `use AWS::SQS`

```stryke
AWS::SQS::send     $queue, $body, %opts → \%resp       # opts: delay_seconds, dedup_id, group_id
AWS::SQS::receive  $queue, %opts → @messages           # opts: max, wait, visibility
AWS::SQS::delete   $queue, $receipt, %opts → { ok: 1 }
AWS::SQS::list     %opts → @urls                       # prefix filter deferred
AWS::SQS::purge    $queue, %opts → 1 | 0               # delete all messages
AWS::SQS::attrs    $queue, %opts → \%attributes        # GetQueueAttributes (All)
AWS::SQS::pump     $queue, %opts → $count              # callback + auto-delete on success
```

### `use AWS::Lambda`

```stryke
AWS::Lambda::invoke $name, $payload, %opts → \%resp    # { function, status_code, result }
AWS::Lambda::call   $name, $payload, %opts → $result   # convenience: just .result (undef unless status_code == 200)
AWS::Lambda::list   %opts → @functions
```

`invocation_type => "event"` (fire-and-forget) is deferred in the v0.2.x
cdylib.

### `use AWS::STS`

```stryke
AWS::STS::caller_identity %opts → { account, arn, user_id }
AWS::STS::assume_role     $role_arn, %opts → \%creds   # opts: session_name; temp credentials
```

### `use AWS::SNS`

```stryke
AWS::SNS::topics    %opts → @topic_arns
AWS::SNS::create    $name, %opts → $topic_arn
AWS::SNS::publish   $message, %opts → $message_id   # opts: topic_arn | target_arn | phone_number, subject
AWS::SNS::subscribe $topic_arn, $protocol, $endpoint, %opts → $subscription_arn
```

### `use AWS::SSM` (Parameter Store)

```stryke
AWS::SSM::get      $name, %opts → $value             # opts: with_decryption
AWS::SSM::put      $name, $value, %opts → $version   # opts: type (String|StringList|SecureString), overwrite
AWS::SSM::by_path  $path, %opts → @{ {name, value} } # opts: recursive, with_decryption
AWS::SSM::delete   $name, %opts → 1 | 0
```

### `use AWS::Secrets` (Secrets Manager)

```stryke
AWS::Secrets::get    $secret_id, %opts → $secret_string
AWS::Secrets::create $name, $secret_string, %opts → { arn, name }
AWS::Secrets::put    $secret_id, $secret_string, %opts → $version_id
AWS::Secrets::list   %opts → @{ {name, arn} }
```

### `use AWS::SES` (email, v2)

```stryke
AWS::SES::send $from, $to_or_aref, %opts → $message_id   # opts: subject, body, html
```

### `use AWS::CloudWatch`

```stryke
AWS::CloudWatch::put  $namespace, $metric_name, $value, %opts → 1   # opts: unit
AWS::CloudWatch::list %opts → @{ {namespace, metric_name, dimensions} }   # opts: namespace, metric_name
```

### Flat extras on `use AWS`

```stryke
AWS::s3_copy_object    $src_bucket, $src_key, $bucket, $key, %opts → \%resp
AWS::s3_delete_objects $bucket, \@keys, %opts → @deleted        # batch delete (≤1000)
AWS::ddb_update_item   $table, \%key, \%updates, %opts → 1 | 0  # SET each attr
AWS::ddb_batch_get_item   $table, \@keys, %opts → @items       # ≤100 keys
AWS::ddb_batch_write_item $table, %opts → $count               # opts: puts (≤25), deletes
```

### Pure helpers (no AWS)

```stryke
AWS::parse_arn($arn)         → { partition, service, region, account_id, resource, resource_type, resource_id }
AWS::build_arn(%opts)        → $arn        # parts → ARN; inverse of parse_arn
AWS::parse_s3_uri($uri)      → { bucket, key }
AWS::build_s3_uri($b, $k?)   → $uri        # bucket+key → s3:// URI; inverse of parse_s3_uri
AWS::s3_uri_to_arn($uri, $partition?) → { arn, bucket, key }   # s3://b/k → arn:aws:s3:::b/k
AWS::arn_to_s3_uri($arn)     → { uri, bucket, key }   # arn:aws:s3:::b/k → s3://b/k; inverse of s3_uri_to_arn
AWS::valid_bucket_name($n)   → { name, valid, reason }   # AWS bucket naming rules
AWS::partition_for_region($r) → { region, partition }   # cn-*→aws-cn, us-gov-*→aws-us-gov, us-iso(b)-*→aws-iso(-b), else aws
AWS::dns_suffix_for_partition($p) → { partition, dns_suffix }   # aws→amazonaws.com, aws-cn→amazonaws.com.cn, aws-iso→c2s.ic.gov, … (botocore)
```

These open no client — pure string parsing/validation, so they run with no
credentials.

## [0x04] FFI layer

Each `AWS::*` wrapper builds a JSON args dict and calls a sibling
`aws__*` symbol resolved out of `libstryke_aws.{dylib,so}`. The cdylib
is dlopened in-process on first `use AWS` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook) and exposes the entry
points listed in the `[ffi]` exports table in `stryke.toml`, spanning
STS, S3, DynamoDB, SQS, and Lambda.

**Persistent state:** a shared tokio runtime + an `aws_config::SdkConfig`
cache per region held in `OnceCell` — no fork-per-call, no full
IMDS/SSO/env creds chain on each call.

Errors come back as `{"error": "<msg>"}` — the wrapper `die`s with it.

## [0x05] DynamoDB type encoding

Plain JSON → `AttributeValue`:

| JSON | AV |
|---|---|
| `null` | `NULL(true)` |
| `bool` | `BOOL` |
| `number` | `N` (stringified to preserve precision) |
| `string` | `S` (or `B` when the string is `"base64:..."`) |
| `array` | `L` (heterogeneous) |
| `object` | `M` |

Set types (`SS` / `NS` / `BS`) round-trip *out* as JSON arrays — on the
write path, pass them as `L` (an array) and DynamoDB will store as a list,
or extend the cdylib export list if you specifically need a typed set.

## [0x06] LocalStack / MinIO

```stryke
my %ls = (endpoint => "http://localhost:4566", region => "us-east-1")
AWS::S3::buckets(%ls) |> ep
AWS::SQS::list(%ls)   |> ep
```

`$ENV{AWS_ENDPOINT_URL}` works as a global default.

## [0x07] Tests

```sh
cargo test                                          # compiles, no live calls
s test t/                                           # creds-aware end-to-end

# Opt into the per-service round-trips with a writable resource:
export STRYKE_AWS_TEST_BUCKET=my-test-bucket
export STRYKE_AWS_TEST_TABLE=stryke-aws-demo        # PK `id: S`
export STRYKE_AWS_TEST_QUEUE=https://sqs.us-east-1.amazonaws.com/.../my-q
s test t/
```

The suite skips cleanly when the cdylib isn't installed, when credentials are
missing, or when the per-service env vars are unset.

## [0x08] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x09] Layout

```
stryke-aws/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # cdylib crate manifest
  Makefile
  src/
    lib.rs                         # cdylib — aws__* extern "C" exports
  lib/
    AWS.stk                        # `use AWS` — plumbing + ping
    S3.stk                         # `use AWS::S3`
    Dynamo.stk                     # `use AWS::Dynamo`
    SQS.stk                        # `use AWS::SQS`
    Lambda.stk                     # `use AWS::Lambda`
    STS.stk                        # `use AWS::STS`
  t/
    test_aws.stk                   # end-to-end (gated on creds + opt-in env vars)
    test_stryke_aws_surface.stk    # wrapper-completeness pin
  examples/
    ddb_demo.stk
    discover.stk
    lambda_call.stk
    s3_browse.stk
    sqs_pump.stk
    whoami.stk
  .github/workflows/
    ci.yml                         # cargo + compile-only (no live AWS)
    release.yml                    # cross-compile + GH release on tag push
```

## [0x0A] Roadmap

| Shipped (v0.2.x) | Later |
|---|---|
| S3 (incl. head), DynamoDB (incl. query/scan/delete/describe), SQS (incl. purge/attrs), Lambda, STS (incl. assume_role) | CloudWatch Logs, EC2, IAM, KMS, Secrets Manager |
| In-process cdylib + persistent SdkConfig cache | Deferred ops: DDB batch_write/scan_stream, S3 presign |
| Plain JSON DDB | Typed-set passthrough, optimistic-locking helpers |
| Buffered S3 put | Streaming multipart upload |

## [0xFF] License

MIT.
