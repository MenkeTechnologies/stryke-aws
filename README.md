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

### `[AWS CLIENT FOR STRYKE // S3 + DYNAMODB + SQS + LAMBDA + STS]`

> *"The cloud, one stryke pipe away."*

AWS client for stryke — S3, DynamoDB, SQS, Lambda, STS. Opt-in package
tier, kept out of the stryke core binary so the daily-driver install stays
slim.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-gcp`](https://github.com/MenkeTechnologies/stryke-gcp) · [`stryke-docker`](https://github.com/MenkeTechnologies/stryke-docker) · [`stryke-k8s`](https://github.com/MenkeTechnologies/stryke-k8s) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Install](#0x01-install)
- [\[0x02\] Quick start](#0x02-quick-start)
- [\[0x03\] CLI: `aws`](#0x03-cli-aws)
- [\[0x04\] API reference](#0x04-api-reference)
- [\[0x05\] Helper protocol](#0x05-helper-protocol)
- [\[0x06\] DynamoDB type encoding](#0x06-dynamodb-type-encoding)
- [\[0x07\] LocalStack / MinIO](#0x07-localstack-minio)
- [\[0x08\] Tests](#0x08-tests)
- [\[0x09\] Dev workflow](#0x09-dev-workflow)
- [\[0x0A\] Layout](#0x0a-layout)
- [\[0x0B\] Roadmap](#0x0b-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is a package, not a builtin

The official aws-sdk-rust crates pull in tokio, hyper, rustls, and a fat
chain of smithy / signing / endpoint-resolution support code. Five SDKs
combined produce a ~12 MB helper binary — way too much to bake into stryke
core. This package ships them once, opt-in.

`stryke-aws` mirrors the `stryke-mysql` / `stryke-postgres` shape: a thin
stryke library spawns a Rust helper binary per call and parses JSON over
the pipe. Credentials and region come from the standard AWS chain (env
vars, `~/.aws/config|credentials`, IMDS) — same as the `aws` CLI.

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
covers a focused subset across S3, DynamoDB, SQS, Lambda, STS; the v1
helper's broader op set can be added incrementally.

## [0x02] Quick start

```stryke
use AWS::S3
use AWS::Dynamo
use AWS::SQS
use AWS::Lambda
use AWS::STS

# Identity check.
p to_json AWS::STS::caller_identity()

# S3 — list, get, put, head, rm, presign.
my @keys = AWS::S3::ls "s3://my-bucket/prefix/", delimiter => "/"
for my $e (@keys) {
    p "$e->{type}: $e->{key}"
}

AWS::S3::put "s3://my-bucket/hello.txt", data => "hello from stryke"
p AWS::S3::get  "s3://my-bucket/hello.txt"
p to_json AWS::S3::head "s3://my-bucket/hello.txt"

p AWS::S3::presign("s3://my-bucket/dl.zip", expires => 600)->{url}

# DynamoDB — items, query, scan. Plain JSON in/out (no AttributeValue).
AWS::Dynamo::put "users", { id => "u-42", name => "alice", score => 1.5 }
p to_json AWS::Dynamo::get "users", { id => "u-42" }

my @hits = AWS::Dynamo::query "users",
    expr => "id = :id",
    vals => { ":id" => "u-42" }

# Scan streaming — no full-result buffering.
AWS::Dynamo::scan_stream "users",
    callback => sub ($row) { process $row }

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

## [0x03] CLI: `aws`

```sh
aws s3 ls s3://bucket/prefix/ --delimiter=/
aws s3 get s3://bucket/key --output=local.bin
aws s3 put s3://bucket/key --input=local.bin
aws s3 head s3://bucket/key
aws s3 rm s3://bucket/key
aws s3 presign s3://bucket/key --method=PUT --expires=600
aws s3 buckets

aws ddb get   users --key='{"id":"u-42"}'
aws ddb put   users --item='{"id":"u-42","name":"alice"}'
aws ddb query users --expr='id = :id' --vals='{":id":"u-42"}'
aws ddb scan  users --filter='attribute_exists(active)' --limit=100
aws ddb batch-write users < items.ndjson
aws ddb tables
aws ddb describe users

aws sqs send    QUEUE_URL --body='...'
aws sqs receive QUEUE_URL --max=10 --wait=20
aws sqs delete  QUEUE_URL --receipt=...
aws sqs attrs   QUEUE_URL
aws sqs list

aws lambda invoke my-fn --payload='{"x":1}'
aws lambda invoke my-fn --invocation-type=event
aws lambda list

aws sts caller-identity
aws sts assume-role arn:aws:iam::123:role/dev --session=demo --duration=3600

aws ping                                  # alias: sts caller-identity
aws build                                 # cargo build --release
aws version
```

Global flags (also pull from env):

```
-r, --region REGION         $AWS_REGION
    --profile NAME          $AWS_PROFILE
    --endpoint URL          $AWS_ENDPOINT_URL    (LocalStack / MinIO / custom)
```

Credentials use the standard AWS chain: env vars → profile in
`~/.aws/credentials` → IMDS (EC2) → SSO. No `--access-key` / `--secret-key`
flags — set the env vars or use a profile.

## [0x04] API reference

### `use AWS`

Plumbing only — `AWS::helper_path()`, `AWS::ensure_built()`,
`AWS::version()`, `AWS::ping(%opts)`.

### `use AWS::S3`

```stryke
AWS::S3::ls       $uri, %opts → @entries
AWS::S3::get      $uri, %opts → $body (or $path when output=>"PATH")
AWS::S3::put      $uri, %opts → \%resp        # data=>$bytes | input=>"PATH"
AWS::S3::head     $uri, %opts → \%resp
AWS::S3::rm       $uri, %opts → \%resp
AWS::S3::presign  $uri, %opts → \%resp        # %opts: method, expires
AWS::S3::buckets  %opts        → @buckets
```

`ls` entries: `{type=>"object", key, size, etag, last_modified,
storage_class}` or `{type=>"prefix", key}` when `delimiter` is set.

### `use AWS::Dynamo`

```stryke
AWS::Dynamo::get          $table, $key, %opts → \%item | undef
AWS::Dynamo::put          $table, $item, %opts → { ok: 1 }
AWS::Dynamo::delete       $table, $key, %opts → { ok: 1 }
AWS::Dynamo::query        $table, %opts → @items       # opts: expr (req), vals, names, index, filter, limit, consistent
AWS::Dynamo::scan         $table, %opts → @items
AWS::Dynamo::scan_stream  $table, %opts → $count       # callback per item
AWS::Dynamo::batch_write  $table, \@rows, %opts → 1    # rows: items or { _delete: {…key…} }
AWS::Dynamo::tables       %opts → @names
AWS::Dynamo::describe     $table, %opts → \%info
```

Plain-JSON in/out. Binary attributes round-trip as `"base64:…"` strings on
the JSON side (the helper unwraps and rewraps the `B` envelope).

### `use AWS::SQS`

```stryke
AWS::SQS::send     $queue, $body, %opts → \%resp       # opts: delay_seconds, dedup_id, group_id
AWS::SQS::receive  $queue, %opts → @messages           # opts: max, wait, visibility
AWS::SQS::delete   $queue, $receipt, %opts → { ok: 1 }
AWS::SQS::purge    $queue, %opts → { ok: 1 }
AWS::SQS::attrs    $queue, %opts → \%attrs
AWS::SQS::list     %opts → @urls
AWS::SQS::pump     $queue, %opts → $count              # callback + auto-delete on success
```

### `use AWS::Lambda`

```stryke
AWS::Lambda::invoke $name, $payload, %opts → \%resp    # status_code, function_error, payload, log_tail, executed_version
AWS::Lambda::call   $name, $payload, %opts → \%payload # convenience: returns just .payload (or undef on function_error)
AWS::Lambda::list   %opts → @functions
```

### `use AWS::STS`

```stryke
AWS::STS::caller_identity %opts → { account, arn, user_id }
AWS::STS::assume_role     $role_arn, session => "...", %opts → { access_key_id, secret_access_key, session_token, expiration, assumed_role_arn }
```

## [0x05] Helper protocol

```sh
stryke-aws-helper s3 ls s3://bucket/prefix --delimiter=/
stryke-aws-helper s3 put s3://bucket/k --input=- < file
stryke-aws-helper ddb put users --item='{"id":"u-42","name":"alice"}'
stryke-aws-helper ddb query users --expr='id = :id' --vals='{":id":"u-42"}'
stryke-aws-helper sqs receive https://sqs… --max=10 --wait=20
stryke-aws-helper lambda invoke my-fn --payload='{"x":1}'
stryke-aws-helper sts caller-identity
```

Output:

* List / stream commands → NDJSON, one JSON object per line.
* Single-object commands → one JSON object + newline.
* All errors → exit non-zero, message on stderr.

## [0x06] DynamoDB type encoding

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
or use the raw helper if you specifically need a typed set.

## [0x07] LocalStack / MinIO

```stryke
my %ls = (endpoint => "http://localhost:4566", region => "us-east-1")
AWS::S3::buckets(%ls) |> ep
AWS::SQS::list(%ls)   |> ep
```

`$ENV{AWS_ENDPOINT_URL}` works as a global default.

## [0x08] Tests

```sh
cargo test                                          # compiles, no live calls
s test t/                                           # creds-aware end-to-end

# Opt into the per-service round-trips with a writable resource:
export STRYKE_AWS_TEST_BUCKET=my-test-bucket
export STRYKE_AWS_TEST_TABLE=stryke-aws-demo        # PK `id: S`
export STRYKE_AWS_TEST_QUEUE=https://sqs.us-east-1.amazonaws.com/.../my-q
s test t/
```

The suite skips cleanly when the helper isn't built, when credentials are
missing, or when the per-service env vars are unset.

## [0x09] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x0A] Layout

```
stryke-aws/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/
    main.rs                        # CLI dispatch
    common.rs                      # shared helpers
    s3.rs / ddb.rs / sqs.rs
    lambda.rs / sts.rs
  lib/
    AWS.stk                        # `use AWS` — plumbing + ping
    S3.stk                         # `use AWS::S3`
    Dynamo.stk                     # `use AWS::Dynamo`
    SQS.stk                        # `use AWS::SQS`
    Lambda.stk                     # `use AWS::Lambda`
    STS.stk                        # `use AWS::STS`
  t/
    test_aws.stk                   # end-to-end (gated on creds + opt-in env vars)
  examples/
    s3_browse.stk
    ddb_demo.stk
    sqs_pump.stk
    lambda_call.stk
  .github/workflows/
    ci.yml                         # cargo + compile-only (no live AWS)
    release.yml                    # cross-compile + GH release on tag push
```

## [0x0B] Roadmap

| v1 (this release) | v2+ |
|---|---|
| S3, DynamoDB, SQS, Lambda, STS | CloudWatch Logs, EC2, IAM, KMS, Secrets Manager |
| Single-shot per call | Persistent serve daemon over a Unix socket |
| Plain JSON DDB | Typed-set passthrough, optimistic-locking helpers |
| Buffered S3 put | Streaming multipart upload |

## [0xFF] License

MIT.
