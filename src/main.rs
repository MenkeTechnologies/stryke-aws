//! `stryke-aws-helper` — bridge binary for the stryke `aws` package.
//!
//! Subcommands map to the AWS SDK calls one-to-one. Output is JSON (single
//! object) or NDJSON (lists / streams). Credentials and region come from
//! the standard chain: env vars, `~/.aws/config|credentials`, IMDS, etc.,
//! same as the `aws` CLI.

use anyhow::Result;
use clap::{Parser, Subcommand};

mod common;
mod ddb;
mod lambda;
mod s3;
mod sqs;
mod sts;

use crate::common::load_aws_config;

#[derive(Parser, Debug)]
#[command(
    name = "stryke-aws-helper",
    version,
    about = "AWS bridge (S3, DynamoDB, SQS, Lambda, STS) for the stryke `aws` package"
)]
struct Cli {
    /// AWS region. Defaults to env / config chain.
    #[arg(long, short = 'r', env = "AWS_REGION", global = true)]
    region: Option<String>,

    /// Profile name from ~/.aws/credentials.
    #[arg(long, env = "AWS_PROFILE", global = true)]
    profile: Option<String>,

    /// Override the service endpoint (for LocalStack / MinIO / custom).
    #[arg(long, env = "AWS_ENDPOINT_URL", global = true)]
    endpoint: Option<String>,

    #[command(subcommand)]
    cmd: Top,
}

#[derive(Subcommand, Debug)]
enum Top {
    /// S3 — buckets and objects.
    #[command(subcommand)]
    S3(s3::S3Cmd),

    /// DynamoDB — items, queries, scans.
    #[command(subcommand)]
    Ddb(ddb::DdbCmd),

    /// SQS — message queues.
    #[command(subcommand)]
    Sqs(sqs::SqsCmd),

    /// Lambda — invoke / list functions.
    #[command(subcommand)]
    Lambda(lambda::LambdaCmd),

    /// STS — credentials & identity.
    #[command(subcommand)]
    Sts(sts::StsCmd),

    /// Alias: STS get-caller-identity, exits 0 on success.
    Ping,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("stryke-aws-helper: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let cfg = load_aws_config(
        cli.region.as_deref(),
        cli.profile.as_deref(),
        cli.endpoint.as_deref(),
    )
    .await;

    match cli.cmd {
        Top::S3(c) => s3::dispatch(&cfg, c).await,
        Top::Ddb(c) => ddb::dispatch(&cfg, c).await,
        Top::Sqs(c) => sqs::dispatch(&cfg, c).await,
        Top::Lambda(c) => lambda::dispatch(&cfg, c).await,
        Top::Sts(c) => sts::dispatch(&cfg, c).await,
        Top::Ping => sts::caller_identity(&aws_sdk_sts::Client::new(&cfg)).await,
    }
}
