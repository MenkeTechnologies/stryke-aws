//! STS commands.

use anyhow::{Context, Result};
use aws_sdk_sts::Client;
use clap::Subcommand;
use serde_json::json;

use crate::common::emit_json;

#[derive(Subcommand, Debug)]
pub enum StsCmd {
    /// `GetCallerIdentity` → `{account, arn, user_id}`.
    CallerIdentity,
    /// AssumeRole and print the temporary credentials.
    AssumeRole {
        role_arn: String,
        #[arg(long, short = 's')]
        session: String,
        /// Duration in seconds (900–43200).
        #[arg(long, default_value_t = 3600)]
        duration: i32,
        #[arg(long)]
        external_id: Option<String>,
    },
}

pub async fn dispatch(cfg: &aws_config::SdkConfig, cmd: StsCmd) -> Result<()> {
    let client = Client::new(cfg);
    match cmd {
        StsCmd::CallerIdentity => caller_identity(&client).await,
        StsCmd::AssumeRole {
            role_arn,
            session,
            duration,
            external_id,
        } => {
            assume_role(
                &client,
                &role_arn,
                &session,
                duration,
                external_id.as_deref(),
            )
            .await
        }
    }
}

pub async fn caller_identity(client: &Client) -> Result<()> {
    let resp = client
        .get_caller_identity()
        .send()
        .await
        .context("get_caller_identity")?;
    emit_json(&json!({
        "account": resp.account(),
        "arn": resp.arn(),
        "user_id": resp.user_id(),
    }))
}

async fn assume_role(
    client: &Client,
    role_arn: &str,
    session_name: &str,
    duration: i32,
    external_id: Option<&str>,
) -> Result<()> {
    let mut req = client
        .assume_role()
        .role_arn(role_arn)
        .role_session_name(session_name)
        .duration_seconds(duration);
    if let Some(eid) = external_id {
        req = req.external_id(eid);
    }
    let resp = req.send().await.context("assume_role")?;
    let creds = resp
        .credentials()
        .ok_or_else(|| anyhow::anyhow!("no credentials in response"))?;
    emit_json(&json!({
        "access_key_id": creds.access_key_id(),
        "secret_access_key": creds.secret_access_key(),
        "session_token": creds.session_token(),
        "expiration": creds.expiration().to_string(),
        "assumed_role_arn": resp.assumed_role_user().map(|u| u.arn().to_string()),
    }))
}
