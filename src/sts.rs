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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct TestCli {
        #[command(subcommand)]
        cmd: StsCmd,
    }

    fn parse(args: &[&str]) -> Result<StsCmd, clap::Error> {
        let mut argv = vec!["stryke-aws-helper"];
        argv.extend_from_slice(args);
        TestCli::try_parse_from(argv).map(|c| c.cmd)
    }

    #[test]
    fn caller_identity_parses_as_unit_variant() {
        let cmd = parse(&["caller-identity"]).expect("parse");
        assert!(matches!(cmd, StsCmd::CallerIdentity));
    }

    #[test]
    fn assume_role_requires_role_arn_positional() {
        let err = parse(&["assume-role"]).expect_err("missing role_arn");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn assume_role_requires_session_flag() {
        // role_arn supplied but --session missing → must error, otherwise
        // AssumeRole would be sent with an empty session name and AWS would
        // reject it server-side. Catching it at parse is the right contract.
        let err =
            parse(&["assume-role", "arn:aws:iam::1234:role/foo"]).expect_err("missing --session");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn assume_role_default_duration_is_3600_seconds() {
        // Pin the AWS-spec default (1 hour). A drift here would silently
        // change credential lifetime for every assume-role call.
        let cmd = parse(&[
            "assume-role",
            "arn:aws:iam::1234:role/foo",
            "--session",
            "my-session",
        ])
        .expect("parse");
        match cmd {
            StsCmd::AssumeRole { duration, .. } => assert_eq!(duration, 3600),
            _ => panic!("expected AssumeRole"),
        }
    }

    #[test]
    fn assume_role_external_id_defaults_none_and_wires_through() {
        let cmd = parse(&[
            "assume-role",
            "arn:aws:iam::1234:role/foo",
            "--session",
            "s",
            "--external-id",
            "ext-42",
        ])
        .expect("parse");
        match cmd {
            StsCmd::AssumeRole { external_id, .. } => {
                assert_eq!(external_id.as_deref(), Some("ext-42"));
            }
            _ => panic!("expected AssumeRole"),
        }

        let default_cmd = parse(&[
            "assume-role",
            "arn:aws:iam::1234:role/foo",
            "--session",
            "s",
        ])
        .expect("parse");
        match default_cmd {
            StsCmd::AssumeRole { external_id, .. } => assert!(external_id.is_none()),
            _ => panic!("expected AssumeRole"),
        }
    }
}
