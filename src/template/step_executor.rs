use std::collections::HashMap;

use anyhow::{Context, Result};
use tracing::debug;

use super::build_spec::{TemplateBuildStep, TemplateBuildStepKind};
use super::errors::{command_output_suffix, TemplateBuildFailure};
use crate::sandbox::{ProcessOpts, SandboxExecutor};
use crate::snapshot::CommandContext;

#[derive(Clone, Debug, Default)]
pub(crate) struct TemplateStepExecutor;

impl TemplateStepExecutor {
    pub(crate) fn new() -> Self {
        Self
    }

    #[tracing::instrument(
        skip(self, sandbox, steps, initial_context),
        fields(step_count = steps.len())
    )]
    pub(crate) async fn execute(
        &self,
        sandbox: &impl SandboxExecutor,
        steps: &[TemplateBuildStep],
        initial_context: CommandContext,
    ) -> Result<CommandContext> {
        let mut context = initial_context;

        debug!("executing template build steps");
        for step in steps {
            match &step.kind {
                TemplateBuildStepKind::Env { key, value } => {
                    context = context.with_env_var(key.clone(), value.clone());
                }
                TemplateBuildStepKind::Workdir { path } => {
                    context = context.with_workdir(path.to_string_lossy());
                }
                TemplateBuildStepKind::User { value } => {
                    context = context.with_user(Some(value.clone()));
                }
                TemplateBuildStepKind::ExposedPort { port } => {
                    let mut ports = context.exposed_ports.clone();
                    if !ports.contains(port) {
                        ports.push(port.clone());
                    }
                    context = context.with_exposed_ports(ports);
                }
                TemplateBuildStepKind::Volume { path } => {
                    let mut volumes = context.volumes.clone();
                    if !volumes.contains(path) {
                        volumes.push(path.clone());
                    }
                    context = context.with_volumes(volumes);
                }
                TemplateBuildStepKind::Label { key, value } => {
                    let mut labels = context.labels.clone();
                    labels.insert(key.clone(), value.clone());
                    context = context.with_labels(labels);
                }
                TemplateBuildStepKind::Run { cmd } => {
                    self.run_step(sandbox, &context.workdir, &context.env_vars, cmd)
                        .await?;
                }
            }
        }
        debug!("template build steps completed");

        Ok(context)
    }

    async fn run_step(
        &self,
        sandbox: &impl SandboxExecutor,
        workdir: &str,
        env: &HashMap<String, String>,
        cmd: &str,
    ) -> Result<()> {
        let opts = ProcessOpts {
            envs: env.clone(),
            cwd: Some(workdir.to_string()),
            ..ProcessOpts::default()
        };

        let output = sandbox
            .run_command_with_opts("/bin/bash", &["-lc", cmd], &opts)
            .await
            .with_context(|| {
                TemplateBuildFailure::with_step("build step failed", format!("RUN {cmd}"))
            })?;
        if output.exit_code != 0 {
            let message = format!(
                "build step failed: command exited with status {}{}",
                output.exit_code,
                command_output_suffix(&output.stdout, &output.stderr)
            );
            return Err(TemplateBuildFailure::with_step(message, format!("RUN {cmd}")).into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    use async_trait::async_trait;

    use super::TemplateStepExecutor;
    use crate::sandbox::{Executor, ProcessHandle, ProcessOpts, ProcessOutput, SandboxExecutor};
    use crate::snapshot::CommandContext;
    use crate::template::build_spec::TemplateBuildStep;

    struct NoopSandbox;

    #[async_trait(?Send)]
    impl SandboxExecutor for NoopSandbox {
        fn executor(&self) -> Result<Executor<'_>> {
            Err(anyhow!("not used"))
        }
        async fn run_command_with_opts(
            &self,
            _cmd: &str,
            _args: &[&str],
            _opts: &ProcessOpts,
        ) -> Result<ProcessOutput> {
            Err(anyhow!("not used"))
        }
        async fn start_process(
            &self,
            _cmd: &str,
            _args: &[&str],
            _opts: &ProcessOpts,
        ) -> Result<ProcessHandle> {
            Err(anyhow!("not used"))
        }
    }

    async fn run(steps: Vec<TemplateBuildStep>) -> CommandContext {
        TemplateStepExecutor::new()
            .execute(&NoopSandbox, &steps, CommandContext::default())
            .await
            .expect("steps should execute without error")
    }

    #[tokio::test]
    async fn user_step_sets_context_user() {
        let ctx = run(vec![TemplateBuildStep::user("zzz")]).await;
        assert_eq!(ctx.user.as_deref(), Some("zzz"));
    }

    #[tokio::test]
    async fn user_step_overrides_base_image_user() {
        let initial = CommandContext::default().with_user(Some("root".to_string()));
        let ctx = TemplateStepExecutor::new()
            .execute(&NoopSandbox, &[TemplateBuildStep::user("zzz")], initial)
            .await
            .unwrap();
        assert_eq!(ctx.user.as_deref(), Some("zzz"));
    }

    #[tokio::test]
    async fn exposed_port_deduplicates() {
        let ctx = run(vec![
            TemplateBuildStep::exposed_port("8080"),
            TemplateBuildStep::exposed_port("8080"),
            TemplateBuildStep::exposed_port("443"),
        ])
        .await;
        assert_eq!(ctx.exposed_ports, vec!["8080", "443"]);
    }

    #[tokio::test]
    async fn exposed_port_from_base_image_is_not_duplicated() {
        let initial = CommandContext::default().with_exposed_ports(vec!["8080".to_string()]);
        let ctx = TemplateStepExecutor::new()
            .execute(
                &NoopSandbox,
                &[TemplateBuildStep::exposed_port("8080")],
                initial,
            )
            .await
            .unwrap();
        assert_eq!(ctx.exposed_ports, vec!["8080"]);
    }

    #[tokio::test]
    async fn volume_deduplicates() {
        let ctx = run(vec![
            TemplateBuildStep::volume("/data"),
            TemplateBuildStep::volume("/data"),
            TemplateBuildStep::volume("/logs"),
        ])
        .await;
        assert_eq!(ctx.volumes, vec!["/data", "/logs"]);
    }

    #[tokio::test]
    async fn env_step_sets_env_var() {
        let ctx = run(vec![TemplateBuildStep::env("FOO", "bar")]).await;
        assert_eq!(ctx.env_vars.get("FOO").map(String::as_str), Some("bar"));
    }

    #[tokio::test]
    async fn workdir_step_updates_workdir() {
        let ctx = run(vec![TemplateBuildStep::workdir("/workspace")]).await;
        assert_eq!(ctx.workdir, "/workspace");
    }
}
