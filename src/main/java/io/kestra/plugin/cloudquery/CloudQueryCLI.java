package io.kestra.plugin.cloudquery;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.exec.scripts.services.ScriptService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotEmpty;
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Execute CloudQuery commands."
)
@Plugin(
    examples = {
        @Example(
            title = "Migrate CloudQuery destination schemas",
            code = {
                "tasks:",
                "  - id: wdir",
                "    type: io.kestra.core.tasks.flows.WorkingDirectory",
                "    tasks:",
                "      - id: config",
                "        type: io.kestra.core.tasks.storages.LocalFiles",
                "        inputs:",
                "          config.yml: |",
                "            kind: source",
                "            spec:",
                "              name: aws",
                "              path: cloudquery/aws",
                "              version: \"v22.4.0\"",
                "              tables: [\"aws_s3*\", \"aws_ec2*\", \"aws_ecs*\", \"aws_iam*\", \"aws_glue*\", \"aws_dynamodb*\"]",
                "              destinations: [\"postgresql\"]",
                "              spec:",
                "            ---",
                "            kind: destination",
                "            spec:",
                "              name: \"postgresql\"",
                "              version: \"v5.0.3\"",
                "              path: \"cloudquery/postgresql\"",
                "              write_mode: \"overwrite-delete-stale\"",
                "              spec:",
                "                connection_string: ${PG_CONNECTION_STRING}",
                "",
                "      - id: cloudQuery",
                "        type: io.kestra.plugin.cloudquery.CloudQueryCLI",
                "        env:",
                "          AWS_ACCESS_KEY_ID: \"{{ secret('AWS_ACCESS_KEY_ID') }}\"",
                "          AWS_SECRET_ACCESS_KEY: \"{{ secret('AWS_SECRET_ACCESS_KEY') }}\"",
                "          AWS_DEFAULT_REGION: \"{{ secret('AWS_DEFAULT_REGION') }}\"    ",
                "          PG_CONNECTION_STRING: \"postgresql://postgres:{{secret('DB_PASSWORD')}}@host.docker.internal:5432/demo?sslmode=disable\"",
                "        commands:",
                "        - \"--version\"",
                "        - migrate config.yml --log-console"
            }
        ),
    }
)
public class CloudQueryCLI extends AbstractCloudQueryCommand implements RunnableTask<ScriptOutput> {

    @Schema(
        title = "List of CloudQuery commands to run"
    )
    @PluginProperty(dynamic = true)
    @NotEmpty
    protected List<String> commands;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commands = new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withRunnerType(RunnerType.DOCKER)
            .withDockerOptions(injectDefaults(getDocker()))
            .withCommands(
                ScriptService.scriptCommands(
                    List.of("/bin/sh", "-c"),
                    List.of(""),
                    this.commands.stream().map(cmd -> "/app/cloudquery " + cmd).toList()
                )
            )
            .withEnv(this.getEnv());

        return commands.run();
    }

    @Override
    protected DockerOptions injectDefaults(DockerOptions original) {
        if (original.getEntryPoint() == null || original.getEntryPoint().isEmpty()) {
            original = original.toBuilder().entryPoint(List.of("")).build();
        }
        return super.injectDefaults(original);
    }
}