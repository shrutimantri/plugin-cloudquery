package io.kestra.plugin.cloudquery;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.NamespaceFiles;
import io.kestra.core.models.tasks.NamespaceFilesInterface;
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
    title = "Execute CloudQuery commands from a CLI."
)
@Plugin(
    examples = {
        @Example(
            title = "Run a CloudQuery sync from CLI",
            full = true,
            code = """
                id: cloudquery_sync_cli
                namespace: dev

                tasks:
                  - id: wdir
                    type: io.kestra.core.tasks.flows.WorkingDirectory
                    tasks:
                      - id: config_files
                        type: io.kestra.core.tasks.storages.LocalFiles
                        inputs:
                        config.yml: |
                          kind: source
                          spec:
                            name: hackernews
                            path: cloudquery/hackernews
                            version: v3.0.13
                            tables: ["*"]
                            backend_options:
                              table_name: cq_cursor
                              connection: "@@plugins.duckdb.connection"
                            destinations:
                              - "duckdb"
                            spec:
                              item_concurrency: 100
                              start_time: "{{ now() | dateAdd(-1, 'DAYS') }}"
                            ---
                            kind: destination
                            spec:
                              name: duckdb
                              path: cloudquery/duckdb
                              version: v4.2.10
                              write_mode: overwrite-delete-stale
                              spec:
                                connection_string: hn.db

                      - id: hn_to_duckdb
                        type: io.kestra.plugin.cloudquery.CloudQueryCLI
                        commands:
                          - cloudquery sync config.yml --log-console"""
        )
    }
)
public class CloudQueryCLI extends AbstractCloudQueryCommand implements RunnableTask<ScriptOutput>, NamespaceFilesInterface {

    @Schema(
        title = "List of CloudQuery commands to run"
    )
    @PluginProperty(dynamic = true)
    @NotEmpty
    protected List<String> commands;

    private NamespaceFiles namespaceFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        CommandsWrapper commands = new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withRunnerType(RunnerType.DOCKER)
            .withDockerOptions(injectDefaults(getDocker()))
            .withCommands(
                ScriptService.scriptCommands(
                    List.of("/bin/sh", "-c"),
                    List.of("alias cloudquery='/app/cloudquery'"),
                    this.commands
                )
            )
            .withEnv(this.getEnv())
            .withNamespaceFiles(namespaceFiles);

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
