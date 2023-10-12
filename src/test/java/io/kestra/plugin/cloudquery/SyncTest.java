package io.kestra.plugin.cloudquery;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
@Testcontainers
class SyncTest {
    public static String LOCALSTACK_VERSION = "localstack/localstack:1.4.0";
    protected static LocalStackContainer localstack;
    @Inject
    private RunContextFactory runContextFactory;

    @BeforeAll
    static void startLocalstack() {
        localstack = new LocalStackContainer(DockerImageName.parse(LOCALSTACK_VERSION));
        localstack.start();
    }

    @AfterAll
    static void stopLocalstack() {
        if (localstack != null) {
            localstack.stop();
        }
    }


    @Test
    void run() throws Exception {

        Sync execute = Sync.builder()
            .id(IdUtils.create())
            .type(Sync.class.getName())
            .env(Map.of(
                "AWS_ACCESS_KEY_ID", localstack.getAccessKey(),
                "AWS_SECRET_ACCESS_KEY", localstack.getSecretKey(),
                "AWS_DEFAULT_REGION", localstack.getRegion()
            ))
            .configs(List.of(
                Map.of(
                    "kind", "destination",
                    "spec", Map.of(
                        "name", "file",
                        "path", "cloudquery/file",
                        "version", "v3.4.8",
                        "spec", Map.of(
                            "path", "./{{TABLE}}-{{UUID}}.{{FORMAT}}",
                            "format", "json"
                        )
                    )
                ),
                Map.of(
                    "kind", "source",
                    "spec", Map.of(
                        "name", "aws",
                        "registry", "github",
                        "path", "cloudquery/aws",
                        "version", "v22.14.0",
                        "tables", List.of("aws_s3*"),
                        "destinations", List.of("file"),
                        "spec", Map.of(
                            "regions", List.of(localstack.getRegion()),
                            "custom_endpoint_url", localstack.getEndpoint().toString(),
                            "custom_endpoint_hostname_immutable", true,
                            "custom_endpoint_partition_id", "aws",
                            "custom_endpoint_signing_region", localstack.getRegion(),
                            "max_retries", "0"
                        )
                    )
                )
            ))
            .incremental(false)// TODO Disabled incremental as there is a bug with sqlite inside cloudquery docker
            .docker(DockerOptions.builder()
                // needed to be able to reach localstack from inside the container
                .networkMode("host")
                .build())
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, execute, Map.of());

        ScriptOutput runOutput = execute.run(runContext);

        assertThat(runOutput.getExitCode(), is(0));
    }

}
