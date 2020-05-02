/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.mssql.util;

import com.github.dockerjava.api.command.InspectContainerResponse;
import io.r2dbc.mssql.util.TestCertificateAuthority.ServerKeyPair;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.String.join;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Arrays.asList;
import static java.util.Comparator.reverseOrder;

/**
 * A MSSQL testcontainer that supports custom TLS certificates.
 */
class TestMsSqlServerContainer extends MSSQLServerContainer<TestMsSqlServerContainer> {

    private static final String PATH_CERT = "/cert.pem";

    private static final String PATH_KEY = "/key.pem";

    private static final String PATH_ENTRYPOINT = "/entrypoint.sh";

    private static final String ENTRYPOINT = join("\n", asList(
        "set -e",
        "/opt/mssql/bin/mssql-conf set network.tlscert " + PATH_CERT,
        "/opt/mssql/bin/mssql-conf set network.tlskey " + PATH_KEY,
        "/opt/mssql/bin/mssql-conf set network.tlsprotocols 1.2",
        "exec \"$@\""
    ));

    private final Path tempDir;

    TestMsSqlServerContainer(ServerKeyPair keyPair) {
        withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(TestMsSqlServerContainer.class)));
        try {
            tempDir = Files.createTempDirectory("r2dbc-mssql-test-");
        } catch (final IOException e) {
            throw new RuntimeException("Failed to create temp directory", e);
        }
        withFile(tempDir, PATH_CERT, keyPair.getCertificate());
        withFile(tempDir, PATH_KEY, keyPair.getPrivateKey());
        withFile(tempDir, PATH_ENTRYPOINT, ENTRYPOINT.getBytes(US_ASCII));
        withCreateContainerCmdModifier(cmd -> {
            cmd.withEntrypoint("sh", PATH_ENTRYPOINT);
            cmd.withCmd("/opt/mssql/bin/sqlservr");
        });
    }

    private void withFile(Path tempDir, String filename, byte[] data) {
        File tempFile = new File(tempDir.toFile(), filename);
        try {
            Files.write(tempFile.toPath(), data);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to write temp file", e);
        }
        withFileSystemBind(tempFile.getPath(), filename);
    }

    @Override
    protected void containerIsStopped(InspectContainerResponse containerInfo) {
        try {
            Files.walk(tempDir).sorted(reverseOrder()).forEach(it -> it.toFile().delete());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to delete temp directory", e);
        }
    }

    protected void configure() {
        this.addExposedPort(MS_SQL_SERVER_PORT);
        this.addEnv("ACCEPT_EULA", "Y");
        this.addEnv("SA_PASSWORD", getPassword());
        this.withReuse(true);
    }
}
