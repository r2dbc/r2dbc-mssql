/*
 * Copyright 2026 the original author or authors.
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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * JUnit {@link ParameterResolver} to inject an {@link ExecutorService} with a fixed thread pool.
 *
 * @see Concurrency
 */
class ConcurrencyExtension implements ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(ConcurrencyExtension.class);

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {

        Class<?> parameterType = parameterContext.getParameter().getType();

        return parameterContext.isAnnotated(Concurrency.class)
                && parameterType.isAssignableFrom(ExecutorService.class);
    }

    @Override
    public ExecutorService resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {

        Concurrency concurrency = parameterContext.findAnnotation(Concurrency.class)
                .orElseThrow(() -> new ParameterResolutionException("@Concurrency is missing"));

        int parallelism = concurrency.value();

        if (parallelism <= 0) {
            throw new ParameterResolutionException("@Concurrency value must be greater than zero");
        }

        ExtensionContext classContext = findTestClassContext(extensionContext);
        ExecutorResource resource = classContext.getStore(NAMESPACE)
                .getOrComputeIfAbsent(parallelism, ExecutorResource::new, ExecutorResource.class);

        return resource.executor();
    }

    private static ExtensionContext findTestClassContext(ExtensionContext context) {

        ExtensionContext current = context;

        while (true) {
            if (current.getTestClass().isPresent() && !current.getTestMethod().isPresent()) {
                return current;
            }

            current = current.getParent().orElseThrow(() -> new ParameterResolutionException(
                    "Could not locate the test-class context"));
        }
    }

    private static class ExecutorResource
            implements AutoCloseable {

        private final ExecutorService executor;

        private ExecutorResource(int parallelism) {
            this.executor = Executors.newFixedThreadPool(parallelism);
        }

        private ExecutorService executor() {
            return this.executor;
        }

        @Override
        public void close() {
            this.executor.shutdown();
            try {
                if (!this.executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    this.executor.shutdownNow();
                    this.executor.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException interrupted) {
                this.executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
