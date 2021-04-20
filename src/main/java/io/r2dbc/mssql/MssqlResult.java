/*
 * Copyright 2021 the original author or authors.
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

package io.r2dbc.mssql;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A strongly typed implementation of {@link Result} for a Microsoft SQL Server database.
 *
 * @author Mark Paluch
 */
public interface MssqlResult extends Result {

    /**
     * {@inheritDoc}
     */
    @Override
    Mono<Integer> getRowsUpdated();

    /**
     * {@inheritDoc}
     */
    @Override
    <T> Flux<T> map(BiFunction<Row, RowMetadata, ? extends T> f);

    /**
     * {@inheritDoc}
     */
    @Override
    Result filter(Predicate<Segment> filter);

    /**
     * {@inheritDoc}
     */
    @Override
    <T> Flux<T> flatMap(Function<Segment, ? extends Publisher<? extends T>> mappingFunction);

}
