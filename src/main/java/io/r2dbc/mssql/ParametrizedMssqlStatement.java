/*
 * Copyright 2018-2019 the original author or authors.
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

import io.r2dbc.mssql.client.Client;
import io.r2dbc.mssql.codec.Codecs;
import io.r2dbc.mssql.codec.Encoded;
import io.r2dbc.mssql.codec.RpcParameterContext;
import io.r2dbc.mssql.message.Message;
import io.r2dbc.mssql.message.token.DoneInProcToken;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parametrized {@link Statement} with parameter markers executed against a Microsoft SQL Server database.
 * <p>
 * T-SQL uses named parameters that are at-prefixed ({@literal @}). Examples for parameter names are:
 * <pre class="code">
 * &#x40;p0
 *
 * &#x40;myparam
 *
 * &#x40;first_name
 * </pre>
 *
 * @author Mark Paluch
 */
final class ParametrizedMssqlStatement implements MssqlStatement {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final Pattern PARAMETER_MATCHER = Pattern.compile("@([\\p{Alpha}@][@$\\d\\w_]{0,127})");

    private final PreparedStatementCache statementCache;

    private final Client client;

    private final Codecs codecs;

    private final boolean preferCursoredExecution;

    private final ParsedQuery parsedQuery;

    private final Bindings bindings = new Bindings();

    private String[] generatedColumns;

    ParametrizedMssqlStatement(Client client, ConnectionOptions connectionOptions, String sql) {

        this.statementCache = connectionOptions.getPreparedStatementCache();
        this.client = client;
        this.codecs = connectionOptions.getCodecs();
        this.preferCursoredExecution = connectionOptions.prefersCursors(sql);
        this.parsedQuery = ParsedQuery.parse(sql);
    }

    @Override
    public ParametrizedMssqlStatement add() {
        this.bindings.finish();
        return this;
    }

    @Override
    public Flux<MssqlResult> execute() {

        Iterator<Binding> iterator = new Vector<>(this.bindings.bindings).iterator();

        if (!iterator.hasNext()) {
            return Flux.empty();
        }

        boolean useGeneratedKeysClause = GeneratedValues.shouldExpectGeneratedKeys(this.generatedColumns);
        String sql = useGeneratedKeysClause ? GeneratedValues.augmentQuery(this.parsedQuery.sql, generatedColumns) : this.parsedQuery.sql;

        EmitterProcessor<Binding> bindingEmitter = EmitterProcessor.create(true);
        FluxSink<Binding> boundRequests = bindingEmitter.sink();

        return bindingEmitter.startWith(iterator.next())
            .flatMap(it -> {

                logger.debug("Start exchange for {}", sql);

                Flux<Message> exchange;

                if (preferCursoredExecution) {
                    exchange = RpcQueryMessageFlow.exchange(this.statementCache, this.client, this.codecs, sql, it, 128);
                } else {
                    exchange = RpcQueryMessageFlow.exchange(this.client, sql, it);
                }

                if (useGeneratedKeysClause) {
                    exchange = exchange.transform(GeneratedValues::reduceToSingleCountDoneToken);
                }

                return exchange.doOnComplete(() -> {
                    tryNextBinding(iterator, boundRequests);
                });

            }).windowUntil(DoneInProcToken.class::isInstance) //
            .map(it -> MssqlResult.toResult(this.codecs, it));
    }

    @Override
    public ParametrizedMssqlStatement returnGeneratedValues(String... columns) {

        Assert.requireNonNull(columns, "columns must not be null");

        this.generatedColumns = columns;
        return this;
    }

    private static void tryNextBinding(Iterator<Binding> iterator, FluxSink<Binding> boundRequests) {

        if (boundRequests.isCancelled()) {
            return;
        }

        try {
            if (iterator.hasNext()) {
                boundRequests.next(iterator.next());
            } else {
                boundRequests.complete();
            }
        } catch (Exception e) {
            boundRequests.error(e);
        }
    }

    @Override
    public ParametrizedMssqlStatement bind(Object identifier, Object value) {

        Assert.requireNonNull(identifier, "identifier must not be null");
        Assert.isInstanceOf(String.class, identifier, "identifier must be a String");

        Encoded encoded = this.codecs.encode(this.client.getByteBufAllocator(), RpcParameterContext.in(this.client.getRequiredCollation()), value);

        String parameterName = (String) identifier;
        validateParameterName(parameterName);
        this.bindings.getCurrent().add(parameterName, encoded);
        return this;
    }

    @Override
    public ParametrizedMssqlStatement bind(int index, Object value) {

        Assert.requireNonNull(value, "value must not be null");

        return bind(getParameterName(index), value);
    }

    @Override
    public ParametrizedMssqlStatement bindNull(Object identifier, Class<?> type) {

        Assert.requireNonNull(identifier, "Identifier must not be null");
        Assert.isInstanceOf(String.class, identifier, "Identifier must be a String");
        Assert.requireNonNull(type, "type must not be null");

        this.bindings.getCurrent().add((String) identifier, this.codecs.encodeNull(this.client.getByteBufAllocator(), type));
        return this;
    }

    @Override
    public ParametrizedMssqlStatement bindNull(int index, Class<?> type) {

        Assert.requireNonNull(type, "Type must not be null");

        this.bindings.getCurrent().add(getParameterName(index), this.codecs.encodeNull(this.client.getByteBufAllocator(), type));
        return this;
    }

    /**
     * Returns the {@link Bindings}.
     *
     * @return the {@link Bindings}.
     */
    Bindings getBindings() {
        return this.bindings;
    }

    /**
     * Validate the parameter name exists.
     *
     * @param parameterName the parameter name.
     */
    private void validateParameterName(String parameterName) {
        this.parsedQuery.getParameter(parameterName);
    }

    private String getParameterName(int index) {
        return this.parsedQuery.getParameterName(index);
    }

    /**
     * Returns whether the {@code sql} query is supported by this statement.
     *
     * @param sql the SQL to check.
     * @return {@literal true} if supported.
     * @throws IllegalArgumentException when {@code sql} is {@code null}.
     */
    public static boolean supports(CharSequence sql) {

        Assert.requireNonNull(sql, "SQL must not be null");
        return PARAMETER_MATCHER.matcher(sql).find();
    }

    /**
     * Locates the first occurrence of {@code needle} in {@code sql} starting at {@code offset}. The SQL string may contain:
     *
     * <ul>
     * <li>Literals, enclosed in single quotes ({@literal '}) </li>
     * <li>Literals, enclosed in double quotes ({@literal "}) </li>
     * <li>Escape sequences, enclosed in square brackets ({@literal []}) </li>
     * <li>Escaped escapes or literal delimiters (i.e. {@literal ''}, {@literal ""} or {@literal ]])</li>
     * <li>C-style single-line comments beginning with {@literal --}</li>
     * <li>C-style multi-line comments beginning enclosed</li>
     * </ul>
     *
     * @param needle the character to search for.
     * @param sql    the SQL string to search in.
     * @param offset the offset to start searching.
     * @return the offset or {@literal -1} if not found.
     */
    @SuppressWarnings({"fallthrough"})
    private static int findCharacter(char needle, CharSequence sql, int offset) {

        char chQuote;
        char character;
        int length = sql.length();

        while (offset < length && offset != -1) {

            character = sql.charAt(offset++);
            switch (character) {
                case '/':
                    if (offset == length) {
                        break;
                    }

                    if (sql.charAt(offset) == '*') { // If '/* ... */' comment
                        while (++offset < length) { // consume comment
                            if (sql.charAt(offset) == '*' && offset + 1 < length && sql.charAt(offset + 1) == '/') { // If
                                // end
                                // of
                                // comment
                                offset += 2;
                                break;
                            }
                        }
                        break;
                    }

                    if (sql.charAt(offset) == '-') {
                        break;
                    }

                    // Fall through - will fail next if and end up in default case
                case '-':
                    if (sql.charAt(offset) == '-') { // If '-- ... \n' comment
                        while (++offset < length) { // consume comment
                            if (sql.charAt(offset) == '\n' || sql.charAt(offset) == '\r') { // If end of comment
                                offset++;
                                break;
                            }
                        }
                        break;
                    }
                    // Fall through to test character
                default:
                    if (needle == character) {
                        return offset - 1;
                    }
                    break;

                case '[':
                    character = ']';
                case '\'':
                case '"':
                    chQuote = character;
                    while (offset < length) {
                        if (sql.charAt(offset++) == chQuote) {
                            if (length == offset || sql.charAt(offset) != chQuote) {
                                break;
                            }

                            ++offset;
                        }
                    }
                    break;
            }
        }

        return -1;
    }

    /**
     * A parsed SQL query with its variable names.
     */
    static class ParsedQuery {

        private final String sql;

        private final List<ParsedParameter> parameters;

        private final Map<String, ParsedParameter> parametersByName = new LinkedHashMap<>();

        ParsedQuery(String sql, List<ParsedParameter> parameters) {

            this.sql = sql;
            this.parameters = parameters;

            for (ParsedParameter parameter : parameters) {
                this.parametersByName.put(parameter.getName(), parameter);
            }
        }

        /**
         * Parse the {@code sql} query and resolve variable parameters.
         *
         * @param sql the SQL query to parse.
         * @return the parsed query.
         * @throws IllegalArgumentException when {@code sql} is {@code null}.
         */
        static ParsedQuery parse(String sql) {

            Assert.requireNonNull(sql, "SQL must not be null");

            List<ParsedParameter> variables = new ArrayList<>();

            int offset = 0;
            while (offset != -1) {

                offset = findCharacter('@', sql, offset);

                if (offset != -1) {

                    Matcher matcher = PARAMETER_MATCHER.matcher(sql.substring(offset));
                    offset++;
                    if (matcher.find()) {

                        String name = matcher.group(1);
                        variables.add(new ParsedParameter(name, offset));
                    }
                }
            }

            return new ParsedQuery(sql, variables);
        }

        /**
         * Returns the  {@link ParsedParameter} by {@code name}.
         *
         * @param name the parameter name.
         * @return the {@link ParsedParameter} whose name matches {@code name}.
         */
        ParsedParameter getParameter(String name) {

            ParsedParameter parsedParameter = this.parametersByName.get(name);

            if (parsedParameter == null) {
                throw new IllegalArgumentException(String.format("Parameter [%s] does not exist in query [%s]", name, this.sql));
            }

            return parsedParameter;
        }

        /**
         * Returns the parameter name at the positional {@code index}.
         *
         * @param index
         * @return
         */
        public String getParameterName(int index) {

            if (index < 0) {
                throw new IndexOutOfBoundsException("Index must be greater or equal to zero");
            }

            if (index >= getParameterCount()) {
                throw new IndexOutOfBoundsException(String.format("No such parameter with index [%d]  in query [%s]", index, this.sql));
            }

            return this.parameters.get(index).getName();
        }

        public String getSql() {
            return this.sql;
        }

        /**
         * @return the number of parameters.
         */
        public int getParameterCount() {
            return this.parameters.size();
        }

        public List<ParsedParameter> getParameters() {
            return this.parameters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ParsedQuery)) {
                return false;
            }
            ParsedQuery that = (ParsedQuery) o;
            return Objects.equals(this.sql, that.sql) &&
                Objects.equals(this.parameters, that.parameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.sql, this.parameters);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [sql='").append(this.sql).append('\'');
            sb.append(", variables=").append(this.parameters);
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * A SQL parameter within a SQL query.
     */
    static class ParsedParameter {

        private final String name;

        private final int position;

        ParsedParameter(String name, int position) {
            this.name = name;
            this.position = position;
        }


        public String getName() {
            return this.name;
        }

        public int getPosition() {
            return this.position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ParsedParameter)) {
                return false;
            }
            ParsedParameter that = (ParsedParameter) o;
            return this.position == that.position &&
                Objects.equals(this.name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.name, this.position);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [name='").append(this.name).append('\'');
            sb.append(", position=").append(this.position);
            sb.append(']');
            return sb.toString();
        }
    }

    static final class Bindings {

        private final List<Binding> bindings = new ArrayList<>();

        private Binding current;

        private void finish() {
            this.current = null;
        }

        Binding first() {
            return this.bindings.stream().findFirst().orElseThrow(() -> new IllegalStateException("No parameters have been bound"));
        }

        Binding getCurrent() {
            if (this.current == null) {
                this.current = new Binding();
                this.bindings.add(this.current);
            }

            return this.current;
        }
    }
}
