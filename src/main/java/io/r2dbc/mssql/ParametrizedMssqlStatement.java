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
import io.r2dbc.mssql.client.ConnectionContext;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
final class ParametrizedMssqlStatement extends MssqlStatementSupport implements MssqlStatement {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParametrizedMssqlStatement.class);

    private static final boolean DEBUG_ENABLED = LOGGER.isDebugEnabled();

    private static final Pattern PARAMETER_MATCHER = Pattern.compile("@([\\p{Alpha}@][@$\\d\\w_]{0,127})");

    private final PreparedStatementCache statementCache;

    private final Client client;

    private final ConnectionContext context;

    private final Codecs codecs;

    private final ParsedQuery parsedQuery;

    private final Bindings bindings = new Bindings();

    private volatile boolean executed = false;

    ParametrizedMssqlStatement(Client client, ConnectionOptions connectionOptions, String sql) {

        super(connectionOptions.prefersCursors(sql));

        Assert.requireNonNull(client, "Client must not be null");
        Assert.requireNonNull(connectionOptions, "ConnectionOptions must not be null");
        Assert.requireNonNull(sql, "SQL must not be null");

        this.statementCache = connectionOptions.getPreparedStatementCache();
        this.client = client;
        this.context = client.getContext();
        this.codecs = connectionOptions.getCodecs();
        this.parsedQuery = this.statementCache.getParsedSql(sql, ParsedQuery::parse);
    }

    @Override
    public ParametrizedMssqlStatement add() {

        assertNotExecuted();
        this.bindings.validate(this.parsedQuery.getParameters());
        this.bindings.finish();
        return this;
    }

    @Override
    public Flux<MssqlResult> execute() {

        if (this.bindings.bindings.isEmpty()) {
            throw new IllegalStateException(String.format("No parameters bound for query '%s'", this.parsedQuery.sql));
        }

        this.bindings.validate(this.parsedQuery.getParameters());

        int effectiveFetchSize = getEffectiveFetchSize();
        return Flux.defer(() -> {

            assertNotExecuted();

            this.executed = true;

            boolean useGeneratedKeysClause = GeneratedValues.shouldExpectGeneratedKeys(this.getGeneratedColumns());
            String sql = useGeneratedKeysClause ? GeneratedValues.augmentQuery(this.parsedQuery.sql, getGeneratedColumns()) : this.parsedQuery.sql;

            if (this.bindings.bindings.size() == 1) {

                Flux<Message> exchange = exchange(effectiveFetchSize, useGeneratedKeysClause, sql, this.bindings.bindings.get(0));

                return exchange.windowUntil(DoneInProcToken.class::isInstance) //
                    .map(it -> MssqlResult.toResult(this.parsedQuery.getSql(), this.context, this.codecs, it));
            }

            Iterator<Binding> iterator = this.bindings.bindings.iterator();
            EmitterProcessor<Binding> bindingEmitter = EmitterProcessor.create(true);
            return bindingEmitter.startWith(iterator.next())
                .concatMap(it -> {

                    Flux<Message> exchange = exchange(effectiveFetchSize, useGeneratedKeysClause, sql, it);

                    return exchange.doOnComplete(() -> {
                        tryNextBinding(iterator, bindingEmitter);
                    });

                }).windowUntil(DoneInProcToken.class::isInstance) //
                .map(it -> MssqlResult.toResult(this.parsedQuery.getSql(), this.context, this.codecs, it))
                .doOnCancel(() -> clearBindings(iterator))
                .doOnError(e -> clearBindings(iterator));
        });
    }

    private Flux<Message> exchange(int effectiveFetchSize, boolean useGeneratedKeysClause, String sql, Binding it) {
        Flux<Message> exchange;

        if (effectiveFetchSize > 0) {

            if (DEBUG_ENABLED) {
                LOGGER.debug(this.context.getMessage("Start cursored exchange for {} with fetch size {}"), sql, effectiveFetchSize);
            }

            exchange = RpcQueryMessageFlow.exchange(this.statementCache, this.client, this.codecs, sql, it, effectiveFetchSize);
        } else {

            if (DEBUG_ENABLED) {
                LOGGER.debug(this.context.getMessage("Start direct exchange for {}"), sql);
            }

            exchange = RpcQueryMessageFlow.exchange(this.client, sql, it);
        }

        if (useGeneratedKeysClause) {
            exchange = exchange.transform(GeneratedValues::reduceToSingleCountDoneToken);
        }
        return exchange;
    }

    private void clearBindings(Iterator<Binding> iterator) {

        while (iterator.hasNext()) {
            // exhaust iterator
        }

        this.bindings.clear();
    }

    @Override
    public ParametrizedMssqlStatement returnGeneratedValues(String... columns) {

        super.returnGeneratedValues(columns);
        return this;
    }

    @Override
    public ParametrizedMssqlStatement fetchSize(int fetchSize) {

        super.fetchSize(fetchSize);
        return this;
    }

    private static void tryNextBinding(Iterator<Binding> iterator, EmitterProcessor<Binding> boundRequests) {

        if (boundRequests.isCancelled()) {
            return;
        }

        try {
            if (iterator.hasNext()) {
                boundRequests.onNext(iterator.next());
            } else {
                boundRequests.onComplete();
            }
        } catch (Exception e) {
            boundRequests.onError(e);
        }
    }

    @Override
    public ParametrizedMssqlStatement bind(String identifier, Object value) {

        Assert.requireNonNull(identifier, "identifier must not be null");
        Assert.isInstanceOf(String.class, identifier, "identifier must be a String");

        Encoded encoded = this.codecs.encode(this.client.getByteBufAllocator(), RpcParameterContext.in(this.client.getRequiredCollation()), value);
        encoded.touch("ParametrizedMssqlStatement.bind(…)");

        addBinding(getParameterName(identifier), encoded);

        return this;
    }

    @Override
    public ParametrizedMssqlStatement bind(int index, Object value) {

        Assert.requireNonNull(value, "value must not be null");

        return bind(getParameterName(index), value);
    }

    @Override
    public ParametrizedMssqlStatement bindNull(String identifier, Class<?> type) {

        Assert.requireNonNull(identifier, "Identifier must not be null");
        Assert.isInstanceOf(String.class, identifier, "Identifier must be a String");
        Assert.requireNonNull(type, "type must not be null");

        if (this.executed) {
            throw new IllegalStateException("Statement was already executed");
        }

        Encoded encoded = this.codecs.encodeNull(this.client.getByteBufAllocator(), type);
        encoded.touch("ParametrizedMssqlStatement.bindNull(…)");
        addBinding(getParameterName(identifier), encoded);
        return this;
    }

    @Override
    public ParametrizedMssqlStatement bindNull(int index, Class<?> type) {

        Assert.requireNonNull(type, "Type must not be null");

        return bindNull(getParameterName(index), type);
    }

    private void addBinding(String name, Encoded parameter) {

        assertNotExecuted();

        this.bindings.getCurrent().add(name, parameter);
    }

    private void assertNotExecuted() {
        if (this.executed) {
            throw new IllegalStateException("Statement was already executed");
        }
    }

    /**
     * Returns the {@link Bindings}.
     *
     * @return the {@link Bindings}.
     */
    Bindings getBindings() {
        return this.bindings;
    }

    private String getParameterName(int index) {
        return this.parsedQuery.getParameterName(index);
    }

    private String getParameterName(String name) {
        return this.parsedQuery.getParameterName(name);
    }

    /**
     * Returns whether the {@code sql} query is supported by this statement.
     *
     * @param sql the SQL to check.
     * @return {@code true} if supported.
     * @throws IllegalArgumentException when {@code sql} is {@code null}.
     */
    public static boolean supports(String sql) {

        Assert.requireNonNull(sql, "SQL must not be null");
        return sql.lastIndexOf('@') != -1;
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
         * Returns the  {@link ParsedParameter} name by {@code name}.
         *
         * @param name the parameter name.
         * @return the {@link ParsedParameter} name.
         */
        String getParameterName(String name) {

            ParsedParameter parsedParameter = this.parametersByName.get(name);

            if (name.startsWith("@")) {
                parsedParameter = this.parametersByName.get(name.substring(1));
            }

            if (parsedParameter == null) {
                throw new IllegalArgumentException(String.format("Parameter [%s] does not exist in query [%s]", name, this.sql));
            }

            return parsedParameter.getName();
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

        public void validate(List<ParsedParameter> parameters) {

            for (Binding binding : this.bindings) {

                Map<String, Encoded> bindingset = binding.getParameters();

                for (ParsedParameter parameter : parameters) {

                    if (!bindingset.containsKey(parameter.getName())) {
                        throw new IllegalStateException("No parameter binding for " + parameter.getName());
                    }
                }
            }
        }

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

        /**
         * Clear/release binding values.
         */
        void clear() {
            this.bindings.forEach(Binding::clear);
        }

    }
}
