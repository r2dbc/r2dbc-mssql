/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import io.r2dbc.mssql.message.rpc.EncodedRpcParameter;
import io.r2dbc.mssql.message.rpc.RpcDirection;
import io.r2dbc.mssql.util.Assert;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Prepared {@link Statement} with parameter markers executed against a Microsoft SQL Server database.
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
public final class PreparedMssqlStatement implements MssqlStatement<PreparedMssqlStatement> {

    private static final Pattern PARAMETER_MATCHER = Pattern.compile("@([\\p{Alpha}@][@$\\d\\w_]{0,127})");

    private final ParsedQuery parsedQuery;

    private final Bindings bindings = new Bindings();

    private final Codecs codecs;

    private final Client client;

    public PreparedMssqlStatement(ParsedQuery parsedQuery, Codecs codecs, Client client) {
        this.parsedQuery = parsedQuery;
        this.codecs = codecs;
        this.client = client;
    }

    @Override
    public PreparedMssqlStatement add() {
        this.bindings.finish();
        return this;
    }

    @Override
    public Flux<MssqlResult> execute() {
        return null;
    }

    @Override
    public PreparedMssqlStatement bind(Object identifier, Object value) {

        Objects.requireNonNull(identifier, "identifier must not be null");
        Assert.isInstanceOf(String.class, identifier, "identifier must be a String");

        // this.bindings.getCurrent().add((String) identifier, this.codecs.encode(type, client.getDatabaseCollation().get()));
        return this;
    }

    @Override
    public PreparedMssqlStatement bind(int index, Object value) {

        Objects.requireNonNull(value, "value must not be null");

        return bind(getVariableName(index), value);
    }

    @Override
    public PreparedMssqlStatement bindNull(Object identifier, Class<?> type) {
        Objects.requireNonNull(identifier, "identifier must not be null");
        Assert.isInstanceOf(String.class, identifier, "identifier must be a String");
        Objects.requireNonNull(type, "type must not be null");

        this.bindings.getCurrent().add((String) identifier, this.codecs.encodeNull(RpcDirection.OUT, type));
        return this;
    }

    @Override
    public PreparedMssqlStatement bindNull(int index, Class<?> type) {
        Objects.requireNonNull(type, "type must not be null");

        this.bindings.getCurrent().add(getVariableName(index), this.codecs.encodeNull(RpcDirection.OUT, type));
        return this;
    }

    private String getVariableName(int index) {

        Assert.isTrue(index >= 0 && index < this.parsedQuery.getVariableCount(), () -> String.format("Index must be greater [0] and less than [%d]", this.parsedQuery.getVariableCount()));

        return this.parsedQuery.getVariableName(index);
    }

    /**
     * Returns whether the {@code sql} query is supported by this statement.
     *
     * @param sql the SQL to check.
     * @return {@literal true} if supported.
     */
    public static boolean supports(String sql) {

        Objects.requireNonNull(sql, "SQL must not be null");
        return PARAMETER_MATCHER.matcher(sql).find();
    }

    /**
     * Parse the {@code sql} query and resolve variable parameters.
     *
     * @param sql the SQL query to parse.
     * @return the parsed query.
     */
    static ParsedQuery parse(String sql) {

        Objects.requireNonNull(sql, "SQL must not be null");

        List<ParsedVariable> variables = new ArrayList<>();

        int offset = 0;
        while (offset != -1) {

            offset = findCharacter('@', sql, offset);

            if (offset != -1) {

                Matcher matcher = PARAMETER_MATCHER.matcher(sql.substring(offset));
                offset++;
                if (matcher.find()) {

                    String name = matcher.group(1);
                    variables.add(new ParsedVariable(name, offset));
                }
            }
        }

        return new ParsedQuery(sql, variables);
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

        private final List<ParsedVariable> variables;

        public ParsedQuery(String sql, List<ParsedVariable> variables) {
            this.sql = sql;
            this.variables = variables;
        }

        public String getSql() {
            return sql;
        }

        public int getVariableCount() {
            return this.variables.size();
        }

        public String getVariableName(int index) {
            return this.variables.get(index).getName();
        }

        public List<ParsedVariable> getVariables() {
            return variables;
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
            return Objects.equals(sql, that.sql) &&
                Objects.equals(variables, that.variables);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sql, variables);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [sql='").append(sql).append('\'');
            sb.append(", variables=").append(variables);
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * A SQL variable within a SQL query.
     */
    static class ParsedVariable {

        private final String name;

        private final int position;

        public ParsedVariable(String name, int position) {
            this.name = name;
            this.position = position;
        }

        public String getName() {
            return name;
        }

        public int getPosition() {
            return position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ParsedVariable)) {
                return false;
            }
            ParsedVariable that = (ParsedVariable) o;
            return position == that.position &&
                Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, position);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [name='").append(name).append('\'');
            sb.append(", position=").append(position);
            sb.append(']');
            return sb.toString();
        }
    }

    private static final class Bindings {

        private final List<Binding> bindings = new ArrayList<>();

        private Binding current;

        @Override
        public String toString() {
            return "Bindings{" +
                "bindings=" + this.bindings +
                ", current=" + this.current +
                '}';
        }

        private void finish() {
            this.current = null;
        }

        private Binding first() {
            return this.bindings.stream().findFirst().orElseThrow(() -> new IllegalStateException("No parameters have been bound"));
        }

        private Binding getCurrent() {
            if (this.current == null) {
                this.current = new Binding();
                this.bindings.add(this.current);
            }

            return this.current;
        }

        private Stream<Binding> stream() {
            return this.bindings.stream();
        }
    }

    /**
     * A collection of {@link Parameter}s for a single bind invocation of a prepared statement.
     */
    static class Binding {

        private final Map<String, EncodedRpcParameter> parameters = new LinkedHashMap<>();

        /**
         * Add a {@link Parameter} to the binding.
         *
         * @param name      the name of the {@link Parameter}
         * @param parameter the {@link Parameter}
         * @return this {@link Binding}
         * @throws NullPointerException if {@code index} or {@code parameter} is {@code null}
         */
        public Binding add(String name, EncodedRpcParameter parameter) {
            Objects.requireNonNull(name, "Name must not be null");
            Objects.requireNonNull(parameter, "parameter must not be null");

            this.parameters.put(name, parameter);

            return this;
        }

        public Map<String, EncodedRpcParameter> getParameters() {
            return parameters;
        }

        private int size() {
            return this.parameters.size();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Binding that = (Binding) o;
            return Objects.equals(this.parameters, that.parameters);
        }


        @Override
        public int hashCode() {
            return Objects.hash(this.parameters);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [parameters=").append(parameters);
            sb.append(']');
            return sb.toString();
        }
    }
}
