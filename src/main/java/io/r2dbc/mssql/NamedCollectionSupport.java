/*
 * Copyright 2018-2021 the original author or authors.
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

import io.r2dbc.mssql.message.token.Column;
import io.r2dbc.mssql.util.Assert;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Object that provides access to items (columns, return values) by index and by name. The index starts at {@literal 0} (zero-based index).
 *
 * @author Mark Paluch
 */
abstract class NamedCollectionSupport<N> implements Collection<String> {

    private final N[] items;

    private final Map<String, N> nameKeyed;

    private final Function<N, String> nameMapper;

    private final String itemName;

    @SuppressWarnings("unchecked")
    NamedCollectionSupport(N[] items, Map<String, N> nameKeyed, Function<N, String> nameMapper, String itemName) {

        this.nameMapper = nameMapper;
        this.itemName = itemName;

        if (shouldStripROWSTAT(items)) {

            this.items = (N[]) Array.newInstance(items.getClass().getComponentType(), items.length - 1);
            System.arraycopy(items, 0, this.items, 0, this.items.length);

            this.nameKeyed = toMap(this.items, nameMapper);
        } else {

            this.items = items;
            this.nameKeyed = nameKeyed;
        }
    }

    static <N> Map<String, N> toMap(N[] named, Function<N, String> nameMapper) {

        Map<String, N> nameKeyed = new HashMap<>(named.length, 1);

        for (N n : named) {

            N old = nameKeyed.put(nameMapper.apply(n), n);
            if (old != null) {
                nameKeyed.put(nameMapper.apply(n), old);
            }
        }
        return nameKeyed;
    }

    // Hide ROWSTAT column from metatada if it's the last column. Typically synthesized in cursored fetch.
    private boolean shouldStripROWSTAT(N[] columns) {
        return columns.length > 0 && "ROWSTAT".equals(this.nameMapper.apply(columns[columns.length - 1]));
    }

    /**
     * Lookup {@link Column} by index or by its name.
     *
     * @param identifier the index or name.
     * @return the identifier.
     * @throws IllegalArgumentException if the item cannot be retrieved.
     * @throws IllegalArgumentException when {@code identifier} is {@code null}.
     */
    N get(Object identifier) {

        Assert.requireNonNull(identifier, "Identifier must not be null");

        if (identifier instanceof Integer) {
            return get((int) identifier);
        }

        if (identifier instanceof String) {
            return get((String) identifier);
        }

        throw new IllegalArgumentException(String.format("Identifier [%s] is not a valid identifier. Should either be an Integer index or a String %s name.", identifier, this.itemName));
    }

    /**
     * Lookup item by its {@code index}.
     *
     * @param index the item index. Must be greater zero and less than {@link #getCount()}.
     * @return the item.
     */
    N get(int index) {

        if (this.items.length > index && index >= 0) {
            return this.items[index];
        }

        throw new IndexOutOfBoundsException(String.format("Index [%d] is larger than the number of %ss [%d]", index, this.itemName, this.items.length));
    }

    /**
     * Lookup item by its {@code name}.
     *
     * @param name the item name.
     * @return the item.
     */
    N get(String name) {

        N item = find(name);

        if (item == null) {
            throw new NoSuchElementException(String.format("[%s] does not exist in %s names %s", name, this.itemName, this.nameKeyed.keySet()));
        }

        return item;
    }

    /**
     * Lookup item by its {@code name}.
     *
     * @param name the item name.
     * @return the item.
     */
    @Nullable
    N find(String name) {

        N item = this.nameKeyed.get(name);

        if (item == null) {
            name = EscapeAwareNameMatcher.find(name, this.nameKeyed.keySet());
            if (name != null) {
                item = this.nameKeyed.get(name);
            }
        }

        return item;
    }

    /**
     * Returns the number of items.
     *
     * @return the number of itemss.
     */
    int getCount() {
        return this.items.length;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [").append(Arrays.stream(this.items).map(this.nameMapper).collect(Collectors.joining(", "))).append("]");
        return sb.toString();
    }

    @Override
    public int size() {
        return this.getCount();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {

        if (o instanceof String) {
            return this.find((String) o) != null;
        }

        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {

        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Iterator<String> iterator() {

        N[] items = this.items;

        return new Iterator<String>() {

            int index = 0;

            @Override
            public boolean hasNext() {
                return items.length > this.index;
            }

            @Override
            public String next() {
                N item = items[this.index++];
                return NamedCollectionSupport.this.nameMapper.apply(item);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {

        if (a.length < size()) {
            a = (T[]) Array.newInstance(a.getClass().getComponentType(), size());
        }

        for (int i = 0; i < size(); i++) {
            a[i] = (T) this.nameMapper.apply(this.get(i));
        }

        return a;
    }

    @Override
    public Object[] toArray() {

        Object[] result = new Object[size()];

        for (int i = 0; i < size(); i++) {
            result[i] = this.nameMapper.apply(this.get(i));
        }

        return result;
    }

    @Override
    public boolean add(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

}
