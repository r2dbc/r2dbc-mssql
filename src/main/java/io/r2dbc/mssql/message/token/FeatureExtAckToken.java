/*
 * Copyright 2018 the original author or authors.
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

package io.r2dbc.mssql.message.token;

import io.netty.buffer.ByteBuf;
import io.r2dbc.mssql.message.tds.Decode;
import io.r2dbc.mssql.message.tds.ProtocolException;
import io.r2dbc.mssql.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Feature Extension Acknowledgement token.
 *
 * @author Mark Paluch
 */
public class FeatureExtAckToken extends AbstractDataToken {

    public static final byte TYPE = (byte) 0xAE;

    public static final byte TERMINATOR = (byte) 0xFF;

    private final List<FeatureToken> featureTokens;

    private FeatureExtAckToken(List<FeatureToken> featureTokens) {
        super(TYPE);
        this.featureTokens = featureTokens;
    }

    /**
     * Decode the {@link FeatureExtAckToken}.
     *
     * @param buffer the data buffer.
     * @return
     */
    public static FeatureExtAckToken decode(ByteBuf buffer) {

        Assert.requireNonNull(buffer, "Buffer must not be null");

        List<FeatureToken> featureTokens = new ArrayList<>();

        while (true) {

            byte featureId = buffer.readByte();

            if (featureId == TERMINATOR) {
                break;
            }

            if (featureId == ColumnEncryption.FEATURE_ID) {
                featureTokens.add(ColumnEncryption.decode(buffer));
                continue;
            }

            featureTokens.add(UnknownFeature.decode(featureId, buffer));
        }

        return new FeatureExtAckToken(featureTokens);
    }

    public List<FeatureToken> getFeatureTokens() {
        return this.featureTokens;
    }

    @Override
    public String getName() {
        return "FEATUREEXTACK";
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append(getClass().getSimpleName());
        sb.append(" [featureTokens=").append(this.featureTokens);
        sb.append(']');
        return sb.toString();
    }

    /**
     * Acknowledged feature token.
     */
    public abstract static class FeatureToken {

        /**
         * The unique identifier number of a feature.
         */
        private final byte featureId;

        /**
         * The length of FeatureAckData, in bytes.
         */
        private final long length;

        public FeatureToken(byte featureId, long length) {
            this.featureId = featureId;
            this.length = length;
        }
    }

    /**
     * Column encryption.
     */
    public final static class ColumnEncryption extends FeatureToken {

        public static final byte FEATURE_ID = 0x04;

        /**
         * Supported table column encryption version.
         */
        private final byte tceVersion;

        public ColumnEncryption(long length, byte tceVersion) {

            super(FEATURE_ID, length);
            this.tceVersion = tceVersion;
        }

        /**
         * Decode an unknown feature.
         *
         * @param buffer the data buffer.
         * @return the decoded {@link ColumnEncryption}.
         */
        public static ColumnEncryption decode(ByteBuf buffer) {

            Assert.requireNonNull(buffer, "Buffer must not be null");

            long length = Decode.dword(buffer);

            if (length != 1) {
                throw ProtocolException.unsupported("Unknown version number for AE");
            }

            byte tceVersion = buffer.readByte();

            if (tceVersion != 1) {
                throw ProtocolException.unsupported("Unsupported version number for AE");
            }

            return new ColumnEncryption(length, tceVersion);
        }

        public byte getTceVersion() {
            return this.tceVersion;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName());
            sb.append(" [tceVersion=").append(this.tceVersion);
            sb.append(']');
            return sb.toString();
        }
    }

    /**
     * Placeholder for unknown features.
     */
    public final static class UnknownFeature extends FeatureToken {

        private final byte[] data;

        public UnknownFeature(byte featureId, long length, byte[] data) {
            super(featureId, length);
            this.data = data;
        }

        /**
         * Decode an unknown feature.
         *
         * @param featureId the passed-through feature Id.
         * @param buffer    the data buffer.
         * @return
         */
        public static UnknownFeature decode(byte featureId, ByteBuf buffer) {

            Assert.requireNonNull(buffer, "Buffer must not be null");

            long length = Decode.dword(buffer);
            byte[] bytes = new byte[Math.toIntExact(length)];

            buffer.readBytes(bytes);

            return new UnknownFeature(featureId, length, bytes);
        }
    }
}
