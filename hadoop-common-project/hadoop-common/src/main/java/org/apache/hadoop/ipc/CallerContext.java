/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A class defining the caller context for auditing coarse granularity
 * operations.
 *
 * This class is immutable.
 */
@InterfaceAudience.LimitedPrivate({
    "HBase", "HDFS", "Hive", "MapReduce",
    "Pig", "Spark", "YARN"})
@InterfaceStability.Evolving
// TODO: 17/3/18 by zmyer
public final class CallerContext {
    public static final Charset SIGNATURE_ENCODING = StandardCharsets.UTF_8;
    /**
     * The caller context.
     *
     * It will be truncated if it exceeds the maximum allowed length in
     * server. The default length limit is
     * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT}
     */
    //caller上下文对象
    private final String context;

    /**
     * The caller's signature for validation.
     *
     * The signature is optional. The null or empty signature will be abandoned.
     * If the signature exceeds the maximum allowed length in server, the caller
     * context will be abandoned. The default length limit is
     * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT}
     */
    private final byte[] signature;

    // TODO: 17/3/18 by zmyer
    private CallerContext(Builder builder) {
        this.context = builder.context;
        this.signature = builder.signature;
    }

    // TODO: 17/3/18 by zmyer
    public String getContext() {
        return context;
    }

    // TODO: 17/3/18 by zmyer
    public byte[] getSignature() {
        return signature == null ?
            null : Arrays.copyOf(signature, signature.length);
    }

    // TODO: 17/3/18 by zmyer
    @InterfaceAudience.Private
    public boolean isContextValid() {
        return context != null && !context.isEmpty();
    }

    // TODO: 17/3/18 by zmyer
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(context).toHashCode();
    }

    // TODO: 17/3/18 by zmyer
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } else if (obj == this) {
            return true;
        } else if (obj.getClass() != getClass()) {
            return false;
        } else {
            CallerContext rhs = (CallerContext) obj;
            return new EqualsBuilder()
                .append(context, rhs.context)
                .append(signature, rhs.signature)
                .isEquals();
        }
    }

    // TODO: 17/3/18 by zmyer
    @Override
    public String toString() {
        if (!isContextValid()) {
            return "";
        }
        String str = context;
        if (signature != null) {
            str += ":";
            str += new String(signature, SIGNATURE_ENCODING);
        }
        return str;
    }

    /** The caller context builder. */
    // TODO: 17/3/18 by zmyer
    public static final class Builder {
        private final String context;
        private byte[] signature;

        public Builder(String context) {
            this.context = context;
        }

        public Builder setSignature(byte[] signature) {
            if (signature != null && signature.length > 0) {
                this.signature = Arrays.copyOf(signature, signature.length);
            }
            return this;
        }

        public CallerContext build() {
            return new CallerContext(this);
        }
    }

    /**
     * The thread local current caller context.
     * <p/>
     * Internal class for defered singleton idiom.
     * https://en.wikipedia.org/wiki/Initialization_on_demand_holder_idiom
     */
    // TODO: 17/3/18 by zmyer
    private static final class CurrentCallerContextHolder {
        static final ThreadLocal<CallerContext> CALLER_CONTEXT =
            new InheritableThreadLocal<>();
    }

    // TODO: 17/3/18 by zmyer
    public static CallerContext getCurrent() {
        return CurrentCallerContextHolder.CALLER_CONTEXT.get();
    }

    // TODO: 17/3/18 by zmyer
    public static void setCurrent(CallerContext callerContext) {
        CurrentCallerContextHolder.CALLER_CONTEXT.set(callerContext);
    }
}
