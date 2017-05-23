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

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A class defining a set of static helper methods to provide conversion between
 * bytes and string for UUID-based client Id.
 */
// TODO: 17/3/19 by zmyer
@InterfaceAudience.Private
public class ClientId {

    /** The byte array of a UUID should be 16 */
    public static final int BYTE_LENGTH = 16;
    private static final int shiftWidth = 8;

    /**
     * Return clientId as byte[]
     */
    // TODO: 17/3/19 by zmyer
    public static byte[] getClientId() {
        //生成随机的UUID
        UUID uuid = UUID.randomUUID();
        //创建clientid缓冲区
        ByteBuffer buf = ByteBuffer.wrap(new byte[BYTE_LENGTH]);
        //保存uuid
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    /** Convert a clientId byte[] to string */
    // TODO: 17/3/19 by zmyer
    public static String toString(byte[] clientId) {
        // clientId can be null or an empty array
        if (clientId == null || clientId.length == 0) {
            return "";
        }
        // otherwise should be 16 bytes
        Preconditions.checkArgument(clientId.length == BYTE_LENGTH);
        long msb = getMsb(clientId);
        long lsb = getLsb(clientId);
        return (new UUID(msb, lsb)).toString();
    }

    // TODO: 17/3/19 by zmyer
    public static long getMsb(byte[] clientId) {
        long msb = 0;
        for (int i = 0; i < BYTE_LENGTH / 2; i++) {
            msb = (msb << shiftWidth) | (clientId[i] & 0xff);
        }
        return msb;
    }

    // TODO: 17/3/19 by zmyer
    public static long getLsb(byte[] clientId) {
        long lsb = 0;
        for (int i = BYTE_LENGTH / 2; i < BYTE_LENGTH; i++) {
            lsb = (lsb << shiftWidth) | (clientId[i] & 0xff);
        }
        return lsb;
    }

    /** Convert from clientId string byte[] representation of clientId */
    // TODO: 17/3/19 by zmyer
    public static byte[] toBytes(String id) {
        if (id == null || "".equals(id)) {
            return new byte[0];
        }
        UUID uuid = UUID.fromString(id);
        ByteBuffer buf = ByteBuffer.wrap(new byte[BYTE_LENGTH]);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

}
