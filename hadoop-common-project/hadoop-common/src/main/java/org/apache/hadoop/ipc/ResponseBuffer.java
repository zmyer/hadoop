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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
/** generates byte-length framed buffers. */
// TODO: 17/3/14 by zmyer
public class ResponseBuffer extends DataOutputStream {

    // TODO: 17/3/19 by zmyer
    public ResponseBuffer() {
        this(1024);
    }

    // TODO: 17/3/19 by zmyer
    public ResponseBuffer(int capacity) {
        super(new FramedBuffer(capacity));
    }

    // update framing bytes based on bytes written to stream.
    // TODO: 17/3/19 by zmyer
    private FramedBuffer getFramedBuffer() {
        //创建帧缓存对象
        FramedBuffer buf = (FramedBuffer) out;
        //设置帧缓存大小
        buf.setSize(written);
        //返回帧缓存对象
        return buf;
    }

    // TODO: 17/3/19 by zmyer
    public void writeTo(OutputStream out) throws IOException {
        //开始向帧缓存写入数据
        getFramedBuffer().writeTo(out);
    }

    // TODO: 17/3/19 by zmyer
    byte[] toByteArray() {
        return getFramedBuffer().toByteArray();
    }

    // TODO: 17/3/19 by zmyer
    int capacity() {
        return ((FramedBuffer) out).capacity();
    }

    // TODO: 17/3/19 by zmyer
    void setCapacity(int capacity) {
        ((FramedBuffer) out).setCapacity(capacity);
    }

    // TODO: 17/3/19 by zmyer
    void ensureCapacity(int capacity) {
        if (((FramedBuffer) out).capacity() < capacity) {
            ((FramedBuffer) out).setCapacity(capacity);
        }
    }

    // TODO: 17/3/19 by zmyer
    ResponseBuffer reset() {
        written = 0;
        ((FramedBuffer) out).reset();
        return this;
    }

    // TODO: 17/3/19 by zmyer
    private static class FramedBuffer extends ByteArrayOutputStream {
        private static final int FRAMING_BYTES = 4;

        // TODO: 17/3/19 by zmyer
        FramedBuffer(int capacity) {
            super(capacity + FRAMING_BYTES);
            reset();
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public int size() {
            return count - FRAMING_BYTES;
        }

        // TODO: 17/3/19 by zmyer
        void setSize(int size) {
            buf[0] = (byte) ((size >>> 24) & 0xFF);
            buf[1] = (byte) ((size >>> 16) & 0xFF);
            buf[2] = (byte) ((size >>> 8) & 0xFF);
            buf[3] = (byte) ((size) & 0xFF);
        }

        // TODO: 17/3/19 by zmyer
        int capacity() {
            return buf.length - FRAMING_BYTES;
        }

        // TODO: 17/3/19 by zmyer
        void setCapacity(int capacity) {
            buf = Arrays.copyOf(buf, capacity + FRAMING_BYTES);
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public void reset() {
            count = FRAMING_BYTES;
            setSize(0);
        }
    }

    ;
}