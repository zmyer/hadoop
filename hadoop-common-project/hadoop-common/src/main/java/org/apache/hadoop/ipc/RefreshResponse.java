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

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Return a response in the handler method for the user to see.
 * Useful since you may want to display status to a user even though an
 * error has not occurred.
 */
// TODO: 17/3/19 by zmyer
@InterfaceStability.Unstable
public class RefreshResponse {
    //返回码
    private int returnCode = -1;
    //消息对象
    private String message;
    //发送者名称
    private String senderName;

    /**
     * Convenience method to create a response for successful refreshes.
     *
     * @return void response
     */
    // TODO: 17/3/19 by zmyer
    public static RefreshResponse successResponse() {
        return new RefreshResponse(0, "Success");
    }

    // Most RefreshHandlers will use this
    // TODO: 17/3/19 by zmyer
    public RefreshResponse(int returnCode, String message) {
        this.returnCode = returnCode;
        this.message = message;
    }

    /**
     * Optionally set the sender of this RefreshResponse.
     * This helps clarify things when multiple handlers respond.
     *
     * @param name The name of the sender
     */
    // TODO: 17/3/19 by zmyer
    public void setSenderName(String name) {
        senderName = name;
    }

    // TODO: 17/3/19 by zmyer
    public String getSenderName() {
        return senderName;
    }

    // TODO: 17/3/19 by zmyer
    public int getReturnCode() {
        return returnCode;
    }

    // TODO: 17/3/19 by zmyer
    public void setReturnCode(int rc) {
        returnCode = rc;
    }

    // TODO: 17/3/19 by zmyer
    public void setMessage(String m) {
        message = m;
    }

    // TODO: 17/3/19 by zmyer
    public String getMessage() {
        return message;
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public String toString() {
        String ret = "";

        if (senderName != null) {
            ret += senderName + ": ";
        }

        if (message != null) {
            ret += message;
        }

        ret += " (exit " + returnCode + ")";
        return ret;
    }
}
