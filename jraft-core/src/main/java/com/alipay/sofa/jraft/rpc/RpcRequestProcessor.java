/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rpc;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;

/**
 * Abstract AsyncUserProcessor for RPC processors.
 *
 * @param <T> Message
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public abstract class RpcRequestProcessor<T extends Message> implements RpcProcessor<T> {

    protected static final Logger LOG = LoggerFactory.getLogger(RpcRequestProcessor.class);

    private final Executor        executor;

    public abstract Message processRequest(final T request, final RpcRequestClosure done);

    public RpcRequestProcessor(Executor executor) {
        super();
        this.executor = executor;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final T request) {
        try {
            final Message msg = processRequest(request, new RpcRequestClosure(rpcCtx));
            if (msg != null) {
                rpcCtx.sendResponse(msg);
            }
        } catch (final Throwable t) {
            LOG.error("handleRequest {} failed", request, t);
            rpcCtx.sendResponse(RpcResponseFactory.newResponse(-1, "handleRequest internal error"));
        }
    }

    @Override
    public Executor executor() {
        return this.executor;
    }
}
