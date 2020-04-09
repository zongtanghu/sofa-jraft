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
package com.alipay.sofa.jraft.rpc.impl;

import java.util.Map;
import java.util.concurrent.Executor;

import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.RejectedExecutionPolicy;
import com.alipay.remoting.Url;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.rpc.RpcAddressParser;
import com.alipay.sofa.jraft.ReplicatorGroup;
import com.alipay.sofa.jraft.error.InvokeTimeoutException;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.core.ClientServiceConnectionEventProcessor;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.SystemPropertyUtil;

/**
 * Bolt rpc client impl.
 *
 * @author jiachun.fjc
 */
public class BoltRpcClient implements RpcClient {

    public static final String                      BOLT_ADDRESS_PARSER               = "BOLT_ADDRESS_PARSER";
    public static final String                      BOLT_CTX                          = "BOLT_CTX";
    public static final String                      BOLT_REJECTED_EXECUTION_POLICY    = "BOLT_REJECTED_EXECUTION_POLICY";

    private static final int                        CHANNEL_WRITE_BUF_LOW_WATER_MARK  = SystemPropertyUtil
                                                                                          .getInt(
                                                                                              "bolt.channel_write_buf_low_water_mark",
                                                                                              256 * 1024);
    private static final int                        CHANNEL_WRITE_BUF_HIGH_WATER_MARK = SystemPropertyUtil
                                                                                          .getInt(
                                                                                              "bolt.channel_write_buf_high_water_mark",
                                                                                              512 * 1024);

    private final com.alipay.remoting.rpc.RpcClient rpcClient;
    private com.alipay.remoting.InvokeContext       defaultInvokeCtx;
    private RpcAddressParser                        defaultAddressParser              = new RpcAddressParser();

    public BoltRpcClient(com.alipay.remoting.rpc.RpcClient rpcClient) {
        this.rpcClient = Requires.requireNonNull(rpcClient, "rpcClient");
    }

    @Override
    public boolean init(final RpcOptions opts) {
        this.rpcClient.switches().turnOn(GlobalSwitch.CODEC_FLUSH_CONSOLIDATION);
        this.rpcClient.initWriteBufferWaterMark(CHANNEL_WRITE_BUF_LOW_WATER_MARK, CHANNEL_WRITE_BUF_HIGH_WATER_MARK);
        this.rpcClient.startup();
        return true;
    }

    @Override
    public void shutdown() {
        this.rpcClient.shutdown();
    }

    @Override
    public boolean checkConnection(final Endpoint endpoint) {
        Requires.requireNonNull(endpoint, "endpoint");
        return this.rpcClient.checkConnection(endpoint.toString());
    }

    @Override
    public void closeConnection(final Endpoint endpoint) {
        Requires.requireNonNull(endpoint, "endpoint");
        this.rpcClient.closeConnection(endpoint.toString());
    }

    @Override
    public void registerConnectEventListener(final ReplicatorGroup replicatorGroup) {
        this.rpcClient.addConnectionEventProcessor(ConnectionEventType.CONNECT,
            new ClientServiceConnectionEventProcessor(replicatorGroup));
    }

    @Override
    public Object invokeSync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                             final long timeoutMs) throws InterruptedException, RemotingException {
        Requires.requireNonNull(endpoint, "endpoint");
        final RpcAddressParser addressParser = getAddressParser(ctx);
        try {
            final Url url = addressParser.parse(endpoint.toString());
            return this.rpcClient.invokeSync(url, request, getBoltInvokeCtx(ctx), (int) timeoutMs);
        } catch (final com.alipay.remoting.rpc.exception.InvokeTimeoutException e) {
            throw new InvokeTimeoutException(e);
        } catch (final com.alipay.remoting.exception.RemotingException e) {
            throw new RemotingException(e);
        }
    }

    @Override
    public void invokeAsync(final Endpoint endpoint, final Object request, final InvokeContext ctx,
                            final InvokeCallback callback, final long timeoutMs) throws InterruptedException,
                                                                                RemotingException {
        Requires.requireNonNull(endpoint, "endpoint");
        final RpcAddressParser addressParser = getAddressParser(ctx);
        try {
            final Url url = addressParser.parse(endpoint.toString());
            this.rpcClient.invokeWithCallback(url, request, getBoltInvokeCtx(ctx), getBoltCallback(callback, ctx),
                (int) timeoutMs);
        } catch (final com.alipay.remoting.rpc.exception.InvokeTimeoutException e) {
            throw new InvokeTimeoutException(e);
        } catch (final com.alipay.remoting.exception.RemotingException e) {
            throw new RemotingException(e);
        }
    }

    public com.alipay.remoting.rpc.RpcClient getRpcClient() {
        return rpcClient;
    }

    public com.alipay.remoting.InvokeContext getDefaultInvokeCtx() {
        return defaultInvokeCtx;
    }

    public void setDefaultInvokeCtx(com.alipay.remoting.InvokeContext defaultInvokeCtx) {
        this.defaultInvokeCtx = defaultInvokeCtx;
    }

    public RpcAddressParser getDefaultAddressParser() {
        return defaultAddressParser;
    }

    public void setDefaultAddressParser(RpcAddressParser defaultAddressParser) {
        this.defaultAddressParser = defaultAddressParser;
    }

    private RpcAddressParser getAddressParser(final InvokeContext ctx) {
        return ctx == null ? this.defaultAddressParser : ctx.getOrDefault(BOLT_ADDRESS_PARSER,
            this.defaultAddressParser);
    }

    private RejectedExecutionPolicy getRejectedPolicy(final InvokeContext ctx) {
        return ctx == null ? RejectedExecutionPolicy.CALLER_HANDLE_EXCEPTION : ctx.getOrDefault(
            BOLT_REJECTED_EXECUTION_POLICY, RejectedExecutionPolicy.CALLER_HANDLE_EXCEPTION);
    }

    private com.alipay.remoting.InvokeContext getBoltInvokeCtx(final InvokeContext ctx) {
        if (ctx == null) {
            return this.defaultInvokeCtx;
        }

        com.alipay.remoting.InvokeContext boltCtx = ctx.get(BOLT_CTX);
        if (boltCtx != null) {
            return boltCtx;
        }

        boltCtx = new com.alipay.remoting.InvokeContext();
        for (Map.Entry<String, Object> entry : ctx.entrySet()) {
            boltCtx.put(entry.getKey(), entry.getValue());
        }
        final Boolean crcSwitch = ctx.get(InvokeContext.CRC_SWITCH);
        if (crcSwitch != null) {
            boltCtx.put(com.alipay.remoting.InvokeContext.BOLT_CRC_SWITCH, crcSwitch);
        }
        return boltCtx;
    }

    private BoltCallback getBoltCallback(final InvokeCallback callback, final InvokeContext ctx) {
        Requires.requireNonNull(callback, "callback");
        return new BoltCallback(callback, getRejectedPolicy(ctx));
    }

    private static class BoltCallback implements com.alipay.remoting.RejectionProcessableInvokeCallback {

        private final InvokeCallback          callback;
        private final RejectedExecutionPolicy rejectedPolicy;

        private BoltCallback(final InvokeCallback callback, final RejectedExecutionPolicy rejectedPolicy) {
            this.callback = callback;
            this.rejectedPolicy = rejectedPolicy;
        }

        @Override
        public void onResponse(final Object result) {
            this.callback.complete(result, null);
        }

        @Override
        public void onException(final Throwable err) {
            this.callback.complete(null, err);
        }

        @Override
        public Executor getExecutor() {
            return this.callback.executor();
        }

        @Override
        public RejectedExecutionPolicy rejectedExecutionPolicy() {
            return this.rejectedPolicy;
        }
    }
}
