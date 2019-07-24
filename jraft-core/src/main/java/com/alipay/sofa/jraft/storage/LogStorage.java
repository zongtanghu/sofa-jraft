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
package com.alipay.sofa.jraft.storage;

import java.util.List;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.option.LogStorageOptions;

/**
 * LogStorage仅是一个Jraft库的日志存储接口,其默认实现基于RocksDB存储
 * 
 * Log entry storage service.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:43:54 PM
 */
public interface LogStorage extends Lifecycle<LogStorageOptions>, Storage {

    /**
     * Returns first log index in log.
     */
    long getFirstLogIndex();

    /**
     * Returns last log index in log.
     */
    long getLastLogIndex();

    /**
     * Get logEntry by index.
     * 按照日志索引获取 Log Entry 及其任期
     * 
     */
    LogEntry getEntry(long index);

    /**
     * Get logEntry's term by index. This method is deprecated, you should use {@link #getEntry(long)} to get the log id's term.
     * @deprecated
     */
    @Deprecated
    long getTerm(long index);

    /**
     * Append entries to log.
     * 把单个 Log Entry 添加到日志存储
     *
     */
    boolean appendEntry(LogEntry entry);

    /**
     * Append entries to log, return append success number.
     * 把批量 Log Entry 添加到日志存储
     *
     */
    int appendEntries(List<LogEntry> entries);

    /**
     * Delete logs from storage's head, [first_log_index, first_index_kept) will
     * be discarded.
     * 从 Log 存储头部删除日志
     *
     */
    boolean truncatePrefix(long firstIndexKept);

    /**
     * Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index]
     * will be discarded.
     * 从 Log 存储末尾删除日志
     *
     */
    boolean truncateSuffix(long lastIndexKept);

    /**
     * Drop all the existing logs and reset next log index to |next_log_index|.
     * This function is called after installing snapshot from leader.
     * 删除所有现有日志，重置下任日志索引
     *
     */
    boolean reset(long nextLogIndex);
}
