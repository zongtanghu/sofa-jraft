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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.alipay.sofa.jraft.option.LogManagerOptions;

/**
 * Log manager.
 *
 * LogManager负责调用底层日志存LogStorage，针对日志存储调用进行缓存、
 * 批量提交、必要的检查和优化
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 3:02:42 PM
 */
public interface LogManager extends Lifecycle<LogManagerOptions> {

    /**
     * Closure to to run in stable state.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 4:35:29 PM
     */
    abstract class StableClosure implements Closure {

        protected long           firstLogIndex = 0;
        protected List<LogEntry> entries;
        protected int            nEntries;

        public StableClosure() {
            // NO-OP
        }

        public long getFirstLogIndex() {
            return this.firstLogIndex;
        }

        public void setFirstLogIndex(long firstLogIndex) {
            this.firstLogIndex = firstLogIndex;
        }

        public List<LogEntry> getEntries() {
            return this.entries;
        }

        public void setEntries(List<LogEntry> entries) {
            this.entries = entries;
            if (entries != null) {
                this.nEntries = entries.size();
            } else {
                this.nEntries = 0;
            }
        }

        public StableClosure(List<LogEntry> entries) {
            super();
            setEntries(entries);
        }

    }

    /**
     * Listen on last log index change event, but it's not reliable,
     * the user should not count on this listener to receive all changed events.
     *
     * @author dennis
     */
    interface LastLogIndexListener {

        /**
         * Called when last log index is changed.
         *
         * @param lastLogIndex last log index
         */
        void onLastLogIndexChanged(long lastLogIndex);
    }

    /**
     * Adds a last log index listener
     */
    void addLastLogIndexListener(LastLogIndexListener listener);

    /**
     * Remove the last log index listener.
     */
    void removeLastLogIndexListener(LastLogIndexListener listener);

    /**
     * Wait the log manager to be shut down.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;

    /**
     * Append log entry vector and wait until it's stable (NOT COMMITTED!)
     *
     * @param entries log entries
     * @param done    callback
     */
    void appendEntries(List<LogEntry> entries, StableClosure done);

    /**
     * Notify the log manager about the latest snapshot, which indicates the
     * logs which can be safely truncated.
     *
     * @param meta snapshot metadata
     */
    void setSnapshot(SnapshotMeta meta);

    /**
     * We don't delete all the logs before last snapshot to avoid installing
     * snapshot on slow replica. Call this method to drop all the logs before
     * last snapshot immediately.
     */
    void clearBufferedLogs();

    /**
     * Get the log entry at index.
     *
     * @param index the index of log entry
     * @return the log entry with {@code index}
     */
    LogEntry getEntry(long index);

    /**
     * Get the log term at index.
     *
     * @param index the index of log entry
     * @return the term of log entry
     */
    long getTerm(long index);

    /**
     * Get the first log index of log
     */
    long getFirstLogIndex();

    /**
     * Get the last log index of log
     */
    long getLastLogIndex();

    /**
     * Get the last log index of log
     *
     * @param isFlush whether to flush from disk.
     */
    long getLastLogIndex(boolean isFlush);

    /**
     * Return the id the last log.
     *
     * @param isFlush whether to flush all pending task.
     */
    LogId getLastLogId(boolean isFlush);

    /**
     * Get the configuration at index.
     */
    ConfigurationEntry getConfiguration(long index);

    /**
     * Check if |current| should be updated to the latest configuration
     * Returns the latest configuration, otherwise null.
     */
    ConfigurationEntry checkAndSetConfiguration(ConfigurationEntry current);

    /**
     * New log notifier callback.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 4:40:04 PM
     */
    interface onNewLogCallback {

        /**
         * Called while new log come in.
         *
         * @param arg       the waiter pass-in argument
         * @param errorCode error code
         */
        boolean onNewLog(Object arg, int errorCode);
    }

    /**
     * Wait until there are more logs since |last_log_index| and |on_new_log|
     * would be called after there are new logs or error occurs, return the waiter id.
     * 
     * @param expectedLastLogIndex  expected last index of log
     * @param cb                    callback
     * @param arg                   the waiter pass-in argument
     */
    long wait(long expectedLastLogIndex, onNewLogCallback cb, Object arg);

    /**
     * Remove a waiter.
     *
     * @param id waiter id
     * @return true on success
     */
    boolean removeWaiter(long id);

    /**
     * Set the applied id, indicating that the log before applied_id (included)
     * can be dropped from memory logs.
     */
    void setAppliedId(LogId appliedId);

    /**
     * Check log consistency, returns the status
     * @return status
     */
    Status checkConsistency();

}