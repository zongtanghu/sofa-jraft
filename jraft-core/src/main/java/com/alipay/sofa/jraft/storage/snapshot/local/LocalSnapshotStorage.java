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
package com.alipay.sofa.jraft.storage.snapshot.local;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.Snapshot;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Snapshot storage based on local file storage.
 * 基于本地文件存储 Raft 状态机镜像，初始化元快照存储 StorageFactory
 * 根据 Raft 镜像快照存储路径和 Raft 配置信息默认创建 LocalSnapshotStorage 快照存储。
 *
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-13 2:11:30 PM
 */
public class LocalSnapshotStorage implements SnapshotStorage {

    private static final Logger                      LOG       = LoggerFactory.getLogger(LocalSnapshotStorage.class);

    private static final String                      TEMP_PATH = "temp";
    private final ConcurrentMap<Long, AtomicInteger> refMap    = new ConcurrentHashMap<>();
    private final String                             path;
    private Endpoint                                 addr;
    private boolean                                  filterBeforeCopyRemote;
    private long                                     lastSnapshotIndex;
    private final Lock                               lock;
    private final RaftOptions                        raftOptions;
    private SnapshotThrottle                         snapshotThrottle;

    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    public boolean hasServerAddr() {
        return this.addr != null;
    }

    public void setServerAddr(Endpoint addr) {
        this.addr = addr;
    }

    public LocalSnapshotStorage(String path, RaftOptions raftOptions) {
        super();
        this.path = path;
        this.lastSnapshotIndex = 0;
        this.raftOptions = raftOptions;
        this.lock = new ReentrantLock();
    }

    public long getLastSnapshotIndex() {
        this.lock.lock();
        try {
            return this.lastSnapshotIndex;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 删除文件命名为 temp 的临时镜像 Snapshot，
     * 销毁文件前缀为 snapshot_ 的旧快照 Snapshot，
     * 获取快照最后一个索引 lastSnapshotIndex。
     *
     * @param v
     * @return
     */
    @Override
    public boolean init(Void v) {
        final File dir = new File(this.path);

        try {
            FileUtils.forceMkdir(dir);
        } catch (final IOException e) {
            LOG.error("Fail to create directory {}", this.path);
            return false;
        }

        // delete temp snapshot
        if (!filterBeforeCopyRemote) {
            final String tempSnapshotPath = this.path + File.separator + TEMP_PATH;
            final File tempFile = new File(tempSnapshotPath);
            if (tempFile.exists()) {
                try {
                    FileUtils.forceDelete(tempFile);
                } catch (final IOException e) {
                    LOG.error("Fail to delete temp snapshot path {}", tempSnapshotPath);
                    return false;
                }
            }
        }
        // delete old snapshot
        final List<Long> snapshots = new ArrayList<>();
        final File[] oldFiles = dir.listFiles();
        if (oldFiles != null) {
            for (final File sFile : oldFiles) {
                final String name = sFile.getName();
                if (!name.startsWith(Snapshot.JRAFT_SNAPSHOT_PREFIX)) {
                    continue;
                }
                final long index = Long.parseLong(name.substring(Snapshot.JRAFT_SNAPSHOT_PREFIX.length()));
                snapshots.add(index);
            }
        }

        // TODO: add snapshot watcher

        // get last_snapshot_index
        if (!snapshots.isEmpty()) {
            Collections.sort(snapshots);
            final int snapshotCount = snapshots.size();

            for (int i = 0; i < snapshotCount - 1; i++) {
                final long index = snapshots.get(i);
                final String snapshotPath = getSnapshotPath(index);
                if (!destroySnapshot(snapshotPath)) {
                    return false;
                }
            }
            this.lastSnapshotIndex = snapshots.get(snapshotCount - 1);
            ref(this.lastSnapshotIndex);
        }

        return true;
    }

    private String getSnapshotPath(long index) {
        return this.path + File.separator + Snapshot.JRAFT_SNAPSHOT_PREFIX + index;
    }

    void ref(long index) {
        final AtomicInteger refs = getRefs(index);
        refs.incrementAndGet();
    }

    private boolean destroySnapshot(String path) {
        LOG.info("Deleting snapshot {}", path);
        final File file = new File(path);
        try {
            FileUtils.deleteDirectory(file);
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to destroy snapshot {}", path);
            return false;
        }
    }

    void unref(long index) {
        final AtomicInteger refs = getRefs(index);
        if (refs.decrementAndGet() == 0) {
            if (this.refMap.remove(index, refs)) {
                destroySnapshot(getSnapshotPath(index));
            }
        }
    }

    AtomicInteger getRefs(long index) {
        AtomicInteger refs = this.refMap.get(index);
        if (refs == null) {
            refs = new AtomicInteger(0);
            final AtomicInteger eRefs = this.refMap.putIfAbsent(index, refs);
            if (eRefs != null) {
                refs = eRefs;
            }
        }
        return refs;
    }

    /**
     * 按照快照最后一个索引 lastSnapshotIndex 和镜像编写器 LocalSnapshotWriter
     * 快照索引重命名临时镜像 Snapshot 文件，销毁编写器 LocalSnapshotWriter
     * 存储路径快照。
     *
     *
     * @param writer
     * @param keepDataOnError
     * @throws IOException
     */
    void close(LocalSnapshotWriter writer, boolean keepDataOnError) throws IOException {
        int ret = writer.getCode();
        // noinspection ConstantConditions
        do {
            if (ret != 0) {
                break;
            }
            try {
                if (!writer.sync()) {
                    ret = RaftError.EIO.getNumber();
                    break;
                }
            } catch (final IOException e) {
                LOG.error("Fail to sync writer {}", writer.getPath());
                ret = RaftError.EIO.getNumber();
                break;
            }
            final long oldIndex = this.getLastSnapshotIndex();
            final long newIndex = writer.getSnapshotIndex();
            if (oldIndex == newIndex) {
                ret = RaftError.EEXISTS.getNumber();
                break;
            }
            // rename temp to new
            final String tempPath = this.path + File.separator + TEMP_PATH;
            final String newPath = getSnapshotPath(newIndex);

            if (!destroySnapshot(newPath)) {
                LOG.warn("Delete new snapshot path failed, path is {}", newPath);
                ret = RaftError.EIO.getNumber();
                break;
            }
            LOG.info("Renaming {} to {}", tempPath, newPath);
            if (!new File(tempPath).renameTo(new File(newPath))) {
                LOG.error("Renamed temp snapshot failed, from path {} to path {}", tempPath, newPath);
                ret = RaftError.EIO.getNumber();
                break;
            }
            ref(newIndex);
            this.lock.lock();
            try {
                Requires.requireTrue(oldIndex == this.lastSnapshotIndex);
                this.lastSnapshotIndex = newIndex;
            } finally {
                lock.unlock();
            }
            unref(oldIndex);
        } while (false);
        if (ret != 0 && !keepDataOnError) {
            destroySnapshot(writer.getPath());
        }
        if (ret == RaftError.EIO.getNumber()) {
            throw new IOException();
        }
    }

    @Override
    public void shutdown() {
        //ignore
    }

    @Override
    public boolean setFilterBeforeCopyRemote() {
        this.filterBeforeCopyRemote = true;
        return true;
    }

    /**
     * 销毁文件命名为 temp 的临时快照 Snapshot，
     * 基于临时镜像存储路径创建初始化快照编写器 LocalSnapshotWriter，
     * 加载文件命名为 _raftsnapshot_meta 的 Raft 快照元数据至内存。
     *
     * @return
     */
    @Override
    public SnapshotWriter create() {
        return this.create(true);
    }

    public SnapshotWriter create(boolean fromEmpty) {
        LocalSnapshotWriter writer = null;
        // noinspection ConstantConditions
        do {
            final String snapshotPath = this.path + File.separator + TEMP_PATH;
            // delete temp
            // TODO: Notify watcher before deleting
            if (new File(snapshotPath).exists() && fromEmpty) {
                if (!destroySnapshot(snapshotPath)) {
                    break;
                }
            }
            writer = new LocalSnapshotWriter(snapshotPath, this, raftOptions);
            if (!writer.init(null)) {
                LOG.error("Fail to init snapshot writer");
                writer = null;
                break;
            }
        } while (false);
        return writer;
    }

    /**
     * 根据快照最后一个索引 lastSnapshotIndex 获取文件前缀为 snapshot_ 快照存储路径，
     * 基于快照存储路径创建初始化快照阅读器 LocalSnapshotReader，加载文件命名为
     * _raftsnapshot_meta 的 Raft 镜像元数据至内存。
     *
     * @return
     */
    @Override
    public SnapshotReader open() {
        long lsIndex = 0;
        lock.lock();
        try {
            if (this.lastSnapshotIndex != 0) {
                lsIndex = this.lastSnapshotIndex;
                ref(lsIndex);
            }
        } finally {
            lock.unlock();
        }
        if (lsIndex == 0) {
            LOG.warn("No data for snapshot reader {}", this.path);
            return null;
        }
        final String snapshotPath = getSnapshotPath(lsIndex);
        final SnapshotReader reader = new LocalSnapshotReader(this, this.snapshotThrottle, this.addr, raftOptions,
            snapshotPath);
        if (!reader.init(null)) {
            LOG.error("Fail to init reader for path {}", snapshotPath);
            unref(lsIndex);
            return null;
        }
        return reader;
    }

    /**
     * 创建初始化状态机快照复制器 LocalSnapshotCopier，
     * 生成远程文件复制器 RemoteFileCopier，基于远程服务地址 Endpoint 获取
     * Raft 客户端 RPC 服务连接指定 Uri，启动后台线程复制 Snapshot 镜像数据，
     * 加载 Raft 快照元数据获取远程快照 Snapshot 镜像文件，
     * 读取远程指定快照存储路径数据拷贝到 BoltSession，
     * 快照复制器 LocalSnapshotCopier 同步 Raft 快照元数据。
     * 
     * @param uri   remote uri
     * @param opts  copy options
     * @return
     */
    @Override
    public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
        final SnapshotCopier copier = this.startToCopyFrom(uri, opts);
        if (copier == null) {
            return null;
        }
        try {
            copier.join();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Join on snapshot copier was interrupted");
            return null;
        }
        final SnapshotReader reader = copier.getReader();
        Utils.closeQuietly(copier);
        return reader;
    }

    @Override
    public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
        final LocalSnapshotCopier copier = new LocalSnapshotCopier();
        copier.setStorage(this);
        copier.setSnapshotThrottle(this.snapshotThrottle);
        copier.setFilterBeforeCopyRemote(this.filterBeforeCopyRemote);
        if (!copier.init(uri, opts)) {
            LOG.error("Fail to init copier to {}", uri);
            return null;
        }
        copier.start();
        return copier;
    }

}
