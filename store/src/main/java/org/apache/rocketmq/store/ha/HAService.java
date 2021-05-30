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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * 主从同步核心实现类
 *
 * RocketMQ HA机制大体可以分为
 * 1）Master 启动并监听 Slave 的连接请求。
 * 2）Slave 启动，与 Master 建立链接。
 * 3）Slave 发送待拉取偏移量待 Master 返回数据，持续该过程。
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * Master维护的连接数。（Slave的个数）
     */
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    /**
     * 具体连接信息
     */
    private final List<HAConnection> connectionList = new LinkedList<>();

    /**
     * 服务端接收连接线程实现类
     */
    private final AcceptSocketService acceptSocketService;

    /**
     * Broker存储实现
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 同步等待实现
     */
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    /**
     * 该Master所有Slave中同步最大的偏移量
     */
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);
    /**
     * 判断主从同步复制是否完成
     */
    private final GroupTransferService groupTransferService;
    /**
     * HA客户端实现，Slave端网络的实现类
     */
    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 该方法是在Master收到从服务器的拉取请求，拉取请求是slave下一次待拉取的消息偏移量，
     * 也可以认为是Slave的拉取偏移量确认信息，如果该信息大于push2SlaveMaxOffset，则更新push2SlaveMaxOffset，
     * 然后唤醒GroupTransferService线程，各消息发送者线程再判断push2SlaveMaxOffset与期望的偏移量进行对比
     * @param offset
     */
    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        // 建立HA服务端监听服务，处理客户Slave客户端监听请求
        this.acceptSocketService.beginAccept();
        // 启动AcceptSocketService，处理监听逻辑
        this.acceptSocketService.start();
        // 启动GroupTransferService线程
        this.groupTransferService.start();
        // 启动HA客户端线程
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Master端监听Slave连接的实现类
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {
        /**
         * Broker服务监听套接字(本地IP+端口号)
         */
        private final SocketAddress socketAddressListen;
        // 服务端Socket通道，基于NIO
        private ServerSocketChannel serverSocketChannel;
        // 事件选择器，基于NIO
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * 创建ServerSocketChannel、创建Selector、设置TCP reuseAddress、绑定监听端口、设置为非阻塞模式，并注册OP_ACCEPT(连接事件)
         * Starts listening to slave connections.
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * 该方法是标准的基于NIO的服务端程式实例，选择器每1s处理一次连接就绪事件。连接事件就绪后，
         * 调用ServerSocketChannel的accept()方法创建SocketChannel，与服务端数据传输的通道。
         * 然后为每一个连接创建一个HAConnection对象，该HAConnection将负责M-S数据同步逻辑
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * 同步主从同步阻塞实现
     * 如果是同步主从模式，消息发送者将消息刷写到磁盘后，需要继续等待新数据被传输到从服务器，
     * 从服务器数据的复制是在另外一个线程HAConnection中去拉取，所以消息发送者在这里需要等待数据传输的结果，GroupTransferService就是实现该功能
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            this.wakeup();
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 对 requestsRead 加锁，顺序处理消息发送者线程提交的【主从同步负责是否成功结束查询请求】，消息发送者线程提交该任务后
         * 将被阻塞直到 GroupTransferService通知唤醒或超时。也就是GroupTransferService的职责就是判断主从同步是否结束。
         *
         * 判断主从同步是否完成的依据是：
         *  所有Slave中已成功复制的最大偏移量是否大于等于消息生产者发送消息后消息服务端返回下一条消息的起始偏移量，
         *  如果是则表示主从同步复制已经完成，唤醒消息发送线程，否则等待1s,再次判断，每一个任务在一批任务中循环判断5次。
         *
         *  消息消费者返回有两种情况：
         *      如果等待超过5s或 GroupTransferService通知主从复制完成则返回。
         *      可以通过syncFlushTimeout来设置等待时间。
        */
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        // 等待Slave上传进度
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // 唤醒请求，并设置是否Slave同步成功
                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    class HAClient extends ServiceThread {
        /**
         * Socket读缓存区大小
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        /**
         * master地址
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        /**
         * Slave 向 Master 发起主从同步的拉取偏移量，固定8个字节
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        /**
         * 网络传输通道
         */
        private SocketChannel socketChannel;
        /**
         * NIO事件选择器
         */
        private Selector selector;
        /**
         * 上一次写入时间戳
         */
        private long lastWriteTimestamp = System.currentTimeMillis();
        /**
         * 反馈 Slave 当前的复制进度，commitlog文件最大偏移量
         */
        private long currentReportedOffset = 0;
        /**
         * 本次已处理读缓存区的指针
         */
        private int dispatchPosition = 0;
        /**
         * 读缓存区，大小为4M
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**
         * 读缓存区备份，与BufferRead进行交换
         */
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * 判断是否需要向Master汇报已拉取消息偏移量。其依据为每次拉取间隔必须大于haSendHeartbeatInterval，默认5s
         * @return
         */
        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 上报进度
         * 如果需要向Master反馈当前拉取偏移量，则向Master发送一个8字节的请求，请求包中包含的数据为当前Broker消息文件的最大偏移量
         * @param maxOffset  进度
         * @return  是否上报成功
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理网络读请求，也就是处理从Master传回的消息数据。同样RocketMQ的作者给出了一个处理网络读的NIO示例。
         *      循环判断readByteBuffer是否还有剩余空间，如果存在剩余空间，则调用SocketChannel#read(ByteBuffer readByteBuffer),
         *      将通道中的数据读入到读缓存区中。
         * 1）如果读取到的字节数大于0，重置读取到0字节的次数，并更新最后一次写入时间戳（lastWriteTimestamp），
         *      然后调用dispatchReadRequest()转发该请求，处理消息的解析、入库。
         * 2）如果连续3次从网络通道读取到0个字节，则结束本次读，返回true。
         * 3）如果读取到的字节数小于0或发生IO异常，则返回false
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * 该方法主要从byteBufferRead中解析一条一条的消息，然后存储到commitlog文件并转发到消息消费队列与索引文件中
         * 如果读取到数据，写入CommitLog
         * 异常原因：
         *  1. Master传输来的数据offset 不等于 Slave的CommitLog数据最大offset
         *  2. 上报到Master进度失败
         *
         * 需要解决如下情况：
         * 1）如果判断byteBufferRead是否包含一条完整的消息。
         * 2）如果不包含一条完整的信息，该如何处理
         *
         * @return  是否异常
         */
        private boolean dispatchReadRequest() {
            // 头部长度，大小为12个字节，包括消息的物理偏移量与消息的长度，长度字节必须首先探测，否则无法判断byteBufferRead缓存区中是否包含一条完整的消息
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            // 记录当前byteBufferRead的当前指针
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                // 先探测byteBufferRead缓冲区中是否包含一条消息的头部，如果包含头部，则读取物理偏移量与消息长度，然后再探测是否包含一条完整的消息，如果不包含，则需要将byteBufferRead中的数据备份，以便更多数据到达再处理
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                if (diff >= msgHeaderSize) {

                    // 如果byteBufferRead中包含一则消息头部，则读取物理偏移量与消息的长度，然后获取Slave当前消息文件的最大物理偏移量，
                    // 如果slave的最大物理偏移量与master给的偏移量不相等，则返回false，从后面的处理逻辑来看，返回false,将会关闭与master的连接，
                    // 在Slave本次周期内将不会再参与主从同步了
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    // 校验 Master传输来的数据offset 是否和 Slave的CommitLog数据最大offset 是否相同。
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    // 读取到消息
                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        // 设置byteBufferRead的position指针为dispatchPosition+msgHeaderSize,然后读取bodySize个字节内容到byte[]字节数组中，
                        // 并调用DefaultMessageStore#appendToCommitLog方法将消息内容追加到消息内存映射文件中，然后唤醒ReputMessageService
                        // 实时将消息转发给消息消费队列与索引文件，更新dispatchPosition，并向服务端及时反馈当前已存储进度。将所读消息存入内存映射文
                        // 件后重新向服务端发送slave最新的偏移量
                        this.byteBufferRead.position(readSocketPos);
                        // 表示byteBufferRead中已转发的指针
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        // 上报到Master进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                // 如果byteBufferRead中未包含一条完整的消息的处理逻辑，具体看一下reallocateByteBuffer的实现。
                // 其核心思想是将readByteBuffer中剩余的有效数据先复制到readByteBufferBak,然后交换readByteBuffer与readByteBufferBak
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * 如果 socketChannel 为空，则尝试连接 Master,如果 master 地址为空，返回false。
         * 如果 master 地址不为空，则建立到 Master 的TCP连接，然后注册OP_READ(网络读事件)，
         * 并初始化 currentReportedOffset 为 commitlog 文件的最大偏移量、lastWriteTimestamp 上次写入时间戳为当前时间戳，并返回true，
         * 原来在 Broker 启动时，如果其角色为 SLAVE 时，将读取 Broker 配置文件中的 haMasterAddress 属性更新 HAClient 的 masterAddrees,
         * 如果角色为SLAVE 但 haMasterAddress 为空，启动不会报错，但不会执行主从复制，该方法最终返回是否成功连接上Master
         * @return
         * @throws ClosedChannelException
         */
        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    if (this.connectMaster()) {

                        // 若到满足上报间隔，上报到Master进度
                        if (this.isTimeToReportOffset()) {
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        // 进行事件选择，其执行间隔为1s
                        this.selector.select(1000);

                        // 处理读取事件
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        // 若进度有变化，上报到Master进度
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        // Master过久未返回数据，关闭连接
                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
