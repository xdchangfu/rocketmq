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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * HA Master-Slave网络连接对象
 */
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * HAService对象
     */
    private final HAService haService;
    /**
     * 网络socket通道
     */
    private final SocketChannel socketChannel;
    /**
     * 客户端连接地址
     */
    private final String clientAddr;
    /**
     * HAConnection网络写封装
     * 服务端向从服务器写数据的服务类
     */
    private WriteSocketService writeSocketService;
    /**
     * HAConnection网络读封装
     * 服务端从从服务器其读数据的服务类
     */
    private ReadSocketService readSocketService;
    /**
     * 从服务器请求拉取数据的偏移量
     */
    private volatile long slaveRequestOffset = -1;
    /**
     * 从服务器反馈已拉取完成的数据偏移量
     */
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    class ReadSocketService extends ServiceThread {
        /**
         * 网络读缓存区大小，默认1M
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        /**
         * NIO网络事件选择器
         */
        private final Selector selector;
        /**
         * 网络通道，用于读写的socket通道
         */
        private final SocketChannel socketChannel;
        /**
         * 网络读写缓存区，默认为1M
         */
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**
         * 处理位置
         */
        private int processPosition = 0;
        /**
         * 上次读取数据的时间戳
         */
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        /**
         * 该方法主要是创建一个事件选择器，并将该网络通道注册在事件选择器上，并注册网络读事件
         * @param socketChannel
         * @throws IOException
         */
        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        /**
         * run方法的核心实现就是每1s执行一次事件就绪选择，然后调用processReadEvent方法处理读请求，读取从服务器的拉取请求。
         */
        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            // 如果 byteBufferRead 没有剩余空间，说明该 position==limit==capacity，调用 byteBufferRead.flip()方法，
            // 其实这里调用 clear()方法会更加容易理解，并设置 processPostion为0，processPostion为 byteBufferRead当前已处理数据的指针
            if (!this.byteBufferRead.hasRemaining()) {
                // 从写模式切换到读模式
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            // NIO网络读的常规方法，由于NIO是非阻塞的，一次网络读写的字节大小不确定，一般都会尝试多次读取
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 如果读取的字节大于0并且本次读取到的内容大于等于8，表明收到从服务器一条拉取消息请求。读取从服务器已拉取偏移量，
                    // 因为有新的从服务器反馈拉取进度，需要通知某些生产者以便返回，因为如果消息发送使用同步方式，需要等待将消息复制到从服务器，
                    // 然后才返回，故这里需要唤醒相关线程去判断自己关注的消息是否已经传输完成
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;

                        // 设置最后读取时间
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            // 读取Slave 请求来的CommitLog的最大位置
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPosition = pos;

                            // 设置Slave CommitLog的最大位置
                            HAConnection.this.slaveAckOffset = readOffset;
                            // 设置Slave 第一次请求的位置
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            // 通知目前Slave进度。主要用于Master节点为同步类型的。
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) { // 如果读取到的字节数等于0，则重复三次，否则结束本次读请求处理；如果读取到的字节数小于0，表示连接被断开，返回false，后续会断开该连接
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 主要负责将消息内容传输给从服务器
     */
    class WriteSocketService extends ServiceThread {
        /**
         * NIO网络事件选择器
         */
        private final Selector selector;
        /**
         * 网络socket通道
         */
        private final SocketChannel socketChannel;
        /**
         * 消息头长度，消息物理偏移量+消息长度
         */
        private final int headerSize = 8 + 4;
        /**
         * 消息头字节
         */
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        /**
         * 下一次传输的物理偏移量
         */
        private long nextTransferFromWhere = -1;
        /**
         * 根据偏移量查找消息的结果
         */
        private SelectMappedBufferResult selectMappedBufferResult;
        /**
         * 上一次数据是否传输完毕
          */
        private boolean lastWriteOver = true;
        /**
         * 上次写入的时间戳
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    // 未获得Slave读取进度请求，sleep等待
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 说明Master还未收到从服务器的拉取请求，放弃本次事件处理。slaveRequestOffset在收到从服务器拉取请求时更新（HAConnectionReadSocketService）
                    // 如果nextTransferFromWhere为-1表示初次进行数据传输，需要计算需要传输的物理偏移量，如果slaveRequestOffset为0，则从当前commitlog文件最大偏移量开始传输，否则根据从服务器的拉取请求偏移量开始传输
                    if (-1 == this.nextTransferFromWhere) {
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    // 判断上次写事件是否将信息已全部写入到客户端
                    // 1）如果已全部写入，判断当前系统是与上次最后写入的时间间隔是否大于HA心跳检测时间，则需要发送一个心跳包，
                    //      心跳包的长度为12个字节(从服务器待拉取偏移量+size),消息长度默认存0，表示本次数据包为心跳包，避免长连接由于空闲被关闭。
                    //      HA心跳包发送间隔通过设置haSendHeartbeatInterval，默认值为5s。
                    //2）如果上次数据未写完，则继续传输上一次的数据，然后再次判断是否传输完成，如果消息还是未全部传输，则结束此次事件处理，
                    //      待下次写事件到底后，继续将未传输完的数据先写入消息从服务器
                    if (this.lastWriteOver) {

                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {    // 未传输完成，继续传输
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 传输消息到从服务器
                    // 1）根据消息从服务器请求的待拉取偏移量，RocketMQ首先获取该偏移量之后所有的可读消息，如果未查到匹配的消息，
                    //      通知所有等待线程继续等待100ms。
                    // 2）如果匹配到消息，判断返回消息总长度是否大于配置的HA传输一次同步任务最大传输的字节数，则通过设置ByteBuffer 的 limit
                    //      来设置只传输指定长度的字节，这就意味着HA客户端收到的信息会包含不完整的消息。HA一批次传输消息最大字节通过
                    //      haTransferBatchSize来设置，默认值为32K。
                    // HA服务端消息的传输一直以上述步骤在循环运行，每次事件处理完成后等待1s
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {
                        // 没新的消息，挂起等待
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            // 断开连接 & 暂停写线程 & 暂停读线程 & 释放CommitLog
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 传输数据
         * @return
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
