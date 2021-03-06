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
package org.apache.rocketmq.broker.transaction;

import io.netty.channel.Channel;
import java.util.Random;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTransactionalMessageCheckListener {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private BrokerController brokerController;

    //queue nums of topic TRANS_CHECK_MAX_TIME_TOPIC
    protected final static int TCMT_QUEUE_NUMS = 1;
    protected final Random random = new Random(System.currentTimeMillis());

    private static ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName("Transaction-msg-check-thread");
            return thread;
        }
    }, new CallerRunsPolicy());

    public AbstractTransactionalMessageCheckListener() {
    }

    public AbstractTransactionalMessageCheckListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public void sendCheckMessage(MessageExt msgExt) throws Exception {
        // 首先构建回查事务状态请求消息，请求核心参数包括：消息offsetId、消息ID(索引)、消息事务ID、事务消息队列中的偏移量（RMQ_SYS_TRANS_HALF_TOPIC）
        CheckTransactionStateRequestHeader checkTransactionStateRequestHeader = new CheckTransactionStateRequestHeader();
        checkTransactionStateRequestHeader.setCommitLogOffset(msgExt.getCommitLogOffset());
        checkTransactionStateRequestHeader.setOffsetMsgId(msgExt.getMsgId());
        checkTransactionStateRequestHeader.setMsgId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        checkTransactionStateRequestHeader.setTransactionId(checkTransactionStateRequestHeader.getMsgId());
        checkTransactionStateRequestHeader.setTranStateTableOffset(msgExt.getQueueOffset());

        // 恢复原消息的主题、队列，并设置storeSize为0
        msgExt.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgExt.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgExt.setStoreSize(0);

        // 获取生产者组名称
        String groupId = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
        Channel channel = brokerController.getProducerManager().getAvailableChannel(groupId);
        if (channel != null) {
            // 根据生产者组获取任意一个生产者，通过与其连接发送事务回查消息，回查消息的请求者为【Broker服务器】，接收者为(client，具体为消息生产者)
            brokerController.getBroker2Client().checkProducerTransactionState(groupId, channel, checkTransactionStateRequestHeader, msgExt);
        } else {
            LOGGER.warn("Check transaction failed, channel is null. groupId={}", groupId);
        }
    }

    /**
     * 发送具体的事务回查机制，这里用一个线程池来异步发送回查消息，为了回查进度保存的简化，这里只要发送了回查消息，当前回查进度会向前推动，
     * 如果回查失败，上一步骤新增的消息将可以再次发送回查消息，那如果回查消息发送成功，那会不会下一次又重复发送回查消息呢？这个可以根据OP
     * 队列中的消息来判断是否重复，如果回查消息发送成功并且消息服务器完成提交或回滚操作，这条消息会发送到OP队列中，
     * 然后首先会通过fillOpRemoveMap根据处理进度获取一批已处理的消息，来与消息判断是否重复，由于fillopRemoveMap一次只拉32条消息，
     * 那又如何保证一定能拉取到与当前消息的处理记录呢？其实就是通过代码@10，如果此批消息最后一条未超过事务延迟消息，
     * 则继续拉取更多消息进行判断（@12）和(@14),op队列也会随着回查进度的推进而推进
     *
     *    其处理类为：org.apache.rocketmq.client.impl.ClientRemotingProcessor#processRequest
     * @param msgExt
     */
    public void resolveHalfMsg(final MessageExt msgExt) {
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    sendCheckMessage(msgExt);
                } catch (Exception e) {
                    LOGGER.error("Send check message error!", e);
                }
            }
        });
    }

    public BrokerController getBrokerController() {
        return brokerController;
    }

    public void shutDown() {
        executorService.shutdown();
    }

    /**
     * Inject brokerController for this listener
     *
     * @param brokerController
     */
    public void setBrokerController(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * In order to avoid check back unlimited, we will discard the message that have been checked more than a certain
     * number of times.
     *
     * @param msgExt Message to be discarded.
     */
    public abstract void resolveDiscardMsg(MessageExt msgExt);
}
