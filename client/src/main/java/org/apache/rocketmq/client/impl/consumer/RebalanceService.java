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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * 首先RebalanceService线程启动，为消费者分配消息队列，其实每一个MessageQueue会构建一个PullRequest 对象，然后通过 RebalanceImpl 将 PullRequest 放入到
 *
 * PullMessageService线程的 LinkedBlockingQueue 进而唤醒 queue.take()方法，然后执行DefaultMQPushConsumerImpl 的 pullMessage,
 * 通过网络从 broker 端拉取消息，一次最多拉取的消息条数可配置，默认为1条，然后将拉取的消息执行过滤等，封装成任务（ConsumeRequest）,
 * 提交到消费者的线程池去执行，每次消费消息后，又将该 PullRequest 放入到 PullMessageService 中（DefaultMQPushConsumerImpl 的机制就是pullInterval - 0
 */
public class RebalanceService extends ServiceThread {

    // 等待间隔，单位：毫秒
    private static long waitInterval =
        Long.parseLong(System.getProperty(
            "rocketmq.client.rebalance.waitInterval", "20000"));
    private final InternalLogger log = ClientLogger.getLog();

    // MQClient对象
    private final MQClientInstance mqClientFactory;

    public RebalanceService(MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            this.waitForRunning(waitInterval);
            this.mqClientFactory.doRebalance();
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return RebalanceService.class.getSimpleName();
    }
}
