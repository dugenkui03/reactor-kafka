/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.kafka.samples;

import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

/**
 * Sample producer application using Reactive API for Kafka.
 * To run sample producer
 * <ol>
 *   <li> Start Zookeeper and Kafka server
 *   <li> Update {@link #BOOTSTRAP_SERVERS} and {@link #TOPIC} if required
 *   <li> Create Kafka topic {@link #TOPIC}
 *   <li> Run {@link SampleProducer} as Java application with all dependent jars in the CLASSPATH (eg. from IDE).
 *   <li> Shutdown Kafka server and Zookeeper when no longer required
 * </ol>
 */
public class SampleProducer {

    private static final Logger log = LoggerFactory.getLogger(SampleProducer.class.getName());

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "demo-topic";

    private final KafkaSender<Integer, String> sender;
    private final DateTimeFormatter dateFormat;

    public SampleProducer(String bootstrapServers) {

        Map<String, Object> props = new HashMap<>();
        // bootstrap.servers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // client.id
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        // acks
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // key.serializer
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        // value.serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);

        // DefaultKafkaSender
        // note ?此时真正的kafka客户端其实并没有创建?
        sender = KafkaSender.create(senderOptions);
        dateFormat = DateTimeFormatter.ofPattern("HH:mm:ss:SSS z dd MMM yyyy");
    }

    /**
     * @param topic 目标 topic
     * @param count 消息条数
     * @param latch 计数用
     * @throws InterruptedException
     */
    public void sendMessages(String topic, int count, CountDownLatch latch) throws InterruptedException {

        //  创建发送的消息
        //      是该组件定义的类
        Flux<SenderRecord<Integer, String, Integer>> messageFlux = Flux.range(1, count)
            .map(i -> SenderRecord.create(new ProducerRecord<>(topic, i, "Message_" + i), i));

        // todo 发送消息
        Flux<SenderResult<Integer>> senderResultFlux = sender.<Integer>send(messageFlux);

        //  建立错误处理管道，并不影响结果
        senderResultFlux = senderResultFlux.doOnError(e -> log.error("Send failed", e));

        //  subscribe 是 final 方法
        senderResultFlux.subscribe(senderResult -> {
                  RecordMetadata metadata = senderResult.recordMetadata();
                  Instant timestamp = Instant.ofEpochMilli(metadata.timestamp());
                  System.out.printf("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
                      senderResult.correlationMetadata(),
                      metadata.topic(),
                      metadata.partition(),
                      metadata.offset(),
                      dateFormat.format(timestamp));
                  latch.countDown();
              });
    }

    public void close() {
        sender.close();
    }

    public static void main(String[] args) throws Exception {
        int count = 20;
        CountDownLatch latch = new CountDownLatch(count);

        // note 创建 producer
        // BOOTSTRAP_SERVERS: localhost:9092
        SampleProducer producer = new SampleProducer(BOOTSTRAP_SERVERS);

        /**
         * 发送消息，其实就是委托给该类中的 {@link sender}
         * 看 sendMessages 具体实现把。
         */
        producer.sendMessages(TOPIC, count, latch);

        latch.await(10, TimeUnit.SECONDS);
        producer.close();
    }
}
