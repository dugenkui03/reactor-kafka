/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.kafka.sender;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;


/**
 * Represents an outgoing record. Along with the record to send to Kafka,
 * additional correlation metadata may also be specified to correlate
 * {@link SenderResult} to its corresponding record.
 *
 * @param <K> Outgoing record key type
 * @param <V> Outgoing record value type
 * @param <T> Correlation metadata type note 与该记录关联的元数据信息
 */
public class SenderRecord<K, V, T> extends ProducerRecord<K, V> {

    private final T correlationMetadata;

    /**
     * 该子类的构造方法
     */
    private SenderRecord(String topic, Integer partition, Long timestamp, K key, V value, T correlationMetadata, Iterable<Header> headers) {
        super(topic, partition, timestamp, key, value, headers);
        this.correlationMetadata = correlationMetadata;
    }

    /**
     * Creates a {@link SenderRecord} to send to Kafka.
     * note 静态工厂方法，创建该子类对象、其实就是调用了一下 private 的构造方法。
     *
     * @param topic Topic to which record is sent
     *              note 要发送的 topic
     *
     * @param partition The partition to which the record is sent. If null, the partitioner configured
     *        for the {@link KafkaSender} will be used to choose the partition.
     *                  note 要发送的分区
     *
     * @param timestamp The timestamp of the record. If null, the current timestamp will be assigned by the producer.
     *        The timestamp will be overwritten by the broker if the topic is configured with
     *        {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME}. The actual timestamp
     *        used will be returned in {@link SenderResult#recordMetadata()}
     *                  note 该消息的创建事件。
     *
     * @param key The key to be included in the record. May be null.
     *            note 消息的 key
     *
     * @param value The contents to be included in the record.
     *              note 消息的 value
     *
     * @param correlationMetadata Additional correlation metadata that is not sent to Kafka, but is
     *        included in the response to match {@link SenderResult} to this record.
     * @return new sender record that can be sent to Kafka using {@link KafkaSender#send(org.reactivestreams.Publisher)}
     */
    public static <K, V, T> SenderRecord<K, V, T> create(String topic, Integer partition, Long timestamp, K key, V value, T correlationMetadata) {
        return new SenderRecord<K, V, T>(topic, partition, timestamp, key, value, correlationMetadata, null);
    }

    /**
     * Converts a {@link ProducerRecord} a {@link SenderRecord} to send to Kafka.
     * note static 方法，将 record 及其元数据信息关联起来并创建 SenderRecord 对象。
     *
     * @param record the producer record to send to Kafka
     *               note 消息
     * @param correlationMetadata Additional correlation metadata that is not sent to Kafka, but is
     *        included in the response to match {@link SenderResult} to this record.
     *                            note 元数据信息并不会发送到 kafka server，但是会放在结果 {@link SenderResult} 中
     *
     * @return new sender record that can be sent to Kafka using {@link KafkaSender#send(org.reactivestreams.Publisher)}
     */
    public static <K, V, T> SenderRecord<K, V, T> create(ProducerRecord<K, V> record, T correlationMetadata) {
        return new SenderRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), record.value(), correlationMetadata, record.headers());
    }

    /**
     * Returns the correlation metadata associated with this instance which is not sent to Kafka,
     * but can be used to correlate response to outbound request.
     * note 获取元数据信息
     *
     * @return metadata associated with sender record that is not sent to Kafka
     *          note 与该 record 关联的元数据信息
     */
    public T correlationMetadata() {
        return correlationMetadata;
    }
}
