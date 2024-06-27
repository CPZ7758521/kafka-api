package com.pandora.www.kafka_api.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MyKafkaConsumerWithOffset {

    private static Logger LOG = LoggerFactory.getLogger(MyKafkaConsumerWithOffset.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groupid1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        String topic = "test";

        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);

        for (PartitionInfo partitionInfo : partitionInfos) {
            LOG.info("kafka partition: " + partitionInfo);
        }

        TopicPartition myTopic = new TopicPartition(topic, 0);
        kafkaConsumer.assign(Arrays.asList(myTopic));
//        kafkaConsumer.seekToBeginning(Arrays.asList(myTopic));
        kafkaConsumer.seekToEnd(Arrays.asList(myTopic));

        do {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100L));

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                LOG.info(consumerRecord.value());
            }
            kafkaConsumer.commitSync();
        } while (true);
    }
}
