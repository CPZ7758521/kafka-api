package com.pandora.www.kafka_api.consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

public class MyKafkaConsumer {
    private static Logger LOG = LoggerFactory.getLogger(MyKafkaConsumer.class);
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

        kafkaConsumer.subscribe(Arrays.asList(topic));

        int count = 0;

        boolean hasmore = true;

        int sumDhr = 0;
        int sumBrs = 0;
        int sumNull = 0;
        int sum = 0;
        HashSet<String> dhrKeySet = new HashSet<>();
        HashSet<String> brsKeySet = new HashSet<>();

        do {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100L));
            count += consumerRecords.count();
            if (consumerRecords.count() <= 0) {
                hasmore = false;
                continue;
            }

            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                String recordKey = consumerRecord.key();
                String recordValue = consumerRecord.value();

                long timestamp = consumerRecord.timestamp();

                JSONObject valueJsonObject = JSONObject.parseObject(recordValue);

                try {
                    String calendarType = valueJsonObject.getString("calendarType");
                    if ("DHR日历".equals(calendarType)) {
                        dhrKeySet.add(recordKey);
                        LOG.info(recordKey + "--" + recordValue + "--" + timestamp + "--" + topic);
                        sumDhr ++;
                    } else if ("".equals(calendarType)) {
                        brsKeySet.add(recordKey);
                        LOG.info(recordKey + "--" + recordValue + "--" + timestamp + "--" + topic);
                        sumBrs ++;
                    }
                } catch (Exception e) {
                    sumNull ++;
                } finally {
                    sum ++;
                }
            }
            kafkaConsumer.commitSync();
        } while (hasmore);

        int dhrSize = dhrKeySet.size();
        int brsSize = brsKeySet.size();

        LOG.info("count:" + count);
        LOG.info("dhr-id个数:" + dhrSize);
        LOG.info("brs-id个数:" + brsSize);
        LOG.info("未去重的dhr数据量:" + sumDhr);
        LOG.info("未去重的brs数据量:" + sumBrs);
        LOG.info("空value的数据量:" + sumNull);

        LOG.info((sumDhr + sumBrs + sumNull) + "");
    }
}
