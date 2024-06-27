package com.pandora.www.kafka_api.producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyKafkaProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topic = "test";
//        String value = "testvalue";
        String value = "{\"value\":\"testvalue\"}";
        JSONObject jsonObject = JSONObject.parseObject(value);
        String jsonString = JSONObject.toJSONString(jsonObject);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, null, 1715075016611L, null, value, null);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "-1");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        Future<RecordMetadata> send = kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
        send.get();
    }
}
