package com.hortonworks.streamline.streams.runtime.beam;

import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.layout.beam.kafka.*;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

import java.util.*;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Created by Satendra Sahu on 12/7/18
 */
public class KafkaConsumerTest {
    KafkaConsumer<String, StreamlineEvent> kafkaConsumer;

    private Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_consumer_group");
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "C1MNV1CUDTY3.local:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StreamlineEventDeserializer.class.getCanonicalName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put("sasl.mechanism", "PLAIN");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        System.setProperty("java.security.auth.login.config", "/Users/satendra.sahu/code/github/streamline/conf/jaas.conf");

        properties.put("schema.registry.url", "http://localhost:8877/api/v1");

        return properties;
    }

    public void init(String topic) {
        kafkaConsumer = new KafkaConsumer<String, StreamlineEvent>(getConsumerProperties());
        kafkaConsumer.subscribe(Arrays.asList(topic));

    }

    private void printRecord() {
        ConsumerRecords<String, StreamlineEvent> records = kafkaConsumer.poll(1000);
        for (ConsumerRecord<String, StreamlineEvent> record : records) {
            System.out.println(record.key() + "      " + record.value().getDataSourceId());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerTest kafkaConsumerTest = new KafkaConsumerTest();
        kafkaConsumerTest.init("beam_test_output");
        while (true){
            kafkaConsumerTest.printRecord();
            Thread.sleep(1000);
        }
    }
}
