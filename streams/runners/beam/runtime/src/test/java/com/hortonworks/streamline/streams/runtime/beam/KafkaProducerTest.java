package com.hortonworks.streamline.streams.runtime.beam;

import com.hortonworks.streamline.streams.*;
import com.hortonworks.streamline.streams.common.*;
import com.hortonworks.streamline.streams.layout.*;
import com.hortonworks.streamline.streams.runtime.beam.kafka.*;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.util.*;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Created by Satendra Sahu on 12/6/18
 */
public class KafkaProducerTest {

    Producer<String, StreamlineEvent> producer;

    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StreamlineEventSerializer.class.getCanonicalName());
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "C1MNV1CUDTY3.local:9092");
        System.setProperty("java.security.auth.login.config", "/Users/satendra.sahu/code/github/streamline/conf/jaas.conf");

        /*properties.put(PARTITIONER_CLASS_CONFIG, PropertyReader.readString(kafkaClientConfigProperties, PARTITIONER_CLASS_CONFIG, Constants.DEFAULT_PARTITIONER_CLASS));
        properties.put(ACKS_CONFIG, PropertyReader.readString(kafkaClientConfigProperties, ACKS_CONFIG, Constants.REQUIRED_ACKS));
        properties.put(COMPRESSION_TYPE_CONFIG, PropertyReader.readString(kafkaClientConfigProperties, COMPRESSION_TYPE_CONFIG, Constants.DEFAULT_COMPRESSION.name));
        properties.put(BATCH_SIZE_CONFIG, PropertyReader.readInt(kafkaClientConfigProperties, BATCH_SIZE_CONFIG, Constants.DEFAULT_BATCH_SIZE));
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, PropertyReader.readInt(kafkaClientConfigProperties, REQUEST_TIMEOUT_MS_CONFIG, Constants.DEFAULT_REQUEST_TIMEOUT));
        properties.put(LINGER_MS_CONFIG, PropertyReader.readLong(kafkaClientConfigProperties, LINGER_MS_CONFIG, Constants.DEFAULT_LINGER_TIMEOUT));
        properties.put(BUFFER_MEMORY_CONFIG, PropertyReader.readLong(kafkaClientConfigProperties, BUFFER_MEMORY_CONFIG, Constants.DEFAULT_BUFFER_MEMORY));
        properties.put(CONNECTIONS_MAX_IDLE_MS_CONFIG, PropertyReader.readLong(kafkaClientConfigProperties, CONNECTIONS_MAX_IDLE_MS_CONFIG, 540000l));
        properties.put(MAX_BLOCK_MS_CONFIG, PropertyReader.readLong(kafkaClientConfigProperties, MAX_BLOCK_MS_CONFIG, Constants.DEFAULT_MAX_BLOCK_MS_CONFIG));
*/
        properties.put("schema.registry.url", "http://localhost:8877/api/v1");
        return properties;
    }

    public void init() {
        producer = new KafkaProducer<String, StreamlineEvent>(getProducerProperties());

    }

    private ProducerRecord<String, StreamlineEvent> getRecord() {
        StreamlineEventImpl event = StreamlineEventImpl.builder().dataSourceId("1").sourceStream("testProducer")
                .put("machine", "testMachine")
                .put("machine_COUNT", 10l)
                .put("building_MAX","MAX")
                .build();
        return new ProducerRecord<>("beam_test_input","testMsgKey", event);
    }

    public void send() {
        producer.send(getRecord());
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducerTest kafkaProducerTest = new KafkaProducerTest();
        kafkaProducerTest.init();
        while(true){
            kafkaProducerTest.send();
            Thread.sleep(1000);
        }
    }
}
