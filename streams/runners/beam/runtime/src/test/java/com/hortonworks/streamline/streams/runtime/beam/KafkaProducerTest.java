package com.hortonworks.streamline.streams.runtime.beam;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Created by Satendra Sahu on 12/6/18
 */
public class KafkaProducerTest {

    Producer<String, String> producer;
    Random random;
    private final ObjectMapper mapper = new ObjectMapper();
    private String fabricEvent = "{\n"
        + "\t\"id\": \"51fd2bc8-3000-4c5a-81cb-0d3f200859a5\",\n"
        + "\t\"metadata\": {\n"
        + "\t\t\"timestamp\": 1550038067418,\n"
        + "\t\t\"schema\": \"filter_booking_rakam\",\n"
        + "\t\t\"schemaVersion\": 1,\n"
        + "\t\t\"type\": \"EVENT\",\n"
        + "\t\t\"routingKey\": {\n"
        + "\t\t\t\"type\": \"simple\",\n"
        + "\t\t\t\"value\": \"1550038067418\"\n"
        + "\t\t},\n"
        + "\t\t\"lookupKey\": {\n"
        + "\t\t\t\"type\": null,\n"
        + "\t\t\t\"value\": \"00f0e972-342d-44f4-9e19-96261347ad43\"\n"
        + "\t\t},\n"
        + "\t\t\"tenant\": \"sharedriver_instrumentation\",\n"
        + "\t\t\"stream\": \"sharedriver_instrumentation_rakam\",\n"
        + "\t\t\"sender\": \"testSender\"\n"
        + "\t},\n"
        + "\t\"data\": {\n"
        + "\t\t\"machine\": \"testMachine\",\n"
        + "\t\t\"machine_COUNT\": 1124235123513,\n"
        + "\t\t\"building_MAX\": \"MAX\"\n"
        + "\t}\n"
        + "}";


    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
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
        producer = new KafkaProducer<String, String>(getProducerProperties());
        random = new Random();
    }

    private ProducerRecord<String, StreamlineEvent> getRecord() {

        StreamlineEventImpl event = StreamlineEventImpl.builder().dataSourceId("1").sourceStream("testProducer")
            .put("machine", "testMachine")
            .put("machine_COUNT", random.nextLong())
            .put("building_MAX", "MAX")
            .build();
        return new ProducerRecord<>("beam_test_input", "testMsgKey", event);
    }

    private ProducerRecord<String, String> getFabricRecord() {
        return new ProducerRecord<>("beam_test_input", "testMsgKey", fabricEvent);
    }

    public void send() {
        producer.send(getFabricRecord());
    }

    public static void main(String[] args) throws InterruptedException {
        KafkaProducerTest kafkaProducerTest = new KafkaProducerTest();
        kafkaProducerTest.init();
        while (true) {
            kafkaProducerTest.send();
            Thread.sleep(1000);
        }
    }
}
